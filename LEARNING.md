# LEARNING.md: how Washington was built, stage by stage

This file is my study guide for interviews. For every stage: what was built and why, the
trickiest bug or problem and how it was solved, and five questions an interviewer is likely to
ask. "Bug" means it actually broke during the real run; "problem" means a pitfall found while
designing or testing, before it could break anything.

---

## 0. The big picture in one paragraph

SEC rules say company insiders (officers, directors, 10% owners) must report trades in their
own company's stock on **Form 4** within two business days. Washington downloads those filings
every day, keeps the raw XML (**bronze**), turns it into clean typed tables (**silver**), models a
star schema with dbt (**gold**), and runs a SQL query that finds **cluster buys**: three or more
different insiders buying on the open market within 10 days for $250k+ in total. Airflow runs it
daily, PySpark loads five years of history, yfinance prices let us measure what happened next,
and Streamlit shows the results.

---

## 1. Skeleton + docker compose

**What / why.** One `docker-compose.yml` starts PostgreSQL, MinIO, Airflow (init, webserver,
scheduler), the Streamlit dashboard, and an on-demand Spark container (`--profile backfill`).
One Postgres server hosts two databases: `washington` (the warehouse) and `airflow` (Airflow's
metadata), created by an init script. Settings come from `.env`; only `.env.example` is committed.

**Trickiest bug.** Docker Desktop crashed mid-build and then refused to start ("unexpected
error … dockerInference: The file cannot be accessed by the system"). The crash left Unix-socket
files on NTFS that Windows can't open, delete or `fsutil`-repair. Fix: quit Docker, *rename the
folders* holding them (`%LOCALAPPDATA%\Docker\run`, `%LOCALAPPDATA%\docker-secrets-engine`);
Docker recreates them on start. Also: Docker Desktop's VM only had ~3.7 GB RAM although the laptop has 16 GB.
My first limits added up to more than that. Fix: every service got an explicit `mem_limit`
sized for the VM, Postgres `shared_buffers` went down to 256 MB, and the Spark backfill runs
while Airflow is stopped. Second surprise: `minio/minio` is no longer published on Docker Hub,
so the stack uses `bitnamilegacy/minio:2025.4.22` (the same MinIO release, archived by Bitnami).

**Interview questions**
1. *Why Docker Compose and not Kubernetes?* One laptop, zero budget, a few services. Compose
   gives reproducible one-command setup; Kubernetes would add ops work with no benefit here.
2. *Why one Postgres for Airflow metadata and the warehouse?* Saves RAM. They're separate
   databases, so they could be split onto separate servers later without code changes.
3. *How are secrets handled?* `.env` is git-ignored; compose injects it as environment
   variables; code reads env vars only. `.env.example` documents every variable.
4. *What does `depends_on: condition: service_healthy` buy you?* Airflow doesn't start until
   Postgres answers `pg_isready` and MinIO's health endpoint is up, so there are no startup races.
5. *How would you deploy this to the cloud?* Postgres → RDS/Cloud SQL, MinIO → S3 (same API,
   only the endpoint changes), Airflow → MWAA/Composer, dashboard → a small container service.

---

## 2. EDGAR client + bronze

**What / why.** `SecClient` wraps `requests` with: the User-Agent SEC requires ("app + email"),
a rate limiter (minimum gap between requests, 6/s by default, refusing ≥ 10/s), and retries
with exponential backoff (1, 2, 4, 8 … s, honouring `Retry-After`) for 429 and 5xx. 404 returns
`None` (no index on weekends/holidays); 403 fails fast because retrying won't help. The daily
**master index** lists every filing; we keep form type `4`, de-duplicate by file name, then
download each full submission `.txt` and cut out the `<XML>…</XML>` block. Raw files go to
MinIO under Hive-style partitions: `edgar/form4/filing_date=2026-11-20/<accession>.xml`.

**Trickiest bug.** The audit table said 2026-11-20 *downloaded* 1,203 filings but only 587
were *parsed*, with zero errors. I suspected MinIO was losing writes, but counting with MinIO's own `mc`
client showed 587 objects and diffing the index against bronze showed nothing missing. The real
cause: the daily index lists every Form 4 **once per related CIK**, under different folders
(`edgar/data/<issuer>/<acc>.txt` and `edgar/data/<owner>/<acc>.txt`). I de-duplicated on the
file name, which differs, instead of the accession number, which doesn't, so every filing was
downloaded twice and written to the same key. Data stayed correct (same key + `ON CONFLICT`), but
it doubled the SEC traffic. My unit test had the same wrong assumption (its "duplicate" reused
the same path), so it passed. Fix: dedupe on accession number; the test now uses the real shape.
Lesson: the audit metrics caught a bug that the unit tests missed.

**Trickiest problem.** Making the rate limit hold *across* Airflow runs. Each task gets its own
client, so two parallel DAG runs would each send 6 req/s (12/s total, which is over SEC's limit). Fix: an
Airflow **pool** `sec_edgar` with one slot for every task that talks to SEC, plus
`max_active_runs=1`. The limit is now global, not per process.

**Interview questions**
1. *Why store raw XML at all?* Replay. If the parser has a bug, I fix it and re-parse bronze
   without re-downloading from SEC. Bronze is the source of truth.
2. *How is the download step idempotent?* It lists what's already in bronze for that date
   and only fetches the missing accession numbers. A re-run after a crash fetches just the gap.
3. *Token bucket vs. fixed interval?* A fixed minimum interval is simpler and can never burst;
   a token bucket allows short bursts. With a strict external limit, no bursts is safer.
4. *What happens if 5% of downloads fail?* Each failure is logged and counted; if more than
   2% of a day fails, the task raises, Airflow retries, and the failure callback fires.
5. *Why partition by filing date?* The pipeline's unit of work is one filing date, so listing
   or reprocessing a day is one prefix scan.

---

## 3. Parser + silver

**What / why.** `parse_form4` uses the standard library `xml.etree` to turn the XML into
dataclasses: filing header, reporting owners, and non-derivative transactions numbered
`line_no = 1..n` in document order. It handles real-world mess: prices that are only a
footnote (no `<value>`), `1/0/true/false` booleans, dates with timezone suffixes
(`2026-11-18-05:00`), "$1,000" numbers, `NONE` tickers, unpadded CIKs. Silver has three tables
with natural primary keys (`accession_no`, `(accession_no, owner_cik)`,
`(accession_no, line_no)`) and every insert is `ON CONFLICT DO NOTHING ... RETURNING 1`, so the
loader also reports how many rows were genuinely new.

**Trickiest problem.** `<nonDerivativeTable>` holds two kinds of rows: `nonDerivativeTransaction`
(a trade) and `nonDerivativeHolding` (a position with no trade). Iterating over all children
would have created fake transactions and shifted every `line_no`. Fix: only
`findall("nonDerivativeTransaction")`, and a test with a holding row in front of a trade.

**Interview questions**
1. *How do you guarantee "re-running a day changes nothing"?* Natural keys + `ON CONFLICT DO
   NOTHING`; `loaded_at` isn't touched on conflict. An integration test loads the same filing twice
   and asserts the second load inserts 0 rows.
2. *Why not upsert (`DO UPDATE`)?* A Form 4 never changes after it's filed; corrections come as a
   new accession (4/A). So the first version is the truth and "do nothing" is correct.
3. *Why `execute_values`?* It batches thousands of rows per statement, which is much faster than
   row-by-row inserts and simpler than `COPY` when we need `ON CONFLICT`.
4. *How do you test a parser?* A real SEC filing as a fixture plus a synthetic XML covering
   each edge case, and asserting exact types (`Decimal`, `date`) rather than strings.
5. *Why Decimal, not float?* Money. `0.1 + 0.2 != 0.3` in float; Postgres `numeric` + Python
   `Decimal` keep exact values.

---

## 4. dbt gold + tests

**What / why.** Staging views clean names and types. The star schema:
`fact_insider_transaction` (grain: one transaction line, **incremental** on `loaded_at`),
`dim_company` (latest name and latest non-null ticker per CIK), `dim_insider` (latest
name/role), `dim_date` (generated with `generate_series`, so no seed file is needed). Tests: `unique`,
`not_null`, `relationships` (fact → dims), `accepted_values` (transaction codes, A/D, source),
a home-made generic test `unique_combination_of_columns` (so there's no package dependency and
CI needs no `dbt deps`), six singular tests, and a **dbt unit test** that feeds hand-made rows
into the cluster model and checks the exact clusters that come out.

**Trickiest bug.** The dbt unit test for the cluster model failed to compile with
`'var' is undefined`, although `dbt run` worked fine and the same error happened on dbt 1.9.4,
1.9.11 and 1.10.23. I bisected it with a throw-away two-model project inside the container:
plain `{{ var('n') }}` works in a unit test, but *any Jinja filter or maths on it*
(`var('n') | int`, `(n | int) - 1`) breaks dbt's unit-test rendering. Fix: interpolate vars
plainly and do the arithmetic in SQL: `interval '1 day' * ({{ var('cluster_window_days') }} - 1)`.
Lesson: when a tool misbehaves, shrink the problem to the smallest project that still fails.

The same run found a real data-quality issue: `not_null` on `transaction_code` failed for 5 of
1.47M rows. They are genuinely empty in SEC's own data sets, so the test was set to `warn`
(visible, documented) instead of deleting rows to make it pass.

**Trickiest problem.** Joint filings. A fund, its general partner and its manager often file *one*
Form 4 together as three reporting owners. If each owner counted as an insider, one filing
would look like a 3-insider "cluster". Fix: the fact attributes each filing to a single
primary insider (the lowest owner CIK) and keeps `n_reporting_owners` for transparency.

**Interview questions**
1. *Why incremental for the fact and table for the dims?* The fact has ~1.5M rows and only
   grows; reprocessing just new `loaded_at` rows is cheap. Dims are small and "latest value
   wins", which is easiest to get right by rebuilding.
2. *What's the grain of your fact table?* One non-derivative transaction line of one filing:
   `(accession_no, line_no)`, surrogate key `md5(accession_no || '-' || line_no)`.
3. *Data test vs. unit test in dbt?* Data tests check real data after it's built; unit tests
   check the *logic* on fixed, hand-made inputs before trusting it on real data.
4. *How do you handle bad dates?* A trade can't happen after it was reported, and typos like
   year 0202 exist. `is_valid_transaction_date` flags them, `transaction_date_key` is NULL for
   them (so the `relationships` test to `dim_date` passes), and clusters ignore them.
5. *Why a custom `generate_schema_name`?* dbt's default would create `gold_stg`/`gold_gold`;
   the override gives clean `stg` and `gold` schemas.

---

## 5. Cluster SQL (plain-English walkthrough)

File: `dbt/models/marts/signals/insider_clusters.sql`. Six steps, each a CTE:

1. **purchases**: take open-market purchases only (code `P`, acquired, shares > 0,
   price > 0, valid date). Insiders often split one buy across many lines at different prices,
   so sum them into **one row per company, per insider, per day**.
2. **rolling**: for every purchase day *D* of a company, look at the window from *D − 9 days*
   to *D* (10 calendar days). `RANGE BETWEEN INTERVAL '9 days' PRECEDING AND CURRENT ROW` makes
   the window about *time*, not row count. In that window we collect the insider IDs into an
   array, sum the dollars, and remember the earliest trade date and the latest *filing* date.
3. **qualifying_windows**: count the *distinct* insiders in that array. (Postgres can't do
   `COUNT(DISTINCT …) OVER (…)`, which is why we use an array and `unnest`.) Keep windows with ≥ 3
   insiders and ≥ $250k.
4. **flagged / numbered** ("gaps and islands"): consecutive qualifying windows usually overlap
   and describe the *same* event. A window starts a *new* cluster only if it begins after the
   previous qualifying window ended (`LAG`). A running `SUM` of those 0/1 flags gives every
   cluster a number.
5. **clusters**: one row per cluster: start, end, first qualifying day, and the
   **signal date**: the day the first qualifying window became public (its latest filing
   date). Measuring returns from the *trade* date would be look-ahead bias, because nobody outside the
   company knew yet.
6. **cluster_totals**: join back to every purchase in the cluster's date span for final
   numbers: distinct insiders, insider-days, total dollars, names.

**Trickiest problem.** The obvious first attempt, `COUNT(*) OVER w >= 3`, counts *rows*, not
insiders: one CEO buying on three days would look like a cluster. Postgres also refuses
`COUNT(DISTINCT …) OVER (…)`. The model therefore collects insider IDs into an array and counts
distinct elements, and the dbt unit test case "D: one insider buying 3 times → no cluster"
locks that behaviour in so a future "simplification" can't break it.

**Interview questions**
1. *Why RANGE and not ROWS?* ROWS counts rows ("previous 9 rows"); we need "previous 9
   calendar days" no matter how many trades happened.
2. *How do you count distinct values in a window in Postgres?* `array_agg(x) OVER w`, then
   `(SELECT count(DISTINCT i) FROM unnest(arr) i)`. Alternatives: a self-join on the date range,
   or a `LATERAL` subquery.
3. *Explain gaps and islands.* Flag rows that start a new group (via `LAG`), then a running
   `SUM` of the flags turns them into group IDs.
4. *Why is the signal date the filing date?* Filings are public only once filed; using trade
   dates assumes knowledge nobody had (look-ahead bias).
5. *How would you make thresholds configurable?* They're dbt vars (`cluster_min_insiders`,
   `cluster_window_days`, `cluster_min_value_usd`), fed from `.env` by the Airflow DAG via
   `--vars`.

---

## 6. Airflow DAG

**What / why.** `washington_daily` runs at 06:00 UTC and processes the previous day (EDGAR's
daily index is complete by ~22:00 New York time). Tasks:
`fetch_index → download_filings → parse_filings → load_silver → dbt_run → dbt_test →
(detect_clusters → send_alerts) and (refresh_prices → dbt_returns)`. Every task works on one
date (`ds`), so a backfill is just `airflow dags backfill -s … -e …`. Failures are visible three
ways: red task in the UI, retries with exponential backoff, and an `on_failure_callback` that
logs loudly (and sends Telegram if configured). dbt lives in its **own virtualenv** inside the
Airflow image because its pinned dependencies clash with Airflow's.

**Trickiest bugs (three, all from the real run).**
1. *"No run dates were found."* `airflow dags backfill -s 2026-11-20 -e 2026-11-20` did
   nothing. The DAG's logical dates are at 06:00 UTC, and `-e 2026-11-20` means midnight, which comes *before*
   the 06:00 slot. Fix: `-e 2026-11-20T12:00:00`. Lesson: logical dates are timestamps, not days.
2. *dbt failed with "could not resize shared memory segment … No space left on device".* The disk
   was fine. Docker gives containers a 64 MB `/dev/shm`; Postgres parallel workers keep hash tables
   there, and `dim_insider`'s window over ~1M owner rows overflowed it. Fix: `shm_size: 256mb` on
   the Postgres service. The failure callback fired and the run went red, so the failure was visible as designed.
3. *A 2-hour backfill tied to my terminal.* Killing the `docker exec` client didn't kill the
   backfill inside the container; it ignored SIGTERM and needed SIGKILL, while its child task kept
   running. Fix: run long backfills detached (`nohup … &` inside the container, log to the mounted
   `airflow/logs`). The single-slot `sec_edgar` pool meant the overlap could never double the
   request rate to SEC.
4. *Saturday's run failed with HTTP 403.* I expected 404 for "no index on weekends". SEC's file
   store returns an S3-style `403 <Code>AccessDenied</Code>` XML body for missing files. A
   blanket "403 = not found" rule would be dangerous, because SEC's *rate-limit block* is also a 403
   (an HTML page). Fix: only the S3 XML form means "not found"; any other 403 still fails
   loudly. Two unit tests pin both cases.

**Interview questions**
1. *Why one date per run?* Idempotent, retryable units: re-running 2026-11-20 touches only
   that day, and backfills are free.
2. *Why is `dbt test` a separate task after `dbt run`?* A failed test stops alerts and price
   refreshes from running on bad data, and the failure is visible on its own.
3. *What is `ds` vs. the run time?* `ds` is the logical date (start of the data interval);
   the run happens after the interval ends. Our 06:00 schedule makes `ds` = yesterday's filings.
4. *How do you stop duplicate alerts?* `ops.cluster_alerts` has `cluster_id` as primary key;
   detection is `INSERT … ON CONFLICT DO NOTHING`, and only pending rows get sent.
5. *Why not pass data between tasks with XCom?* XCom is for small metadata. Thousands of
   records go through MinIO (`staging/form4-parsed/...jsonl`); XCom only carries counts.

---

## 7. PySpark backfill

**What / why.** SEC publishes quarterly "Insider Transactions Data Sets" (TSV files per
table). The job loops over 20 quarters, keeps each zip in bronze, reads `SUBMISSION`,
`REPORTINGOWNER` and `NONDERIV_TRANS` with Spark, filters Form 4, casts types, and numbers
transactions with `row_number() OVER (PARTITION BY accession ORDER BY NONDERIV_TRANS_SK)`.
It writes to `staging.*` over JDBC, then one SQL statement per table does
`INSERT INTO silver … SELECT … ON CONFLICT DO NOTHING`.

**Trickiest problem.** Making the two sources share one key. The data sets have no line number,
only a surrogate key `NONDERIV_TRANS_SK`. I downloaded one filing's XML and compared it by hand:
the SK order equals the XML document order, so `row_number()` over SK reproduces the parser's
`line_no`, and the same filing from either source dedupes to the same rows.

**Interview questions**
1. *Why Spark for 1.5M rows?* Honestly, pandas could do it. Spark shows the pattern that
   scales (parallel reads, window functions, JDBC writes) and runs in `local[*]` mode here.
2. *Why staging tables instead of writing straight into silver?* Spark's JDBC writer can't do
   `ON CONFLICT`. Staging + SQL merge keeps the job idempotent.
3. *Why `quote=""` when reading?* SEC TSVs aren't quoted; a stray `"` in a company name would
   otherwise make Spark swallow following lines into one field.
4. *How did you parse `31-MAR-2026`?* `to_date(initcap(lower(col)), 'dd-MMM-yyyy')`; title
   case avoids locale/case issues with month abbreviations.
5. *Is it idempotent?* Yes: a re-run of 2026q1 read the zip from bronze and inserted 0 rows.

---

## 8. Returns analysis

**What / why.** `refresh_prices` finds each cluster ticker's needed date range, fetches only
what's missing from yfinance in batches (plus SPY), and stores adjusted closes in
`silver.stock_prices`. The maths is SQL in dbt (`cluster_returns`): entry = first close
*after* the signal date, exit = first close on/after entry + 30 / + 90 calendar days, excess =
stock return − SPY return over the same dates.

**Trickiest bug.** In the daily DAG, `dbt_returns` took **~13 minutes** per run while every
other task took seconds. The model put the prices in a CTE (`prices as (select * from
stg_stock_prices)`) and used it in four `LATERAL` lookups. Since Postgres 12, a CTE that is
referenced more than once is **materialised**: computed once into a temporary result with
**no indexes**. So each "first price after date X for ticker Y" lookup scanned 1.2M rows
instead of using the `(ticker, price_date)` primary key. Fix: each lateral reads the view
directly, which Postgres inlines, so the index is used. Build time went from ~13 min to
**~2 s**. Lesson: when something is slow, measure per task first (I queried Airflow's
`task_instance.duration`), then check whether the plan uses the index you expect.

**Interview questions**
1. *Why adjusted close?* Splits and dividends would otherwise look like crashes or jumps.
2. *Why enter the day after the signal?* Many Form 4s are filed after the market closes.
3. *What biases remain?* Survivorship (delisted tickers have no Yahoo data), small samples,
   no transaction costs; not a trading strategy.
4. *Why compute returns in SQL, not Python?* It's set-based, testable with dbt, and lives next
   to the cluster model; Python only does I/O.
5. *How is price fetching incremental?* A ticker is skipped when stored prices already cover
   its range; failures are logged and retried after 7 days.

---

## 9. Streamlit + alerts, 10. CI

**What / why.** Four tabs: recent clusters (filters), company drill-down (purchase scatter),
returns vs SPY (summary + histogram), pipeline health (`ops.ingest_audit`). Telegram is off
unless both `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set. GitHub Actions runs `ruff
check`, `ruff format --check`, `pytest` (with a Postgres service container, so the
idempotency integration test really runs) and `dbt parse`.

**Interview questions**
1. *Why Streamlit over Superset/Metabase?* One Python file, no extra server or metadata DB;
   good enough for a portfolio dashboard.
2. *Why `st.cache_data(ttl=600)`?* Avoids re-querying Postgres on every widget interaction.
3. *Why run integration tests in CI?* Idempotency is the core promise; a mocked DB can't prove
   `ON CONFLICT` behaviour.
4. *What does `dbt parse` catch?* Broken Jinja, bad `ref()`s, YAML errors, all without a database.
5. *What would you add next?* Great Expectations or Elementary for data-quality reporting,
   freshness checks on sources, and alerting on dbt test failures with links.
