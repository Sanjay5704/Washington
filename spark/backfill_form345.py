"""PySpark backfill: SEC quarterly "Insider Transactions Data Sets" -> silver.

For each quarter (BACKFILL_START_QUARTER .. BACKFILL_END_QUARTER):
  1. download <quarter>_form345.zip from SEC (rate-limited client) and keep it in bronze
  2. read SUBMISSION / REPORTINGOWNER / NONDERIV_TRANS (tab-separated) with Spark
  3. keep Form 4 only, cast types, and number transactions 1..n per filing in the same
     order the XML uses (NONDERIV_TRANS_SK follows document order; verified by hand)
  4. write to staging tables over JDBC, then INSERT ... ON CONFLICT DO NOTHING into silver

Step 4 makes the job idempotent: re-running a quarter inserts nothing new, and filings
that the daily pipeline already loaded are skipped, because both sources share the key
(accession_no, line_no).
"""

from __future__ import annotations

import logging
import os
import tempfile
import zipfile
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from washington.bronze import BronzeStore, dataset_key
from washington.config import load_settings
from washington.edgar.client import SecClient
from washington.silver import connect, init_db, record_audit

log = logging.getLogger("backfill")

DATASET_URL = (
    "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/"
    "{quarter}_form345.zip"
)
WANTED_FILES = ("SUBMISSION.tsv", "REPORTINGOWNER.tsv", "NONDERIV_TRANS.tsv")


def quarters(start: str, end: str) -> list[str]:
    """quarters('2021q2', '2022q1') -> ['2021q2', '2021q3', '2021q4', '2022q1']"""
    year, q = int(start[:4]), int(start[-1])
    end_year, end_q = int(end[:4]), int(end[-1])
    out = []
    while (year, q) <= (end_year, end_q):
        out.append(f"{year}q{q}")
        year, q = (year + 1, 1) if q == 4 else (year, q + 1)
    return out


# ---------- Spark helpers ----------


def read_tsv(spark: SparkSession, path: Path) -> DataFrame:
    # SEC files are tab-separated with NO quoting; quote="" disables quote handling so a
    # stray " inside a company name can't swallow the rest of the file.
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .option("encoding", "UTF-8")
        .csv(str(path))
    )


def clean(col: str) -> F.Column:
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)


def to_bool(col: str) -> F.Column:
    c = F.lower(F.trim(F.col(col)))
    return F.when(c.isin("1", "true"), True).when(c.isin("0", "false"), False)


def sec_date(col: str) -> F.Column:
    # "31-MAR-2026" -> 2026-03-31
    return F.to_date(F.initcap(F.lower(F.trim(F.col(col)))), "dd-MMM-yyyy")


def cik(col: str) -> F.Column:
    return F.lpad(F.trim(F.col(col)), 10, "0")


def transform(spark: SparkSession, folder: Path) -> tuple[DataFrame, DataFrame, DataFrame]:
    sub = read_tsv(spark, folder / "SUBMISSION.tsv")
    owners = read_tsv(spark, folder / "REPORTINGOWNER.tsv")
    txns = read_tsv(spark, folder / "NONDERIV_TRANS.tsv")

    ticker = F.upper(clean("ISSUERTRADINGSYMBOL"))
    filings = (
        sub.where(F.trim("DOCUMENT_TYPE") == "4")
        .select(
            F.trim("ACCESSION_NUMBER").alias("accession_no"),
            sec_date("FILING_DATE").alias("filing_date"),
            F.lit("4").alias("document_type"),
            sec_date("PERIOD_OF_REPORT").alias("period_of_report"),
            cik("ISSUERCIK").alias("issuer_cik"),
            clean("ISSUERNAME").alias("issuer_name"),
            F.when(ticker.isin("NONE", "N/A", "NA", "NULL", "-"), None)
            .otherwise(ticker)
            .alias("issuer_ticker"),
            to_bool("AFF10B5ONE").alias("aff10b5one")
            if "AFF10B5ONE" in sub.columns
            else F.lit(None).cast("boolean").alias("aff10b5one"),
        )
        .where(F.col("filing_date").isNotNull() & F.col("issuer_cik").rlike("^[0-9]{10}$"))
        .dropDuplicates(["accession_no"])
    )
    keys = filings.select("accession_no")

    rel = F.coalesce(F.col("RPTOWNER_RELATIONSHIP"), F.lit(""))
    owners_out = (
        owners.select(
            F.trim("ACCESSION_NUMBER").alias("accession_no"),
            cik("RPTOWNERCIK").alias("owner_cik"),
            clean("RPTOWNERNAME").alias("owner_name"),
            rel.contains("Director").alias("is_director"),
            rel.contains("Officer").alias("is_officer"),
            rel.contains("TenPercentOwner").alias("is_ten_percent_owner"),
            rel.contains("Other").alias("is_other"),
            clean("RPTOWNER_TITLE").alias("officer_title"),
        )
        .where(F.col("owner_cik").rlike("^[0-9]{10}$"))
        .join(keys, "accession_no", "inner")
        .dropDuplicates(["accession_no", "owner_cik"])
    )

    doc_order = Window.partitionBy("accession_no").orderBy(F.col("sk").cast("long"))
    txns_out = (
        txns.select(
            F.trim("ACCESSION_NUMBER").alias("accession_no"),
            F.col("NONDERIV_TRANS_SK").alias("sk"),
            clean("SECURITY_TITLE").alias("security_title"),
            sec_date("TRANS_DATE").alias("transaction_date"),
            clean("TRANS_FORM_TYPE").alias("transaction_form_type"),
            F.upper(clean("TRANS_CODE")).alias("transaction_code"),
            to_bool("EQUITY_SWAP_INVOLVED").alias("equity_swap"),
            clean("TRANS_SHARES").cast("decimal(28,6)").alias("shares"),
            clean("TRANS_PRICEPERSHARE").cast("decimal(28,6)").alias("price_per_share"),
            F.upper(clean("TRANS_ACQUIRED_DISP_CD")).alias("acquired_disposed"),
            clean("SHRS_OWND_FOLWNG_TRANS").cast("decimal(28,6)").alias("shares_owned_after"),
            F.upper(clean("DIRECT_INDIRECT_OWNERSHIP")).alias("direct_indirect"),
        )
        .join(keys, "accession_no", "inner")
        .withColumn("line_no", F.row_number().over(doc_order))
        .drop("sk")
    )
    return filings, owners_out, txns_out


# ---------- load ----------

MERGE_SQL = """
INSERT INTO silver.form4_filings
    (accession_no, filing_date, document_type, period_of_report, issuer_cik, issuer_name,
     issuer_ticker, aff10b5one, source)
SELECT accession_no, filing_date, document_type, period_of_report, issuer_cik, issuer_name,
       issuer_ticker, aff10b5one, 'sec_dataset'
FROM staging.ds_filings
ON CONFLICT (accession_no) DO NOTHING;

INSERT INTO silver.form4_reporting_owners
    (accession_no, owner_cik, owner_name, is_director, is_officer, is_ten_percent_owner,
     is_other, officer_title)
SELECT accession_no, owner_cik, owner_name, is_director, is_officer, is_ten_percent_owner,
       is_other, officer_title
FROM staging.ds_owners
ON CONFLICT (accession_no, owner_cik) DO NOTHING;

INSERT INTO silver.form4_nonderiv_transactions
    (accession_no, line_no, security_title, transaction_date, transaction_form_type,
     transaction_code, equity_swap, shares, price_per_share, acquired_disposed,
     shares_owned_after, direct_indirect)
SELECT accession_no, line_no, security_title, transaction_date, transaction_form_type,
       transaction_code, equity_swap, shares, price_per_share, acquired_disposed,
       shares_owned_after, direct_indirect
FROM staging.ds_txns
ON CONFLICT (accession_no, line_no) DO NOTHING;
"""


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()
    store = BronzeStore.from_settings(settings)
    store.ensure_bucket()
    client = SecClient(settings.sec_user_agent, max_requests_per_second=settings.sec_max_rps)

    conn = connect(settings)
    init_db(conn)

    spark = (
        SparkSession.builder.appName("washington-form345-backfill")
        .master("local[*]")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.jars", "/opt/jars/postgresql.jar")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    jdbc_url = f"jdbc:postgresql://{settings.pg_host}:{settings.pg_port}/{settings.pg_db}"
    jdbc_props = {
        "user": settings.pg_user,
        "password": settings.pg_password,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
    }

    todo = quarters(
        os.environ.get("BACKFILL_START_QUARTER", "2021q2"),
        os.environ.get("BACKFILL_END_QUARTER", "2026q1"),
    )
    totals = {"quarters": 0, "filings": 0, "owners": 0, "transactions": 0}
    for quarter in todo:
        key = dataset_key(quarter)
        if store.exists(key):
            content = store.get_bytes(key)
        else:
            response = client.get(DATASET_URL.format(quarter=quarter))
            if response is None:
                log.warning("%s: not published by SEC (404), skipping", quarter)
                continue
            content = response.content
            store.put_bytes(key, content)  # bronze: keep the raw zip
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "data.zip"
            zip_path.write_bytes(content)
            with zipfile.ZipFile(zip_path) as zf:
                for name in WANTED_FILES:
                    zf.extract(name, tmp)
            filings, owners, txns = transform(spark, Path(tmp))
            for df, table in ((filings, "ds_filings"), (owners, "ds_owners"), (txns, "ds_txns")):
                df.write.jdbc(jdbc_url, f"staging.{table}", mode="overwrite", properties=jdbc_props)

        with conn, conn.cursor() as cur:
            counts = {}
            for table in ("form4_filings", "form4_reporting_owners", "form4_nonderiv_transactions"):
                cur.execute(f"SELECT count(*) FROM silver.{table}")
                counts[table] = cur.fetchone()[0]
            cur.execute(MERGE_SQL)
            new = {}
            for table in counts:
                cur.execute(f"SELECT count(*) FROM silver.{table}")
                new[table] = cur.fetchone()[0] - counts[table]
        metrics = {
            "new_filings": new["form4_filings"],
            "new_owners": new["form4_reporting_owners"],
            "new_transactions": new["form4_nonderiv_transactions"],
        }
        record_audit(conn, date.today(), f"spark_backfill_{quarter}", metrics)
        totals["quarters"] += 1
        totals["filings"] += metrics["new_filings"]
        totals["owners"] += metrics["new_owners"]
        totals["transactions"] += metrics["new_transactions"]
        log.info("%s loaded: %s", quarter, metrics)

    with conn, conn.cursor() as cur:
        cur.execute(
            "ANALYZE silver.form4_filings, silver.form4_reporting_owners, "
            "silver.form4_nonderiv_transactions"
        )
    conn.close()
    spark.stop()
    log.info("Backfill done: %s", totals)


if __name__ == "__main__":
    main()
