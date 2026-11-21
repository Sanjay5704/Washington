"""The daily pipeline steps. Each step works on one filing date and is idempotent.

fetch_index  -> download_filings -> parse_filings -> load_silver
(SEC)            (SEC -> bronze)     (bronze -> staging jsonl)  (jsonl -> postgres)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

from washington import bronze
from washington.bronze import BronzeStore
from washington.config import Settings, load_settings
from washington.edgar.client import SecClient, SecClientError
from washington.edgar.index import daily_index_url, extract_ownership_xml, parse_master_index
from washington.parser import Form4ParseError, parse_form4
from washington.silver import connect, init_db, load_records, record_audit

log = logging.getLogger(__name__)

# A step fails (and shows red in Airflow) if more than this share of items failed.
MAX_FAILURE_RATIO = 0.02


@dataclass
class Context:
    settings: Settings
    store: BronzeStore

    @classmethod
    def create(cls) -> Context:
        settings = load_settings()
        store = BronzeStore.from_settings(settings)
        store.ensure_bucket()
        return cls(settings, store)

    def sec_client(self) -> SecClient:
        return SecClient(
            self.settings.sec_user_agent, max_requests_per_second=self.settings.sec_max_rps
        )


def _audit(ctx: Context, day: date, step: str, metrics: dict[str, int]) -> None:
    conn = connect(ctx.settings)
    try:
        init_db(conn)
        record_audit(conn, day, step, metrics)
    finally:
        conn.close()


def _check_failures(step: str, failed: int, total: int) -> None:
    if total and failed / total > MAX_FAILURE_RATIO:
        raise RuntimeError(f"{step}: {failed}/{total} items failed (> {MAX_FAILURE_RATIO:.0%})")


def fetch_index(day: date, ctx: Context | None = None) -> int:
    """Download the EDGAR daily master index into bronze. Returns # of Form 4 filings."""
    ctx = ctx or Context.create()
    response = ctx.sec_client().get(daily_index_url(day))
    if response is None:
        log.info("No EDGAR index for %s (weekend or holiday)", day)
        _audit(ctx, day, "fetch_index", {"form4_in_index": 0})
        return 0
    ctx.store.put_text(bronze.index_key(day), response.text)
    count = len(parse_master_index(response.text))
    log.info("Index %s lists %d Form 4 filings", day, count)
    _audit(ctx, day, "fetch_index", {"form4_in_index": count})
    return count


def download_filings(day: date, ctx: Context | None = None) -> dict[str, int]:
    """Download Form 4 XML for filings not yet in bronze (so re-runs only fetch the gaps)."""
    ctx = ctx or Context.create()
    stats = {"downloaded": 0, "already_in_bronze": 0, "missing": 0, "no_xml": 0, "failed": 0}
    index_key = bronze.index_key(day)
    if not ctx.store.exists(index_key):
        log.info("No index in bronze for %s; nothing to download", day)
        return stats

    entries = parse_master_index(ctx.store.get_text(index_key))
    existing = set(ctx.store.list_keys(bronze.filings_prefix(day)))
    client = ctx.sec_client()
    for i, entry in enumerate(entries, start=1):
        key = bronze.filing_key(day, entry.accession_no)
        if key in existing:
            stats["already_in_bronze"] += 1
            continue
        try:
            response = client.get(entry.url)
        except SecClientError as exc:
            log.error("Giving up on %s: %s", entry.accession_no, exc)
            stats["failed"] += 1
            continue
        if response is None:
            stats["missing"] += 1
            continue
        xml = extract_ownership_xml(response.text)
        if xml is None:
            stats["no_xml"] += 1  # very old paper-style filings have no XML
            continue
        ctx.store.put_text(key, xml, content_type="application/xml")
        stats["downloaded"] += 1
        if i % 200 == 0:
            log.info("%s: %d/%d processed", day, i, len(entries))

    log.info("Download %s: %s", day, stats)
    _audit(ctx, day, "download_filings", stats)
    _check_failures("download_filings", stats["failed"], len(entries))
    return stats


def parse_filings(day: date, ctx: Context | None = None) -> dict[str, int]:
    """Parse every bronze XML for the day into one JSON-lines file in staging."""
    ctx = ctx or Context.create()
    keys = ctx.store.list_keys(bronze.filings_prefix(day))
    lines: list[str] = []
    stats = {"parsed": 0, "parse_errors": 0, "transactions": 0}
    for key in keys:
        accession_no = key.rsplit("/", 1)[-1].removesuffix(".xml")
        try:
            filing = parse_form4(ctx.store.get_text(key), accession_no, day)
        except Form4ParseError as exc:
            log.warning("Parse error: %s", exc)
            stats["parse_errors"] += 1
            continue
        if filing.document_type != "4":
            continue
        stats["parsed"] += 1
        stats["transactions"] += len(filing.transactions)
        lines.append(json.dumps(filing.to_record()))
    ctx.store.put_text(bronze.parsed_key(day), "\n".join(lines), content_type="application/json")
    log.info("Parse %s: %s", day, stats)
    _audit(ctx, day, "parse_filings", stats)
    _check_failures("parse_filings", stats["parse_errors"], len(keys))
    return stats


def load_silver(day: date, ctx: Context | None = None) -> dict[str, int]:
    """Upsert the day's parsed records into PostgreSQL silver tables."""
    ctx = ctx or Context.create()
    key = bronze.parsed_key(day)
    if not ctx.store.exists(key):
        log.info("No parsed records for %s", day)
        return {"filings": 0, "owners": 0, "transactions": 0}
    text = ctx.store.get_text(key)
    records = [json.loads(line) for line in text.splitlines() if line.strip()]
    conn = connect(ctx.settings)
    try:
        init_db(conn)
        result = load_records(conn, records, source="edgar_daily")
    finally:
        conn.close()
    stats = {
        "new_filings": result.filings,
        "new_owners": result.owners,
        "new_transactions": result.transactions,
    }
    log.info("Load %s: %s", day, stats)
    _audit(ctx, day, "load_silver", stats)
    return stats


def run_day(day: date) -> None:
    """Run all ingestion steps for one day (handy outside Airflow)."""
    ctx = Context.create()
    fetch_index(day, ctx)
    download_filings(day, ctx)
    parse_filings(day, ctx)
    load_silver(day, ctx)
