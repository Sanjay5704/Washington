"""Silver layer loader: typed, de-duplicated rows in PostgreSQL.

Every insert uses ON CONFLICT DO NOTHING on the natural key, so loading the same
day twice is a no-op. That makes every pipeline step safe to retry.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from washington.config import Settings

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def connect(settings: Settings) -> PgConnection:
    return psycopg2.connect(settings.pg_dsn)


def schema_sql() -> str:
    # In containers the sql/ folder is mounted next to src/ (see docker-compose.yml).
    return (SQL_DIR / "silver_schema.sql").read_text(encoding="utf-8")


def init_db(conn: PgConnection) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(schema_sql())


@dataclass
class LoadResult:
    filings: int = 0
    owners: int = 0
    transactions: int = 0


FILING_COLS = (
    "accession_no",
    "filing_date",
    "document_type",
    "period_of_report",
    "issuer_cik",
    "issuer_name",
    "issuer_ticker",
    "aff10b5one",
    "source",
)
OWNER_COLS = (
    "accession_no",
    "owner_cik",
    "owner_name",
    "is_director",
    "is_officer",
    "is_ten_percent_owner",
    "is_other",
    "officer_title",
)
TXN_COLS = (
    "accession_no",
    "line_no",
    "security_title",
    "transaction_date",
    "transaction_form_type",
    "transaction_code",
    "equity_swap",
    "shares",
    "price_per_share",
    "acquired_disposed",
    "shares_owned_after",
    "direct_indirect",
)


def _insert(cur: Any, table: str, cols: tuple[str, ...], key: str, rows: list[tuple]) -> int:
    """Insert rows, skipping existing keys. Returns how many rows were actually new."""
    if not rows:
        return 0
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({key}) DO NOTHING RETURNING 1"
    )
    inserted = execute_values(cur, sql, rows, page_size=1000, fetch=True)
    return len(inserted)


def load_records(
    conn: PgConnection, records: Iterable[dict[str, Any]], source: str = "edgar_daily"
) -> LoadResult:
    """Load parsed Form 4 records (Form4Filing.to_record() dicts) in one transaction."""
    filings, owners, txns = [], [], []
    for rec in records:
        acc = rec["accession_no"]
        filings.append(tuple(rec.get(c) if c != "source" else source for c in FILING_COLS))
        for o in rec.get("owners", []):
            owners.append((acc, *(o.get(c) for c in OWNER_COLS[1:])))
        for t in rec.get("transactions", []):
            txns.append((acc, *(t.get(c) for c in TXN_COLS[1:])))

    result = LoadResult()
    with conn, conn.cursor() as cur:
        result.filings = _insert(cur, "silver.form4_filings", FILING_COLS, "accession_no", filings)
        result.owners = _insert(
            cur, "silver.form4_reporting_owners", OWNER_COLS, "accession_no, owner_cik", owners
        )
        result.transactions = _insert(
            cur, "silver.form4_nonderiv_transactions", TXN_COLS, "accession_no, line_no", txns
        )
    return result


def record_audit(conn: PgConnection, run_date: date, step: str, metrics: dict[str, int]) -> None:
    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO ops.ingest_audit (run_date, step, metric, value) VALUES %s",
            [(run_date, step, k, int(v)) for k, v in metrics.items()],
        )
