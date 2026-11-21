"""Needs a real PostgreSQL. Runs in CI (service container) and locally against docker compose:

$env:WASHINGTON_TEST_DB=1; $env:POSTGRES_HOST="localhost"; $env:POSTGRES_PORT=5433; pytest
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

from washington.config import load_settings
from washington.edgar.index import extract_ownership_xml
from washington.parser import parse_form4
from washington.silver import connect, init_db, load_records

pytestmark = pytest.mark.integration

if not os.environ.get("WASHINGTON_TEST_DB"):
    pytest.skip("set WASHINGTON_TEST_DB=1 to run database tests", allow_module_level=True)

FIXTURES = Path(__file__).parent / "fixtures"
ACC = "TEST-0001193125-26-470298"


@pytest.fixture
def conn():
    connection = connect(load_settings())
    init_db(connection)
    yield connection
    with connection, connection.cursor() as cur:
        for table in ("form4_nonderiv_transactions", "form4_reporting_owners", "form4_filings"):
            cur.execute(f"DELETE FROM silver.{table} WHERE accession_no = %s", (ACC,))
    connection.close()


def _record() -> dict:
    xml = extract_ownership_xml((FIXTURES / "form4_submission_ufi.txt").read_text())
    assert xml is not None
    return parse_form4(xml, ACC, date(2026, 11, 20)).to_record()


def _counts(conn) -> tuple[int, ...]:
    with conn.cursor() as cur:
        out = []
        for table in ("form4_filings", "form4_reporting_owners", "form4_nonderiv_transactions"):
            cur.execute(f"SELECT count(*) FROM silver.{table} WHERE accession_no = %s", (ACC,))
            out.append(cur.fetchone()[0])
    return tuple(out)


def test_load_is_idempotent(conn) -> None:
    first = load_records(conn, [_record()])
    assert (first.filings, first.owners, first.transactions) == (1, 1, 2)
    assert _counts(conn) == (1, 1, 2)

    second = load_records(conn, [_record()])  # re-running the same day
    assert (second.filings, second.owners, second.transactions) == (0, 0, 0)
    assert _counts(conn) == (1, 1, 2)


def test_types_are_stored_correctly(conn) -> None:
    load_records(conn, [_record()])
    with conn.cursor() as cur:
        cur.execute(
            "SELECT price_per_share, transaction_date FROM silver.form4_nonderiv_transactions "
            "WHERE accession_no = %s AND line_no = 2",
            (ACC,),
        )
        price, txn_date = cur.fetchone()
    assert str(price) == "5.88"
    assert txn_date == date(2026, 11, 18)
