from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from washington.edgar.index import extract_ownership_xml
from washington.parser import Form4ParseError, normalise_ticker, parse_form4, to_bool, to_date

FIXTURES = Path(__file__).parent / "fixtures"
FILED = date(2026, 11, 20)


@pytest.fixture
def real_xml() -> str:
    xml = extract_ownership_xml((FIXTURES / "form4_submission_ufi.txt").read_text())
    assert xml is not None
    return xml


def test_parse_real_filing_header(real_xml: str) -> None:
    f = parse_form4(real_xml, "0001193125-26-470298", FILED)
    assert f.document_type == "4"
    assert f.issuer_cik == "0000100726"
    assert f.issuer_name == "UNIFI INC"
    assert f.issuer_ticker == "UFI"
    assert f.period_of_report == date(2026, 8, 14)
    assert f.aff10b5one is False


def test_parse_real_filing_owner(real_xml: str) -> None:
    f = parse_form4(real_xml, "acc", FILED)
    assert len(f.owners) == 1
    owner = f.owners[0]
    assert owner.owner_cik == "0001991763"
    assert owner.is_officer is True and owner.is_director is False
    assert owner.officer_title == "EVP, CFO, & Treasurer"


def test_parse_real_filing_transactions(real_xml: str) -> None:
    f = parse_form4(real_xml, "acc", FILED)
    assert [t.line_no for t in f.transactions] == [1, 2]
    first, second = f.transactions
    assert first.transaction_code == "M"
    assert first.shares == Decimal("14280")
    assert first.price_per_share is None  # only a footnote, no <value>
    assert second.transaction_code == "F"
    assert second.price_per_share == Decimal("5.88")
    assert second.acquired_disposed == "D"
    assert second.shares_owned_after == Decimal("102788")
    assert second.transaction_date == date(2026, 11, 18)


SYNTHETIC = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-11-17</periodOfReport>
  <issuer><issuerCik>12345</issuerCik><issuerName>Acme</issuerName>
    <issuerTradingSymbol> acme </issuerTradingSymbol></issuer>
  <reportingOwner><reportingOwnerId><rptOwnerCik>111</rptOwnerCik>
    <rptOwnerName>Fund A</rptOwnerName>
    </reportingOwnerId><reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship></reportingOwner>
  <reportingOwner><reportingOwnerId><rptOwnerCik>222</rptOwnerCik>
    <rptOwnerName>Fund GP</rptOwnerName>
    </reportingOwnerId></reportingOwner>
  <reportingOwner><reportingOwnerId><rptOwnerCik>111</rptOwnerCik></reportingOwnerId></reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeHolding><securityTitle><value>Common</value></securityTitle></nonDerivativeHolding>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-11-17-05:00</value></transactionDate>
      <transactionCoding><transactionCode>p</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1,000</value></transactionShares>
        <transactionPricePerShare><value>$12.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


def test_parse_synthetic_edge_cases() -> None:
    f = parse_form4(SYNTHETIC, "acc-2", FILED)
    assert f.issuer_cik == "0000012345"  # zero padded
    assert f.issuer_ticker == "ACME"  # trimmed, upper-cased
    assert [o.owner_cik for o in f.owners] == ["0000000111", "0000000222"]  # dup owner dropped
    assert f.owners[0].is_ten_percent_owner is True
    assert len(f.transactions) == 1  # holding row ignored
    t = f.transactions[0]
    assert t.transaction_code == "P"
    assert t.transaction_date == date(2026, 11, 17)  # timezone suffix stripped
    assert t.shares == Decimal("1000") and t.price_per_share == Decimal("12.50")


def test_to_record_is_json_serialisable() -> None:
    record = parse_form4(SYNTHETIC, "acc-2", FILED).to_record()
    data = json.loads(json.dumps(record))
    assert data["filing_date"] == "2026-11-20"
    assert data["transactions"][0]["price_per_share"] == "12.50"


def test_invalid_xml_raises() -> None:
    with pytest.raises(Form4ParseError):
        parse_form4("<ownershipDocument><broken>", "acc", FILED)


def test_missing_issuer_cik_raises() -> None:
    with pytest.raises(Form4ParseError):
        parse_form4("<ownershipDocument><issuer/></ownershipDocument>", "acc", FILED)


@pytest.mark.parametrize(
    "raw,expected", [("1", True), ("true", True), ("0", False), ("false", False), ("x", None)]
)
def test_to_bool(raw: str, expected: bool | None) -> None:
    assert to_bool(raw) is expected


def test_small_converters() -> None:
    assert to_date("bad") is None
    assert normalise_ticker("NONE") is None
    assert normalise_ticker(None) is None
