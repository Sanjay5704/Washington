from __future__ import annotations

from datetime import date
from pathlib import Path

from washington.edgar.index import daily_index_url, extract_ownership_xml, parse_master_index

FIXTURES = Path(__file__).parent / "fixtures"


def test_daily_index_url_uses_quarter() -> None:
    assert daily_index_url(date(2026, 11, 20)) == (
        "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261120.idx"
    )
    assert "/2026/QTR1/master.20260102.idx" in daily_index_url(date(2026, 1, 2))


def test_parse_master_index_keeps_only_form4() -> None:
    text = (FIXTURES / "master_sample.idx").read_text()
    entries = parse_master_index(text)
    assert len(entries) == 5
    assert {e.form_type for e in entries} == {"4"}
    first = entries[0]
    assert first.accession_no == "0001193125-26-470298"
    assert first.date_filed == date(2026, 11, 20)
    assert first.url == "https://www.sec.gov/Archives/edgar/data/100726/0001193125-26-470298.txt"


def test_parse_master_index_dedupes_same_filing_listed_twice() -> None:
    # Real EDGAR shape: the same accession listed under the issuer's AND the owner's folder.
    line = "100726|UNIFI INC|4|20261120|edgar/data/100726/0001193125-26-470298.txt"
    dup = "1991763|EAKER ANDREW JAMES|4|20261120|edgar/data/1991763/0001193125-26-470298.txt"
    entries = parse_master_index(f"{line}\n{dup}\n")
    assert len(entries) == 1
    assert entries[0].accession_no == "0001193125-26-470298"


def test_parse_master_index_can_include_amendments() -> None:
    text = (FIXTURES / "master_sample.idx").read_text()
    assert len(parse_master_index(text, form_types=("4", "4/A"))) == 7


def test_extract_ownership_xml_from_submission() -> None:
    text = (FIXTURES / "form4_submission_ufi.txt").read_text()
    xml = extract_ownership_xml(text)
    assert xml is not None
    assert xml.startswith("<?xml")
    assert xml.rstrip().endswith("</ownershipDocument>")


def test_extract_ownership_xml_returns_none_without_xml() -> None:
    assert extract_ownership_xml("<SEC-DOCUMENT>plain text</SEC-DOCUMENT>") is None
