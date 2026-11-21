"""Parse EDGAR daily master index files and pull the XML out of a full submission."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

ARCHIVES_URL = "https://www.sec.gov/Archives/"


@dataclass(frozen=True)
class IndexEntry:
    cik: str
    company_name: str
    form_type: str
    date_filed: date
    file_name: str  # e.g. edgar/data/100726/0001193125-26-470298.txt

    @property
    def accession_no(self) -> str:
        return self.file_name.rsplit("/", 1)[-1].removesuffix(".txt")

    @property
    def url(self) -> str:
        return ARCHIVES_URL + self.file_name


def daily_index_url(day: date) -> str:
    quarter = (day.month - 1) // 3 + 1
    return f"{ARCHIVES_URL}edgar/daily-index/{day.year}/QTR{quarter}/master.{day:%Y%m%d}.idx"


def parse_master_index(text: str, form_types: tuple[str, ...] = ("4",)) -> list[IndexEntry]:
    """Return one entry per filing of the wanted form types.

    The master index is pipe-delimited: CIK|Company Name|Form Type|Date Filed|File Name.
    A filing can be listed more than once (once per related CIK), so we de-duplicate on the
    file name, which contains the accession number.
    """
    entries: dict[str, IndexEntry] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) != 5 or not parts[0].strip().isdigit():
            continue  # header, separator line, or blank
        cik, name, form_type, filed, file_name = (p.strip() for p in parts)
        if form_type not in form_types:
            continue
        if file_name in entries:
            continue
        entries[file_name] = IndexEntry(
            cik=cik,
            company_name=name,
            form_type=form_type,
            date_filed=date(int(filed[:4]), int(filed[4:6]), int(filed[6:8])),
            file_name=file_name,
        )
    return list(entries.values())


_XML_BLOCK = re.compile(r"<XML>\s*(.*?)\s*</XML>", re.DOTALL | re.IGNORECASE)


def extract_ownership_xml(submission_text: str) -> str | None:
    """A full submission (.txt) wraps the Form 4 XML in <XML>...</XML>. Return the XML."""
    for block in _XML_BLOCK.findall(submission_text):
        if "<ownershipDocument" in block:
            return block.strip()
    return None
