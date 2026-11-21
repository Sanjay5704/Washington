"""Parse a Form 4 ownershipDocument XML into typed records.

We keep three things per filing:
  * the filing header (issuer, dates)
  * every reporting owner (a filing can be a joint filing by several owners)
  * every non-derivative transaction, numbered 1..n in document order (line_no)

(accession_no, line_no) is the natural key of a transaction in the silver layer.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any


class Form4ParseError(ValueError):
    pass


@dataclass
class ReportingOwner:
    owner_cik: str
    owner_name: str | None
    is_director: bool | None
    is_officer: bool | None
    is_ten_percent_owner: bool | None
    is_other: bool | None
    officer_title: str | None


@dataclass
class NonDerivTransaction:
    line_no: int
    security_title: str | None
    transaction_date: date | None
    transaction_form_type: str | None
    transaction_code: str | None
    equity_swap: bool | None
    shares: Decimal | None
    price_per_share: Decimal | None
    acquired_disposed: str | None
    shares_owned_after: Decimal | None
    direct_indirect: str | None


@dataclass
class Form4Filing:
    accession_no: str
    filing_date: date
    document_type: str
    period_of_report: date | None
    issuer_cik: str
    issuer_name: str | None
    issuer_ticker: str | None
    aff10b5one: bool | None
    owners: list[ReportingOwner] = field(default_factory=list)
    transactions: list[NonDerivTransaction] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        """JSON-serialisable dict (dates and decimals as strings)."""

        def convert(value: Any) -> Any:
            if isinstance(value, date | Decimal):
                return str(value)
            if isinstance(value, list):
                return [convert(v) for v in value]
            if isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            return value

        return convert(asdict(self))


# ---------- small helpers ----------


def _strip_namespaces(root: ET.Element) -> None:
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def _text(parent: ET.Element | None, path: str) -> str | None:
    if parent is None:
        return None
    el = parent.find(path)
    if el is None or el.text is None:
        return None
    value = el.text.strip()
    return value or None


def _value(parent: ET.Element | None, path: str) -> str | None:
    """Most Form 4 fields look like <transactionShares><value>100</value></transactionShares>."""
    return _text(parent, f"{path}/value")


def to_bool(raw: str | None) -> bool | None:
    if raw is None:
        return None
    raw = raw.strip().lower()
    if raw in {"1", "true", "y", "yes"}:
        return True
    if raw in {"0", "false", "n", "no"}:
        return False
    return None


def to_date(raw: str | None) -> date | None:
    """Dates are YYYY-MM-DD, sometimes with a timezone suffix like 2026-11-18-05:00."""
    if not raw or len(raw) < 10:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def to_decimal(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    try:
        value = Decimal(raw.replace(",", "").replace("$", "").strip())
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def normalise_ticker(raw: str | None) -> str | None:
    if raw is None:
        return None
    ticker = raw.strip().upper()
    if ticker in {"", "NONE", "N/A", "NA", "NULL", "-"}:
        return None
    return ticker


def normalise_cik(raw: str | None) -> str | None:
    """CIKs come zero-padded or not; store them as 10-digit strings."""
    if raw is None or not raw.strip().isdigit():
        return None
    return raw.strip().zfill(10)


# ---------- main entry point ----------


def parse_form4(xml_text: str, accession_no: str, filing_date: date) -> Form4Filing:
    try:
        root = ET.fromstring(xml_text.encode("utf-8"))
    except ET.ParseError as exc:
        raise Form4ParseError(f"{accession_no}: invalid XML ({exc})") from exc
    _strip_namespaces(root)
    if root.tag != "ownershipDocument":
        raise Form4ParseError(f"{accession_no}: root is <{root.tag}>, not <ownershipDocument>")

    issuer = root.find("issuer")
    issuer_cik = normalise_cik(_text(issuer, "issuerCik"))
    if issuer_cik is None:
        raise Form4ParseError(f"{accession_no}: missing issuerCik")

    filing = Form4Filing(
        accession_no=accession_no,
        filing_date=filing_date,
        document_type=_text(root, "documentType") or "",
        period_of_report=to_date(_text(root, "periodOfReport")),
        issuer_cik=issuer_cik,
        issuer_name=_text(issuer, "issuerName"),
        issuer_ticker=normalise_ticker(_text(issuer, "issuerTradingSymbol")),
        aff10b5one=to_bool(_text(root, "aff10b5One")),
    )

    seen_owners: set[str] = set()
    for owner in root.findall("reportingOwner"):
        rel = owner.find("reportingOwnerRelationship")
        cik = normalise_cik(_text(owner, "reportingOwnerId/rptOwnerCik"))
        if cik is None or cik in seen_owners:
            continue
        seen_owners.add(cik)
        filing.owners.append(
            ReportingOwner(
                owner_cik=cik,
                owner_name=_text(owner, "reportingOwnerId/rptOwnerName"),
                is_director=to_bool(_text(rel, "isDirector")),
                is_officer=to_bool(_text(rel, "isOfficer")),
                is_ten_percent_owner=to_bool(_text(rel, "isTenPercentOwner")),
                is_other=to_bool(_text(rel, "isOther")),
                officer_title=_text(rel, "officerTitle"),
            )
        )

    # Only <nonDerivativeTransaction>; <nonDerivativeHolding> rows are positions, not trades.
    table = root.find("nonDerivativeTable")
    rows = table.findall("nonDerivativeTransaction") if table is not None else []
    for line_no, txn in enumerate(rows, start=1):
        coding = txn.find("transactionCoding")
        amounts = txn.find("transactionAmounts")
        filing.transactions.append(
            NonDerivTransaction(
                line_no=line_no,
                security_title=_value(txn, "securityTitle"),
                transaction_date=to_date(_value(txn, "transactionDate")),
                transaction_form_type=_text(coding, "transactionFormType"),
                transaction_code=(_text(coding, "transactionCode") or "").upper() or None,
                equity_swap=to_bool(_text(coding, "equitySwapInvolved")),
                shares=to_decimal(_value(amounts, "transactionShares")),
                price_per_share=to_decimal(_value(amounts, "transactionPricePerShare")),
                acquired_disposed=_value(amounts, "transactionAcquiredDisposedCode"),
                shares_owned_after=to_decimal(
                    _value(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction")
                ),
                direct_indirect=_value(txn, "ownershipNature/directOrIndirectOwnership"),
            )
        )
    return filing
