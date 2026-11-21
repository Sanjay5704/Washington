from __future__ import annotations

from datetime import date
from decimal import Decimal

from washington.alerts import format_alert
from washington.config import load_settings
from washington.prices import yahoo_symbol


def test_format_alert() -> None:
    text = format_alert(
        {
            "issuer_cik": "0000100726",
            "issuer_name": "UNIFI INC",
            "ticker": "UFI",
            "n_insiders": 3,
            "total_value": Decimal("1250000.5"),
            "signal_date": date(2026, 11, 20),
        }
    )
    assert "UNIFI INC (UFI)" in text
    assert "3 insiders bought $1,250,000" in text
    assert "not investment advice" in text


def test_yahoo_symbol() -> None:
    assert yahoo_symbol("brk.b") == "BRK-B"
    assert yahoo_symbol("AAPL") == "AAPL"


def test_telegram_disabled_without_token(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    assert load_settings().telegram_enabled is False
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "abc")
    assert load_settings().telegram_enabled is True
