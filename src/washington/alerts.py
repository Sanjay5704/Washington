"""Alerting: record newly detected clusters and notify (Telegram if configured, else log)."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import requests

from washington.config import Settings, load_settings
from washington.silver import connect, init_db

log = logging.getLogger(__name__)

# Only clusters that became public recently are "news". Older ones (e.g. from the 5-year
# backfill) are history, not alerts.
ALERT_LOOKBACK_DAYS = 7


def format_alert(row: dict[str, Any]) -> str:
    ticker = row.get("ticker") or "n/a"
    return (
        f"Insider cluster buy: {row.get('issuer_name') or row['issuer_cik']} ({ticker})\n"
        f"{row['n_insiders']} insiders bought ${float(row['total_value']):,.0f} "
        f"(public on {row['signal_date']}).\n"
        "Data-engineering demo, not investment advice."
    )


def send_telegram(settings: Settings, text: str) -> None:
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    response = requests.post(
        url, json={"chat_id": settings.telegram_chat_id, "text": text}, timeout=15
    )
    response.raise_for_status()


def detect_new_clusters(day: date, settings: Settings | None = None) -> int:
    """Insert clusters whose signal date falls in the last week into ops.cluster_alerts."""
    settings = settings or load_settings()
    conn = connect(settings)
    try:
        init_db(conn)
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.cluster_alerts
                    (cluster_id, issuer_cik, ticker, issuer_name, signal_date,
                     n_insiders, total_value)
                SELECT cluster_id, issuer_cik, ticker, issuer_name, signal_date,
                       n_insiders, total_value_usd
                FROM gold.insider_clusters
                WHERE signal_date BETWEEN %s AND %s
                ON CONFLICT (cluster_id) DO NOTHING
                RETURNING cluster_id
                """,
                (day - timedelta(days=ALERT_LOOKBACK_DAYS), day),
            )
            new = len(cur.fetchall())
    finally:
        conn.close()
    log.info("%d new clusters detected for %s", new, day)
    return new


def send_pending_alerts(settings: Settings | None = None) -> dict[str, int]:
    settings = settings or load_settings()
    conn = connect(settings)
    stats = {"sent": 0, "logged": 0}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cluster_id, issuer_cik, ticker, issuer_name, signal_date, n_insiders, "
                "total_value FROM ops.cluster_alerts WHERE status = 'pending' ORDER BY signal_date"
            )
            cols = [c.name for c in cur.description]
            pending = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        for row in pending:
            text = format_alert(row)
            if settings.telegram_enabled:
                send_telegram(settings, text)  # raises -> task fails -> visible in Airflow
                status = "sent"
            else:
                log.info("ALERT (Telegram disabled):\n%s", text)
                status = "logged"
            stats[status] += 1
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE ops.cluster_alerts SET status = %s, sent_at = now() "
                    "WHERE cluster_id = %s",
                    (status, row["cluster_id"]),
                )
    finally:
        conn.close()
    return stats


def notify_failure(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback: make failures loud, not just red in the UI."""
    ti = context.get("task_instance")
    text = (
        f"Washington pipeline FAILED: task {getattr(ti, 'task_id', '?')} "
        f"(run {context.get('ds')}). Log: {getattr(ti, 'log_url', '')}"
    )
    log.error(text)
    settings = load_settings()
    if settings.telegram_enabled:
        try:
            send_telegram(settings, text)
        except requests.RequestException as exc:
            log.error("Could not send failure alert: %s", exc)
