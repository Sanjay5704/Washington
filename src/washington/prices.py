"""Fetch daily prices with yfinance for every cluster ticker (plus SPY as the benchmark).

Incremental: a ticker is only (re)fetched when the prices we hold don't yet cover the
range its clusters need (signal date - 5 days .. signal date + 140 days, capped at today).
The returns themselves are computed in SQL by the dbt model `cluster_returns`.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any

from psycopg2.extras import execute_values

from washington.config import Settings, load_settings
from washington.silver import connect, init_db

log = logging.getLogger(__name__)

BENCHMARK = "SPY"
BATCH_SIZE = 40
DAYS_BEFORE = 5
DAYS_AFTER = 140  # 90 calendar days + slack for weekends/holidays
RETRY_FAILED_AFTER_DAYS = 7


def yahoo_symbol(ticker: str) -> str:
    """SEC uses BRK.B, Yahoo uses BRK-B."""
    return ticker.strip().upper().replace(".", "-").replace("/", "-")


def needed_ranges(conn: Any, today: date) -> dict[str, tuple[date, date]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, min(signal_date), max(signal_date)
            FROM gold.insider_clusters
            WHERE ticker IS NOT NULL AND ticker ~ '^[A-Z0-9.\\-]{1,10}$'
            GROUP BY ticker
            """
        )
        rows = cur.fetchall()
    ranges: dict[str, tuple[date, date]] = {}
    for ticker, first, last in rows:
        ranges[ticker] = (
            first - timedelta(days=DAYS_BEFORE),
            min(last + timedelta(days=DAYS_AFTER), today),
        )
    if ranges:
        ranges[BENCHMARK] = (min(r[0] for r in ranges.values()), today)
    return ranges


def tickers_to_fetch(conn: Any, ranges: dict[str, tuple[date, date]], today: date) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, min(price_date), max(price_date) FROM silver.stock_prices GROUP BY 1"
        )
        have = {t: (lo, hi) for t, lo, hi in cur.fetchall()}
        cur.execute(
            "SELECT ticker FROM silver.price_fetch_log WHERE status <> 'ok' AND fetched_at > %s",
            (today - timedelta(days=RETRY_FAILED_AFTER_DAYS),),
        )
        recently_failed = {r[0] for r in cur.fetchall()}
    todo = []
    for ticker, (start, end) in sorted(ranges.items()):
        if ticker in recently_failed:
            continue
        lo_hi = have.get(ticker)
        # 4-day slack: the range edges may fall on weekends/holidays
        if lo_hi and lo_hi[0] <= start + timedelta(days=4) and lo_hi[1] >= end - timedelta(days=4):
            continue
        todo.append(ticker)
    return todo


def _download(symbols: list[str], start: date, end: date) -> dict[str, list[tuple[date, float]]]:
    import yfinance as yf  # imported lazily: heavy, and not needed by the rest of the package

    frame = yf.download(
        symbols,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        auto_adjust=True,
        group_by="ticker",
        threads=True,
        progress=False,
    )
    out: dict[str, list[tuple[date, float]]] = {}
    if frame is None or frame.empty:
        return out
    for symbol in symbols:
        try:
            closes = frame[symbol]["Close"].dropna()
        except KeyError:
            continue
        out[symbol] = [(ts.date(), float(px)) for ts, px in closes.items() if px > 0]
    return out


def refresh_prices(settings: Settings | None = None, today: date | None = None) -> dict[str, int]:
    settings = settings or load_settings()
    today = today or date.today()
    conn = connect(settings)
    stats = {"tickers_needed": 0, "tickers_fetched": 0, "tickers_failed": 0, "price_rows": 0}
    try:
        init_db(conn)
        ranges = needed_ranges(conn, today)
        todo = tickers_to_fetch(conn, ranges, today)
        stats["tickers_needed"] = len(ranges)
        log.info("%d tickers need prices, %d to fetch", len(ranges), len(todo))
        for i in range(0, len(todo), BATCH_SIZE):
            batch = todo[i : i + BATCH_SIZE]
            start = min(ranges[t][0] for t in batch)
            end = max(ranges[t][1] for t in batch)
            symbols = {yahoo_symbol(t): t for t in batch}
            try:
                data = _download(list(symbols), start, end)
            except Exception as exc:  # network / Yahoo hiccup: skip batch, retry next run
                log.warning("yfinance batch failed: %s", exc)
                data = {}
            rows, log_rows = [], []
            for symbol, ticker in symbols.items():
                points = data.get(symbol, [])
                rows.extend((ticker, d, px) for d, px in points)
                log_rows.append((ticker, "ok" if points else "no_data", len(points)))
                stats["tickers_fetched" if points else "tickers_failed"] += 1
            with conn, conn.cursor() as cur:
                if rows:
                    execute_values(
                        cur,
                        "INSERT INTO silver.stock_prices (ticker, price_date, adj_close) VALUES %s "
                        "ON CONFLICT (ticker, price_date) "
                        "DO UPDATE SET adj_close = EXCLUDED.adj_close",
                        rows,
                        page_size=5000,
                    )
                execute_values(
                    cur,
                    "INSERT INTO silver.price_fetch_log (ticker, status, rows_loaded) VALUES %s "
                    "ON CONFLICT (ticker) DO UPDATE SET status = EXCLUDED.status, "
                    "rows_loaded = EXCLUDED.rows_loaded, fetched_at = now()",
                    log_rows,
                )
            stats["price_rows"] += len(rows)
            log.info("Prices batch %d/%d done", i // BATCH_SIZE + 1, -(-len(todo) // BATCH_SIZE))
            time.sleep(1.0)  # be gentle with Yahoo
    finally:
        conn.close()
    return stats
