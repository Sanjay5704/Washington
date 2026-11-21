"""Command line entry point: `python -m washington.cli <command>`."""

from __future__ import annotations

import argparse
import logging
from datetime import date

from washington.config import load_settings
from washington.silver import connect, init_db


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="washington")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="create silver/ops schemas and tables")
    run = sub.add_parser("run-day", help="ingest one filing date (index -> bronze -> silver)")
    run.add_argument("day", type=date.fromisoformat)
    sub.add_parser("fetch-prices", help="fetch prices for cluster tickers + SPY")
    detect = sub.add_parser("detect", help="record new clusters and send alerts")
    detect.add_argument("day", type=date.fromisoformat)
    args = parser.parse_args(argv)

    if args.command == "init-db":
        conn = connect(load_settings())
        try:
            init_db(conn)
        finally:
            conn.close()
        print("silver/ops schemas ready")
    elif args.command == "run-day":
        from washington.pipeline import run_day

        run_day(args.day)
    elif args.command == "fetch-prices":
        from washington.prices import refresh_prices

        print(refresh_prices())
    elif args.command == "detect":
        from washington.alerts import detect_new_clusters, send_pending_alerts

        print(detect_new_clusters(args.day))
        print(send_pending_alerts())


if __name__ == "__main__":
    main()
