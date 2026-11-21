"""Washington daily pipeline.

    fetch_index -> download_filings -> parse_filings -> load_silver
        -> dbt_run -> dbt_test -> detect_clusters -> send_alerts
                              \\-> refresh_prices -> dbt_returns

Runs at 06:00 UTC and processes the PREVIOUS day's filings (EDGAR's daily index is complete
by ~22:00 New York time). Each run handles exactly one filing date = data_interval_start, so
backfilling a date range is just `airflow dags backfill -s ... -e ...`.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from washington.alerts import notify_failure

DBT_VARS = json.dumps(
    {
        "cluster_min_insiders": int(os.environ.get("CLUSTER_MIN_INSIDERS", 3)),
        "cluster_window_days": int(os.environ.get("CLUSTER_WINDOW_DAYS", 10)),
        "cluster_min_value_usd": int(os.environ.get("CLUSTER_MIN_VALUE_USD", 250000)),
    }
)


def dbt(command: str) -> str:
    return (
        f'cd "$WASHINGTON_DBT_DIR" && "$DBT_BIN" {command} --profiles-dir . --vars \'{DBT_VARS}\''
    )


def _day(ds: str) -> date:
    return date.fromisoformat(ds)


@dag(
    dag_id="washington_daily",
    description="SEC Form 4 -> bronze -> silver -> gold -> cluster alerts",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one SEC client at a time + dbt runs never overlap
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
    },
    tags=["washington", "sec", "form4"],
)
def washington_daily() -> None:
    @task(pool="sec_edgar")
    def fetch_index(ds: str | None = None) -> int:
        from washington.pipeline import fetch_index as step

        return step(_day(ds))

    @task(pool="sec_edgar")
    def download_filings(ds: str | None = None) -> dict:
        from washington.pipeline import download_filings as step

        return step(_day(ds))

    @task
    def parse_filings(ds: str | None = None) -> dict:
        from washington.pipeline import parse_filings as step

        return step(_day(ds))

    @task
    def load_silver(ds: str | None = None) -> dict:
        from washington.pipeline import load_silver as step

        return step(_day(ds))

    dbt_run = BashOperator(task_id="dbt_run", bash_command=dbt("run --exclude cluster_returns"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=dbt("test --exclude cluster_returns"))

    @task
    def detect_clusters(ds: str | None = None) -> int:
        from washington.alerts import detect_new_clusters

        return detect_new_clusters(_day(ds))

    @task
    def send_alerts() -> dict:
        from washington.alerts import send_pending_alerts

        return send_pending_alerts()

    @task
    def refresh_prices() -> dict:
        from washington.prices import refresh_prices as step

        return step()

    dbt_returns = BashOperator(
        task_id="dbt_returns", bash_command=dbt("build --select cluster_returns")
    )

    ingest = fetch_index() >> download_filings() >> parse_filings() >> load_silver()
    ingest >> dbt_run >> dbt_test
    dbt_test >> detect_clusters() >> send_alerts()
    dbt_test >> refresh_prices() >> dbt_returns


washington_daily()
