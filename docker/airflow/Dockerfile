FROM apache/airflow:2.10.5-python3.11

# Pipeline dependencies, pinned against Airflow's constraints so we don't break Airflow itself.
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# dbt lives in its own virtualenv: its pinned dependencies clash with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.4" "dbt-postgres==1.9.0"
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt
