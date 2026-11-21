#!/bin/sh
# Runs once, on first start of an empty data volume.
# The warehouse DB ($POSTGRES_DB) is created by the image; Airflow gets its own metadata DB.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE airflow;"
