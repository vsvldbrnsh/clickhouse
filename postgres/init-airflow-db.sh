#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB_NAME:-airflow};
    CREATE USER ${AIRFLOW_DB_USER:-airflow} WITH PASSWORD '${AIRFLOW_DB_PASSWORD:-airflow}';
    ALTER DATABASE ${AIRFLOW_DB_NAME:-airflow} OWNER TO ${AIRFLOW_DB_USER:-airflow};
EOSQL
