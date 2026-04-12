import sys
from pathlib import Path
from datetime import datetime, timedelta

# Airflow 3.0 bundle loader doesn't add dags/ to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import clickhouse_connect
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from jinja2 import Template
from nyc_taxi.config import config


DEFAULT_ARGS = {
    "retries": 2,
    "max_active_runs": 2,
    "retry_delay": timedelta(seconds=30),
}


SQL_DIR = Path(__file__).parent / "sql"

def render(name: str, **ctx) -> str:
    return Template((SQL_DIR / name).read_text()).render(**ctx)


@dag(
    dag_id=config.DAG_ID,
    schedule=config.SCHEDULE,
    start_date=config.START_DATE,
    end_date=config.END_DATE,
    catchup=config.CATCHUP,
    max_active_runs=config.MAX_ACTIVE_RUNS,
    tags=config.TAGS,
    default_args=DEFAULT_ARGS,
)
def clickhouse_load_nyc_taxi():

    @task()
    def load_month():
        context = get_current_context()
        dt = context["data_interval_start"]
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        partition = f"{year}{month}"

        client = clickhouse_connect.get_client(host=config.CLICKHOUSE_HOST, port=config.CLICKHOUSE_PORT)

        # Fire-and-forget: DDL executes in background on all nodes
        client.command(render("drop_partition.sql", partition=partition))
        client.command(render("load.sql", year=year, month=month))

        loaded = client.command(
            f"SELECT count() FROM nyc_taxi.trips_local "
            f"WHERE toYYYYMM(tpep_pickup_datetime) = {partition}"
        )
        print(f"DONE {year}-{month}: {loaded} rows on this shard")

    load_month()


clickhouse_load_nyc_taxi()
