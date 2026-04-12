import sys
from pathlib import Path

# Airflow 3.0 bundle loader doesn't add dags/ to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import clickhouse_connect
from airflow.decorators import dag, task
from jinja2 import Template

from taxi_zones.config.prod import (
    CATCHUP,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    DAG_ID,
    DEFAULT_ARGS,
    MAX_ACTIVE_RUNS,
    SCHEDULE,
    START_DATE,
    TAGS,
)

SQL_DIR = Path(__file__).parent / "sql"


def render(name: str, **ctx) -> str:
    return Template((SQL_DIR / name).read_text()).render(**ctx)


@dag(
    dag_id=DAG_ID,
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=TAGS,
    default_args=DEFAULT_ARGS,
)
def clickhouse_load_taxi_zones():

    @task()
    def load_zones():
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

        sql = render("load.sql")

        print(sql)

        for statement in (s.strip() for s in sql.split(";") if s.strip()):
            client.command(statement)
            

        loaded = client.command("SELECT count() FROM nyc_taxi.taxi_zones_local")
        print(f"DONE: {loaded} zones loaded")

    load_zones()


clickhouse_load_taxi_zones()
