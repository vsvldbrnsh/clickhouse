from datetime import datetime, timedelta

CLICKHOUSE_HOST = "clickhouse1"
CLICKHOUSE_PORT = 8123

DAG_ID = "clickhouse_load_taxi_zones"
SCHEDULE = "@once"
START_DATE = datetime(2025, 1, 1)
CATCHUP = False
MAX_ACTIVE_RUNS = 1
TAGS = ["clickhouse", "ingestion"]

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": False,
}
