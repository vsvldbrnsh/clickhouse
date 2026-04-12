from datetime import datetime, timedelta

CLICKHOUSE_HOST = "clickhouse1"
CLICKHOUSE_PORT = 8123

DAG_ID = "clickhouse_load_nyc_taxi"
SCHEDULE = "0 5 3 * *"
START_DATE = datetime(2025, 1, 1)
CATCHUP = False
MAX_ACTIVE_RUNS = 3
TAGS = ["clickhouse", "ingestion"]

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}
