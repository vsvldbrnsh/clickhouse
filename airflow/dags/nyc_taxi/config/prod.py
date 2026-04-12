from datetime import datetime, timedelta

CLICKHOUSE_HOST = "clickhouse1"
CLICKHOUSE_PORT = 8123

DAG_ID = "clickhouse_load_nyc_taxi"
SCHEDULE = "0 5 3 * *"
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 3, 1)  # TLC Parquet files verified available through 2026-02
CATCHUP = True
MAX_ACTIVE_RUNS = 1  # Sequential: prevents CloudFront rate-limiting ClickHouse's IP
TAGS = ["clickhouse", "ingestion"]

