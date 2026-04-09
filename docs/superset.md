# Managing the Superset Service

## Directory Structure

```
superset/
├── Dockerfile              # Custom image: apache/superset base + clickhouse-connect, gevent, redis
├── superset_config.py      # PostgreSQL URI, Redis cache, Celery config, CSRF, 300s timeout
├── dashboards/
│   ├── __init__.py
│   ├── _base.py            # Shared REST API helpers (session, database, dataset, chart, dashboard)
│   ├── trip_volume.py      # Trip Volume Overview dashboard
│   ├── fare_analysis.py    # Fare Analysis dashboard
│   ├── geographic.py       # Geographic Insights dashboard
│   └── passenger_distance.py  # Passenger & Distance Patterns dashboard
└── scripts/
    └── init-superset.py    # Orchestrates dashboard bootstrap via Superset REST API
```

## Bootstrapping Dashboards

Run the init script inside the running `superset` container. It creates the ClickHouse database connection, datasets, charts, and dashboards via the Superset REST API. The script is idempotent — existing objects are replaced, not duplicated.

```bash
docker compose exec superset python /app/init-superset.py
```

## Adding a New Dashboard

1. Create `superset/dashboards/<name>.py`. The module must export a `create(s, db_id)` function that uses the helpers from `_base.py` to create datasets, charts, and a dashboard. Follow the pattern of any existing dashboard module.

2. Import the new module in `superset/scripts/init-superset.py` and call its `create` function:

```python
from dashboards import trip_volume, fare_analysis, geographic, passenger_distance, <name>

def main():
    ...
    <name>.create(s, db_id)
```

3. Re-run the bootstrap command:

```bash
docker compose exec superset python /app/init-superset.py
```

## Services Overview

Four containers make up the Superset stack:

| Container        | Role                                                                 |
|------------------|----------------------------------------------------------------------|
| `superset`       | Gunicorn web server, 4 workers, gevent async, port 8088              |
| `superset-worker`| Celery worker (prefork pool, 2 concurrent tasks, fair scheduling)    |
| `superset-beat`  | Celery beat scheduler (reports.scheduler runs every minute)          |
| `redis`          | Message broker for Celery and backing store for all cache layers     |

The `superset-init` container runs once at startup to apply database migrations and create the admin user. It must complete successfully before `superset` starts (enforced by `depends_on: service_completed_successfully`).

## Caching

All cache layers are backed by Redis, configured in `superset/superset_config.py`:

| Cache                      | Redis DB | Key Prefix            | Default TTL |
|----------------------------|----------|-----------------------|-------------|
| UI cache (`CACHE_CONFIG`)  | 1        | `superset_`           | 300 s       |
| Query results              | 1        | `superset_data_`      | 600 s       |
| Filter state               | 1        | `superset_filter_`    | 300 s       |
| Explore form data          | 1        | `superset_explore_`   | 300 s       |

To adjust timeouts, edit `CACHE_DEFAULT_TIMEOUT` in the relevant config dict and restart the `superset` and `superset-worker` containers:

```bash
docker compose restart superset superset-worker
```

## ClickHouse Connection

Superset connects to ClickHouse using the `clickhousedb` SQLAlchemy engine (provided by the `clickhouse-connect` driver installed in the custom Dockerfile):

```
clickhousedb://default@clickhouse1:8123/nyc_taxi
```

- **Host**: `clickhouse1` (Docker internal hostname, port 8123 HTTP)
- **User**: `default` (no password)
- **Database**: `nyc_taxi`

> Note: use `clickhousedb://` as the scheme, not `clickhouse+http://`. The two use different drivers and only `clickhousedb` is installed in this image.

## Access

| URL                    | Credentials  |
|------------------------|--------------|
| http://localhost:8088  | admin / admin |

The admin password is set from the `SUPERSET_ADMIN_PASSWORD` environment variable in `.env` (default: `admin`). To change it, update `.env` before the first `docker compose up`, or use the Superset UI under Settings > List Users after the service is running.
