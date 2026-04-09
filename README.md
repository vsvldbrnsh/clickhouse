# ClickHouse + NYC Taxi + Apache Superset

A 2-shard ClickHouse cluster with ClickHouse Keeper, loaded with NYC yellow taxi trip data (2019-2023), visualized through Apache Superset dashboards.

## Table of Contents

- [Manage ClickHouse](docs/clickhouse.md)
- [Manage Superset](docs/superset.md)

## Architecture

- **ClickHouse cluster**: 2 shards (clickhouse1, clickhouse2), `ReplicatedMergeTree` + `Distributed` tables
- **ClickHouse Keeper**: 3-node Raft quorum (keeper1, keeper2, keeper3)
- **Apache Superset**: dashboard UI with `clickhouse-connect` driver
- **PostgreSQL**: Superset metadata store

## Quick Start

```bash
# 1. Start all services (waits for healthchecks automatically)
docker compose up -d

# 2. Wait for services (~60s for Superset init to complete)
docker compose logs -f superset-init

# 3. Create tables, RBAC, and load NYC taxi data (~50GB, takes a while)
#    Safe to re-run: skips months that are already loaded
docker compose exec clickhouse1 bash /clickhouse/scripts/init-clickhouse.sh

# 4. Bootstrap Superset dashboards
docker compose exec superset python /app/init-superset.py
```

## Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Superset | http://localhost:8088 | admin / admin |
| ClickHouse 1 (HTTP) | http://localhost:8123 | default (no password) |
| ClickHouse 2 (HTTP) | http://localhost:8124 | default (no password) |

## Dashboards

1. **Trip Volume Overview** - monthly/daily trip counts, revenue trends, payment method breakdown
2. **Fare Analysis** - fare distribution, tip percentages, hourly fare patterns
3. **Geographic Insights** - top pickup/dropoff locations, busiest routes, revenue by location
4. **Passenger & Distance Patterns** - passenger count analysis, distance vs fare, hourly heatmap

## Verify Cluster

```bash
# Check cluster nodes
docker compose exec clickhouse1 clickhouse-client -q "SELECT * FROM system.clusters WHERE cluster = 'taxi_cluster'"

# Check keeper quorum
docker compose exec clickhouse1 clickhouse-client -q "SELECT * FROM system.zookeeper WHERE path = '/'"

# Row counts per shard
docker compose exec clickhouse1 clickhouse-client -q "SELECT hostName(), count() FROM nyc_taxi.trips_local"
docker compose exec clickhouse2 clickhouse-client -q "SELECT hostName(), count() FROM nyc_taxi.trips_local"

# Total rows via distributed table
docker compose exec clickhouse1 clickhouse-client -q "SELECT count() FROM nyc_taxi.trips"
```

## Stop

```bash
docker compose down        # stop containers, keep data
docker compose down -v     # stop containers and delete all data
```
