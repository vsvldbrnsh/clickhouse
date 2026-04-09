# Managing the ClickHouse Cluster

## Directory Structure

```
clickhouse/
├── cluster/
│   ├── clickhouse1.xml     # Shard 1 cluster topology, ZooKeeper nodes, shard macros
│   └── clickhouse2.xml     # Shard 2 cluster topology, ZooKeeper nodes, shard macros
├── keeper/
│   ├── keeper1.xml         # Raft quorum config for keeper1
│   ├── keeper2.xml         # Raft quorum config for keeper2
│   └── keeper3.xml         # Raft quorum config for keeper3
├── schemas/
│   ├── 000_database.sql    # Creates the nyc_taxi database
│   ├── 001_rbac.sql        # Roles and users (reader, loader, admin_role)
│   ├── 002_trips_local.sql # ReplicatedMergeTree table per shard
│   └── 003_trips.sql       # Distributed table across all shards
├── data/
│   └── nyc_taxi/
│       └── load.sh         # Idempotent S3 Parquet ingestion with retry logic
├── scripts/
│   └── init-clickhouse.sh  # Entrypoint: applies schemas then runs data loaders
└── users.xml               # User profiles (default user, no password, 10GB memory limit)
```

## Applying Schemas

Run the init script inside the clickhouse1 container. It applies all numbered SQL files in `clickhouse/schemas/` in order, then runs every `load.sh` found under `clickhouse/data/`. The script is idempotent — safe to re-run.

```bash
docker compose exec clickhouse1 bash /clickhouse/scripts/init-clickhouse.sh
```

## Adding a New Table

Add a numbered SQL file to `clickhouse/schemas/`. Files are applied in lexicographic order, so prefix with the next available number:

```bash
# Example: create a new lookup table
clickhouse/schemas/004_zones.sql
```

Use `ON CLUSTER taxi_cluster` in DDL statements so the schema propagates automatically to all shards via ClickHouse Keeper:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi.zones ON CLUSTER taxi_cluster (
    location_id UInt16,
    zone        String,
    borough     String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nyc_taxi/zones', '{replica}')
ORDER BY location_id;
```

## Adding a New Data Source

Create a `load.sh` script under a new subdirectory of `clickhouse/data/`. The init script automatically discovers and runs every `load.sh` it finds there:

```
clickhouse/data/<source>/load.sh
```

The script is called with `bash`, so use standard shell. Model it after the existing `clickhouse/data/nyc_taxi/load.sh`, which includes 3-retry logic with exponential backoff and the `input_format_parquet_allow_missing_columns=1` setting for Parquet ingestion.

## Managing RBAC

Roles and users are defined in `clickhouse/schemas/001_rbac.sql`. The file ships with three roles:

| Role         | Permissions                     |
|--------------|---------------------------------|
| `reader`     | `SELECT` on `nyc_taxi.*`        |
| `loader`     | `SELECT`, `INSERT` on `nyc_taxi.*` |
| `admin_role` | `ALL` on `*.*`                  |

Predefined users: `reader_user` (password: `reader_pass`) and `loader_user` (password: `loader_pass`).

To add a new role or user, edit `001_rbac.sql` and re-run the init script. All statements use `IF NOT EXISTS` / `ON CLUSTER taxi_cluster`, so re-runs are safe.

## Verifying the Cluster

```bash
# Total row count via Distributed table
docker compose exec clickhouse1 clickhouse-client -q "SELECT count() FROM nyc_taxi.trips"

# Row counts per shard
docker compose exec clickhouse1 clickhouse-client -q \
  "SELECT hostName(), count() FROM nyc_taxi.trips_local"
docker compose exec clickhouse2 clickhouse-client -q \
  "SELECT hostName(), count() FROM nyc_taxi.trips_local"

# Cluster topology
docker compose exec clickhouse1 clickhouse-client -q \
  "SELECT * FROM system.clusters WHERE cluster = 'taxi_cluster'"

# Keeper quorum health
docker compose exec clickhouse1 clickhouse-client -q \
  "SELECT * FROM system.zookeeper WHERE path = '/'"

# Disk usage
docker compose exec clickhouse1 clickhouse-client -q \
  "SELECT formatReadableSize(sum(bytes_on_disk)), count() AS active_parts
   FROM system.parts WHERE database = 'nyc_taxi' AND table = 'trips_local' AND active"
```

## Key Gotchas

- **NULL passenger_count**: The column can be NULL in the source data. Always use `COALESCE(passenger_count, 0)` in queries and aggregations.
- **Parquet schema variance**: NYC taxi Parquet files differ across years. The loader uses `input_format_parquet_allow_missing_columns=1` to tolerate missing columns without failing.
- **ON CLUSTER DDL**: All DDL must include `ON CLUSTER taxi_cluster` so Keeper propagates it to both shards. Omitting this will create tables only on the node where the statement runs.
- **Schema numbering**: Schema files are applied in lexicographic order. Zero-pad the prefix (e.g. `004_`, not `4_`) to maintain correct ordering as the list grows.
