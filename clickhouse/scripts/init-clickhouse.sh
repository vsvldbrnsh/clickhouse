#!/bin/bash
set -uo pipefail

CH_CLIENT="clickhouse-client --host clickhouse1 --port 9000"

echo "=== ClickHouse Init ==="
echo ""

# Step 1: Run all schema files in order
echo "Applying schemas..."
for sql_file in /clickhouse/schemas/*.sql; do
    echo "  Running $(basename "$sql_file")..."
    $CH_CLIENT --multiquery < "$sql_file"
done
echo "Schemas applied."
echo ""

# Step 2: Verification
echo "=== Verification ==="

$CH_CLIENT --query "SELECT count() AS total_rows, min(tpep_pickup_datetime) AS earliest_trip, max(tpep_pickup_datetime) AS latest_trip FROM nyc_taxi.trips"

$CH_CLIENT --query "SELECT hostName() AS host, count() AS rows_on_shard FROM clusterAllReplicas(taxi_cluster, nyc_taxi.trips_local) GROUP BY host ORDER BY host"

$CH_CLIENT --query "SELECT formatReadableSize(sum(bytes_on_disk)) AS total_disk_size, count() AS active_parts FROM system.parts WHERE database = 'nyc_taxi' AND table = 'trips_local' AND active"

echo ""
echo "=== Init complete ==="
