#!/bin/bash
set -uo pipefail

CH_CLIENT="clickhouse-client --host clickhouse1 --port 9000"
CSV_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

echo "--- Taxi Zone Lookup ---"

existing=$($CH_CLIENT --query "SELECT count() FROM nyc_taxi.taxi_zones_local" 2>/dev/null || echo "0")
if [ "$existing" -gt 0 ] 2>/dev/null; then
    echo "SKIP: ${existing} zones already loaded"
    exit 0
fi

echo -n "LOAD  taxi_zone_lookup.csv... "
$CH_CLIENT --query "
    INSERT INTO nyc_taxi.taxi_zones
    SELECT LocationID, Borough, Zone, service_zone
    FROM url('${CSV_URL}', 'CSVWithNames', 'LocationID Int32, Borough String, Zone String, service_zone String')
"

loaded=$($CH_CLIENT --query "SELECT count() FROM nyc_taxi.taxi_zones_local" 2>/dev/null || echo "?")
echo "done (${loaded} zones loaded)"
