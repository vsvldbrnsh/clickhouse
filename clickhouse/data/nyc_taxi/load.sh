#!/bin/bash
set -uo pipefail

CH_CLIENT="clickhouse-client --host clickhouse1 --port 9000"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

COLUMNS="VendorID Int32, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count Nullable(Float64), trip_distance Float64, RatecodeID Nullable(Float64), store_and_fwd_flag Nullable(String), PULocationID Int32, DOLocationID Int32, payment_type Nullable(Float64), fare_amount Float64, extra Float64, mta_tax Float64, tip_amount Float64, tolls_amount Float64, improvement_surcharge Float64, total_amount Float64, congestion_surcharge Nullable(Float64), airport_fee Nullable(Float64)"

MAX_RETRIES=3
FAILED_MONTHS=""

load_month() {
    local year=$1
    local month=$2
    local partition="${year}$(printf '%02d' "$month")"
    local file="yellow_tripdata_${year}-$(printf '%02d' "$month").parquet"
    local label="${year}-$(printf '%02d' "$month")"

    existing=$($CH_CLIENT --query "SELECT count() FROM nyc_taxi.trips_local WHERE toYYYYMM(tpep_pickup_datetime) = ${partition}" 2>/dev/null || echo "0")

    if [ "$existing" -gt 10000 ] 2>/dev/null; then
        echo "SKIP  ${label}: ${existing} rows already loaded"
        return 0
    fi

    for attempt in $(seq 1 $MAX_RETRIES); do
        echo -n "LOAD  ${label} (attempt ${attempt}/${MAX_RETRIES})... "

        if $CH_CLIENT --input_format_parquet_allow_missing_columns=1 \
            --query "INSERT INTO nyc_taxi.trips SELECT * FROM url('${BASE_URL}/${file}', 'Parquet', '${COLUMNS}')" 2>&1; then

            loaded=$($CH_CLIENT --query "SELECT count() FROM nyc_taxi.trips_local WHERE toYYYYMM(tpep_pickup_datetime) = ${partition}" 2>/dev/null || echo "?")
            echo "done (${loaded} rows on this shard)"
            return 0
        fi

        echo "RETRY after error"
        sleep $((attempt * 10))
    done

    echo "FAIL  ${label}: all ${MAX_RETRIES} attempts failed"
    FAILED_MONTHS="${FAILED_MONTHS} ${label}"
    return 0
}

echo "--- NYC Taxi Data ---"
for year in 2019 2020 2021 2022 2023; do
    echo "--- ${year} ---"
    for month in $(seq 1 12); do
        load_month "$year" "$month"
    done
    echo ""
done

if [ -n "$FAILED_MONTHS" ]; then
    echo "WARNING: Failed months:${FAILED_MONTHS}"
    echo "Re-run to retry them."
fi
