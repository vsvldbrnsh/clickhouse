CREATE TABLE IF NOT EXISTS nyc_taxi.trips_local ON CLUSTER taxi_cluster
(
    VendorID                Int32,
    tpep_pickup_datetime    DateTime,
    tpep_dropoff_datetime   DateTime,
    passenger_count         Nullable(Float64),
    trip_distance           Float64,
    RatecodeID              Nullable(Float64),
    store_and_fwd_flag      Nullable(String),
    PULocationID            Int32,
    DOLocationID            Int32,
    payment_type            Nullable(Float64),
    fare_amount             Float64,
    extra                   Float64,
    mta_tax                 Float64,
    tip_amount              Float64,
    tolls_amount            Float64,
    improvement_surcharge   Float64,
    total_amount            Float64,
    congestion_surcharge    Nullable(Float64),
    airport_fee             Nullable(Float64)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nyc_taxi/trips', '{replica}')
PARTITION BY toYYYYMM(tpep_pickup_datetime)
ORDER BY (PULocationID, tpep_pickup_datetime)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS nyc_taxi.trips ON CLUSTER taxi_cluster
AS nyc_taxi.trips_local
ENGINE = Distributed(taxi_cluster, nyc_taxi, trips_local, rand());
