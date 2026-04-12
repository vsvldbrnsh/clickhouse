INSERT INTO nyc_taxi.trips
SELECT * FROM url(
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ year }}-{{ month }}.parquet',
    'Parquet',
    'VendorID Int32, 
    tpep_pickup_datetime DateTime, 
    tpep_dropoff_datetime DateTime,
    passenger_count Nullable(Float64), 
    trip_distance Float64, 
    RatecodeID Nullable(Float64),
    store_and_fwd_flag Nullable(String), 
    PULocationID Int32, 
    DOLocationID Int32,
    payment_type Nullable(Float64), 
    fare_amount Float64, 
    extra Float64, 
    mta_tax Float64,
    tip_amount Float64, 
    tolls_amount Float64, 
    improvement_surcharge Float64,
    total_amount Float64, 
    congestion_surcharge Nullable(Float64), 
    airport_fee Nullable(Float64)'
)
SETTINGS input_format_parquet_allow_missing_columns = 1
