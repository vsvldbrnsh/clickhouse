CREATE TABLE IF NOT EXISTS nyc_taxi.trips ON CLUSTER taxi_cluster
AS nyc_taxi.trips_local
ENGINE = Distributed(taxi_cluster, nyc_taxi, trips_local, rand());
