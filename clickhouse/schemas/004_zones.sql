CREATE TABLE IF NOT EXISTS nyc_taxi.taxi_zones_local ON CLUSTER taxi_cluster
(
    LocationID   Int32,
    Borough      String,
    Zone         String,
    service_zone String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nyc_taxi/taxi_zones', '{replica}')
ORDER BY LocationID;

CREATE TABLE IF NOT EXISTS nyc_taxi.taxi_zones ON CLUSTER taxi_cluster
AS nyc_taxi.taxi_zones_local
ENGINE = Distributed(taxi_cluster, nyc_taxi, taxi_zones_local, rand());
