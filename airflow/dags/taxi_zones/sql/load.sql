TRUNCATE TABLE nyc_taxi.taxi_zones_local ON CLUSTER taxi_cluster;

INSERT INTO nyc_taxi.taxi_zones
SELECT LocationID, Borough, Zone, service_zone
FROM url(
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
    'CSVWithNames',
    'LocationID Int32, Borough String, Zone String, service_zone String'
)
