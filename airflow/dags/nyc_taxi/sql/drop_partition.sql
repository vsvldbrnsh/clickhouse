ALTER TABLE nyc_taxi.trips_local ON CLUSTER taxi_cluster
DROP PARTITION '{{ partition }}'
SETTINGS distributed_ddl_task_timeout = -1
