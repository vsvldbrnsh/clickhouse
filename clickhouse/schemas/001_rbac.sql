-- Roles
CREATE ROLE IF NOT EXISTS reader ON CLUSTER taxi_cluster;
CREATE ROLE IF NOT EXISTS loader ON CLUSTER taxi_cluster;
CREATE ROLE IF NOT EXISTS admin_role ON CLUSTER taxi_cluster;

-- Reader: SELECT only
GRANT SELECT ON nyc_taxi.* TO reader;

-- Loader: INSERT + SELECT
GRANT SELECT, INSERT ON nyc_taxi.* TO loader;

-- Admin: full access
GRANT ALL ON *.* TO admin_role;

-- Users
CREATE USER IF NOT EXISTS reader_user ON CLUSTER taxi_cluster IDENTIFIED BY 'reader_pass' DEFAULT ROLE reader;
CREATE USER IF NOT EXISTS loader_user ON CLUSTER taxi_cluster IDENTIFIED BY 'loader_pass' DEFAULT ROLE loader;
