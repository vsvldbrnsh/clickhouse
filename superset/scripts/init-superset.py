"""Bootstrap Superset with ClickHouse connection and NYC taxi dashboards."""

import sys
import os

sys.path.insert(0, "/app")

from dashboards._base import wait_for_superset, get_session, create_database
from dashboards import trip_volume, fare_analysis, geographic, passenger_distance


def main():
    wait_for_superset()
    s = get_session()
    db_id = create_database(s)

    print("\n--- Creating dashboards ---")
    trip_volume.create(s, db_id)
    fare_analysis.create(s, db_id)
    geographic.create(s, db_id)
    passenger_distance.create(s, db_id)

    print("\n=== All dashboards created successfully! ===")
    print("Open Superset at http://localhost:8088 (login: admin/admin)")


if __name__ == "__main__":
    main()
