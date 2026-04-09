"""Geographic Insights dashboard."""

from ._base import create_chart, create_dataset, create_empty_dashboard, set_dashboard_layout

TITLE = "NYC Taxi - Geographic Insights"

DATASETS = {
    "top_pickup_locations": """
        SELECT
            PULocationID,
            count() AS trip_count,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(trip_distance), 2) AS avg_distance,
            round(sum(total_amount), 2) AS total_revenue
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY PULocationID
        ORDER BY trip_count DESC
        LIMIT 30
    """,
    "top_dropoff_locations": """
        SELECT
            DOLocationID,
            count() AS trip_count,
            round(avg(total_amount), 2) AS avg_fare,
            round(sum(total_amount), 2) AS total_revenue
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY DOLocationID
        ORDER BY trip_count DESC
        LIMIT 30
    """,
    "top_routes": """
        SELECT
            PULocationID,
            DOLocationID,
            count() AS trip_count,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(trip_distance), 2) AS avg_distance
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY PULocationID, DOLocationID
        ORDER BY trip_count DESC
        LIMIT 25
    """,
}

CHARTS = [
    {
        "dataset": "top_pickup_locations",
        "name": "Top 30 Pickup Locations by Trip Count",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "PULocationID",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "row_limit": 30,
            "show_legend": False,
            "order_desc": True,
        },
    },
    {
        "dataset": "top_dropoff_locations",
        "name": "Top 30 Dropoff Locations by Trip Count",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "DOLocationID",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "row_limit": 30,
            "show_legend": False,
            "order_desc": True,
        },
    },
    {
        "dataset": "top_routes",
        "name": "Top 25 Busiest Routes",
        "viz_type": "table",
        "params": {
            "all_columns": ["PULocationID", "DOLocationID", "trip_count", "avg_fare", "avg_distance"],
            "row_limit": 25,
            "order_by_cols": ["trip_count"],
            "order_desc": True,
        },
    },
    {
        "dataset": "top_pickup_locations",
        "name": "Revenue by Pickup Location (Top 30)",
        "viz_type": "pie",
        "params": {
            "groupby": ["PULocationID"],
            "metrics": [{"label": "total_revenue", "expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "MAX"}],
            "row_limit": 15,
            "show_legend": True,
            "donut": True,
            "show_labels": True,
        },
    },
]


def create(s, db_id):
    dataset_ids = {}
    for name, sql in DATASETS.items():
        dataset_ids[name] = create_dataset(s, db_id, name, sql)

    dash_id = create_empty_dashboard(s, TITLE)

    chart_ids = []
    for chart_def in CHARTS:
        ds_id = dataset_ids[chart_def["dataset"]]
        cid = create_chart(s, ds_id, chart_def["name"], chart_def["viz_type"], chart_def["params"], dashboard_ids=[dash_id])
        chart_ids.append((cid, chart_def["name"]))

    set_dashboard_layout(s, dash_id, TITLE, chart_ids)
    return dash_id
