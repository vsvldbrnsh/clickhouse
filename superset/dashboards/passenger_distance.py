"""Passenger & Distance Patterns dashboard."""

from ._base import create_chart, create_dataset, create_empty_dashboard, set_dashboard_layout

TITLE = "NYC Taxi - Passenger & Distance Patterns"

DATASETS = {
    "passenger_analysis": """
        SELECT
            toInt32(coalesce(passenger_count, 0)) AS passengers,
            count() AS trip_count,
            round(avg(trip_distance), 2) AS avg_distance,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(tip_amount), 2) AS avg_tip
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
          AND (passenger_count IS NULL OR (passenger_count >= 0 AND passenger_count <= 9))
        GROUP BY passengers
        ORDER BY passengers
    """,
    "distance_vs_fare": """
        SELECT
            round(trip_distance) AS distance_miles,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(tip_amount), 2) AS avg_tip,
            count() AS trip_count
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
          AND trip_distance > 0
          AND trip_distance < 50
          AND total_amount > 0
          AND total_amount < 500
        GROUP BY distance_miles
        ORDER BY distance_miles
    """,
    "hourly_patterns": """
        SELECT
            toHour(tpep_pickup_datetime) AS pickup_hour,
            toDayOfWeek(tpep_pickup_datetime) AS day_of_week,
            count() AS trip_count,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(tip_amount), 2) AS avg_tip
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY pickup_hour, day_of_week
        ORDER BY day_of_week, pickup_hour
    """,
}

CHARTS = [
    {
        "dataset": "passenger_analysis",
        "name": "Trip Count by Passenger Count",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "passengers",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "row_limit": 10,
            "show_legend": False,
        },
    },
    {
        "dataset": "distance_vs_fare",
        "name": "Average Fare vs Trip Distance",
        "viz_type": "echarts_timeseries_scatter",
        "params": {
            "x_axis": "distance_miles",
            "metrics": [{"label": "avg_fare", "expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "MAX"}],
            "row_limit": 50,
            "show_legend": False,
        },
    },
    {
        "dataset": "hourly_patterns",
        "name": "Trip Volume Heatmap (Hour vs Day of Week)",
        "viz_type": "heatmap",
        "params": {
            "all_columns_x": "pickup_hour",
            "all_columns_y": "day_of_week",
            "metric": {"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"},
            "row_limit": 200,
            "show_legend": True,
            "linear_color_scheme": "blue_white_yellow",
        },
    },
    {
        "dataset": "passenger_analysis",
        "name": "Avg Distance & Fare by Passenger Count",
        "viz_type": "table",
        "params": {
            "all_columns": ["passengers", "trip_count", "avg_distance", "avg_fare", "avg_tip"],
            "row_limit": 10,
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
