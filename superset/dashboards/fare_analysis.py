"""Fare Analysis dashboard."""

from ._base import create_chart, create_dataset, create_empty_dashboard, set_dashboard_layout

TITLE = "NYC Taxi - Fare Analysis"

DATASETS = {
    "fare_distribution": """
        SELECT
            multiIf(
                total_amount < 5, '0-5',
                total_amount < 10, '5-10',
                total_amount < 15, '10-15',
                total_amount < 20, '15-20',
                total_amount < 30, '20-30',
                total_amount < 50, '30-50',
                total_amount < 100, '50-100',
                '100+'
            ) AS fare_bucket,
            multiIf(
                total_amount < 5, 1,
                total_amount < 10, 2,
                total_amount < 15, 3,
                total_amount < 20, 4,
                total_amount < 30, 5,
                total_amount < 50, 6,
                total_amount < 100, 7,
                8
            ) AS bucket_order,
            count() AS trip_count,
            round(avg(tip_amount / nullIf(fare_amount, 0)) * 100, 1) AS avg_tip_pct
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
          AND total_amount > 0
          AND total_amount < 500
        GROUP BY fare_bucket, bucket_order
        ORDER BY bucket_order
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
    "monthly_trips": """
        SELECT
            toStartOfMonth(tpep_pickup_datetime) AS month,
            count() AS trip_count,
            round(sum(total_amount), 2) AS total_revenue,
            round(avg(total_amount), 2) AS avg_fare,
            round(avg(trip_distance), 2) AS avg_distance
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY month
        ORDER BY month
    """,
}

CHARTS = [
    {
        "dataset": "fare_distribution",
        "name": "Fare Amount Distribution",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "fare_bucket",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "row_limit": 20,
            "show_legend": False,
        },
    },
    {
        "dataset": "fare_distribution",
        "name": "Average Tip % by Fare Bucket",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "fare_bucket",
            "metrics": [{"label": "avg_tip_pct", "expressionType": "SIMPLE", "column": {"column_name": "avg_tip_pct"}, "aggregate": "MAX"}],
            "row_limit": 20,
            "show_legend": False,
            "y_axis_format": ".1f",
        },
    },
    {
        "dataset": "hourly_patterns",
        "name": "Average Fare by Hour of Day",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "pickup_hour",
            "metrics": [{"label": "avg_fare", "expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "AVG"}],
            "row_limit": 24,
            "show_legend": False,
        },
    },
    {
        "dataset": "monthly_trips",
        "name": "Average Fare Over Time",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "month",
            "metrics": [{"label": "avg_fare", "expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "MAX"}],
            "row_limit": 100,
            "show_legend": True,
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
