"""Trip Volume Overview dashboard."""

from ._base import create_chart, create_dataset, create_empty_dashboard, set_dashboard_layout

TITLE = "NYC Taxi - Trip Volume Overview"

DATASETS = {
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
    "daily_revenue": """
        SELECT
            toDate(tpep_pickup_datetime) AS trip_date,
            count() AS trip_count,
            round(sum(total_amount), 2) AS total_revenue,
            round(sum(tip_amount), 2) AS total_tips
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY trip_date
        ORDER BY trip_date
    """,
    "payment_type_monthly": """
        SELECT
            toStartOfMonth(tpep_pickup_datetime) AS month,
            multiIf(
                payment_type = 1, 'Credit Card',
                payment_type = 2, 'Cash',
                payment_type = 3, 'No Charge',
                payment_type = 4, 'Dispute',
                'Other'
            ) AS payment_method,
            count() AS trip_count,
            round(sum(total_amount), 2) AS total_revenue
        FROM nyc_taxi.trips
        WHERE tpep_pickup_datetime >= '2019-01-01'
          AND tpep_pickup_datetime < '2024-01-01'
        GROUP BY month, payment_method
        ORDER BY month, payment_method
    """,
}

CHARTS = [
    {
        "dataset": "monthly_trips",
        "name": "Monthly Trip Count (2019-2023)",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "month",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "row_limit": 100,
            "time_grain_sqla": "P1M",
            "show_legend": True,
            "rich_tooltip": True,
        },
    },
    {
        "dataset": "monthly_trips",
        "name": "Monthly Revenue Trend",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "month",
            "metrics": [{"label": "total_revenue", "expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "MAX"}],
            "row_limit": 100,
            "show_legend": True,
        },
    },
    {
        "dataset": "daily_revenue",
        "name": "Daily Trip Count & Revenue",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "x_axis": "trip_date",
            "metrics": [
                {"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"},
            ],
            "row_limit": 2000,
            "show_legend": True,
            "rich_tooltip": True,
        },
    },
    {
        "dataset": "payment_type_monthly",
        "name": "Payment Methods Over Time",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "month",
            "metrics": [{"label": "trip_count", "expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "MAX"}],
            "groupby": ["payment_method"],
            "row_limit": 500,
            "show_legend": True,
            "stack": True,
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
