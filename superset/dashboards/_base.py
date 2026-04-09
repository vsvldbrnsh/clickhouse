"""Shared helpers for Superset dashboard bootstrap via REST API."""

import json
import sys
import time

import requests

SUPERSET_URL = "http://localhost:8088"
CH_HOST = "clickhouse1"
CH_PORT = 8123
CH_DATABASE = "nyc_taxi"

ADMIN_USER = "admin"
ADMIN_PASS = "admin"

MAX_RETRIES = 30
RETRY_DELAY = 5


def wait_for_superset():
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(f"{SUPERSET_URL}/health")
            if r.status_code == 200:
                print("Superset is ready.")
                return
        except requests.ConnectionError:
            pass
        print(f"Waiting for Superset... ({i + 1}/{MAX_RETRIES})")
        time.sleep(RETRY_DELAY)
    print("Superset did not become ready in time.")
    sys.exit(1)


def get_session():
    s = requests.Session()

    login_resp = s.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
            "provider": "db",
            "refresh": True,
        },
    )
    login_resp.raise_for_status()
    token = login_resp.json()["access_token"]
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    )

    csrf_resp = s.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    if csrf_resp.status_code == 200:
        csrf = csrf_resp.json().get("result")
        if csrf:
            s.headers["X-CSRFToken"] = csrf
            s.cookies.set("csrf_token", csrf)

    return s


def create_database(s):
    existing = s.get(f"{SUPERSET_URL}/api/v1/database/")
    existing.raise_for_status()
    for db in existing.json().get("result", []):
        if db["database_name"] == "ClickHouse NYC Taxi":
            print(f"Database connection already exists (id={db['id']}).")
            return db["id"]

    resp = s.post(
        f"{SUPERSET_URL}/api/v1/database/",
        json={
            "database_name": "ClickHouse NYC Taxi",
            "engine": "clickhousedb",
            "sqlalchemy_uri": f"clickhousedb://default@{CH_HOST}:{CH_PORT}/{CH_DATABASE}",
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "extra": json.dumps(
                {
                    "engine_params": {"connect_args": {"connect_timeout": 60}},
                    "allows_virtual_table_explore": True,
                }
            ),
        },
    )
    resp.raise_for_status()
    db_id = resp.json()["id"]
    print(f"Created database connection (id={db_id}).")
    return db_id


def create_dataset(s, db_id, name, sql):
    existing = s.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": name}]})},
    )
    existing.raise_for_status()
    for ds in existing.json().get("result", []):
        if ds["table_name"] == name:
            print(f"  Dataset '{name}' already exists (id={ds['id']}).")
            return ds["id"]

    resp = s.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json={
            "database": db_id,
            "table_name": name,
            "sql": sql,
            "schema": CH_DATABASE,
        },
    )
    resp.raise_for_status()
    ds_id = resp.json()["id"]
    print(f"  Created dataset '{name}' (id={ds_id}).")
    return ds_id


def find_chart_by_name(s, name):
    page = 0
    page_size = 100
    while True:
        resp = s.get(
            f"{SUPERSET_URL}/api/v1/chart/",
            params={"q": json.dumps({"page": page, "page_size": page_size})},
        )
        resp.raise_for_status()
        results = resp.json().get("result", [])
        for c in results:
            if c["slice_name"] == name:
                return c["id"]
        if len(results) < page_size:
            return None
        page += 1


def create_chart(s, ds_id, name, viz_type, params, dashboard_ids=None):
    existing = find_chart_by_name(s, name)
    if existing:
        print(f"  Chart '{name}' already exists (id={existing}), deleting...")
        s.delete(f"{SUPERSET_URL}/api/v1/chart/{existing}")

    payload = {
        "datasource_id": ds_id,
        "datasource_type": "table",
        "slice_name": name,
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    if dashboard_ids:
        payload["dashboards"] = dashboard_ids

    resp = s.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    print(f"  Created chart '{name}' (id={chart_id}).")
    return chart_id


def find_dashboard_by_title(s, title):
    page = 0
    page_size = 100
    while True:
        resp = s.get(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            params={"q": json.dumps({"page": page, "page_size": page_size})},
        )
        resp.raise_for_status()
        results = resp.json().get("result", [])
        for d in results:
            if d["dashboard_title"] == title:
                return d["id"]
        if len(results) < page_size:
            return None
        page += 1


def create_empty_dashboard(s, title):
    existing = find_dashboard_by_title(s, title)
    if existing:
        print(f"  Dashboard '{title}' already exists (id={existing}), deleting...")
        s.delete(f"{SUPERSET_URL}/api/v1/dashboard/{existing}")

    resp = s.post(
        f"{SUPERSET_URL}/api/v1/dashboard/",
        json={"dashboard_title": title, "published": True},
    )
    resp.raise_for_status()
    dash_id = resp.json()["id"]
    print(f"  Created dashboard '{title}' (id={dash_id}).")
    return dash_id


def set_dashboard_layout(s, dash_id, title, chart_ids_and_names):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": title},
        },
    }

    for cid, cname in chart_ids_and_names:
        row_id = f"ROW-r{cid}"
        chart_comp_id = f"CHART-c{cid}"

        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_comp_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        position[chart_comp_id] = {
            "type": "CHART",
            "id": chart_comp_id,
            "children": [],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": cid,
                "sliceName": cname,
            },
        }

    resp = s.put(
        f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}",
        json={"position_json": json.dumps(position)},
    )
    if not resp.ok:
        print(f"  WARNING: PUT position_json failed ({resp.status_code}): {resp.text}")
    else:
        print(f"  Layout set for dashboard id={dash_id}.")
