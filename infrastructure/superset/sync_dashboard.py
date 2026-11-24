"""Synchronize the Superset dashboards backed by the current Gold marts."""

import json
import os
from uuid import NAMESPACE_URL, uuid5

from retail import DASHBOARDS, DATASETS
from sales import fingerprint as original_sales_fingerprint
from dashboard import chart_payload, layout


def trino_tls_options():
    return {"http_scheme": "https", "verify": os.environ["TRINO_CA_FILE"]}


def configure_trino_tls(database):
    extra = json.loads(database.extra or "{}")
    connect_args = extra.setdefault("engine_params", {}).setdefault("connect_args", {})
    connect_args.update(trino_tls_options())
    database.extra = json.dumps(extra)


def trino_connection():
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication

    return connect(
        host=os.environ.get("SUPERSET_TRINO_HOST", "trino"),
        port=int(os.environ.get("SUPERSET_TRINO_PORT", "8443")),
        user=os.environ.get("SUPERSET_TRINO_USER", "analytics_reader"),
        catalog="lakehouse",
        schema="gold",
        timezone="UTC",
        auth=BasicAuthentication(
            os.environ.get("SUPERSET_TRINO_USER", "analytics_reader"), os.environ["TRINO_READER_PASSWORD"]
        ),
        request_timeout=60,
        **trino_tls_options(),
    )


def check_datasets():
    connection = trino_connection()
    try:
        cursor = connection.cursor()
        for dataset in DATASETS.values():
            cursor.execute("SELECT * FROM (" + dataset.sql + ") gold LIMIT 1")
            cursor.fetchall()
    finally:
        connection.close()


def asset_uuid(fingerprint, key):
    return uuid5(NAMESPACE_URL, f"tlcn/retail-sales/{fingerprint}/{key}")


def sync_dashboard(session, database, owner, key, spec):
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    version = spec.fingerprint()
    identifier = asset_uuid("dashboard", key)
    dashboard = session.query(Dashboard).filter_by(uuid=identifier).one_or_none()
    if dashboard is None and key == "sales":
        previous_id = asset_uuid(original_sales_fingerprint(), "dashboard")
        dashboard = session.query(Dashboard).filter_by(uuid=previous_id).one_or_none()
    if dashboard is not None and json.loads(dashboard.json_metadata or "{}").get("asset_fingerprint") == version:
        return dashboard_result(dashboard, version, reused=True)

    datasets = {}
    for name in dict.fromkeys(chart["dataset"] for chart in spec.charts):
        definition = DATASETS[name]
        table_id = asset_uuid(version, f"dataset-{name}")
        table = session.query(SqlaTable).filter_by(uuid=table_id).one_or_none()
        if table is not None:
            datasets[name] = table
            continue
        table = SqlaTable(
            table_name=f"retail_{name}_{version}",
            database=database,
            schema="gold",
            catalog="lakehouse",
            sql=definition.sql,
            main_dttm_col=definition.date,
            owners=[owner],
            uuid=table_id,
        )
        table.columns = [
            TableColumn(
                column_name=column, type=kind, is_dttm=column == definition.date, groupby=True, filterable=True
            )
            for column, kind in definition.columns.items()
        ]
        table.metrics = [
            SqlMetric(metric_name=metric, verbose_name=label, expression=expression, d3format=fmt)
            for metric, (label, expression, fmt) in definition.metrics.items()
        ]
        table.metrics.append(
            SqlMetric(metric_name="count", verbose_name="Số dòng mart", expression="COUNT(*)", d3format=",.0f")
        )
        session.add(table)
        datasets[name] = table
    session.flush()

    charts = []
    for definition in spec.charts:
        chart_id = asset_uuid(version, f"chart-{definition['key']}")
        chart = session.query(Slice).filter_by(uuid=chart_id).one_or_none()
        if chart is not None:
            charts.append(chart)
            continue
        dataset = datasets[definition["dataset"]]
        params, context = chart_payload(definition, dataset.id)
        chart = Slice(
            slice_name=definition["title"],
            datasource_id=dataset.id,
            datasource_type="table",
            datasource_name=dataset.table_name,
            viz_type=definition["viz"],
            params=json.dumps(params),
            query_context=json.dumps(context),
            owners=[owner],
            uuid=chart_id,
        )
        session.add(chart)
        charts.append(chart)
    session.flush()
    if dashboard is None:
        dashboard = Dashboard(slug=f"retail-{key}", owners=[owner])
        session.add(dashboard)
    dashboard.uuid = identifier
    dashboard.dashboard_title = spec.title
    dashboard.published = True
    dashboard.slices = charts
    dashboard.description = spec.description
    dashboard.position_json = json.dumps(layout(charts, title=spec.title, description=spec.description, rows=spec.rows))
    dashboard.json_metadata = json.dumps({
        "native_filter_configuration": [
            {
                "id": "NATIVE_FILTER-date",
                "name": "Khoảng ngày",
                "filterType": "filter_time",
                "targets": [
                    {"datasetId": table.id, "column": {"name": DATASETS[name].date}}
                    for name, table in datasets.items()
                ],
                "defaultDataMask": {
                    "extraFormData": {"time_range": "No filter"},
                    "filterState": {"value": "No filter"},
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "controlValues": {},
            }
        ],
        "asset_fingerprint": version,
        "refresh_frequency": 0,
        "cross_filters_enabled": False,
    })
    session.flush()
    return dashboard_result(dashboard, version, reused=False)


def dashboard_result(dashboard, version, *, reused):
    return {
        "title": dashboard.dashboard_title,
        "dashboard_id": dashboard.id,
        "url": f"/superset/dashboard/{dashboard.slug}/",
        "fingerprint": version,
        "charts": len(dashboard.slices),
        "reused": reused,
    }


def sync():
    check_datasets()
    from flask import g
    from sqlalchemy.engine import URL
    from superset.app import create_app
    from superset import db, security_manager

    app = create_app()
    from superset.models.core import Database

    with app.app_context():
        owner = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USER", "admin"))
        if owner is None:
            raise ValueError("Initialize the Superset admin before synchronizing assets")
        g.user = owner
        try:
            database = db.session.query(Database).filter_by(database_name="Retail Lakehouse (read only)").one_or_none()
            if database is None:
                database = Database(
                    database_name="Retail Lakehouse (read only)",
                    sqlalchemy_uri=URL.create(
                        "trino",
                        username=os.environ.get("SUPERSET_TRINO_USER", "analytics_reader"),
                        host=os.environ.get("SUPERSET_TRINO_HOST", "trino"),
                        port=int(os.environ.get("SUPERSET_TRINO_PORT", "8443")),
                        password=os.environ["TRINO_READER_PASSWORD"],
                        database="lakehouse/gold",
                    ).render_as_string(hide_password=False),
                    expose_in_sqllab=False,
                    allow_dml=False,
                    allow_ctas=False,
                    allow_cvas=False,
                    allow_file_upload=False,
                    allow_run_async=False,
                )
                db.session.add(database)
            configure_trino_tls(database)
            db.session.flush()
            dashboards = {
                key: sync_dashboard(db.session, database, owner, key, spec)
                for key, spec in DASHBOARDS.items()
            }
            db.session.commit()
            return {**dashboards["sales"], "dashboards": dashboards}
        except BaseException:
            db.session.rollback()
            raise


if __name__ == "__main__":
    print("SUPERSET_RESULT " + json.dumps(sync(), ensure_ascii=False), flush=True)
