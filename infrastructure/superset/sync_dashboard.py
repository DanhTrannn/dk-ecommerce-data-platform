"""Create the Superset sales assets over the current Gold marts, once per asset definition."""

import json
import os
from uuid import NAMESPACE_URL, uuid5

from sales import COLUMNS, DATE_COLUMNS, METRICS, chart_specs, dataset_sql, fingerprint
from dashboard import chart_payload, layout


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
        http_scheme="https",
        verify=os.environ["TRINO_CA_FILE"],
        auth=BasicAuthentication(
            os.environ.get("SUPERSET_TRINO_USER", "analytics_reader"), os.environ["TRINO_READER_PASSWORD"]
        ),
        request_timeout=60,
    )


def check_datasets():
    connection = trino_connection()
    try:
        cursor = connection.cursor()
        for name in COLUMNS:
            cursor.execute("SELECT * FROM (" + dataset_sql(name) + ") gold LIMIT 1")
            cursor.fetchall()
    finally:
        connection.close()


def asset_uuid(fingerprint, key):
    return uuid5(NAMESPACE_URL, f"tlcn/retail-sales/{fingerprint}/{key}")


def sync():
    check_datasets()  # Fail before changing any metadata when a Gold mart cannot be read.
    version = fingerprint()
    from flask import g
    from sqlalchemy.engine import URL
    from superset.app import create_app
    from superset import db, security_manager

    app = create_app()
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    with app.app_context():
        owner = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USER", "admin"))
        if owner is None:
            raise ValueError("Initialize the Superset admin before synchronizing assets")
        g.user = owner
        identifier = asset_uuid(version, "dashboard")
        existing = db.session.query(Dashboard).filter_by(uuid=identifier).one_or_none()
        if existing:
            return {
                "reused": True,
                "dashboard_id": existing.id,
                "url": f"/superset/dashboard/{existing.slug}/",
                "fingerprint": version,
            }
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
                        query={"http_scheme": "https"},
                    ).render_as_string(hide_password=False),
                    extra=json.dumps({"engine_params": {"connect_args": {"verify": os.environ["TRINO_CA_FILE"]}}}),
                    expose_in_sqllab=False,
                    allow_dml=False,
                    allow_ctas=False,
                    allow_cvas=False,
                    allow_file_upload=False,
                    allow_run_async=False,
                )
                db.session.add(database)
                db.session.flush()
            datasets = {}
            for name, columns in COLUMNS.items():
                table = SqlaTable(
                    table_name=f"retail_{name}_{version}",
                    database=database,
                    schema="gold",
                    catalog="lakehouse",
                    sql=dataset_sql(name),
                    main_dttm_col=DATE_COLUMNS[name],
                    owners=[owner],
                    uuid=asset_uuid(version, name),
                )
                table.columns = [
                    TableColumn(
                        column_name=column, type=kind, is_dttm=column == DATE_COLUMNS[name], groupby=True, filterable=True
                    )
                    for column, kind in columns.items()
                ]
                table.metrics = [
                    SqlMetric(metric_name=metric, verbose_name=label, expression=expression, d3format=fmt)
                    for metric, (label, expression, fmt) in METRICS.items()
                    if metric in columns
                ]
                # Native filter plugins request this conventional helper metric.
                # It counts mart rows only; business KPIs use the mart metrics.
                table.metrics.append(
                    SqlMetric(
                        metric_name="count",
                        verbose_name="Mart rows (filter support)",
                        expression="COUNT(*)",
                        d3format=",.0f",
                    )
                )
                db.session.add(table)
                db.session.flush()
                datasets[name] = table
            charts = []
            for spec in chart_specs():
                dataset = datasets[spec["dataset"]]
                params, context = chart_payload(spec, dataset.id)
                chart = Slice(
                    slice_name=spec["title"],
                    datasource_id=dataset.id,
                    datasource_type="table",
                    datasource_name=dataset.table_name,
                    viz_type=spec["viz"],
                    params=json.dumps(params),
                    query_context=json.dumps(context),
                    owners=[owner],
                    uuid=asset_uuid(version, spec["key"]),
                )
                db.session.add(chart)
                db.session.flush()
                charts.append(chart)
            metadata = {
                "native_filter_configuration": [
                    {
                        "id": "NATIVE_FILTER-date",
                        "name": "Ngày (đặt hàng / ghi nhận doanh thu)",
                        "filterType": "filter_time",
                        "targets": [
                            {"datasetId": table.id, "column": {"name": DATE_COLUMNS[name]}}
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
            }
            dashboard = Dashboard(
                dashboard_title="Doanh thu bán lẻ",
                slug="retail-sales-" + version,
                published=True,
                owners=[owner],
                slices=charts,
                position_json=json.dumps(layout(charts)),
                json_metadata=json.dumps(metadata),
                description="Đọc bảng Gold hiện tại. VND. Đơn đặt theo ngày đặt; doanh thu theo ngày đơn hoàn tất và đã thanh toán, hoàn tiền trừ vào ngày hoàn.",
                uuid=identifier,
            )
            db.session.add(dashboard)
            db.session.commit()
            return {
                "reused": False,
                "dashboard_id": dashboard.id,
                "url": f"/superset/dashboard/{dashboard.slug}/",
                "fingerprint": version,
                "datasets": len(datasets),
                "charts": len(charts),
            }
        except BaseException:
            db.session.rollback()
            raise


if __name__ == "__main__":
    print("SUPERSET_RESULT " + json.dumps(sync(), ensure_ascii=False), flush=True)
