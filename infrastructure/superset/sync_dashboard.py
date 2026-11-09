"""Publish immutable Superset sales assets from one validated Iceberg publication."""

import json
import os
from uuid import NAMESPACE_URL, uuid5

from sales import COLUMNS, METRICS, Publication, chart_specs
from dashboard import chart_payload, layout


def trino_connection():
    from trino.dbapi import connect

    return connect(
        host=os.environ.get("SUPERSET_TRINO_HOST", "trino"),
        port=int(os.environ.get("SUPERSET_TRINO_PORT", "8080")),
        user=os.environ.get("SUPERSET_TRINO_USER", "superset_reader"),
        catalog="lakehouse",
        schema="gold",
        timezone="UTC",
        request_timeout=60,
    )


def read_publication():
    connection = trino_connection()
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT run_id, published_at, snapshots_json, schema_version, source_mode
            FROM lakehouse.system.analytics_publications ORDER BY published_at DESC, run_id DESC LIMIT 1""")
        rows = cursor.fetchall()
        if not rows:
            raise ValueError("No completed OLTP publication; run the batch pipeline first")
        publication = Publication.parse(dict(zip([column[0] for column in cursor.description], rows[0])))
        for name in COLUMNS:
            cursor.execute("SELECT * FROM (" + publication.dataset_sql(name) + ") published LIMIT 1")
            cursor.fetchall()
        return publication
    finally:
        connection.close()


def asset_uuid(fingerprint, key):
    return uuid5(NAMESPACE_URL, f"tlcn/retail-sales/{fingerprint}/{key}")


def sync():
    publication = read_publication()  # Fail before changing any metadata when snapshots cannot be read.
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
        identifier = asset_uuid(publication.fingerprint, "dashboard")
        existing = db.session.query(Dashboard).filter_by(uuid=identifier).one_or_none()
        if existing:
            return {
                "reused": True,
                "dashboard_id": existing.id,
                "url": f"/superset/dashboard/{existing.slug}/",
                "fingerprint": publication.fingerprint,
            }
        try:
            database = db.session.query(Database).filter_by(database_name="Retail Lakehouse (read only)").one_or_none()
            if database is None:
                database = Database(
                    database_name="Retail Lakehouse (read only)",
                    sqlalchemy_uri=URL.create(
                        "trino",
                        username=os.environ.get("SUPERSET_TRINO_USER", "superset_reader"),
                        host=os.environ.get("SUPERSET_TRINO_HOST", "trino"),
                        port=int(os.environ.get("SUPERSET_TRINO_PORT", "8080")),
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
                db.session.flush()
            datasets = {}
            for name, columns in COLUMNS.items():
                table = SqlaTable(
                    table_name=f"retail_{name}_{publication.fingerprint}",
                    database=database,
                    schema="gold",
                    catalog="lakehouse",
                    sql=publication.dataset_sql(name),
                    main_dttm_col="order_date",
                    owners=[owner],
                    uuid=asset_uuid(publication.fingerprint, name),
                )
                table.columns = [
                    TableColumn(
                        column_name=column, type=kind, is_dttm=column == "order_date", groupby=True, filterable=True
                    )
                    for column, kind in {
                        **columns,
                        "publication_run_id": "VARCHAR",
                        "publication_at_utc": "VARCHAR",
                    }.items()
                ]
                table.metrics = [
                    SqlMetric(metric_name=metric, verbose_name=label, expression=expression, d3format=fmt)
                    for metric, (label, expression, fmt) in METRICS.items()
                    if metric in columns
                ]
                # Native filter plugins request this conventional helper metric.
                # It counts mart rows only; business order KPIs use total_orders.
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
                    uuid=asset_uuid(publication.fingerprint, spec["key"]),
                )
                db.session.add(chart)
                db.session.flush()
                charts.append(chart)
            metadata = {
                "native_filter_configuration": [
                    {
                        "id": "NATIVE_FILTER-order-date",
                        "name": "Ngày đặt hàng",
                        "filterType": "filter_time",
                        "targets": [
                            {"datasetId": table.id, "column": {"name": "order_date"}} for table in datasets.values()
                        ],
                        "defaultDataMask": {
                            "extraFormData": {"time_range": "No filter"},
                            "filterState": {"value": "No filter"},
                        },
                        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                        "controlValues": {},
                    }
                ],
                "publication": {
                    "run_id": publication.run_id,
                    "published_at_utc": publication.published_at,
                    "fingerprint": publication.fingerprint,
                },
                "refresh_frequency": 0,
                "cross_filters_enabled": False,
            }
            dashboard = Dashboard(
                dashboard_title=f"Doanh thu bán lẻ · {publication.published_at}",
                slug="retail-sales-" + publication.fingerprint,
                published=True,
                owners=[owner],
                slices=charts,
                position_json=json.dumps(layout(charts, publication)),
                json_metadata=json.dumps(metadata),
                description=f"Publication: {publication.run_id} | Published (UTC): {publication.published_at}. VND; ngày đặt hàng. COD chưa thu không được tính thành doanh thu ghi nhận.",
                uuid=identifier,
            )
            db.session.add(dashboard)
            db.session.commit()
            return {
                "reused": False,
                "dashboard_id": dashboard.id,
                "url": f"/superset/dashboard/{dashboard.slug}/",
                "fingerprint": publication.fingerprint,
                "run_id": publication.run_id,
                "datasets": 2,
                "charts": len(charts),
            }
        except BaseException:
            db.session.rollback()
            raise


if __name__ == "__main__":
    print("SUPERSET_RESULT " + json.dumps(sync(), ensure_ascii=False), flush=True)
