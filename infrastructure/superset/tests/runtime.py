"""Run real Superset queries, HTTP access checks and persistence assertions inside BI."""

import json
import sys

from flask import g
from superset.app import create_app
from superset import db, security_manager

app = create_app()
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.connectors.sqla.models import SqlaTable
from superset.commands.chart.data.get_data_command import ChartDataCommand

with app.app_context():
    owner = security_manager.find_user(username="admin")
    g.user = owner
    mode = sys.argv[1]
    if mode == "query":
        dashboards = db.session.query(Dashboard).order_by(Dashboard.id).all()
        output = []
        for dashboard in dashboards:
            rows = []
            for chart in sorted(dashboard.slices, key=lambda x: x.id):
                context = chart.get_query_context()
                payload = ChartDataCommand(context).run()
                queries = payload["queries"]
                assert all(not query.get("error") for query in queries), queries
                rows.append(
                    dict(
                        id=chart.id,
                        name=chart.slice_name,
                        data=[q.get("data") for q in queries],
                        queries=[q.get("query") for q in queries],
                    )
                )
            output.append(
                dict(id=dashboard.id, slug=dashboard.slug, metadata=json.loads(dashboard.json_metadata), charts=rows)
            )
        result = output
    elif mode == "empty-filter":
        dashboard = db.session.query(Dashboard).order_by(Dashboard.id).first()
        rows = []
        for chart in dashboard.slices:
            context = json.loads(chart.query_context)
            context["queries"][0]["time_range"] = "1900-01-01 : 1900-01-02"
            context["force"] = True
            payload = ChartDataCommand(chart.get_query_context_factory().create(**context, current_slice=chart)).run()
            for query in payload["queries"]:
                data = query.get("data") or []
                assert not data or all(all(v is None for v in row.values()) for row in data), data
            rows.append(chart.id)
        result = dict(empty_range_charts=len(rows))
    elif mode == "deny-write":
        from sync_dashboard import trino_connection

        connection = trino_connection()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute("DELETE FROM lakehouse.gold.mart_order_daily WHERE false")
                cursor.fetchall()
            except Exception as error:
                assert any(
                    word in str(error).lower() for word in ["denied", "read only", "read_only", "not supported"]
                ), type(error).__name__
            else:
                raise AssertionError("Warehouse unexpectedly accepted a write")
        finally:
            connection.close()
        result = dict(write_denied=True)
    elif mode == "http":
        with app.test_client() as client:
            response = client.get("/superset/dashboard/" + sys.argv[2] + "/")
            assert response.status_code in (302, 401), response.status_code
            from html.parser import HTMLParser

            class CSRFParser(HTMLParser):
                token = None

                def handle_starttag(self, tag, attrs):
                    values = dict(attrs)
                    if tag == "input" and values.get("name") == "csrf_token":
                        self.token = values.get("value")

            credentials = json.loads(sys.stdin.read())
            parser = CSRFParser()
            parser.feed(client.get("/login/").get_data(as_text=True))
            assert parser.token, "Login did not provide a CSRF token"
            login = client.post("/login/", data={**credentials, "csrf_token": parser.token})
            assert login.status_code == 302, login.status_code
            response = client.get("/api/v1/me/")
            assert response.status_code == 200, (response.status_code, response.location)
            assert response.json["result"]["username"] == credentials["username"]
            result = dict(anonymous_denied=True, authorized_status=response.status_code)
            if sys.argv[2] != "auth-only":
                dashboard = client.get("/superset/dashboard/" + sys.argv[2] + "/", follow_redirects=True)
                assert dashboard.status_code == 200 and "/login" not in dashboard.request.path, (
                    dashboard.status_code,
                    dashboard.request.path,
                )
                result["dashboard_status"] = dashboard.status_code
    else:
        result = dict(
            dashboards=db.session.query(Dashboard).count(),
            charts=db.session.query(Slice).count(),
            datasets=db.session.query(SqlaTable).count(),
        )
    print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
