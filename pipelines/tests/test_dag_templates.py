"""Manual Airflow 3 runs need not expose logical-date/interval template macros."""

import ast
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[2]


def date_template(filename):
    module = ast.parse((ROOT / "airflow/dags" / filename).read_text())
    return next(
        node.value.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "DATE" for target in node.targets)
    )


class DagDateTests(unittest.TestCase):
    def render(self, name, **context):
        return (
            Environment(undefined=StrictUndefined)
            .from_string(date_template(name))
            .render(dag=SimpleNamespace(timezone=ZoneInfo("Asia/Ho_Chi_Minh")), **context)
        )

    def test_initial_run_without_logical_date_or_interval_macros(self):
        result = self.render(
            "init_lakehouse_oltp.py", dag_run=SimpleNamespace(start_date=datetime(2026, 11, 8, 20, tzinfo=UTC))
        )
        self.assertEqual(result, "2026-11-09")

    def test_initial_date_stays_fixed_on_retry(self):
        run = SimpleNamespace(start_date=datetime(2026, 11, 8, 16, 59, tzinfo=UTC))
        first = self.render("init_lakehouse_oltp.py", dag_run=run, ts="2026-11-08T16:59:00Z")
        retry = self.render("init_lakehouse_oltp.py", dag_run=run, ts="2026-11-09T01:00:00Z")
        self.assertEqual(first, "2026-11-08")
        self.assertEqual(retry, first)

    def test_interval_date_uses_interval_not_run_start(self):
        result = self.render(
            "lakehouse_oltp_pipeline.py",
            data_interval_start=datetime(2026, 11, 7, 20, tzinfo=UTC),
            dag_run=SimpleNamespace(start_date=datetime(2026, 11, 9, tzinfo=UTC)),
        )
        self.assertEqual(result, "2026-11-08")
