import ast
import unittest
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.operator_helpers import KeywordParameters

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks.oltp import OLTP_LOADED

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
CONTEXT_KEYS = ("run_id", "dag_run", "ti", "params", "ds", "logical_date", "data_interval_start", "data_interval_end")


def load_dag(name):
    namespace = {"__name__": name}
    exec(compile((AIRFLOW_DIR / f"dags/{name}.py").read_text(), f"{name}.py", "exec"), namespace)
    return namespace["dag"]


class DagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.init, cls.incremental = load_dag("lakehouse_oltp_init"), load_dag("lakehouse_oltp_incremental")
        cls.sync = load_dag("lakehouse_oltp_sync")

    def test_only_init_creates_tables_and_reconciles(self):
        self.assertEqual([task.task_id for task in self.init.roots], ["create_schema"])
        self.assertIn("reconcile.orders", self.init.task_dict)
        self.assertFalse([task for task in self.incremental.task_dict if task.startswith("reconcile.")])
        self.assertNotIn("create_schema", self.incremental.task_dict)

    def test_schedules(self):
        self.assertIsNone(self.init.schedule)
        self.assertEqual(self.incremental.schedule, "0 3 * * *")
        self.assertTrue(self.incremental.catchup)

    def test_sync_runs_after_each_incremental_load(self):
        self.assertEqual(self.incremental.task_dict["maintenance.orphan"].outlets, [OLTP_LOADED])
        self.assertEqual(self.sync.timetable.asset_condition.objects, (OLTP_LOADED,))
        self.assertFalse(self.sync.catchup)
        self.assertEqual(self.sync.max_active_runs, 1)

    def test_sync_detects_then_loads_each_table(self):
        for table in SOURCE_TABLES:
            detect = self.sync.task_dict[f"detect.{table.name}"]
            self.assertIn(f"bronze.{table.name}", detect.downstream_task_ids)
            self.assertIn(f"silver.{table.name}", self.sync.task_dict[f"bronze.{table.name}"].downstream_task_ids)
        self.assertIn("gold.fact_order", self.sync.task_dict)
        self.assertIn("maintenance.orphan", self.sync.task_dict)

    def test_every_python_task_gets_its_arguments_from_airflow(self):
        for dag in (self.init, self.incremental, self.sync):
            for task in dag.tasks:
                if isinstance(task, PythonOperator):
                    context = {key: object() for key in CONTEXT_KEYS} | task.op_kwargs
                    KeywordParameters.determine(task.python_callable, task.op_args, context)

    def test_each_dag_calls_its_own_load_functions(self):
        for dag, kind in ((self.init, "full"), (self.incremental, "incremental")):
            for layer in ("staging", "bronze"):
                self.assertTrue(dag.task_dict[f"{layer}.orders"].python_callable.__name__.endswith(kind))

    def test_layers_run_in_order(self):
        self.assertIn("reconcile.orders", self.init.task_dict["bronze.orders"].upstream_task_ids)
        self.assertIn("staging.orders", self.init.task_dict["reconcile.orders"].upstream_task_ids)
        self.assertIn("bronze.orders", self.incremental.task_dict["silver.orders"].upstream_task_ids)
        self.assertEqual(self.incremental.task_dict["maintenance.compact"].upstream_task_ids, {"gold.tests"})
        self.assertEqual(self.incremental.task_dict["maintenance.expire"].upstream_task_ids, {"maintenance.optimize"})

    def test_only_init_rebuilds_gold(self):
        self.assertIn("--full-refresh", self.init.task_dict["gold.fact_order"].bash_command)
        self.assertNotIn("--full-refresh", self.incremental.task_dict["gold.fact_order"].bash_command)
        self.assertNotIn("--full-refresh", self.init.task_dict["gold.delivery_sla"].bash_command)


class CodeRuleTests(unittest.TestCase):
    def test_imports_are_at_the_top_of_every_file(self):
        for path in AIRFLOW_DIR.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    inner = [child for child in ast.walk(node) if isinstance(child, (ast.Import, ast.ImportFrom))]
                    self.assertEqual(inner, [], f"{path.name}:{node.name}")


if __name__ == "__main__":
    unittest.main()
