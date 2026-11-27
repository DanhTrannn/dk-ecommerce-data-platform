import ast
import unittest
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.utils.operator_helpers import KeywordParameters

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp as oltp_tasks
from tasks.dbt import REFERENCE_GOLD
from tasks.oltp import OLTP_LOADED

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
REFERENCE = [table.name for table in SOURCE_TABLES if table.full_load]
TRANSACTIONAL = [table.name for table in SOURCE_TABLES if not table.full_load]
CONTEXT_KEYS = ("run_id", "dag_run", "ti", "params", "ds", "logical_date", "data_interval_start", "data_interval_end")


def load_dag(name):
    namespace = {"__name__": name}
    exec(compile((AIRFLOW_DIR / f"dags/{name}.py").read_text(), f"{name}.py", "exec"), namespace)
    return namespace["dag"]


class DagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.init, cls.incremental = load_dag("lakehouse_oltp_init"), load_dag("lakehouse_oltp_incremental_daily")
        cls.sync, cls.purge = load_dag("lakehouse_oltp_sync"), load_dag("lakehouse_oltp_purge")
        cls.full_daily = load_dag("lakehouse_oltp_full_daily")
        cls.dags = (cls.init, cls.incremental, cls.sync, cls.purge, cls.full_daily)

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
        for name in TRANSACTIONAL:
            detect = self.sync.task_dict[f"detect.{name}"]
            self.assertIn(f"bronze.{name}", detect.downstream_task_ids)
            self.assertIn(f"silver.{name}", self.sync.task_dict[f"bronze.{name}"].downstream_task_ids)
        self.assertIn("gold.fact_order", self.sync.task_dict)
        self.assertIn("maintenance.orphan", self.sync.task_dict)

    def test_sync_checks_the_schema_before_detecting(self):
        for name in TRANSACTIONAL:
            self.assertIn(f"detect.{name}", self.sync.task_dict[f"schema.{name}"].downstream_task_ids)
        self.assertFalse([task for task in self.incremental.task_dict if task.startswith("schema.")])
        self.assertIs(self.sync.task_dict["bronze.orders"].python_callable, oltp_tasks.bronze_sync)

    def test_sync_skips_reference_tables_after_the_schema_check(self):
        for name in REFERENCE:
            self.assertIn(f"schema.{name}", self.sync.task_dict)
            for layer in ("detect", "bronze", "silver"):
                self.assertNotIn(f"{layer}.{name}", self.sync.task_dict)
        for node in REFERENCE_GOLD:
            self.assertNotIn(f"gold.{node}", self.sync.task_dict)

    def test_full_daily_loads_only_reference_tables(self):
        dag = self.full_daily
        self.assertEqual({task[len("staging."):] for task in dag.task_dict if task.startswith("staging.")}, set(REFERENCE))
        for name in REFERENCE:
            self.assertIs(dag.task_dict[f"staging.{name}"].python_callable, oltp_tasks.staging_full)
            self.assertIs(dag.task_dict[f"bronze.{name}"].python_callable, oltp_tasks.bronze_overwrite)
        self.assertEqual({task[len("gold."):] for task in dag.task_dict if task.startswith("gold.")}, set(REFERENCE_GOLD))
        self.assertEqual((dag.timetable.expression, dag.timetable.timezone), (self.incremental.timetable.expression, self.incremental.timetable.timezone))
        self.assertEqual(dag.start_date, self.incremental.start_date)
        self.assertTrue(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)

    def test_incremental_loads_only_transactional_tables(self):
        names = {task.split(".", 1)[1] for task in self.incremental.task_dict if "." in task}
        self.assertFalse(names & {*REFERENCE, *REFERENCE_GOLD})
        self.assertIn("gold.fact_order_item", self.incremental.task_dict)
        self.assertIn("gold.tests", self.incremental.task_dict)

    def test_incremental_gold_waits_for_the_full_daily_run(self):
        sensor = self.incremental.task_dict["wait_full_daily"]
        self.assertIsInstance(sensor, ExternalTaskSensor)
        self.assertEqual(sensor.external_dag_id, "lakehouse_oltp_full_daily")
        self.assertEqual(sensor.mode, "reschedule")
        for task in ("gold.fact_order", "gold.dim_customer", "gold.dim_date"):
            self.assertIn(task, sensor.downstream_task_ids)
        self.assertNotIn("wait_full_daily", self.incremental.task_dict["staging.orders"].get_flat_relative_ids(upstream=True))

    def test_dbt_tasks_use_the_dbt_pool(self):
        for dag in self.dags:
            for task in dag.tasks:
                if isinstance(task, BashOperator):
                    self.assertEqual(task.pool, "lakehouse_dbt", f"{dag.dag_id}.{task.task_id}")

    def test_purge_runs_by_hand_from_gold_down_to_bronze(self):
        self.assertIsNone(self.purge.schedule)
        downstream = self.purge.task_dict["silver_gold"].get_flat_relative_ids(upstream=False)
        self.assertEqual(downstream, {f"bronze.{table.name}" for table in SOURCE_TABLES})

    def test_marts_wait_for_the_facts_they_read_through_live(self):
        for dag in (self.init, self.incremental, self.sync):
            self.assertIn("gold.mart_reviews_daily", dag.task_dict["gold.fact_review"].get_flat_relative_ids(upstream=False))
            self.assertIn("gold.mart_sales_daily", dag.task_dict["gold.fact_order"].get_flat_relative_ids(upstream=False))

    def test_every_python_task_gets_its_arguments_from_airflow(self):
        for dag in self.dags:
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
        self.assertEqual(self.incremental.task_dict["maintenance.optimize"].upstream_task_ids, {"gold.tests"})
        for dag in (self.init, self.incremental, self.sync, self.full_daily):
            group = {task for task in dag.task_dict if task.startswith("maintenance.")}
            self.assertEqual(group, {"maintenance.optimize", "maintenance.expire", "maintenance.orphan"}, dag.dag_id)
            self.assertIs(dag.task_dict["maintenance.optimize"].python_callable, oltp_tasks.maintain)
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
