"""Manual CDC delete synchronization; preview by default, never purge MySQL."""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from lakehouse.config import HARD_DELETE_TABLES
from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL

JOBS = "/opt/project/pipelines/src/jobs/oltp/"
CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/project/pipelines/config/default.yml")
RUN = "{{ run_id }}"
DATE = "{{ dag_run.start_date.astimezone(dag.timezone).strftime('%Y-%m-%d') }}"
AS_OF = "{{ dag_run.start_date.isoformat() }}"

with DAG(
    dag_id="manual_lakehouse_hard_delete",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "tables": Param([], type="array", uniqueItems=True, items={"type": "string", "enum": list(HARD_DELETE_TABLES)}),
        "apply": Param(False, type="boolean", description="False: preview only. True: apply the selected CDC deletes."),
    },
    tags=["lakehouse", "oltp", "cdc", "manual"],
) as dag:
    sync = SparkSubmitOperator(
        task_id="sync_hard_deletes",
        application=JOBS + "sync_hard_deletes.py",
        application_args=[
            "--config-path",
            CONFIG_PATH,
            "--tables",
            "{{ params.tables | tojson }}",
            "--apply",
            "{{ params.apply | tojson }}",
            "--run-id",
            RUN,
            "--as-of",
            AS_OF,
        ],
    )
    apply_confirmed = ShortCircuitOperator(
        task_id="apply_confirmed",
        python_callable=lambda **context: context["params"]["apply"],
    )
    with TaskGroup(group_id="gold") as gold:
        gold_tasks = {}
        for table in GOLD_TABLE_DDL:
            if table.startswith("mart_"):
                continue
            gold_tasks[table] = SparkSubmitOperator(
                task_id=table,
                application=JOBS + "build_oltp_gold.py",
                application_args=["--table", table, "--run-id", RUN, "--snapshot-date", DATE, "--as-of", AS_OF],
            )
        gold_tasks["fact_order_item"] >> gold_tasks["fact_order"]
        gold_tasks["fact_inventory_daily_snapshot"] >> gold_tasks["fact_inventory_current"]
    with TaskGroup(group_id="mart") as mart:
        mart_tasks = {}
        for table in GOLD_TABLE_DDL:
            if not table.startswith("mart_"):
                continue
            mart_tasks[table] = SparkSubmitOperator(
                task_id=table,
                application=JOBS + "build_oltp_gold.py",
                application_args=["--table", table, "--run-id", RUN, "--snapshot-date", DATE, "--as-of", AS_OF],
            )
        mart_tasks["mart_inventory_current"] >> [
            mart_tasks["mart_inventory_summary"],
            mart_tasks["mart_store_overview"],
        ]
    publish = SparkSubmitOperator(
        task_id="validate_and_publish",
        application=JOBS + "run_gold_quality_gate.py",
        application_args=["--config-path", CONFIG_PATH, "--run-id", RUN, "--batch-date", DATE],
    )
    sync >> apply_confirmed >> gold >> mart >> publish
