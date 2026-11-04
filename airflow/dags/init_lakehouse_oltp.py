"""Manual initial source load: all rows -> Bronze -> Silver -> Gold publication."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from lakehouse.config import SOURCE_TABLES
from lakehouse.oltp.gold_ddl import GOLD_TABLE_DDL

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/project/pipelines/config/default.yml")
JOBS = "/opt/project/pipelines/src/jobs/oltp/"
RUN = "{{ run_id }}"
DATE = "{{ ti.xcom_pull(task_ids='check_pipeline_context', key='batch_date') }}"


def begin_run(**context):
    ti = context["ti"]
    ti.xcom_push(key="run_id", value=context["run_id"])
    ti.xcom_push(
        key="batch_date", value=context["data_interval_start"].in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d")
    )


with DAG(
    dag_id="init_lakehouse_oltp",
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "oltp", "init"],
) as dag:
    context = PythonOperator(task_id="check_pipeline_context", python_callable=begin_run)
    with TaskGroup(group_id="staging") as staging:
        for table in SOURCE_TABLES:
            SparkSubmitOperator(
                task_id=table.name,
                application=JOBS + "init_oltp.py",
                application_args=["--config-path", CONFIG_PATH, "--table", table.name],
            )
    with TaskGroup(group_id="bronze") as bronze:
        for table in SOURCE_TABLES:
            SparkSubmitOperator(
                task_id=table.name,
                application=JOBS + "ingest_oltp_to_bronze.py",
                application_args=["--config-path", CONFIG_PATH, "--table", table.name],
            )
    with TaskGroup(group_id="silver") as silver:
        for table in SOURCE_TABLES:
            SparkSubmitOperator(
                task_id=table.silver_table,
                application=JOBS + "ingest_oltp_silver.py",
                application_args=["--config-path", CONFIG_PATH, "--table", table.name, "--run-id", RUN],
            )
    with TaskGroup(group_id="gold") as gold:
        gold_tasks = {}
        for table in GOLD_TABLE_DDL:
            if table.startswith("mart_"):
                continue
            gold_tasks[table] = SparkSubmitOperator(
                task_id=table,
                application=JOBS + "build_oltp_gold.py",
                application_args=[
                    "--table",
                    table,
                    "--run-id",
                    RUN,
                    "--snapshot-date",
                    DATE,
                    "--as-of",
                    "{{ dag_run.start_date.isoformat() }}",
                ],
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
                application_args=[
                    "--table",
                    table,
                    "--run-id",
                    RUN,
                    "--snapshot-date",
                    DATE,
                    "--as-of",
                    "{{ dag_run.start_date.isoformat() }}",
                ],
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
    context >> staging >> bronze >> silver >> gold >> mart >> publish
