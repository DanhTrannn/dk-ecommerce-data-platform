"""Manual CDC delete synchronization for removed cart/wishlist items; never purge MySQL."""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = "/opt/project/pipelines/src/jobs/oltp/"
CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/project/pipelines/config/default.yml")
AS_OF = "{{ dag_run.start_date.isoformat() }}"

with DAG(
    dag_id="manual_lakehouse_hard_delete",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "cdc", "manual"],
) as dag:
    SparkSubmitOperator(
        task_id="sync_hard_deletes",
        application=JOBS + "sync_hard_deletes.py",
        application_args=[
            "--config-path",
            CONFIG_PATH,
            "--as-of",
            AS_OF,
        ],
    )
