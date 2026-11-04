"""Hourly Landing-first log processing, independent of maintenance."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="lakehouse_logs_marts",
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    description="Landing -> Bronze -> Silver -> fact -> three published log marts; independent of maintenance",
    tags=["lakehouse", "logs", "marts"],
) as dag:
    SparkSubmitOperator(
        task_id="rollup_logs_gold_marts",
        application="/opt/project/pipelines/src/jobs/logs/build_logs_gold.py",
        application_args=["--run-id", "logs-{{ run_id }}", "--as-of", "{{ dag_run.start_date.isoformat() }}"],
    )
