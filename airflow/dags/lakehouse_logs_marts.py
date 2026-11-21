"""Hourly native log ingestion and dbt/Trino publication."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from jobs.logs.build_logs_gold import run

with DAG(
    dag_id="lakehouse_logs_marts",
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "logs", "native"],
) as dag:
    PythonOperator(
        task_id="rollup_logs_gold_marts",
        python_callable=run,
        op_kwargs={"run_id": "logs-{{ run_id }}", "as_of": "{{ dag_run.start_date.isoformat() }}"},
    )
