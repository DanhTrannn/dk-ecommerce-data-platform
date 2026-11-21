"""Hourly native log ingestion and dbt/Trino publication."""

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG

PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")


# Airflow sends the function's source to PIPELINE_PYTHON and runs it there, so the import stays inside.
def rollup_logs_gold_marts(run_id, as_of):
    from jobs.logs.build_logs_gold import run

    run(namespace_prefix="serving", run_id=run_id, as_of=as_of)


with DAG(
    dag_id="lakehouse_logs_marts",
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "logs", "native"],
) as dag:
    ExternalPythonOperator(
        task_id="rollup_logs_gold_marts",
        python=PIPELINE_PYTHON,
        python_callable=rollup_logs_gold_marts,
        op_args=["logs-{{ run_id }}", "{{ dag_run.start_date.isoformat() }}"],
        expect_airflow=False,
    )
