"""Hourly native log ingestion and dbt/Trino publication."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_logs_marts",
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "logs", "native"],
) as dag:
    BashOperator(
        task_id="rollup_logs_gold_marts",
        append_env=True,
        env={"PIPELINE_RUN_ID": "logs-{{ run_id }}", "PIPELINE_AS_OF": "{{ dag_run.start_date.isoformat() }}"},
        bash_command=os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
        + ' -m jobs.logs.build_logs_gold --namespace-prefix serving --run-id "$PIPELINE_RUN_ID" --as-of "$PIPELINE_AS_OF"',
    )
