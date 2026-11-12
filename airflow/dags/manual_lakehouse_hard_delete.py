"""Manual, bounded CDC hard-delete synchronization using PyIceberg."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="manual_lakehouse_hard_delete",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "cdc", "manual"],
) as dag:
    BashOperator(
        task_id="sync_hard_deletes",
        append_env=True,
        env={"PIPELINE_AS_OF": "{{ dag_run.start_date.isoformat() }}"},
        bash_command=os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
        + ' -m jobs.oltp.sync_hard_deletes --namespace-prefix serving --as-of "$PIPELINE_AS_OF"',
    )
