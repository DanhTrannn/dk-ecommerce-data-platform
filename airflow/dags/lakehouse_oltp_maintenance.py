"""Native compaction, publication-safe snapshot expiration and orphan reporting."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_oltp_maintenance",
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "maintenance", "native"],
) as dag:
    tasks = []
    for action in ["compact", "expire", "orphan", "quarantine"]:
        tasks.append(
            BashOperator(
                task_id=action,
                bash_command=os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
                + " -m jobs.maintenance.iceberg_table_maintenance --namespace-prefix serving --profile oltp --action "
                + action,
            )
        )
    for before, after in zip(tasks, tasks[1:]):
        before >> after
