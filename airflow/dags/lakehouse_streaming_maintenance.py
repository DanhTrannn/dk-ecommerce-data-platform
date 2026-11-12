"""Native compaction, publication-safe snapshot expiration and orphan reporting."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def check_flink_streaming_health():
    import requests

    host = os.environ.get("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
    port = os.environ.get("FLINK_JOBMANAGER_PORT", "8081")
    response = requests.get(f"http://{host}:{port}/jobs/overview", timeout=15)
    response.raise_for_status()
    required = tuple(
        filter(
            None,
            os.environ.get("FLINK_REQUIRED_JOBS", "kafka_to_lakehouse_pure_streaming").split(","),
        )
    )
    if not required:
        raise ValueError("FLINK_REQUIRED_JOBS must not be empty")
    running = {job.get("name"): job for job in response.json().get("jobs", []) if job.get("state") == "RUNNING"}
    missing = set(required) - running.keys()
    if missing:
        raise RuntimeError(f"Required streaming jobs are not RUNNING: {sorted(missing)}")
    return [running[name] for name in required]


with DAG(
    dag_id="lakehouse_streaming_maintenance",
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "maintenance", "native"],
) as dag:
    tasks = []
    monitor = PythonOperator(task_id="check_flink_streaming_health", python_callable=check_flink_streaming_health)
    for action in ["compact", "expire", "orphan"]:
        tasks.append(
            BashOperator(
                task_id=action,
                bash_command=os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
                + " -m jobs.maintenance.iceberg_table_maintenance --namespace-prefix serving --profile streaming --action "
                + action,
            )
        )
    for before, after in zip(tasks, tasks[1:]):
        before >> after
    monitor >> tasks[0]
