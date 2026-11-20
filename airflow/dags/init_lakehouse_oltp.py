"""Full OLTP batch: every source row to Bronze, pre-merge checks, Silver replaced, dbt/Trino Gold."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
ENV = {
    "PIPELINE_RUN_ID": "{{ run_id }}",
    "PIPELINE_AS_OF": "{{ dag_run.start_date.isoformat() }}",
}

with DAG(
    dag_id="init_lakehouse_oltp",
    schedule=None,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    tags=["lakehouse", "oltp", "native"],
) as dag:
    run_batch = BashOperator(
        task_id="run_batch_pipeline",
        env=ENV,
        append_env=True,
        bash_command=PYTHON
        + ' -m jobs.oltp.run_batch_pipeline --namespace-prefix serving --run-id "$PIPELINE_RUN_ID" --mode full --as-of "$PIPELINE_AS_OF"',
    )
