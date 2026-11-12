"""Native PostgreSQL/Silver ingestion followed by dbt/Trino Gold publication."""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
DATE = "{{ dag_run.start_date.astimezone(dag.timezone).strftime('%Y-%m-%d') }}"
ENV = {
    "PIPELINE_RUN_ID": "{{ run_id }}",
    "PIPELINE_AS_OF": "{{ dag_run.start_date.isoformat() }}",
    "PIPELINE_BATCH_DATE": DATE,
    "PIPELINE_START": '{{ data_interval_start.isoformat() if data_interval_start else "" }}',
    "PIPELINE_END": '{{ data_interval_end.isoformat() if data_interval_end else "" }}',
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
    ingest = BashOperator(
        task_id="ingest_source",
        env=ENV,
        append_env=True,
        bash_command=PYTHON + ' -m jobs.oltp.run_polars_batch --namespace-prefix serving --run-id "$PIPELINE_RUN_ID"',
    )
    publish = BashOperator(
        task_id="build_validate_publish",
        env=ENV,
        append_env=True,
        bash_command=PYTHON
        + ' -m jobs.oltp.build_oltp_gold --namespace-prefix serving --run-id "$PIPELINE_RUN_ID" --as-of "$PIPELINE_AS_OF" --batch-date "$PIPELINE_BATCH_DATE"',
    )
    ingest >> publish
