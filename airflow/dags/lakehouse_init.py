"""Create the lakehouse namespaces and the OLTP Bronze/Silver tables, so lakehouse_oltp only loads data.

Trigger it once per namespace prefix before the first lakehouse_oltp run, and again after a source column is added
to the batch. It is safe to repeat: existing tables keep their data and only gain new columns. dbt owns Gold tables.
"""

import os

import pendulum
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG, Param

from lakehouse.oltp.dag_plan import NAMESPACE_PATTERN

PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")


# Airflow sends the function's source to PIPELINE_PYTHON and runs it there, so the import stays inside.
def create_schema(namespace_prefix):
    from jobs.oltp import tasks

    tasks.create_schema(namespace_prefix)


with DAG(
    dag_id="lakehouse_init",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    params={"namespace_prefix": Param("serving", type="string", pattern=NAMESPACE_PATTERN)},
    default_args={"owner": "lakehouse"},
    tags=["lakehouse", "oltp", "init"],
) as dag:
    ExternalPythonOperator(
        task_id="create_schema",
        python=PIPELINE_PYTHON,
        python_callable=create_schema,
        op_args=["{{ params.namespace_prefix }}"],
        expect_airflow=False,
    )
