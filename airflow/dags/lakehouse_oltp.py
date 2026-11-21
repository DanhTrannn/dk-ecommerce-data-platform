"""OLTP batch, one task per table and per dbt model: Bronze → checks → Silver → Gold → maintenance.

Daily runs are incremental over the data interval; trigger with {"mode": "full"} for a full load. A failed task
is retried alone; Silver changes only after every check passed, and Gold is rebuilt only after every Silver table.
Run lakehouse_init once first: these tasks only write to the tables it creates.

Python steps run in the pipeline venv (Polars, PyIceberg) through ExternalPythonOperator; only dbt runs in bash.
"""

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG, Param, TaskGroup

from lakehouse.oltp.dag_plan import (
    DBT_PROJECT,
    GOLD_TESTS,
    POOL,
    RUN,
    dbt_command,
    dbt_env,
    gold_dependencies,
)
from lakehouse.oltp.sources import SOURCE_TABLES

PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
SEEDS = {path.stem for path in (Path(DBT_PROJECT) / "seeds").glob("*.csv")}


# Airflow sends each function's source to PIPELINE_PYTHON and runs it there, so the imports stay inside.
def bronze(table, run):
    from jobs.oltp import tasks

    tasks.bronze(table, run)


def check(table, run):
    from jobs.oltp import tasks

    tasks.check(table, run)


def foreign_keys(run):
    from jobs.oltp import tasks

    tasks.foreign_keys(run)


def silver(table, run):
    from jobs.oltp import tasks

    tasks.silver(table, run)


def maintain(action):
    from jobs.maintenance.iceberg_table_maintenance import maintain

    maintain(profile="oltp", action=action)


def pipeline_task(task_id, python_callable, op_args, pool=POOL):
    return ExternalPythonOperator(
        task_id=task_id,
        python=PIPELINE_PYTHON,
        python_callable=python_callable,
        op_args=op_args,
        expect_airflow=False,
        pool=pool,
    )


with DAG(
    dag_id="lakehouse_oltp",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    params={"mode": Param("incremental", enum=["incremental", "full"])},
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "native"],
) as dag:
    with TaskGroup("bronze") as bronze_group:
        for table in SOURCE_TABLES:
            pipeline_task(table.name, bronze, [table.name, RUN])

    with TaskGroup("checks") as checks_group:
        table_checks = [pipeline_task(table.name, check, [table.name, RUN]) for table in SOURCE_TABLES]
        table_checks >> pipeline_task("foreign_keys", foreign_keys, [RUN])

    with TaskGroup("silver") as silver_group:
        for table in SOURCE_TABLES:
            pipeline_task(table.name, silver, [table.name, RUN])

    with TaskGroup("gold") as gold_group:
        graph = gold_dependencies(Path(DBT_PROJECT))
        tasks = {
            node: BashOperator(
                task_id=node,
                bash_command=dbt_command("seed" if node in SEEDS else "build", node),
                env=dbt_env(node),
                append_env=True,
                pool=POOL,
            )
            for node in graph
        }
        for node, refs in graph.items():
            for ref in refs:
                tasks[ref] >> tasks[node]
        list(tasks.values()) >> BashOperator(
            task_id="tests", bash_command=dbt_command("test", GOLD_TESTS), env=dbt_env("tests"), append_env=True, pool=POOL
        )

    with TaskGroup("maintenance") as maintenance_group:
        compact = pipeline_task("compact", maintain, ["compact"], pool=None)
        expire = pipeline_task("expire", maintain, ["expire"], pool=None)
        orphan = pipeline_task("orphan", maintain, ["orphan"], pool=None)
        compact >> expire >> orphan

    bronze_group >> checks_group >> silver_group >> gold_group >> maintenance_group
