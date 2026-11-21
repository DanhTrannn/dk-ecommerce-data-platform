"""OLTP batch DAGs, one task per table and per dbt model: Bronze → checks → Silver → Gold → maintenance.

- lakehouse_oltp_init: trigger by hand to set the lakehouse up. It creates the namespaces and the Bronze/Silver
  tables, then loads every source row created before the run started. Trigger it again after a source column is
  added to the batch, or to reload everything.
- lakehouse_oltp_incremental: daily at 03:00, each run loads the rows whose cursor falls in its data interval.
  Catchup runs the intervals missed while the DAG was paused; `airflow backfill create --dag-id
  lakehouse_oltp_incremental --from-date ... --to-date ...` reloads a past range.

Both write the same tables: keep the incremental DAG paused while the init DAG runs. A failed task is retried
alone; Silver changes only after every check passed, and Gold is rebuilt only after every Silver table.
Python steps run in the pipeline venv (Polars, PyIceberg) through ExternalPythonOperator; only dbt runs in bash.
"""

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.dag_plan import (
    DBT_PROJECT,
    FULL_RUN,
    GOLD_TESTS,
    INCREMENTAL_RUN,
    POOL,
    dbt_command,
    dbt_env,
    gold_dependencies,
)
from lakehouse.oltp.sources import SOURCE_TABLES

PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")
SEEDS = {path.stem for path in (Path(DBT_PROJECT) / "seeds").glob("*.csv")}


# Airflow sends each function's source to PIPELINE_PYTHON and runs it there, so the imports stay inside.
def create_schema():
    from jobs.oltp import tasks

    tasks.create_schema()


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


def oltp_dag(dag_id, *, run, schedule, start_date, catchup, create_tables):
    """The load graph both DAGs share; `run` is FULL_RUN or INCREMENTAL_RUN."""
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        max_active_runs=1,
        default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
        tags=["lakehouse", "oltp", run["mode"]],
    ) as dag:
        with TaskGroup("bronze") as bronze_group:
            for table in SOURCE_TABLES:
                pipeline_task(table.name, bronze, [table.name, run])

        with TaskGroup("checks") as checks_group:
            table_checks = [pipeline_task(table.name, check, [table.name, run]) for table in SOURCE_TABLES]
            table_checks >> pipeline_task("foreign_keys", foreign_keys, [run])

        with TaskGroup("silver") as silver_group:
            for table in SOURCE_TABLES:
                pipeline_task(table.name, silver, [table.name, run])

        with TaskGroup("gold") as gold_group:
            graph = gold_dependencies(Path(DBT_PROJECT))
            models = {
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
                    models[ref] >> models[node]
            list(models.values()) >> BashOperator(
                task_id="tests", bash_command=dbt_command("test", GOLD_TESTS), env=dbt_env("tests"), append_env=True, pool=POOL
            )

        with TaskGroup("maintenance") as maintenance_group:
            compact = pipeline_task("compact", maintain, ["compact"], pool=None)
            expire = pipeline_task("expire", maintain, ["expire"], pool=None)
            orphan = pipeline_task("orphan", maintain, ["orphan"], pool=None)
            compact >> expire >> orphan

        bronze_group >> checks_group >> silver_group >> gold_group >> maintenance_group
        if create_tables:
            pipeline_task("create_schema", create_schema, [], pool=None) >> bronze_group
    return dag


init_dag = oltp_dag(
    "lakehouse_oltp_init",
    run=FULL_RUN,
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    create_tables=True,
)

incremental_dag = oltp_dag(
    "lakehouse_oltp_incremental",
    run=INCREMENTAL_RUN,
    schedule="0 3 * * *",
    # Catchup runs every interval from here on; rows created before the init DAG's full load are already loaded.
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    create_tables=False,
)
