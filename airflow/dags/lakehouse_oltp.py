"""OLTP batch DAGs, one task per table and per dbt model: Bronze → checks → Silver → Gold → maintenance.

- lakehouse_oltp_init: trigger by hand to set the lakehouse up. It creates the namespaces and the Bronze/Silver
  tables, then loads every source row created before the run started. Trigger it again after a source column is
  added to the batch, or to reload everything.
- lakehouse_oltp_incremental: daily at 03:00, each run loads the rows whose cursor falls in its data interval.
  Catchup runs the intervals missed while the DAG was paused; `airflow backfill create --dag-id
  lakehouse_oltp_incremental --from-date ... --to-date ...` reloads a past range.

Both write the same tables: keep the incremental DAG paused while the init DAG runs. A failed task is retried
alone; Silver changes only after every check passed, and Gold is rebuilt only after every Silver table.
Python steps call the pipeline's job functions directly; only dbt runs in bash.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from jobs.maintenance.iceberg_table_maintenance import maintain
from jobs.oltp import tasks

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

SEEDS = {path.stem for path in (Path(DBT_PROJECT) / "seeds").glob("*.csv")}


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
                PythonOperator(task_id=table.name, python_callable=tasks.bronze, op_args=[table.name, run], pool=POOL)

        with TaskGroup("checks") as checks_group:
            table_checks = [
                PythonOperator(task_id=table.name, python_callable=tasks.check, op_args=[table.name, run], pool=POOL)
                for table in SOURCE_TABLES
            ]
            table_checks >> PythonOperator(
                task_id="foreign_keys", python_callable=tasks.foreign_keys, op_args=[run], pool=POOL
            )

        with TaskGroup("silver") as silver_group:
            for table in SOURCE_TABLES:
                PythonOperator(task_id=table.name, python_callable=tasks.silver, op_args=[table.name, run], pool=POOL)

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
            steps = [
                PythonOperator(task_id=action, python_callable=maintain, op_kwargs={"profile": "oltp", "action": action})
                for action in ("compact", "expire", "orphan")
            ]
            steps[0] >> steps[1] >> steps[2]

        bronze_group >> checks_group >> silver_group >> gold_group >> maintenance_group
        if create_tables:
            PythonOperator(task_id="create_schema", python_callable=tasks.create_schema) >> bronze_group
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
