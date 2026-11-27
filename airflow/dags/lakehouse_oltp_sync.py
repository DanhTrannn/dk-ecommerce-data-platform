"""Sync source changes the incremental load cannot see: column changes, then rows deleted or restored.

`schema` adds the columns approved in sources.py and widens types in staging and Bronze, and fails on an
unreviewed new column, a dropped column or an incompatible type. `detect` flags deleted rows in Bronze and stages
restored ones, or the whole table when a new column still has to be filled. Silver and Gold then pick them up.
Reference tables only go through `schema`: lakehouse_oltp_full_daily reloads them whole every day.

It runs after each successful lakehouse_oltp_incremental_daily run, which writes the same tables. On the daily
schedule the two never overlap; when incremental catches up several days they would, so pause this DAG while it
catches up and trigger it once by hand afterwards.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import DBT_POOL, dbt_command, dbt_env, gold_task_group

# Each task holds a table's keys in memory, so the pool (4 slots) bounds memory use.
POOL = "lakehouse_oltp"
TRANSACTIONAL_TABLES = [table for table in SOURCE_TABLES if not table.full_load]

with DAG(
    dag_id="lakehouse_oltp_sync",
    schedule=[oltp.OLTP_LOADED],
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "sync"],
) as dag:
    with TaskGroup("schema") as schema:
        for table in SOURCE_TABLES:
            PythonOperator(task_id=table.name, python_callable=oltp.schema, op_kwargs={"table": table.name}, pool=POOL)

    with TaskGroup("detect") as detect:
        for table in TRANSACTIONAL_TABLES:
            PythonOperator(task_id=table.name, python_callable=oltp.detect, op_kwargs={"table": table.name}, pool=POOL)

    with TaskGroup("bronze") as bronze:
        for table in TRANSACTIONAL_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_sync, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("silver") as silver:
        for table in TRANSACTIONAL_TABLES:
            BashOperator(
                task_id=table.name,
                bash_command=dbt_command("build", table.name),
                env=dbt_env(table.name),
                append_env=True,
                pool=DBT_POOL,
            )

    gold = gold_task_group(full_refresh=False, reference=False)

    with TaskGroup("maintenance") as maintenance:
        kwargs = {"full_load": False}
        optimize = PythonOperator(
            task_id="optimize", python_callable=oltp.maintain, op_kwargs={"action": "optimize", **kwargs}
        )
        expire = PythonOperator(task_id="expire", python_callable=oltp.maintain, op_kwargs={"action": "expire", **kwargs})
        orphan = PythonOperator(task_id="orphan", python_callable=oltp.maintain, op_kwargs={"action": "orphan", **kwargs})
        optimize >> expire >> orphan

    schema >> detect >> bronze >> silver >> gold >> maintenance
