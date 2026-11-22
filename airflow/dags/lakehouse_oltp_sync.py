"""Sync rows deleted or restored at the source: flag them in Bronze, then carry them through Silver and Gold.

It runs after each successful lakehouse_oltp_incremental run, which writes the same tables. On the daily schedule
the two never overlap; during a backfill they would, so pause this DAG while backfilling and trigger it once by
hand afterwards.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import dbt_command, dbt_env, gold_task_group

# Each task holds a table's keys in memory, so the pool (4 slots) bounds memory use.
POOL = "lakehouse_oltp"

with DAG(
    dag_id="lakehouse_oltp_sync",
    schedule=[oltp.OLTP_LOADED],
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "sync"],
) as dag:
    with TaskGroup("detect") as detect:
        for table in SOURCE_TABLES:
            PythonOperator(task_id=table.name, python_callable=oltp.detect, op_kwargs={"table": table.name}, pool=POOL)

    with TaskGroup("bronze") as bronze:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_incremental, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("silver") as silver:
        for table in SOURCE_TABLES:
            BashOperator(
                task_id=table.name,
                bash_command=dbt_command("build", table.name),
                env=dbt_env(table.name),
                append_env=True,
                pool=POOL,
            )

    gold = gold_task_group(POOL, full_refresh=False)

    with TaskGroup("maintenance") as maintenance:
        compact = PythonOperator(task_id="compact", python_callable=oltp.maintain, op_kwargs={"action": "compact"})
        optimize = BashOperator(
            task_id="optimize", bash_command="dbt run-operation optimize", env=dbt_env("optimize"), append_env=True
        )
        expire = PythonOperator(task_id="expire", python_callable=oltp.maintain, op_kwargs={"action": "expire"})
        orphan = PythonOperator(task_id="orphan", python_callable=oltp.maintain, op_kwargs={"action": "orphan"})
        compact >> optimize >> expire >> orphan

    detect >> bronze >> silver >> gold >> maintenance
