"""Reload the OLTP reference tables whole every day: staging, Bronze, Silver and the Gold built only from them.

Reference rows gone from the source drop out of every layer. lakehouse_oltp_incremental_daily waits for this DAG's run
of the same day before its Gold, and catchup keeps a run for every day it loads.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import DBT_POOL, dbt_command, dbt_env, gold_task_group

# Each task holds a table's batches in memory, so the pool (4 slots) bounds memory use.
POOL = "lakehouse_oltp"
REFERENCE_TABLES = [table for table in SOURCE_TABLES if table.full_load]

with DAG(
    dag_id="lakehouse_oltp_full_daily",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 25, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "full"],
) as dag:
    with TaskGroup("staging") as staging:
        for table in REFERENCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.staging_full, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("bronze") as bronze:
        for table in REFERENCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_overwrite, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("silver") as silver:
        for table in REFERENCE_TABLES:
            BashOperator(
                task_id=table.name,
                bash_command=dbt_command("build", table.name),
                env=dbt_env(table.name),
                append_env=True,
                pool=DBT_POOL,
            )

    gold = gold_task_group(full_refresh=False, reference=True)

    with TaskGroup("maintenance") as maintenance:
        kwargs = {"full_load": True}
        optimize = PythonOperator(
            task_id="optimize", python_callable=oltp.maintain, op_kwargs={"action": "optimize", **kwargs}
        )
        expire = PythonOperator(task_id="expire", python_callable=oltp.maintain, op_kwargs={"action": "expire", **kwargs})
        orphan = PythonOperator(task_id="orphan", python_callable=oltp.maintain, op_kwargs={"action": "orphan", **kwargs})
        optimize >> expire >> orphan

    staging >> bronze >> silver >> gold >> maintenance
