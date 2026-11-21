"""Daily incremental load of the OLTP source at 03:00: staging → Bronze → checks → Silver → Gold.

Each run loads the rows whose cursor falls in its data interval upserts them into Bronze, then dbt merges the changed Bronze rows into Silver. Catchup runs the
intervals missed while the DAG was paused; `airflow backfill create --dag-id lakehouse_oltp_incremental
--from-date ... --to-date ...` reloads a past range. Run lakehouse_oltp_init first, and keep this DAG paused while
it runs: both write the same tables.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import dbt_command, dbt_env, gold_task_group

POOL = "lakehouse_oltp"  # 4 slots (docker-compose.yml); each task holds a table's batches in memory

with DAG(
    dag_id="lakehouse_oltp_incremental",
    schedule="0 3 * * *",
    # Catchup runs every interval from here on; rows created before the init DAG's full load are already loaded.
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "incremental"],
) as dag:
    with TaskGroup("staging") as staging:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.staging_incremental, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("bronze") as bronze:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_incremental, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("checks") as checks:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.check_incremental, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("silver") as silver:
        # Each Silver model merges the Bronze rows written since its last run.
        for table in SOURCE_TABLES:
            BashOperator(
                task_id=table.name,
                bash_command=dbt_command("build", table.name) + "",
                env=dbt_env(table.name),
                append_env=True,
                pool=POOL,
            )

    gold = gold_task_group(POOL)

    with TaskGroup("maintenance") as maintenance:
        compact = PythonOperator(task_id="compact", python_callable=oltp.maintain, op_kwargs={"action": "compact"})
        expire = PythonOperator(task_id="expire", python_callable=oltp.maintain, op_kwargs={"action": "expire"})
        orphan = PythonOperator(task_id="orphan", python_callable=oltp.maintain, op_kwargs={"action": "orphan"})
        compact >> expire >> orphan

    staging >> bronze >> checks >> silver >> gold >> maintenance
