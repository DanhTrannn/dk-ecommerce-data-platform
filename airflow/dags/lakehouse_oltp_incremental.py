"""Load the OLTP rows changed in each daily interval: staging, Bronze, Silver and Gold.

Run lakehouse_oltp_init first and keep this DAG paused while it runs; both DAGs write the same tables. To reload a
past range, use `airflow backfill create --dag-id lakehouse_oltp_incremental --from-date ... --to-date ...`.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import dbt_command, dbt_env, gold_task_group

# Each task holds a table's batches in memory, so the pool (4 slots) bounds memory use.
POOL = "lakehouse_oltp"

with DAG(
    dag_id="lakehouse_oltp_incremental",
    schedule="0 3 * * *",
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

    staging >> bronze >> silver >> gold >> maintenance
