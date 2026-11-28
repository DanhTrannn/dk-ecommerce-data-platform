"""Load the transactional OLTP rows changed in each daily interval: staging, Bronze, Silver and Gold.

Run lakehouse_oltp_init first and keep this DAG paused while it runs; both DAGs write the same tables. Gold waits
for lakehouse_oltp_full_daily's run of the same day, since facts look up the reference tables it reloads. To reload
past data, trigger lakehouse_oltp_init.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import DBT_POOL, dbt_command, dbt_env, gold_task_group

# Each task holds a table's batches in memory, so the pool (4 slots) bounds memory use.
POOL = "lakehouse_oltp"
TRANSACTIONAL_TABLES = [table for table in SOURCE_TABLES if not table.full_load]

with DAG(
    dag_id="lakehouse_oltp_incremental_daily",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 25, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "incremental"],
) as dag:
    with TaskGroup("staging") as staging:
        for table in TRANSACTIONAL_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.staging_incremental, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("bronze") as bronze:
        for table in TRANSACTIONAL_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_incremental, op_kwargs={"table": table.name}, pool=POOL
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

    # Facts look up the reference tables, so they wait until today's reload of them is complete.
    wait_full_daily = ExternalTaskSensor(
        task_id="wait_full_daily",
        external_dag_id="lakehouse_oltp_full_daily",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poll_interval=60,
        timeout=6 * 3600,
    )

    gold = gold_task_group(full_refresh=False, reference=False)

    with TaskGroup("maintenance") as maintenance:
        kwargs = {"full_load": False}
        optimize = PythonOperator(
            task_id="optimize", python_callable=oltp.maintain, op_kwargs={"action": "optimize", **kwargs}
        )
        expire = PythonOperator(task_id="expire", python_callable=oltp.maintain, op_kwargs={"action": "expire", **kwargs})
        orphan = PythonOperator(
            task_id="orphan",
            python_callable=oltp.maintain,
            op_kwargs={"action": "orphan", **kwargs},
            outlets=[oltp.OLTP_LOADED],
        )
        optimize >> expire >> orphan

    staging >> bronze >> silver
    [silver, wait_full_daily] >> gold >> maintenance
