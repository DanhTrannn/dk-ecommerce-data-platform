"""Load the whole OLTP source: create the tables, then staging, reconcile, Bronze, Silver and Gold.

Trigger it by hand to set the lakehouse up, after a source column is added, or to reload everything. Keep
lakehouse_oltp_incremental paused while it runs; both DAGs write the same tables.
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
    dag_id="lakehouse_oltp_init",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "full"],
) as dag:
    create_schema = PythonOperator(task_id="create_schema", python_callable=oltp.create_schema)

    with TaskGroup("staging") as staging:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.staging_full, op_kwargs={"table": table.name}, pool=POOL
            )

    # Runs before Bronze so that an incomplete extract never replaces Bronze, Silver or Gold.
    with TaskGroup("reconcile") as reconcile:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.reconcile, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("bronze") as bronze:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=table.name, python_callable=oltp.bronze_full, op_kwargs={"table": table.name}, pool=POOL
            )

    with TaskGroup("silver") as silver:
        for table in SOURCE_TABLES:
            BashOperator(
                task_id=table.name,
                bash_command=dbt_command("build", table.name) + " --full-refresh",
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

    create_schema >> staging >> reconcile >> bronze >> silver >> gold >> maintenance
