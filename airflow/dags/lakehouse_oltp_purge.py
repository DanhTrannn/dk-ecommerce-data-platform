"""Delete for good the rows flagged as deleted: Gold and Silver through dbt, then Bronze.

Trigger it by hand while lakehouse_oltp_incremental_daily and lakehouse_oltp_sync are idle, and only after a sync run
has succeeded: a deleted row purged from Silver before Gold read its flag, or from Gold before every mart read it,
stays live downstream until the next init.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import DBT_POOL, dbt_env

with DAG(
    dag_id="lakehouse_oltp_purge",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "purge"],
) as dag:
    silver_gold = BashOperator(
        task_id="silver_gold",
        bash_command="dbt run-operation purge_deleted",
        env=dbt_env("purge"),
        append_env=True,
        pool=DBT_POOL,
    )

    with TaskGroup("bronze") as bronze:
        for table in SOURCE_TABLES:
            PythonOperator(task_id=table.name, python_callable=oltp.purge, op_kwargs={"table": table.name})

    silver_gold >> bronze
