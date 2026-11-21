"""Full load of the OLTP source, triggered by hand: create the tables, then staging → reconcile → Bronze → Silver → Gold.

Run it once to set the lakehouse up, again after a source column is added, or to reload everything. Each table
reads the source rows created before the run started, so every table stops at the same moment. Keep
lakehouse_oltp_incremental paused while this runs: both write the same tables.
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

    # Stops the run before Bronze and Silver change if the extract is not the whole source at the cutoff.
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
        # --full-refresh rebuilds each Silver table from all of Bronze.
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
