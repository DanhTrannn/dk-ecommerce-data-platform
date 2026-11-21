"""OLTP batch DAGs, one task per table and per dbt model: Bronze → checks → Silver → Gold → maintenance.

- lakehouse_oltp_init: trigger by hand to set the lakehouse up. It creates the namespaces and the Bronze/Silver
  tables, then loads every source row created before the run started. Trigger it again after a source column is
  added to the batch, or to reload everything.
- lakehouse_oltp_incremental: daily at 03:00, each run loads the rows whose cursor falls in its data interval.
  Catchup runs the intervals missed while the DAG was paused; `airflow backfill create --dag-id
  lakehouse_oltp_incremental --from-date ... --to-date ...` reloads a past range.

Both write the same tables: keep the incremental DAG paused while the init DAG runs. A failed task is retried
alone; Silver changes only after every check passed, and Gold is rebuilt only after every Silver table.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from lakehouse.oltp.sources import SOURCE_TABLES
from tasks import oltp
from tasks.dbt import GOLD_TESTS, dbt_command, dbt_env, gold_dependencies, gold_seeds

POOL = "lakehouse_oltp"  # 4 slots (docker-compose.yml); each task holds a table's batches in memory


def oltp_dag(dag_id, *, mode, schedule, catchup):
    """The load graph both DAGs share; the full one also creates the tables and checks foreign keys."""
    full = mode == "full"
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 11, 21, tz="Asia/Ho_Chi_Minh"),
        catchup=catchup,
        max_active_runs=1,
        default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
        tags=["lakehouse", "oltp", mode],
    ) as dag:
        with TaskGroup("bronze") as bronze_group:
            for table in SOURCE_TABLES:
                PythonOperator(
                    task_id=table.name,
                    python_callable=oltp.bronze,
                    op_kwargs={"table": table.name, "mode": mode},
                    pool=POOL,
                )

        with TaskGroup("checks") as checks_group:
            table_checks = [
                PythonOperator(
                    task_id=table.name,
                    python_callable=oltp.check,
                    op_kwargs={"table": table.name, "mode": mode},
                    pool=POOL,
                )
                for table in SOURCE_TABLES
            ]
            if full:
                table_checks >> PythonOperator(task_id="foreign_keys", python_callable=oltp.foreign_keys, pool=POOL)

        with TaskGroup("silver") as silver_group:
            for table in SOURCE_TABLES:
                PythonOperator(
                    task_id=table.name,
                    python_callable=oltp.silver,
                    op_kwargs={"table": table.name, "mode": mode},
                    pool=POOL,
                )

        with TaskGroup("gold") as gold_group:
            graph, seeds = gold_dependencies(), gold_seeds()
            models = {
                node: BashOperator(
                    task_id=node,
                    bash_command=dbt_command("seed" if node in seeds else "build", node),
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
                task_id="tests",
                bash_command=dbt_command("test", GOLD_TESTS),
                env=dbt_env("tests"),
                append_env=True,
                pool=POOL,
            )

        with TaskGroup("maintenance") as maintenance_group:
            compact, expire, orphan = (
                PythonOperator(task_id=action, python_callable=oltp.maintain, op_kwargs={"action": action})
                for action in ("compact", "expire", "orphan")
            )
            compact >> expire >> orphan

        bronze_group >> checks_group >> silver_group >> gold_group >> maintenance_group
        if full:
            PythonOperator(task_id="create_schema", python_callable=oltp.create_schema) >> bronze_group
    return dag


init_dag = oltp_dag("lakehouse_oltp_init", mode="full", schedule=None, catchup=False)

# Catchup runs every interval from the start date on; rows created before the init DAG's full load are already loaded.
incremental_dag = oltp_dag("lakehouse_oltp_incremental", mode="incremental", schedule="0 3 * * *", catchup=True)
