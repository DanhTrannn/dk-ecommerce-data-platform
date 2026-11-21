"""OLTP batch, one task per table and per dbt model: Bronze → checks → Silver → Gold → maintenance.

Daily runs are incremental over the data interval; trigger with {"mode": "full"} for a full load. A failed task
is retried alone; Silver changes only after every check passed, and Gold is rebuilt only after every Silver table.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, TaskGroup

from lakehouse.oltp.dag_plan import DBT_PROJECT, GOLD_TESTS, POOL, command, dbt_command, dbt_env, gold_dependencies
from lakehouse.oltp.sources import SOURCE_TABLES

SEEDS = {path.stem for path in (Path(DBT_PROJECT) / "seeds").glob("*.csv")}


def per_table(module):
    return [
        BashOperator(task_id=table.name, bash_command=command(module, "--table", table.name), pool=POOL)
        for table in SOURCE_TABLES
    ]


with DAG(
    dag_id="lakehouse_oltp",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    params={
        "mode": Param("incremental", enum=["incremental", "full"]),
        "namespace_prefix": Param("serving", type="string"),
    },
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "oltp", "native"],
) as dag:
    with TaskGroup("bronze") as bronze:
        per_table("bronze")

    with TaskGroup("checks") as checks:
        per_table("check") >> BashOperator(task_id="foreign_keys", bash_command=command("check", "--foreign-keys"), pool=POOL)

    with TaskGroup("silver") as silver:
        per_table("silver")

    with TaskGroup("gold") as gold:
        graph = gold_dependencies(Path(DBT_PROJECT))
        tasks = {
            node: BashOperator(
                task_id=node,
                bash_command=dbt_command("seed" if node in SEEDS else "build", node),
                env=dbt_env(node),
                append_env=True,
                pool=POOL,
            )
            for node in graph
        }
        for node, refs in graph.items():
            for ref in refs:
                tasks[ref] >> tasks[node]
        list(tasks.values()) >> BashOperator(
            task_id="tests", bash_command=dbt_command("test", GOLD_TESTS), env=dbt_env("tests"), append_env=True, pool=POOL
        )

    with TaskGroup("maintenance") as maintenance:
        steps = [
            BashOperator(
                task_id=action,
                bash_command='"$PIPELINE_PYTHON" -m jobs.maintenance.iceberg_table_maintenance '
                f"--namespace-prefix '{{{{ params.namespace_prefix }}}}' --profile oltp --action {action}",
            )
            for action in ("compact", "expire", "orphan")
        ]
        steps[0] >> steps[1] >> steps[2]

    bronze >> checks >> silver >> gold >> maintenance
