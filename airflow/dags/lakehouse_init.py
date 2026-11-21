"""Create the lakehouse namespaces and the OLTP Bronze/Silver tables, so lakehouse_oltp only loads data.

Trigger it once per namespace prefix before the first lakehouse_oltp run, and again after a source column is added
to the batch. It is safe to repeat: existing tables keep their data and only gain new columns. dbt owns Gold tables.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

from lakehouse.oltp.dag_plan import NAMESPACE_PATTERN, init_command

with DAG(
    dag_id="lakehouse_init",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 15, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    params={"namespace_prefix": Param("serving", type="string", pattern=NAMESPACE_PATTERN)},
    default_args={"owner": "lakehouse"},
    tags=["lakehouse", "oltp", "init"],
) as dag:
    BashOperator(task_id="create_schema", bash_command=init_command())
