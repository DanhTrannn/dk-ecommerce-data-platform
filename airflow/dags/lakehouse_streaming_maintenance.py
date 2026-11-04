"""Lakehouse Streaming Maintenance & Data Marts DAG.

Chuyên trách duy trì, tối ưu hóa và tổng hợp dữ liệu cho luồng Pure Streaming:
  1. Healthcheck & giám sát luồng Flink streaming (JobManager REST API).
  2. Iceberg Compaction (rewrite_data_files & rewrite_manifests) gom nhỏ files Parquet.
  3. Expire Snapshots dọn dẹp metadata lịch sử, giảm dung lượng catalog.
  4. Remove Orphan Files dọn dẹp các tệp rác không được tham chiếu trên MinIO.
  Gold mart rollup runs independently in lakehouse_logs_marts.
"""

import os
from datetime import timedelta

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SPARK_APP_MAINTENANCE = "/opt/project/pipelines/src/jobs/maintenance/iceberg_table_maintenance.py"

TABLES_LIST = "landing.access_logs,bronze.web_events,silver.silver_logs,gold.fact_web_events"

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def check_flink_streaming_health():
    import requests

    host = os.environ.get("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
    port = os.environ.get("FLINK_JOBMANAGER_PORT", "8081")
    response = requests.get(f"http://{host}:{port}/jobs/overview", timeout=15)
    response.raise_for_status()
    required = tuple(
        filter(
            None,
            os.environ.get("FLINK_REQUIRED_JOBS", "kafka_to_lakehouse_pure_streaming,oltp_cdc_to_bronze").split(","),
        )
    )
    if not required:
        raise ValueError("FLINK_REQUIRED_JOBS must not be empty")
    running = {job.get("name"): job for job in response.json().get("jobs", []) if job.get("state") == "RUNNING"}
    missing = set(required) - running.keys()
    if missing:
        raise RuntimeError(f"Required streaming jobs are not RUNNING: {sorted(missing)}")
    return [running[name] for name in required]


with DAG(
    dag_id="lakehouse_streaming_maintenance",
    default_args=DEFAULT_ARGS,
    schedule="0 */2 * * *",  # Every 2 hours
    max_active_runs=1,
    catchup=False,
    start_date=pendulum.datetime(2026, 8, 15, tz=VN_TZ),
    description=("Pure Streaming Lakehouse Maintenance DAG: Flink Healthcheck -> Iceberg Compaction & Snapshot Expiry"),
    tags=["lakehouse", "streaming", "maintenance", "iceberg", "compaction"],
) as dag:
    # 1. STREAMING HEALTH MONITORING
    with TaskGroup(
        group_id="stream_monitoring",
        tooltip="Verify Flink streaming pipeline is actively RUNNING",
    ) as tg_stream:
        monitor_flink = PythonOperator(
            task_id="check_flink_streaming_health",
            python_callable=check_flink_streaming_health,
            do_xcom_push=False,
        )

    # 2. ICEBERG COMPACTION & MAINTENANCE
    with TaskGroup(
        group_id="iceberg_maintenance",
        tooltip="Compact small files, rewrite manifests, expire old snapshots & remove orphan files",
    ) as tg_maintenance:
        compact_tables = SparkSubmitOperator(
            task_id="compact_iceberg_tables",
            application=SPARK_APP_MAINTENANCE,
            application_args=[
                "--action",
                "compact",
                "--tables",
                TABLES_LIST,
            ],
        )

        expire_snapshots = SparkSubmitOperator(
            task_id="expire_iceberg_snapshots",
            application=SPARK_APP_MAINTENANCE,
            application_args=[
                "--action",
                "expire",
                "--retain-snapshots",
                "20",
                "--tables",
                TABLES_LIST,
            ],
        )

        remove_orphans = SparkSubmitOperator(
            task_id="remove_orphan_files",
            application=SPARK_APP_MAINTENANCE,
            application_args=[
                "--action",
                "orphan",
                "--tables",
                TABLES_LIST,
            ],
        )

        compact_tables >> expire_snapshots >> remove_orphans

    tg_stream >> tg_maintenance
