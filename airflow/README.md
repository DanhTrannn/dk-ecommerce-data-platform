# OLTP Pipeline

Airflow loads the 19 analytical tables of the PostgreSQL source into Iceberg (Polaris catalog, MinIO storage) and builds Silver and Gold with dbt on Trino.

```text
PostgreSQL ──► staging ──► [reconcile] ──► bronze ──► silver (dbt) ──► gold (dbt) ──► maintenance
               PyIceberg    init only      PyIceberg   merge + test     27 models
```

## Layout

```text
airflow/
├── dags/            # lakehouse_oltp_init, lakehouse_oltp_incremental
├── tasks/           # oltp.py: PythonOperator callables; dbt.py: dbt commands and the Gold task group
├── lakehouse/
│   ├── oltp/        # source tables, extract, staging, Bronze, reconcile, table creation
│   └── utils/       # Iceberg catalog, PostgreSQL SQL, maintenance
├── tests/           # unittest suite (make test)
└── pyproject.toml   # pipeline dependencies, installed into the Airflow image
```

The dbt project is at the repo root in [`dbt/`](../dbt): `models/silver` (19 models), `models/oltp` (Gold), `seeds/`, `tests/`.

## DAGs

| DAG | Schedule | Flow |
|---|---|---|
| `lakehouse_oltp_init` | manual | `create_schema` → `staging` → `reconcile` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_incremental` | `0 3 * * *` Asia/Ho_Chi_Minh, catchup | `staging` → `bronze` → `silver` → `gold` → `maintenance` |

Each group has one task per table or dbt node, so a failure names the table and **Clear** reruns only that task. Tasks share the pool `lakehouse_oltp` (4 slots), retry twice after two minutes, and only one run of each DAG is active. Keep the incremental DAG paused while the init DAG runs: both write the same tables.

## Layers

| Layer | Task | What it does |
|---|---|---|
| Tables | `create_schema` (init) | Creates the `staging`, `bronze`, `silver`, `gold` namespaces and the staging and Bronze tables, typed from PostgreSQL. A rerun adds new source columns and keeps the data. |
| Staging | `staging.<table>` | Truncates `staging.<table>`, then extracts in one repeatable-read transaction: a server-side cursor reads 10,000 rows per fetch, and 200,000 rows are written per Iceberg commit, with `_ingested_at`. Init reads every row created before `dag_run.run_after`, so all tables stop at the same moment. Incremental reads rows whose cursor (`updated_at`, or `created_at` for append-only tables) is in the data interval. |
| Reconcile | `reconcile.<table>` (init) | Compares staging with the source at the same cutoff: row count, money totals (`orders`, `payments`, `refunds`) and stock ledger balances. A mismatch fails the task before Bronze changes. |
| Bronze | `bronze.<table>` | Source rows with source types, current version per key. Init replaces the table in one commit; incremental upserts by primary key and never replaces a row with an older cursor. |
| Silver | `silver.<table>` | dbt incremental merge of the Bronze rows newer than Silver's latest `_ingested_at`; init uses `--full-refresh`. A post-hook runs `optimize`. The `matches_bronze` test fails the task if Silver and Bronze keys differ. |
| Gold | `gold.<node>`, `gold.tests` | One dbt task per model or seed, ordered by `ref()`; tables are rebuilt with `CREATE OR REPLACE`. `DBT_AS_OF` (the run's `run_after`) is Gold's current day. `gold.tests` runs `dbt/tests/oltp` last at `warn` severity. |
| Maintenance | `compact` → `expire` → `orphan` | Compacts staging, Bronze and Gold (Silver is optimized by dbt); expires snapshots outside the 20 newest, refs and the last three days; reports orphan files without deleting them. |

PostgreSQL constraints already guarantee keys, required fields and foreign keys, so the pipeline does not check them again.

Gold rules:

- Revenue is recognized on the later of `completed_at` and `paid_at`. Refunds of recognized orders reduce revenue on the refund day.
- Gross profit = net revenue − cost of goods. Returned goods accepted back into stock take their cost back out on the refund day.
- A shipment is on time when its local delivery date is at most `sla_days` (seed `delivery_sla`) after the local order date.

## Connections and credentials

| Resource | How the pipeline reaches it |
|---|---|
| PostgreSQL source | Airflow connection `oltp_postgres`, created by `airflow-init` from `POSTGRES_ECOMMERCE_DB` and `POSTGRES_ECOMMERCE_READER_*` on first start; edit it later in the Airflow UI. Its Extra makes the session read-only and UTC. The reader role comes from `make source-reader`. |
| Polaris and MinIO | Principal credential in `/run/polaris/clients.env` (workload `batch-airflow`, assigned in Streamlit). Polaris vends per-table storage credentials. |
| Trino (dbt) | User `dbt_writer` over HTTPS on `trino:8443`, catalog `lakehouse_etl`, allowed to write the `silver` and `gold` schemas. Password `DBT_TRINO_PASSWORD`. |

## Run

```bash
# First load:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
# After it succeeds, enable the daily load:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
# Reload a past range:
docker compose exec -T airflow-scheduler airflow backfill create --dag-id lakehouse_oltp_incremental \
  --from-date 2026-11-21 --to-date 2026-11-23
```

Run the init DAG again after a source column is added, or to reload everything. To repeat a dbt task by hand, copy `DBT_AS_OF` from the first line of its log:

```bash
docker compose exec -T -e DBT_AS_OF=2026-11-21T03:00:00+07:00 \
  -e DBT_PROJECT_DIR=/opt/project/dbt -e DBT_PROFILES_DIR=/opt/project/dbt \
  -e DBT_TARGET_PATH=/tmp/dbt/manual -e DBT_LOG_PATH=/tmp/dbt/manual \
  airflow-scheduler dbt build --select dim_city
```

## Development

`airflow/` is mounted into the Airflow containers and is on `PYTHONPATH`, so code changes apply without a rebuild. Changing `pyproject.toml` needs a new image (`make build-all`).

```bash
make test        # unittest suite in the scheduler container
uvx ruff check .
```

The tests use a SQLite-backed PyIceberg catalog and fake PostgreSQL cursors. They do not run Trino or dbt.
