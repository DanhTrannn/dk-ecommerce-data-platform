# OLTP Pipeline

Airflow loads the 19 analytical tables of the PostgreSQL source into Iceberg (Polaris catalog, MinIO storage) and builds Silver and Gold with dbt on Trino. A sync DAG applies reviewed source column changes and carries rows deleted at the source through every layer as soft deletes.

```text
PostgreSQL ──► staging ──► [reconcile] ──► bronze ──► silver (dbt) ──► gold (dbt) ──► maintenance
               PyIceberg    init only      PyIceberg   merge + test     27 models
           └─► schema, detect (sync: columns, deleted and restored keys) ─┘
```

## Layout

```text
airflow/
├── dags/            # lakehouse_oltp_init, _incremental, _sync, _purge
├── tasks/           # oltp.py: PythonOperator callables; dbt.py: dbt commands and the Gold task group
├── lakehouse/
│   ├── oltp/        # source tables, extract, staging, Bronze, column drift, sync, reconcile, table creation
│   └── utils/       # Iceberg catalog, PostgreSQL SQL, maintenance
├── tests/           # unittest suite (make test)
└── pyproject.toml   # pipeline dependencies, installed into the Airflow image
```

The dbt project is at the repo root in [`dbt/`](../dbt): `models/silver` (19 models), `models/oltp` (Gold), `macros/`, `seeds/`, `tests/`.

## DAGs

| DAG | Schedule | Flow |
|---|---|---|
| `lakehouse_oltp_init` | manual | `create_schema` → `staging` → `reconcile` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_incremental` | `0 3 * * *` Asia/Ho_Chi_Minh, catchup | `staging` → `bronze` → `silver` → `gold` → `maintenance`; the last task emits the Asset `lakehouse_oltp_loaded` |
| `lakehouse_oltp_sync` | after each `lakehouse_oltp_loaded` event | `schema` → `detect` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_purge` | manual | `silver_gold` → `bronze` |

Each group has one task per table or dbt node, so a failure names the table and **Clear** reruns only that task. Tasks share the pool `lakehouse_oltp` (4 slots), retry twice after two minutes, and only one run of each DAG is active. `max_active_runs` does not span DAGs, and all four write the same tables, so:

- keep incremental and sync paused while init runs;
- pause sync while backfilling incremental, then trigger it once by hand: a sync started by one backfill run would run beside the next one and could empty its staging before Bronze reads it;
- trigger purge only when incremental and sync are idle and the last sync succeeded.

## Layers

| Layer | Task | What it does |
|---|---|---|
| Tables | `create_schema` (init) | Creates the `staging`, `bronze`, `silver`, `gold` namespaces, the staging and Bronze tables typed from PostgreSQL, and `gold._mart_state`. A rerun keeps the data and applies [column changes](#column-changes) like the sync. |
| Staging | `staging.<table>` | Truncates `staging.<table>`, then extracts in one repeatable-read transaction: a server-side cursor reads 10,000 rows per fetch, and 200,000 rows are written per Iceberg commit, with `_ingested_at`. Init reads every row created before `dag_run.run_after`, so all tables stop at the same moment. Incremental reads rows whose cursor (`updated_at`, or `created_at` for append-only tables) is in the data interval, only the configured columns Bronze already has, and fails on a dropped or incompatible column before writing. |
| Reconcile | `reconcile.<table>` (init) | Compares staging with the source at the same cutoff: row count, money totals (`orders`, `payments`, `refunds`) and stock ledger balances. A mismatch fails the task before Bronze changes. |
| Bronze | `bronze.<table>` | Source rows with source types, current version per key, plus `_is_deleted` and `_deleted_at`. Init replaces the table in one commit but keeps the rows already flagged, and flags the rows the source no longer has. Incremental upserts by primary key and never replaces a row with an older cursor. |
| Silver | `silver.<table>` | dbt incremental merge of the Bronze rows newer than Silver's latest `_ingested_at`; init uses `--full-refresh`. The `matches_bronze` test fails the task if Silver and Bronze keys differ. |
| Gold | `gold.<node>`, `gold.tests` | One dbt task per model or seed, ordered by `ref()` and `live()`. Dimensions and facts merge the rows whose inputs changed since their latest `_ingested_at`. Marts delete and rebuild the days (or customers) touched since their watermark in `gold._mart_state`, which a post-hook advances only after the mart is written. `mart_customer_cohort_monthly` and `mart_inventory_current` are rebuilt. Init uses `--full-refresh`. `DBT_AS_OF` (the run's `run_after`) is Gold's current day. `gold.tests` runs `dbt/tests/oltp` last at `warn` severity. |
| Maintenance | `compact` → `optimize` → `expire` → `orphan` | Compacts Bronze; `optimize` (dbt) rewrites small files of Silver, Gold and `_mart_state`; expires snapshots outside the 20 newest, refs and the last three days; reports orphan files without deleting them. |

Every task can be rerun: staging is truncated, Bronze upserts by key, Silver and Gold merge by key from their watermark, and a mart's watermark only advances once it is written. A retry, a cleared run, the next run after a failure, or a backfill gives the same tables.

PostgreSQL constraints already guarantee keys, required fields and foreign keys, so the pipeline does not check them again.

Gold rules:

- Revenue is recognized on the later of `completed_at` and `paid_at`. Refunds of recognized orders reduce revenue on the refund day.
- Gross profit = net revenue − cost of goods. Returned goods accepted back into stock take their cost back out on the refund day.
- A shipment is on time when its local delivery date is at most `sla_days` (seed `delivery_sla`) after the local order date.

## Deleted rows

Incremental loads read changed rows only, so they never see a row deleted at the source. The sync DAG does:

1. `detect.<table>` compares every source primary key with Bronze's live keys. Rows gone from the source are flagged in Bronze (`_is_deleted = true`, `_deleted_at` = the sync run's `run_after`, kept on later runs). Keys back at the source, or missing from Bronze, are staged again.
2. `bronze.<table>` upserts the staged rows as live, then Silver and Gold pick the changes up through `_ingested_at` like any other change.

Deleted rows stay as history in Bronze, Silver and the Gold dimensions and facts. A fact drops deleted child rows from its values but keeps their `_ingested_at`, so the change still reaches it. In `fact_inventory_daily`, deleted movements move nothing, and the days before a position's first remaining movement are flagged. Marts read only live rows through the `live()` macro, while the macros that choose the days to rebuild still see the flagged rows. Anything reading Gold dimensions or facts directly filters `NOT _is_deleted`.

`lakehouse_oltp_purge` deletes the flagged rows for good: Gold dimensions and facts, then Silver (`dbt run-operation purge_deleted`), then Bronze. It has no preconditions.

## Column changes

[`sources.py`](lakehouse/oltp/sources.py) is the allowlist: each table lists the `columns` it loads and the `excluded` ones it leaves out on purpose. Each source table is compared with that list and with Bronze:

| Change | What happens |
|---|---|
| New column in neither list | Incremental ignores it. `schema.<table>` fails until it is added to `columns` (and the Silver model) or to `excluded` |
| Column in `columns`, missing from Bronze | `schema.<table>` adds it to staging and Bronze and sets the Bronze property `lakehouse.reload-pending` in the same commit. `detect` then stages every source row, and `bronze` replaces the table in one commit, keeping deleted rows. Any full Bronze load clears the property |
| Wider type (`int` → `long`, `float` → `double`, larger decimal precision) | `schema.<table>` widens staging and Bronze |
| Narrower type | Nothing |
| Column in `columns`, gone from the source, or an incompatible type | Incremental, init and sync fail before writing |

`schema.<table>` also logs `silver_missing=[...]` when Bronze has a configured column the Silver table lacks. Bronze keeps dropped columns as history. What to do on each error is in [OPERATIONS.md](../docs/OPERATIONS.md#4-xử-lý-sự-cố).

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
# After it succeeds, enable the daily load and the sync:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
```

Reload past intervals with a backfill. A run's logical date is the start of its interval in UTC: the interval from 03:00 on 2026-11-21 (Asia/Ho_Chi_Minh) has the logical date `2026-11-20T20:00:00+00:00`.

```bash
docker compose exec -T airflow-scheduler airflow dags pause lakehouse_oltp_sync
docker compose exec -T airflow-scheduler airflow backfill create --dag-id lakehouse_oltp_incremental \
  --from-date 2026-11-20T20:00:00+00:00 --to-date 2026-11-22T20:00:00+00:00 \
  --reprocess-behavior completed --max-active-runs 1
# When every backfill run has succeeded:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_sync
```

A manual trigger of the incremental DAG fails at staging, since it has no data interval. Run the init DAG again to reload everything. To repeat a dbt task by hand, copy `DBT_AS_OF` from the first line of its log:

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
