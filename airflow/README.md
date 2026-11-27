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
├── dags/            # lakehouse_oltp_init, _full_daily, _incremental, _sync, _purge
├── tasks/           # oltp.py: PythonOperator callables; dbt.py: dbt commands and the Gold task group
├── lakehouse/
│   ├── oltp/        # source tables, extract, staging, Bronze, column drift, sync, reconcile, table creation
│   └── utils/       # Iceberg catalog, PostgreSQL SQL, Trino maintenance
├── tests/           # unittest suite (make test)
└── pyproject.toml   # pipeline dependencies, installed into the Airflow image
```

The dbt project is at the repo root in [`dbt/`](../dbt): `models/silver` (19 models), `models/oltp` (Gold), `macros/`, `seeds/`, `tests/`.

## DAGs

| DAG | Schedule | Flow |
|---|---|---|
| `lakehouse_oltp_init` | manual | `create_schema` → `staging` → `reconcile` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_full_daily` | `0 3 * * *` Asia/Ho_Chi_Minh, catchup | Reference tables only: `staging` → `bronze` → `silver` → `gold` (seeds and the dimensions built only from reference data) → `maintenance` |
| `lakehouse_oltp_incremental_daily` | `0 3 * * *` Asia/Ho_Chi_Minh, catchup | Transactional tables only: `staging` → `bronze` → `silver`; `wait_full_daily` waits for the full daily run of the same day; then `gold` → `maintenance`; the last task emits the Asset `lakehouse_oltp_loaded` |
| `lakehouse_oltp_sync` | after each `lakehouse_oltp_loaded` event | `schema` (every table) → `detect` → `bronze` → `silver` (transactional tables) → `gold` → `maintenance` |
| `lakehouse_oltp_purge` | manual | `silver_gold` → `bronze` |

Each group has one task per table or dbt node, so a failure names the table and **Clear** reruns only that task. Python tasks share the pool `lakehouse_oltp` (4 slots), which bounds the memory their batches hold; dbt tasks share `lakehouse_dbt` (8 slots), since Trino does their work. Tasks retry twice after two minutes, and only one run of each DAG is active.

The reference tables (`full_load` in `sources.py`: `categories`, `cities`, `stores`, `coupons`, `delivery_staff`, `products`, `product_variants`) belong to `lakehouse_oltp_full_daily`; the transactional tables to incremental and sync. The two groups write separate tables, and so does their maintenance. Facts look up the reference tables, so incremental's Gold waits for the full daily run of the same day. `max_active_runs` does not span DAGs, so:

- keep full daily, incremental and sync paused while init runs;
- pause sync while incremental catches up several days, then trigger it once by hand: a sync started by one run would run beside the next one and could empty its staging before Bronze reads it;
- trigger purge only when incremental and sync are idle and the last sync succeeded.

## Layers

| Layer | Task | What it does |
|---|---|---|
| Tables | `create_schema` (init) | Creates the `staging`, `bronze`, `silver`, `gold` namespaces, the staging and Bronze tables typed from PostgreSQL. A rerun keeps the data and applies [column changes](#column-changes) like the sync. |
| Staging | `staging.<table>` | Replaces the extract for each table, using a separate repeatable-read source transaction. A server-side cursor fetches 10,000 rows at a time. Iceberg commits batches of 200,000 rows with `_ingested_at`. Init applies `created_at < dag_run.run_after` to every table. Full daily reads every reference row created before `dag_run.run_after`. Incremental reads `updated_at` or `created_at` in the run's interval, projects the configured columns already in Bronze, and fails on a dropped or incompatible column. |
| Reconcile | `reconcile.<table>` (init) | Compares staging with the source at the same cutoff: row count, money totals (`orders`, `payments`, `refunds`) and stock ledger balances. A mismatch fails the task before Bronze changes. |
| Bronze | `bronze.<table>` | Source rows with source types, current version per key, plus `_is_deleted` and `_deleted_at`. Init replaces the table in one commit but keeps the rows already flagged, and flags the rows the source no longer has. Full daily overwrites a reference table in one commit: rows gone from the source are dropped, and an unchanged row keeps its `_ingested_at`, so facts recompute only the keys that changed. Incremental upserts by primary key and never replaces a row with an older cursor. |
| Silver | `silver.<table>` | Reference tables are rebuilt from Bronze. Transactional tables merge the Bronze rows newer than Silver's latest `_ingested_at`; init uses `--full-refresh`. The `matches_bronze` test fails the task if Silver and Bronze keys differ. |
| Gold | `gold.<node>`, `gold.tests` | One dbt task per model or seed, ordered by `ref()` and `live()`. Facts merge the rows whose inputs changed since their latest `_ingested_at`. Dimensions and marts are rebuilt every run. Init uses `--full-refresh`. `DBT_AS_OF` (the run's `run_after`) is Gold's current day. `gold.tests` runs `dbt/tests/oltp` last at `warn` severity. |
| Maintenance | `optimize` → `expire` → `orphan` | Trino table procedures on the DAG's own tables, run as `lakehouse_maintainer`: `optimize` rewrites small files in Bronze, Silver and Gold (staging is replaced on every load), `expire_snapshots` and `remove_orphan_files` keep seven days (Trino's minimum) in every layer. |

Staging is replaced, Bronze is overwritten (reference) or upserted by key (transactional), transactional Silver and the facts merge from their watermarks, and dimensions and marts are rebuilt. Clear a failed task and its affected downstream tasks to resume the run. To reload past data, trigger the init DAG. Sources with a row update that commits after its window can also require re-extraction.

PyIceberg file inspection is skipped for a current snapshot with no manifests. This applies to schema-related file rewrites.

PostgreSQL enforces the source constraints. The pipeline also reconciles the extracts, compares Silver and Bronze keys, and runs the dbt model and cross-table tests.

Gold rules:

- Revenue is recognized on the later of `completed_at` and `paid_at`. Refunds of recognized orders reduce revenue on the refund day.
- Gross profit = net revenue − cost of goods. Returned goods accepted back into stock take their cost back out on the refund day.
- A shipment is on time when its local delivery date is at most `sla_days` (seed `delivery_sla`) after the local order date.

## Deleted rows

Incremental loads read changed rows only, so they never see a row deleted at the source. Design decisions for this and for column changes are in [docs/SYNC_DESIGN.md](../docs/SYNC_DESIGN.md). Reference tables need no detection: full daily overwrites them, so a deleted reference row simply drops out of every layer. For transactional tables the sync DAG does:

1. `detect.<table>` compares every source primary key with Bronze's live keys. Rows gone from the source are flagged in Bronze (`_is_deleted = true`, `_deleted_at` = the sync run's `run_after`, kept on later runs). Keys back at the source, or missing from Bronze, are staged again.
2. `bronze.<table>` upserts the staged rows as live, then Silver and Gold pick the changes up through `_ingested_at` like any other change.

Deleted rows stay as history in Bronze, Silver and the Gold dimensions and facts. A fact drops deleted child rows from its values but keeps their `_ingested_at`, so the change still reaches it. In `fact_inventory_daily`, deleted movements move nothing, and the days before a position's first remaining movement are flagged. Marts read only live rows through the `live()` macro. Anything reading Gold dimensions or facts directly filters `NOT _is_deleted`.

`lakehouse_oltp_purge` deletes the flagged rows for good: Gold dimensions and facts, then Silver (`dbt run-operation purge_deleted`), then Bronze. It has no preconditions.

## Column changes

[`sources.py`](lakehouse/oltp/sources.py) is the allowlist: each table lists the `columns` it loads and the `excluded` ones it leaves out on purpose. Each source table is compared with that list and with Bronze:

| Change | What happens |
|---|---|
| New column in neither list | Incremental ignores it. `schema.<table>` fails until it is added to `columns` (and the Silver model) or to `excluded` |
| Column in `columns`, missing from Bronze | `schema.<table>` adds it to staging and Bronze and sets the Bronze property `lakehouse.reload-pending` in the same commit. For a transactional table, `detect` then stages every source row, and `bronze` replaces the table in one commit, keeping deleted rows; a reference table is filled by the next full daily run. Any full Bronze load or overwrite clears the property |
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
| Trino (maintenance) | User `lakehouse_maintainer` over HTTPS on `trino:8443`, catalog `lakehouse_etl`, `SELECT` only on `staging`, `bronze`, `silver`, `gold`, which is enough for `ALTER TABLE … EXECUTE` but not for writing rows or dropping tables. Password `TRINO_MAINTAINER_PASSWORD`. |

## Run

```bash
# First load:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
# After it succeeds, enable the daily loads and the sync:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_full_daily
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental_daily
```

A manual trigger of the incremental DAG fails at staging, since it has no data interval. Incremental is not backfilled: run the init DAG again to reload past data. To repeat a dbt task by hand, copy `DBT_AS_OF` from the first line of its log:

```bash
docker compose exec -T -e DBT_AS_OF=2026-11-21T03:00:00+07:00 \
  -e DBT_PROJECT_DIR=/opt/project/dbt -e DBT_PROFILES_DIR=/opt/project/dbt \
  -e DBT_TARGET_PATH=/tmp/dbt/manual -e DBT_LOG_PATH=/tmp/dbt/manual \
  airflow-scheduler dbt build --select dim_city
```

## Development

`airflow/` is mounted into the Airflow containers and is on `PYTHONPATH`, so code changes apply without a rebuild. After changing `pyproject.toml`, run `make up` to rebuild the image.

```bash
make test        # unittest suite in the scheduler container
uvx ruff check .
```

The tests use a SQLite-backed PyIceberg catalog and fake PostgreSQL cursors. They do not run Trino or dbt.
