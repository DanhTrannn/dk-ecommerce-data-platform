# Changelog

Notable changes to this project. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project will use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from its first release. There is no tagged release yet, so changes are grouped by milestone under **Unreleased**.

## [Unreleased]

### 2026-11-24: Compose startup, retail BI and Iceberg maintenance

- Compose now starts the full stack with `make up`. `make up SERVICES=platform-app` starts the Console and API with their dependencies for first-time access setup. Superset metadata initializes during startup.
- Removed Kafka, Flink, their Compose profiles and the API Kafka producer. FastAPI writes structured access logs to stdout. The Parquet log generator and historical SQL remain available for research and design.
- Added four Superset dashboards for retail sales, operations, POS and customer and marketing analysis. `make superset-sync` creates or updates the assets over the current OLTP Gold marts. The connection passes HTTPS and CA verification to Trino through SQLAlchemy.
- Superset uses a separate session cookie so opening it alongside Airflow preserves both login forms.
- Iceberg compaction, schema-related rewrites and orphan scans handle snapshots that contain no manifests.

### 2026-11-22: Incremental Gold, delete sync and schema sync

#### Added

- `lakehouse_oltp_sync`, run after each incremental load through the Asset `lakehouse_oltp_loaded`. It compares every source primary key with Bronze, soft-deletes rows gone from the source (`_is_deleted`, `_deleted_at`) and reloads rows that came back.
- Delete flags in Bronze, Silver and the Gold dimensions and facts. Marts read only live rows through the `live()` macro.
- `lakehouse_oltp_purge`, a manual DAG that hard-deletes the flagged rows from Gold, Silver and Bronze.
- `gold._mart_state`, each mart's watermark and last built day, created by the init DAG.
- The `100k` OLTP preset for quick pipeline runs.
- Source column changes. `sources.py` lists the loaded `columns` and the `excluded` ones. A `schema` group in `lakehouse_oltp_sync` adds approved columns to staging and Bronze, reloads the table to fill them, widens types, and fails on unreviewed new columns. Dropped columns and incompatible types stop every DAG with a clear error.

#### Changed

- Gold dimensions and facts merge the rows whose inputs changed, instead of being rebuilt on every run. Facts keep `_previous_dates` so marts also rebuild the days a row moved away from.
- Marts delete and rebuild only the days, or customers, touched since their watermark. The watermark advances only after the mart is written, so a failed run is picked up by the next one. `mart_customer_cohort_monthly` and `mart_inventory_current` are still rebuilt.
- An init load keeps the rows already flagged as deleted and flags rows the source no longer has, so delete history survives a full reload.
- Maintenance compacts Bronze only. A new `optimize` task runs `ALTER TABLE … EXECUTE optimize` on Silver, Gold and `_mart_state`, replacing the Silver post-hook.
- Backfilling the incremental DAG requires pausing the sync DAG and triggering it once afterwards.
- Access administration covers Polaris only. A one-shot `minio-init` service creates the bucket and the storage account Polaris uses. Writes no longer carry a revision or read back their outcome; each one is audited before and after.
- Incremental loads read only the configured columns Bronze already has. Init no longer loads new source columns that are not approved.

#### Removed

- MinIO administration in the API and Streamlit, and the unused `MINIO_BATCH_*`, `MINIO_FLINK_*`, `MINIO_TRINO_*` variables.

### 2026-11-21: OLTP pipeline redesign

#### Added

- Two explicit DAGs: `lakehouse_oltp_init` (manual full load that creates the tables) and `lakehouse_oltp_incremental` (daily at 03:00, catchup and `airflow backfill`).
- A `staging` layer, truncated and re-extracted on every load, so every task can be rerun safely.
- A `reconcile` task group in the init DAG. It compares the extract with PostgreSQL at the run's cutoff (row counts, money totals, stock ledger) before Bronze changes.
- Silver built by dbt incremental merge models, with a `matches_bronze` test that fails when Silver and Bronze keys differ.
- Storage credentials vended per table by Polaris, so no workload holds MinIO keys.
- Source reads through the Airflow connection `oltp_postgres` (PostgresHook) with a server-side cursor.
- `make test` runs every test suite. All tests are tracked in git and run offline.
- `CHANGELOG.md`.

#### Changed

- The pipeline moved to `airflow/` (`dags/`, `tasks/`, `lakehouse/`, `tests/`) and dbt to `dbt/` at the repo root. The library is installed into Airflow's own Python.
- Bronze keeps the current source row per key with source types: the init load replaces it, the incremental load upserts it.
- Gold models read Silver through `ref()`. The Gold task graph follows each model's `ref()` calls.
- Maintenance compacts staging, Bronze and Gold. dbt optimizes Silver after each merge.
- Documentation rewritten to describe the current state briefly. File names in `docs/` are uppercase.
- Docstrings and comments kept only where needed, following PEP 257 and PEP 8.

#### Removed

- The single DAG with a `mode` parameter, run IDs, and the namespace prefix for trial runs.
- Pre-merge row rules and the duplicate-key and foreign-key checks, since PostgreSQL constraints already enforce them.
- The access-log pipeline: the Flink job, the log DAGs and the publication manifests. The designs are kept in `streaming/sql` and `dbt/models/logs` for the redesign.
- Opt-in tests that needed a dedicated PostgreSQL, MinIO or Polaris stack.
- Unused dependencies, folders and environment variables (`trino`, `airflow/logs`, `POSTGRES_SOURCE_*`).

#### Fixed

- Gross profit subtracts refunds and restores the cost of returned goods.
- Delivery SLA is set per city from carrier zones plus handling days, and the backlog age buckets use it.

### 2026-11-20: Gold rebuilt with dbt

#### Changed

- OLTP Gold rebuilt as 7 dimensions, 6 facts, 14 marts and 2 seeds with dbt on Trino.
- Superset and the API read the current Gold tables directly.
- The batch loads 19 source tables. Silver pseudonymization was replaced by analytical projections.

### 2026-11-15: Platform consolidation

#### Changed

- Lakehouse access administration merged into the `platform-app` API at `/api/v1/access`.
- Simpler startup commands (`make up`, `make core`, `make stream`, `make bi`).

#### Removed

- The legacy Next.js storefront and its CI job.
- The preset lakehouse provisioning flow. Access is now created explicitly in Streamlit.

### 2026-11-12 to 2026-11-14: Data generator

#### Added

- `datagen generate`, `verify` and `reset` for 12 months of OLTP history from the `1m`/`10m`/`50m` presets, loaded in one PostgreSQL transaction with indexes rebuilt afterwards.
- `datagen logs` for synthetic access logs of the sale season, written as daily Parquet files.
- Streamlit management of MinIO and Polaris access and of workload credentials.

### 2026-11-04 to 2026-11-11: PostgreSQL source and native lakehouse

#### Added

- PostgreSQL source schema rendered from the ORM, with ported generator imports.
- Streamlit console for online purchase scenarios against the API.
- Polars and PyIceberg ingestion of the source tables, Gold quality gates and access-log marts.
- Superset sales dashboard proof of concept.

### 2026-07-24 to 2026-11-03: Initial platform

#### Added

- FastAPI source application, MinIO and Polaris with per-service identities and least-privilege policies, Trino, Airflow and the first batch and CDC pipelines.
