# Delete Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect rows deleted at the OLTP source and soft-delete them in Bronze, Silver and Gold; add a manual DAG that hard-deletes them.

**Architecture:** A new `lakehouse_oltp_sync` DAG runs after each successful incremental run (Airflow Asset). Per table it diffs source primary keys with Bronze live keys:
- it flags vanished rows in Bronze;
- it re-extracts keys that came back;
- then it reuses the incremental Bronze, Silver, Gold and maintenance steps.

Flags travel through dbt like any other change (`_ingested_at`). Gold keeps flagged rows so marts still recompute their days; marts read only live rows.

**Tech Stack:** Airflow 3.3 (PythonOperator, BashOperator, Asset), PyIceberg 0.12, polars, dbt-trino 1.11, Trino.

**Spec:** `docs/DELETE_SYNC_DESIGN.md`

## Global Constraints

- Source DB is read-only; no triggers or new source tables.
- Columns: `_is_deleted boolean`, `_deleted_at timestamp(6) with time zone`. They live in Bronze, Silver, and Gold dims and facts; not in staging or marts.
- `_deleted_at` is the detection time (`dag_run.run_after` of the sync run) and is never overwritten once set.
- Rows loaded from staging (incremental, init, restored) carry `_is_deleted = false` and `_deleted_at = NULL`.
- Init keeps tombstones: a full load never erases delete history.
- Every task is idempotent: retry, clear, a failed run followed by the next run, and backfill all give the same tables.
- Code rules (CLAUDE.md):
  - PythonOperator with PostgresHook; BashOperator only for dbt.
  - No TaskFlow, custom operators or custom hooks.
  - Imports at the top of the file.
  - Docstrings and comments only where needed (PEP 257, PEP 8).
  - No new dependency.
- Do not edit README, `docs/` (other than the design doc in Task 2) or `.env.example`; list drifted docs in the final report.
- Commits follow Conventional Commits, on branch `leekhoa`, with no Claude attribution.
- Run host Python with `python3 -B`.
- Pipeline tests run in the scheduler container. Below, `TEST <module>` means:
  `docker compose exec -T -w /tmp -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/opt/project/airflow:/opt/project/airflow/tests airflow-scheduler python -m unittest <module> -v`

## Design Correction

The design (§5.2 step 5) stages deleted keys in one shared table, `staging._deleted_keys`. Nineteen detect tasks appending to one Iceberg table in parallel conflict on commit; `_mart_state` failed the same way. So each detect task flags rows directly in its own Bronze table, and that table has one writer. Task 2 updates the design doc to match.

The design (§5.7) also has the purge refuse while incremental or sync is running, or while Gold has not consumed the newest Silver tombstone. The user dropped both checks: the purge only finds and deletes flagged rows, and the operator triggers it by hand when the pipeline is idle. Task 7 updates §5.7.

## Review Focus

1. **A deleted child row inside a Gold aggregate** (an order item of a live order). The order's totals must drop it, and `fact_order._ingested_at` must still advance past the tombstone; otherwise the same order is merged again on every run. Task 5 pins it.
2. **A row deleted, then restored with the same `updated_at`.** It must come back live. `filter_current` would otherwise see "no change" and drop it. Task 1 pins it.
3. **Init after deletions.** Tombstones survive, and rows missing from the source are flagged rather than erased. Task 1 pins it.
4. **Existing Bronze rows written before this change** have `_is_deleted = NULL`. Silver coalesces it to false. Task 4 pins it.
5. **Purge run twice.** The second run must delete nothing and fail on nothing. Task 7 pins it.

---

### Task 1: Bronze delete flags

**Files:**
- Modify: `airflow/lakehouse/oltp/bronze.py`
- Modify: `airflow/lakehouse/oltp/schema.py` (`create_staging_and_bronze`)
- Test: `airflow/tests/test_bronze.py`, `airflow/tests/test_schema.py`

**Interfaces:**
- Produces:
  - `DELETE_FIELDS: tuple[pa.Field, pa.Field]` in `bronze.py`, the two columns in the order above.
  - `live_keys(catalog, table: TableSpec) -> pa.Array`: primary keys of Bronze rows where `_is_deleted` is not true. NULL counts as live.
  - `mark_deleted(catalog, table: TableSpec, keys: pa.Array, deleted_at: datetime, ingested_at: datetime) -> int`: upserts the Bronze rows of `keys` with the flag set, and returns the number of rows flagged.
  - `merge_bronze(catalog, table, *, full)`: signature unchanged.
- `create_staging_and_bronze(catalog, table, schema)` adds `DELETE_FIELDS` to the Bronze schema only.

- [ ] **Step 1: Write the failing tests.** In `BronzeTests`, with the `self.load` helper:
  - `test_loaded_rows_are_live`: after a full load, every row has `_is_deleted is False` and `_deleted_at is None`.
  - `test_mark_deleted_flags_rows_and_keeps_the_first_detection_time`:
    1. Full load of `category(1), category(2)`.
    2. `mark_deleted(..., pa.array([1]), AT, AT)` returns 1, and row 1 has `_is_deleted True, _deleted_at == AT`.
    3. `live_keys` returns `[2]`.
    4. A second call with `AT + timedelta(days=1)` for key 1: `live_keys` is unchanged, and `_deleted_at` is still `AT`. The second call is not given key 1 by detect; the test proves `mark_deleted` keeps an existing `_deleted_at`.
  - `test_restored_row_with_the_same_cursor_comes_back_live`: full load `category(1)`; `mark_deleted` key 1; incremental load of the identical `category(1)` writes 1 row; row 1 is live again.
  - `test_full_load_keeps_tombstones_and_flags_rows_missing_from_the_source`:
    1. Full load of `category(1), category(2), category(3)`, then `mark_deleted` key 1.
    2. Full load of `category(3)` only.
    3. Rows 1, 2 and 3 all exist. 1 and 2 are flagged; 1 keeps its `_deleted_at` and 2 gets the load's `ingested_at`. 3 is live.
  - In `test_schema.py`: Bronze columns end with `"_ingested_at", "_is_deleted", "_deleted_at"`, and staging columns end with `"_ingested_at"` only. Update the existing column-list assertion.
- [ ] **Step 2: Run** `TEST test_bronze test_schema`. Expected: the new tests FAIL (`ImportError: cannot import name 'mark_deleted'` or column mismatch).
- [ ] **Step 3: Implement.**
  - `merge_bronze` sets `_is_deleted = false` and `_deleted_at = NULL` on staged rows before aligning them.
  - `filter_current` adds `_is_deleted` to the compared columns, so a flag change counts as a change.
  - Full load: before replacing, anti-join current Bronze against staging on `pk`. Rows absent from staging are written along with staging as tombstones. Rows already flagged keep their flag and `_deleted_at`; live ones get `_is_deleted = true`, `_deleted_at = ingested_at`.
  - `mark_deleted` reads the current rows of `keys`, sets `_is_deleted = true`, `_deleted_at = coalesce(existing, deleted_at)`, `_ingested_at = ingested_at`, and upserts on `pk`.
  - `ingested_at` for a full load is the staging rows' `_ingested_at`.
- [ ] **Step 4: Run** `TEST test_bronze test_schema test_staging`. Expected: all PASS.
- [ ] **Step 5: Commit:** `feat: flag deleted rows in Bronze and keep them on full loads`

### Task 2: Detect deleted and restored keys

**Files:**
- Modify: `airflow/lakehouse/utils/postgres.py`, `airflow/lakehouse/oltp/source.py`
- Create: `airflow/lakehouse/oltp/sync.py`
- Test: `airflow/tests/test_sync.py`, `airflow/tests/test_postgres.py`
- Modify: `docs/DELETE_SYNC_DESIGN.md` §5.2 (Design Correction)

**Interfaces:**
- Consumes: `live_keys`, `mark_deleted` (Task 1); `write_staging`, `source_read_transaction`.
- Produces:
  - `source_keys_query(table) -> str`: `SELECT "<pk>" FROM "public"."<table>"`.
  - `source_query(table, ..., keys=False)`: with `keys=True`, appends `WHERE "<pk>" = ANY(%(keys)s)`. It cannot be combined with an interval or a cutoff; raise `ValueError` if it is.
  - `source_keys(connection, table) -> pa.Array` (int64), read with a named cursor in chunks of `CHUNK_ROWS`.
  - `source_chunks(connection, table, *, keys: list[int] | None = None)` gains the `keys` mode.
  - `detect_deletes(catalog, connection, table, detected_at: datetime) -> dict[str, int]` returns `{"deleted": n, "restored": m}` and prints `SYNC table=<name> deleted=<n> restored=<m>`.

- [ ] **Step 1: Write the failing tests.**
  - `test_postgres.py`:
    - `source_keys_query(TABLES["orders"]) == 'SELECT "order_id" FROM "public"."orders"'`;
    - `source_query(orders, keys=True)` ends with `WHERE "order_id" = ANY(%(keys)s)`;
    - combining `keys=True` with `created_before` raises `ValueError`.
  - `test_sync.py`: use a fake connection, modelled on `Source` in `test_schema.py`, that returns given key rows and full rows, plus the Iceberg fixture. Bronze holds categories 1, 2 and 3, with 3 flagged; the source holds 2, 3 and 4.
    - `test_detect_flags_vanished_rows_and_stages_returned_ones`: the result is `{"deleted": 1, "restored": 2}`; Bronze row 1 is flagged with `_deleted_at == detected_at`; staging holds keys `[3, 4]`.
    - `test_detect_rerun_changes_nothing`: after `merge_bronze(full=False)`, a second `detect_deletes` returns `{"deleted": 0, "restored": 0}`.
- [ ] **Step 2: Run** `TEST test_postgres test_sync`. Expected: FAIL (`ModuleNotFoundError: lakehouse.oltp.sync`).
- [ ] **Step 3: Implement `detect_deletes`.**
  1. Inside one `source_read_transaction`, read the source keys and the Bronze live keys.
  2. `deleted = live - source`; `restored = source - live`. Use a polars anti-join on int64 columns.
  3. `mark_deleted(deleted, detected_at, now)`.
  4. `write_staging` with `source_chunks(keys=restored)`, in lots of `CHUNK_ROWS` keys. Always write, so staging is truncated even when nothing was restored.
- [ ] **Step 4: Run** `TEST test_postgres test_sync test_source`. Expected: PASS.
- [ ] **Step 5: Update `docs/DELETE_SYNC_DESIGN.md`.** In §5.2, replace step 5 and the `bronze` bullet with: detect flags deleted rows directly in its table's Bronze, because one writer per table avoids commit conflicts; restored rows go to staging, and the bronze task upserts them.
- [ ] **Step 6: Commit:** `feat: detect rows deleted or restored at the source`

### Task 3: Sync DAG

**Files:**
- Modify: `airflow/tasks/oltp.py`, `airflow/dags/lakehouse_oltp_incremental.py`
- Create: `airflow/dags/lakehouse_oltp_sync.py`
- Test: `airflow/tests/test_dags.py`, `airflow/tests/test_tasks.py`

**Interfaces:**
- Consumes: `detect_deletes` (Task 2), `oltp.bronze_incremental`, `oltp.maintain`, `gold_task_group`, `dbt_command`, `dbt_env`.
- Produces:
  - `OLTP_LOADED = Asset("lakehouse_oltp_loaded")` in `tasks/oltp.py`.
  - `detect(table, dag_run)`: calls `detect_deletes` with `detected_at = dag_run.run_after`.

- [ ] **Step 1: Write the failing tests** in `test_dags.py`:
  - `lakehouse_oltp_sync` imports with no errors;
  - its schedule is `[OLTP_LOADED]`; `max_active_runs == 1`; `catchup is False`;
  - each table has `detect.<t>` upstream of `bronze.<t>`, which is upstream of `silver.<t>`; the `gold` group and `maintenance.orphan` exist;
  - in `lakehouse_oltp_incremental`, `maintenance.orphan.outlets == [OLTP_LOADED]`.
  In `test_tasks.py`, `detect` passes `dag_run.run_after` as `detected_at` (patch `detect_deletes`).
- [ ] **Step 2: Run** `TEST test_dags test_tasks`. Expected: FAIL (DAG `lakehouse_oltp_sync` missing).
- [ ] **Step 3: Implement.** The DAG mirrors `lakehouse_oltp_incremental`: same pool, `default_args` and maintenance group, with `detect` in place of `staging`. Its docstring states the rule: it runs after each incremental run, and both DAGs write the same tables.
- [ ] **Step 4: Run** `TEST test_dags test_tasks`, then `docker compose exec -T airflow-dag-processor airflow dags reserialize` and `docker compose exec -T airflow-scheduler airflow dags list-import-errors`. Expected: PASS, and `No data found`.
- [ ] **Step 5: Commit:** `feat: add the lakehouse_oltp_sync DAG`

### Task 4: Silver carries the flags

**Files:**
- Modify: the 19 files `dbt/models/silver/*.sql`, `dbt/dbt_project.yml`

- [ ] **Step 1: Add these two lines before `_ingested_at` in every Silver model:**
  ```sql
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  ```
- [ ] **Step 2: Set `+on_schema_change: append_new_columns`** under `models.retail_lakehouse.silver` in `dbt_project.yml`, so existing incremental Silver tables gain the columns.
- [ ] **Step 3: Verify.** `docker compose exec -T airflow-scheduler sh -c 'cd /opt/project/dbt && dbt parse'` is clean, and `make test` passes.
- [ ] **Step 4: Commit:** `feat: carry delete flags through Silver`

### Task 5: Gold dimensions and facts

**Files:**
- Modify: `dbt/macros/incremental.sql`
- Modify: the `dbt/models/oltp/dim_*.sql` files except `dim_date`
- Modify: `fact_order.sql`, `fact_order_item.sql`, `fact_return.sql`, `fact_review.sql`, `fact_shipment.sql`, `fact_inventory_daily.sql`

**Interfaces:**
- Produces: macro `live(model)` renders `(SELECT * FROM {{ ref(model) }} WHERE NOT _is_deleted)`. Task 6 uses it.

Rules, model by model:

| Model | Grain (flags come from) |
|---|---|
| `dim_city` | `cities` |
| `dim_customer` | `customers` |
| `dim_product` | `products` |
| `dim_store` | `stores` |
| `dim_variant` | `product_variants` |
| `dim_staff` | `customers` (sales staff) or `delivery_staff` (delivery staff) |
| `fact_order` | `orders` |
| `fact_order_item` | `order_items` |
| `fact_return` | `return_items` |
| `fact_review` | `product_reviews` |
| `fact_shipment` | `shipments` |

- Each model outputs `<grain>._is_deleted` and `<grain>._deleted_at`, just before `_ingested_at`.
- `changed` CTEs keep plain `ref()`: a tombstone has a new `_ingested_at`, so it is detected.
- Every other read of a Silver table is a lookup or a child:
  - a lookup or join that supplies values uses `live()`;
  - a child aggregate keeps `ref()`, filters the value aggregates with `FILTER (WHERE NOT _is_deleted)`, and leaves `max(_ingested_at)` unfiltered (Review Focus 1).
- `fact_inventory_daily`:
  - movement quantities sum only live ledger rows;
  - `_is_deleted` is true for every row of a position with no live movement (a window `count(*) FILTER (WHERE NOT _is_deleted)` over the position, equal to 0);
  - `_deleted_at` is NULL.
- `dim_date` is unchanged.

- [ ] **Step 1: Add `live()`** and make the model edits above.
- [ ] **Step 2: Validate on the live stack.** On the 100k lakehouse after Task 4's Silver, compile every Gold model on its live incremental branch and on `--full-refresh`. Count stages with `EXPLAIN (TYPE DISTRIBUTED)` exactly as in the earlier validation. Expected: every statement plans, and each is under 150 stages.
- [ ] **Step 3: Check Review Focus 1 with SQL.** Delete one `order_items` row of a completed order in the source; run incremental, then sync. Expected:
  - `fact_order_item` has the row with `_is_deleted = true`;
  - the order's `fact_order` totals exclude it;
  - `fact_order._ingested_at` is at least the tombstone's Silver `_ingested_at`;
  - a second sync merges zero `fact_order` rows (`dbt` log `MERGE 0`, or an unchanged checksum).
- [ ] **Step 4: Commit:** `feat: carry delete flags into Gold dimensions and facts`

### Task 6: Marts read live rows

**Files:**
- Modify: `dbt/models/oltp/mart_*.sql` (all 14)

- [ ] **Step 1: Replace reads in the mart models.** In every mart model's own SQL, replace `ref('fact_…')`, `ref('dim_…')` and Silver refs (`refunds`, `payments`) with `live('…')`. Leave `dbt/macros/mart_days.sql` on plain `ref()`, so tombstones still name the days to recompute.
- [ ] **Step 2: Validate** with the same compile and `EXPLAIN` as Task 5 Step 2. Expected: every statement plans, under 150 stages.
- [ ] **Step 3: Commit:** `feat: build marts from live Gold rows`

### Task 7: Purge DAG

**Files:**
- Create: `dbt/macros/purge.sql`, `airflow/dags/lakehouse_oltp_purge.py`
- Modify: `airflow/lakehouse/oltp/bronze.py`, `airflow/tasks/oltp.py`
- Test: `airflow/tests/test_bronze.py`, `airflow/tests/test_dags.py`

**Interfaces:**
- Produces:
  - dbt macro `purge_deleted()`;
  - `purge_bronze(catalog, table) -> int` in `bronze.py`;
  - `purge(table)` in `tasks/oltp.py`.

- [ ] **Step 1: Write the failing tests.**
  - `test_bronze.py`, `test_purge_bronze_removes_only_flagged_rows`: three rows with one flagged; the call returns 1, and the two live rows remain.
  - `test_bronze.py`, `test_purge_bronze_twice_deletes_nothing_the_second_time`: a second call returns 0.
  - `test_dags.py`: `lakehouse_oltp_purge` has `schedule is None`, and `silver_gold` is upstream of every `bronze.<t>` task.
- [ ] **Step 2: Run** `TEST test_bronze test_dags`. Expected: FAIL.
- [ ] **Step 3: Implement.**
  - `purge_bronze`: `target.delete(EqualTo("_is_deleted", True))`.
  - DAG: `silver_gold` (`BashOperator`, `dbt run-operation purge_deleted`, `env=dbt_env("purge")`) >> TaskGroup `bronze` (one `PythonOperator` per table). The docstring says to trigger it by hand while incremental and sync are idle.
  - `purge_deleted()` loops over the graph nodes tagged `oltp` (Gold dims and facts, those with a `_is_deleted` column) and then `silver`. On each it runs `DELETE FROM <relation> WHERE _is_deleted` and logs `PURGE <relation>`. There are no preconditions.
- [ ] **Step 4: Run** `TEST test_bronze test_dags`, reserialize, and list import errors. Expected: PASS, `No data found`.
- [ ] **Step 5: Update `docs/DELETE_SYNC_DESIGN.md` §5.7:** no preconditions; the operator triggers it while the pipeline is idle (Design Correction).
- [ ] **Step 6: Commit:** `feat: add the manual lakehouse_oltp_purge DAG`

### Task 8: Live verification on the 100k data set

No new code; this proves the spec's §7 on the running stack. Reuse the fingerprint script from the earlier run, and extend it to keep `_is_deleted` and `_deleted_at` in the checksums.

- [ ] **Step 1: Migrate.** Run `lakehouse_oltp_init`. `create_schema` adds the Bronze columns, Silver appends them, and Gold is fully refreshed. Ask the user before this step: it writes to the real namespaces.
- [ ] **Step 2: Delete source rows** in one transaction:
  - one `order_items` row of a completed order;
  - one `product_reviews` row;
  - one `inventory_transactions` row, adjusting `inventory.on_hand` back;
  - one customer with no orders, which leaves the `SET NULL` foreign keys untouched.
- [ ] **Step 3: Run incremental, then sync.** Sync is triggered by the Asset. Fingerprint as `sync_1`. Expected: the four rows are flagged in Bronze, Silver and Gold, and marts exclude them.
- [ ] **Step 4: Clear and rerun the sync run.** Expected: fingerprint equals `sync_1`.
- [ ] **Step 5: Run init.** Expected: fingerprint equals `sync_1`, tombstones included.
- [ ] **Step 6: Restore the review row** at the source with its old values. Run sync. Expected: the review is live again everywhere.
- [ ] **Step 7: Pause incremental and trigger purge by hand.** Expected:
  - no `_is_deleted` rows remain;
  - mart checksums are unchanged from before the purge;
  - a second purge deletes nothing.
- [ ] **Step 8: Run `make test`, `uvx ruff check .` and `uv lock --check`.** Report the outcomes, plus the drifted docs: `airflow/README.md`, `docs/DATA.md`, `docs/OPERATIONS.md`, `CHANGELOG.md`.
