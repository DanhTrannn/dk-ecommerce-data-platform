# Schema Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the OLTP pipeline react to source column changes. Approved columns and widened types flow into staging and Bronze automatically. Unreviewed new columns, dropped columns and incompatible types stop with a clear error.

**Architecture:**
- A pure classification function compares the source's live columns with `sources.py` (allowlist plus a new `excluded` list) and the Bronze schema.
- Incremental extracts only allowlisted columns that Bronze already has, and refuses breaking changes.
- A new `schema` group in the sync DAG evolves staging and Bronze, then fails on unreviewed columns.
- Its `detect` task reloads a whole table when an allowlisted column is all-NULL in Bronze but not at the source.
- Init applies the same evolution and checks.

**Tech Stack:** Airflow 3.3 (PythonOperator), PyIceberg 0.12 (`update_schema`, `inspect.files()`), psycopg, polars.

**Spec:** `docs/SCHEMA_SYNC_DESIGN.md`

## Global Constraints

- Source DB is read-only.
- `sources.py` is the allowlist: only its `columns` are ever extracted.
- **Init and sync own schema; incremental only loads.** Incremental never alters a table.
- Drift classes, exactly:
  - `unlisted`: at the source, in neither `columns` nor `excluded`.
  - `approved`: in `columns` and at the source, missing from Bronze.
  - `widened`: source type is a legal Iceberg promotion of the Bronze type: int→long, float→double, decimal(P,S)→decimal(P2,S) with P2>P.
  - `narrowed`: the reverse of a promotion.
  - `dropped`: in `columns`, missing at the source.
  - `incompatible`: any other type change.
- Reactions:
  - `dropped` and `incompatible` fail incremental, init and sync.
  - `unlisted` fails sync's `schema.<table>`, after the safe changes are applied. Incremental ignores it.
  - `approved` and `widened` are applied by sync and init; incremental reads around them.
  - `narrowed` does nothing.
- Types are computed only for allowlisted columns. Unlisted and excluded columns are compared by name, since some have types the extract does not support (`return_requests.image_urls` is `jsonb`).
- `silver_missing` (allowlisted Bronze columns missing from the Silver table) is a log warning only.
- Code rules (CLAUDE.md):
  - PythonOperator only.
  - Imports at the top.
  - Minimal PEP 257 / PEP 8 docstrings and comments.
  - No new dependency.
- Docs other than the two design docs are not edited; drift is listed in the final report.
- Conventional Commits on `leekhoa`, with no Claude attribution. Host Python runs with `python3 -B`.
- `TEST <modules>` means:
  `docker compose exec -T -w /tmp -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/opt/project/airflow:/opt/project/airflow/tests airflow-scheduler python -m unittest <modules> -v`

## Review Focus

1. **A source column whose type the extract cannot map** (jsonb, arrays) appears as unlisted. It must produce the `unlisted` error, not an `Unsupported PostgreSQL type` crash. Task 1 pins it.
2. **Incremental runs between an approval in `sources.py` and the next sync.** It must succeed and simply not read the new column. Task 3 pins it.
3. **A whole sync run is cleared after the column was added but before Bronze was reloaded.** The next `detect` must still reload, because the signal is all-NULL in Bronze. Task 4 pins it.
4. **An allowlisted column that is genuinely all-NULL at the source** must never trigger a reload. Task 4 pins it.
5. **A widened column whose new values overflow the old Bronze type**, in incremental before sync runs. The error must tell the operator to trigger the sync. Task 3 pins it.

---

### Task 1: Drift classification

**Files:**
- Modify: `airflow/lakehouse/oltp/sources.py`: `TableSpec.excluded: tuple[str, ...] = ()`, filled for the eight tables below.
- Create: `airflow/lakehouse/oltp/drift.py`
- Test: `airflow/tests/test_drift.py`

`excluded`, exactly the 17 columns left out today:

| Table | Excluded columns |
|---|---|
| `coupons` | `archived_at`, `archived_by_customer_id`, `archive_reason` |
| `customers` | `anonymized_at`, `cod_reinstated_at`, `cod_reinstated_by`, `cod_reinstatement_reason` |
| `delivery_staff` | `phone` |
| `orders` | `receiver_name`, `receiver_phone`, `shipping_address_text` |
| `product_reviews` | `moderated_by_customer_id` |
| `products` | `archived_at`, `archived_by_customer_id`, `archive_reason` |
| `refunds` | `requested_by_customer_id` |
| `return_requests` | `image_urls` |

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Drift` with tuple fields `unlisted`, `approved`, `widened`, `narrowed`, `dropped`, `incompatible` (column names, sorted). It has `breaking` (`dropped + incompatible`) and `evolve` (`approved + widened`) properties.
  - `class SchemaChanged(RuntimeError)`, whose message names the table, the classes and the columns, and the action from design §5.5.
  - `source_columns(connection, table) -> dict[str, pa.DataType | None]`: one `SELECT * FROM "public"."<t>" LIMIT 0`. The Arrow type is computed for names in `table.columns`; every other name maps to `None`.
  - `classify(table, source: dict[str, pa.DataType | None], bronze: pa.Schema) -> Drift`. `bronze` is the Bronze table's `schema().as_arrow()`; `_`-prefixed columns are ignored.

- [ ] **Step 1: Write the failing tests** in `test_drift.py`. Use `TABLES["categories"]` with a Bronze Arrow schema built from `page("categories", []).schema`. One test per class, each asserting the exact `Drift`:
  - `test_identical_schemas_have_no_drift`: `Drift()` is empty; `breaking` and `evolve` are empty.
  - `test_new_source_column_is_unlisted`: the source adds `"loyalty_tier": None`; `unlisted == ("loyalty_tier",)`.
  - `test_excluded_column_is_not_reported`: on `TABLES["delivery_staff"]`, the source has `"phone": None`; empty drift.
  - `test_allowlisted_column_missing_from_bronze_is_approved`.
  - `test_long_over_int_is_widened` and `test_int_under_long_is_narrowed`.
  - `test_missing_allowlisted_column_is_dropped`.
  - `test_string_over_long_is_incompatible`.
  - `test_unmappable_type_of_an_unlisted_column_does_not_fail`: with a fake cursor, a `jsonb` column (type code 3802) not in `columns` gives `None` from `source_columns`, and no exception.
  - `test_excluded_lists_the_seventeen_columns`: `sum(len(t.excluded) for t in SOURCE_TABLES) == 17`, and no excluded name is also in `columns`.
- [ ] **Step 2: Run** `TEST test_drift`. Expected: FAIL (`ModuleNotFoundError: lakehouse.oltp.drift`).
- [ ] **Step 3: Implement.** In `classify`, compare Arrow types. Normalise Bronze's types before comparing: Iceberg `string` comes back as `large_string`, and the timestamp timezone may be spelled `UTC` or `+00:00`. Promotions are exactly the three in Global Constraints.
- [ ] **Step 4: Run** `TEST test_drift test_source`. Expected: PASS.
- [ ] **Step 5: Commit:** `feat: classify source column changes against the allowlist and Bronze`

### Task 2: Schema evolution, and init applies it

**Files:**
- Modify: `airflow/lakehouse/oltp/schema.py`
- Test: `airflow/tests/test_schema.py`

**Interfaces:**
- Consumes: `Drift`, `SchemaChanged`, `classify`, `source_columns` (Task 1).
- Produces: `evolve_schema(catalog, table, source: dict[str, pa.DataType | None]) -> Drift`:
  - adds `approved` columns to `staging.<t>` and `bronze.<t>`, typed from `source`;
  - promotes `widened` columns in both tables (`update_schema().update_column(name, field_type=...)`);
  - raises `SchemaChanged` on `breaking`, before altering anything;
  - returns the drift it found.

  `create_oltp_schema` calls it for every existing table, so init widens and fails like sync. Table creation stays as today.

- [ ] **Step 1: Write the failing tests** in `test_schema.py`:
  - `test_evolve_adds_an_approved_column_to_staging_and_bronze`: patch `TABLES["categories"]` with `columns + ("loyalty_tier",)` and a source dict holding it. Both tables gain `loyalty_tier`, and the existing rows read NULL.
  - `test_evolve_widens_a_column`: Bronze `parent_category_id` is int32; the source gives int64; afterwards both tables have `long`.
  - `test_evolve_refuses_a_dropped_column_without_altering`: `SchemaChanged` is raised, and both schemas are unchanged.
  - `test_rerun_of_evolve_changes_nothing`: a second call returns a `Drift` with empty `evolve`.
  - `test_init_refuses_a_breaking_change`: the `Source` fake with a column removed makes `create_oltp_schema` raise `SchemaChanged`.
- [ ] **Step 2: Run** `TEST test_schema`. Expected: FAIL (`ImportError: evolve_schema`).
- [ ] **Step 3: Implement** `evolve_schema`, and have `create_oltp_schema` read `source_columns` per table.
- [ ] **Step 4: Run** `TEST test_schema test_bronze`. Expected: PASS.
- [ ] **Step 5: Commit:** `feat: evolve staging and Bronze for approved columns and widened types`

### Task 3: Incremental reads around safe changes and refuses breaking ones

**Files:**
- Modify: `airflow/lakehouse/utils/postgres.py` (`source_query(..., columns=None)`, defaulting to `table.columns`), `airflow/lakehouse/oltp/source.py` (`source_chunks(..., columns=None)`), `airflow/lakehouse/oltp/staging.py` (`extract_table`)
- Test: `airflow/tests/test_staging.py`, `airflow/tests/test_postgres.py`

**Interfaces:**
- Consumes: `classify`, `source_columns`, `SchemaChanged` (Task 1).
- Produces: `extract_table(catalog, connection, table, ingested_at, *, start=None, end=None, created_before=None)`, signature unchanged. With an interval (incremental):
  - it classifies first and raises `SchemaChanged` on `breaking`;
  - it selects `[c for c in table.columns if c in bronze_columns]`.

  Init (`created_before`) keeps reading all of `table.columns`: `create_schema` already evolved the tables.
- A cast failure while aligning a chunk to the staging schema is re-raised as `SchemaChanged`, naming the column and telling the operator to trigger `lakehouse_oltp_sync`.

- [ ] **Step 1: Write the failing tests:**
  - `test_postgres.py`: `source_query(orders, columns=("order_id", "updated_at"), created_before=...)` selects exactly those two columns.
  - `test_staging.py`:
    - `test_incremental_skips_an_approved_column_bronze_lacks`: `columns` includes `loyalty_tier`, which Bronze lacks; the query sent to the fake cursor has no `loyalty_tier`, and staging is written.
    - `test_incremental_refuses_a_dropped_column`: `SchemaChanged`, and staging is untouched.
    - `test_overflow_of_a_widened_value_names_the_sync`: the source sends int64 `2**40` into an int32 column; `SchemaChanged` is raised with a message containing `lakehouse_oltp_sync`.
- [ ] **Step 2: Run** `TEST test_postgres test_staging`. Expected: FAIL.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run** `TEST test_postgres test_staging test_source test_tasks`. Expected: PASS.
- [ ] **Step 5: Commit:** `feat: let incremental loads read around safe column changes`

### Task 4: Sync schema group and table reload

**Files:**
- Modify: `airflow/lakehouse/oltp/sync.py`, `airflow/tasks/oltp.py`, `airflow/dags/lakehouse_oltp_sync.py`
- Test: `airflow/tests/test_sync.py`, `airflow/tests/test_dags.py`, `airflow/tests/test_tasks.py`

**Interfaces:**
- Consumes: `evolve_schema` (Task 2), `source_columns`, `SchemaChanged`, `extract_table` (Task 3), `detect_deletes`.
- Produces:
  - `sync_schema(catalog, connection, table) -> Drift`:
    1. Calls `evolve_schema`.
    2. Logs `SCHEMA table=<t> evolved=[...]`.
    3. If `silver.<t>` exists, logs `SCHEMA table=<t> silver_missing=[...]` for allowlisted Bronze columns its schema lacks. Silver is read by metadata only, with `catalog.load_table(...).schema()`.
    4. Raises `SchemaChanged` when `unlisted` is non-empty.
  - `needs_reload(catalog, connection, table) -> list[str]`: the allowlisted Bronze columns whose summed `null_value_counts` (from `inspect.files()`, by field id) equals the summed `record_count`, and that have a non-NULL value at the source (`SELECT 1 FROM "public"."<t>" WHERE "<c>" IS NOT NULL LIMIT 1`). An empty Bronze table returns `[]`.
  - `detect_deletes` flags deletions as today. When `needs_reload` is non-empty, it stages every source row (no window, Bronze's allowlisted columns) instead of the restored rows only. It logs `SYNC table=<t> deleted=<n> restored=<m> reload=[...]`.
  - Task callable `schema(table)` in `tasks/oltp.py`.
  - DAG: TaskGroup `schema` (one `PythonOperator` per table), upstream of `detect`.

- [ ] **Step 1: Write the failing tests:**
  - `test_sync.py`:
    - `test_sync_schema_applies_safe_changes_then_fails_on_unlisted`: `loyalty_tier` is unlisted and a widened column exists. The widening is applied, then `SchemaChanged` mentions `loyalty_tier`.
    - `test_detect_reloads_a_table_whose_new_column_is_all_null_in_bronze`: an approved column has been added to Bronze (all NULL) and the fake source has a value. Staging gets every source row.
    - `test_detect_does_not_reload_a_column_null_at_both_ends`.
    - `test_reload_is_retried_after_a_cleared_run`: `evolve_schema` ran, Bronze was not reloaded, and a fresh `detect_deletes` still reloads.
  - `test_dags.py`: each `schema.<t>` is upstream of `detect.<t>` in `lakehouse_oltp_sync`; incremental has no `schema.` tasks.
  - `test_tasks.py`: `schema` calls `sync_schema` with the table spec.
- [ ] **Step 2: Run** `TEST test_sync test_dags test_tasks`. Expected: FAIL.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run** the whole pipeline suite (`make test`), `uvx ruff check .`, DAG reserialize, and `list-import-errors`. Expected: all PASS, `No data found`.
- [ ] **Step 5: Update `docs/SCHEMA_SYNC_DESIGN.md`** wherever the implementation settled a "để người làm tự quyết" item (module name, type comparison).
- [ ] **Step 6: Commit:** `feat: add the sync schema group and reload tables with new columns`

### Task 5: Live verification on the 100k data set

Ask the user before Step 1: it alters the source schema and writes to the real namespaces. Reuse the fingerprint script from part 1. Pause incremental and sync between steps as in the backfill rule.

- [ ] **Step 1:** `ALTER TABLE customers ADD COLUMN loyalty_tier text` and fill it for some rows. Run incremental (backfill of the 11-21 interval). Expected: incremental is green, `schema.customers` fails with `unlisted`, and the lakehouse is unchanged.
- [ ] **Step 2:** Add `loyalty_tier` to `sources.py` `customers.columns`, to `dbt/models/silver/customers.sql`, and to the test fixture types if needed. Trigger sync. Expected:
  - `SCHEMA ... evolved=[loyalty_tier]` and `SYNC ... reload=[loyalty_tier]`;
  - Bronze and Silver have the values.

  Fingerprint, run init, fingerprint again. Expected: equal.
- [ ] **Step 3:** `ALTER TABLE coupons ALTER COLUMN total_usage_limit TYPE bigint` (or another `integer` column). Trigger sync. Expected: `evolved=[...]` shows the widening, and the Bronze type is `long`.
- [ ] **Step 4:** Drop an allowlisted column, for example `ALTER TABLE customers DROP COLUMN loyalty_tier`. Run incremental. Expected: `staging.customers` fails with `SchemaChanged ... dropped`. Remove the column from `sources.py` and the Silver model, then Clear. Expected: green.
- [ ] **Step 5:** Restore the source: revert the type change, and confirm `loyalty_tier` is gone. Revert any trial edits to `sources.py` or the Silver model, so the branch holds only the feature. Run `make test`, `uvx ruff check .` and `uv lock --check`. Report the outcomes and the drifted docs: `airflow/README.md`, `docs/DATA.md`, `docs/OPERATIONS.md`, `docs/ARCHITECTURE.md`, `CHANGELOG.md`.
