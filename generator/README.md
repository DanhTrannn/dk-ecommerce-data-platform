# OLTP & Access Log Data Generator

The Data Generator (`data-generator` package, v0.10.0) creates deterministic synthetic datasets with configured Vietnamese retail seasonality and purchase patterns. It exports relational SQL inserts for PostgreSQL 16 OLTP and synthetic HTTP access logs using the runtime log contract. These are modeled distributions, not calibration results from a measured customer dataset.

---

## Manual Export Workflow

### Machine-readable summaries

`run`, `export-sql`, and `export-logs` accept `--json` for job runners and other tools. The JSON summary is printed to stdout; the existing human-readable lines remain the default.

```bash
uv run --locked --package data-generator -- generator run \
  --config generator/configs/small.yml \
  --output data/generator/manifest \
  --json
```

### 1. Export SQL History

```bash
uv run --locked --package data-generator -- generator export-sql \
  --config generator/configs/small.yml \
  --output data/generator/small.sql
```

The output file is written to `data/generator/small.sql`.

### 2. Import into PostgreSQL

Apply the [28-table source schema](../database/README.md) to an empty source database first. Configure host-reachable `API_DATABASE_URL` (`postgresql+psycopg2`) and `API_SECRET_KEY` using `.env.example`, then run from the repository root:

```bash
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.import_dataset data/generator/small.sql
make seed
```

The importer accepts **trusted SQL emitted by this generator**, not arbitrary uploads. It checks the source schema, rejects an existing generation run, inserts with FK/CHECK enforcement, verifies counts/payment/return/stock consistency, and restarts identity sequences above loaded IDs without moving them backwards. All writes and sequence restarts share one transaction; a failure rolls them back. The JSON receipt includes SQL SHA-256, expected schema SHA-256, dataset identity, customer/order counts and placed order value (not realized revenue). Configured customers exclude the extra synthetic review moderator.

Imports are serialized and take write-blocking locks on generated tables. Run bulk imports before concurrent application writers; the DB role must own the affected tables/sequences. Seeds may run before or after import, each in its own transaction. Rerunning a seed repairs missing reference rows without resetting existing credentials or inventory. Rerunning an import is rejected rather than merged. Existing ID/key collisions also fail; there is no destructive reset or data migration.

Streamlit creates artifacts only; run this command explicitly to load a selected SQL artifact. SQL format is `postgresql-v1`, also recorded in CLI JSON and manifests. Regenerate legacy MySQL SQL; it cannot be loaded into PostgreSQL. Historical SQL/logs from 0.8 have different dataset identities.

### Cities and stores

Locations now belong to the generator. Default SQL includes the same five cities and six sample retail stores previously maintained by the location seed. To select a subset in a YAML configuration:

```yaml
locations:
  store_codes:
    - HCM-CENTRE
    - HN-CENTRE
```

Available codes: `HCM-CENTRE`, `HCM-NORTHEAST`, `HN-CENTRE`, `DN-CENTRE`, `CT-CENTRE`, `HP-CENTRE`. Only cities used by the selected stores are included. Omit `locations` to select all six stores, or use `locations: {store_codes: []}` to generate none. Codes are case-insensitive and normalized; unknown/duplicate codes are rejected. Streamlit exposes the same selection as **Cửa hàng tham chiếu**.

SQL ensures cities before stores and resolves shared references by code. Later datasets reuse existing locations without changing IDs, names, addresses, active flags or timestamps. A store code already linked to another city aborts the transaction. New rows use the historical master-data timestamp. Location IDs depend on the target database; the SQL artifact and code selection remain deterministic. The generator takes table locks, allocates explicit IDs and restarts identity sequences transactionally; existing database records are not reset.

SQL metadata, manifest, CLI summary and import receipt report selected city/store counts, including reused records, not the number newly inserted. Selection participates in dataset identity, so SQL and matching access logs must be exported with the same version/configuration. Location selection does not assign orders/customers to cities or generate POS sales, store inventory, staff assignments or receipts. Those workflows remain separate. `make seed` continues to initialize application catalog/admin data; it no longer creates locations.

### Synthetic Customer Accounts
Generated active accounts use `customer.<index>.<logical-prefix>@web.local` and the synthetic-data password `Synthetic@12345`. The traffic generator uses the same mapping. No featured customer receives a reserved order quota or a special timeline; order allocation follows the configured customer classes.

These are development data-source accounts. SQL and access logs are generated data, not measurements of real customer activity.

Version 0.10 adds location selection to dataset identity and SQL artifacts; the retail sampling assumptions from 0.8 are retained. PostgreSQL SQL format remains postgresql-v1; the importer also accepts older 0.9 artifacts, which do not supply locations. Export SQL and access logs with the same version/configuration. Existing databases and exported artifacts are not migrated or repaired automatically.

### Business consistency in 0.8

- COD starts confirmed with a pending payment. Payment succeeds and `paid_at` is set on completion; cancelled or failed-delivery COD has no successful payment or cash refund. VietQR cancellation retains the original paid timestamp and records its refund.
- Return lines reference the selected order item and variant. Refunds use the discounted unit price rounded to VND, matching the current API formula; completed monetary refunds have payment-linked records. A full refund of all ordered units marks the order returned. An exchange without another variant becomes a refund.
- Each order has a checkout stock withdrawal; cancellations and failed deliveries have compensating movements. Passed returns add stock; modeled exchanges also withdraw the replacement SKU. Opening balance plus net movements equals closing stock. The arbitrary 500-unit inbound rows have been removed: opening stock is an explicit assumption sized from total withdrawals plus an 80–200 unit buffer, not a simulated receipt or measured replenishment policy.
- Return creation/resolution fits between completion and the anchor. Order status history follows COD/VietQR paths. Historical JSONL exports carry `data_origin=synthetic`; live middleware events remain `observed`, including requests from a simulator.

These checks establish internal consistency within the covered scenarios. Product sampling, customer order rescaling, COD/return selection and seasonality still use configured assumptions. POS, actual inbound receipts, reference calibration and real API exchange execution are not supplied by these SQL fixes. A modeled exchange does not certify that an equivalent API workflow exists.

---

### 3. Export Matching Access Logs

```bash
uv run --locked --package data-generator -- generator export-logs \
  --config generator/configs/small.yml \
  --output-directory data/generator/access-logs \
  --expected-requests 60000
```

Output files are structured in Hive-style partition directories under `data/generator/access-logs/landing/logs/`:

```text
landing/logs/ingest_date=YYYY-MM-DD/ingest_hour=HH/service=ecommerce-api/<uuid>.jsonl.gz
```

- **Generation Window:** Exports a 30-day log window relative to the dataset anchor; this is not object-storage retention.
- **Format:** Gzip-compressed newline-delimited JSON (`.jsonl.gz`).
- **Telemetry Standards:** Standardized to OpenTelemetry-compatible fields (`service.name=ecommerce-api`, `data_origin=synthetic`, 12-character hex container IDs).

Historical files are local Landing artifacts; this command does not upload them to MinIO or send them to Kafka. The old backfill/import/upload helper scripts have been removed. Provisioning and pipeline conversion remain separate steps.

---

## 5. Dataset Scale Configurations

The `configs/` directory provides pre-configured scenarios:

| Config | Customers | Products | Variants | Configured Orders | History |
|---|---|---|---|---|---|
| `small.yml` | 500 | 60 | 240 | 3,000 | 12 months |
| `medium.yml` | 5,000 | 200 | 1,000 | 30,000 | 12 months |
| `large-local.yml` | 30,000 | 500 | 3,000 | 250,000 | 12 months |
| `large-10m.yml` | 200,000 | 500 | 3,000 | 1,000,000 | 12 months |
| `month-test.yml` | 150 | 60 | 240 | 400 | 1 month |

---

These are YAML inputs, not measured loaded row counts. Despite its filename, `large-10m.yml` currently configures 1,000,000 orders. Dates follow `anchor_time`, not the date the command runs.

## 6. Vietnamese E-Commerce Behavioral Distributions

The generator uses the following retail scenario assumptions:

- **Timezone & Seasonality:** Uses `Asia/Ho_Chi_Minh` (stored as UTC). Models a configured Tet window, double-day campaigns (e.g. 9/9, 11/11, 12/12), and Black Friday. The fixed Tet window is an assumption, not a year-specific lunar-calendar lookup.
- **Diurnal Rhythm:** Normal day peaks occur at 19:00–22:00. Campaign days feature 00:00–02:00 midnight spikes, lunch rushes (12:00), and evening surges.
- **Customer Segmentation:** Customers are segmented into `loyal`, `regular`, and `one_off` cohorts with distinct purchase intervals, coupon affinity, review propensities, and cancellation rates.
- **Review Behavior:** Reviews originate exclusively from verified `completed` order items. Default synthetic ratio features 94% published immediately and 6% post-moderated (`rejected`) with Vietnamese audit reasons.
- **Soft Archive:** Simulates product and coupon lifecycle archives without breaking historical relational lineage.

---

## 7. Deterministic Identity Strategy

- **Surrogate PKs:** Uses nonnegative PostgreSQL `BIGINT` keys (maximum 2^63−1) for internal joins and performant bulk imports.
- **Public Identifiers:** Uses deterministic UUIDv5 for `public_id`, `logical_identity`, `generation_run_id`, and transaction idempotency keys.
- **Business Keys:** Keeps human-readable `order_number`, SKU, slug, and coupon code identifiers.

## Live Access Log Source

Runtime middleware publishes observed HTTP events to Kafka. The old traffic-simulator script has been removed; API workflows and downstream readers still need PostgreSQL verification before an end-to-end run. Historical `.jsonl.gz` files are synthetic Landing artifacts. The current Flink log job reads Kafka and does not automatically process those files into Gold.

## Reference Profiling

`profile-reference` measures a local copy of UCI Online Retail with a pinned checksum and explicit cohort policy. It reads the official ZIP/XLSX layout or an eight-column UTF-8 CSV with naive ISO invoice timestamps, using standard-library readers. It does not download data, import SQL, change the current sampler or load the resulting profile into exports.

```bash
export UCI_RETAIL_SOURCE=/path/to/online_retail.zip
uv run --locked --package data-generator -- generator profile-reference \
  --input "$UCI_RETAIL_SOURCE" \
  --spec generator/profiles/uci-reference.yml \
  --output data/generator/reference/uci-online-retail-uk-v1.json
```

The [spec](profiles/uci-reference.yml) pins the source/license, UK cohort, stock-code pattern, retained-duplicate policy and separate six-month fit/evaluation windows. The [measured profile](profiles/uci-online-retail-uk-v1.json) contains histograms, repeat-purchase observations, ranked unit totals and an audit of excluded rows. It publishes no invoice/customer/SKU identifiers. Its content-derived ID changes with the source, measurement policy or results; generation identity is unchanged because exports do not use this profile yet.

See the [measurement definitions and results](../docs/project/GENERATOR_REFERENCE_PROFILE.md) before interpreting or applying the profile. The source contains wholesale activity and does not represent Vietnamese fashion retail; long-tail baskets and quantities require an explicit adaptation decision before calibration. Keep raw downloads outside Git. Checksums reject a different artifact, so converted CSV/XLSX files need a separate pinned specification rather than silently reusing the official ZIP checksum.

## Regression Checks

Run from the repository root, using Python's standard-library unittest runner:

```bash
uv run --locked --package data-generator -- python -m unittest discover -s generator/tests -v
```

The checks generate temporary datasets without contacting databases, Kafka or external APIs. They cover payment/paid timestamps, return lineage/refunds, stock balances and nonnegative stock history, status continuity, single-variant/free-item edges, multiple seeds and INSERT batches, preset loading, deterministic SQL output and historical-log provenance. Generated SQL/log files are removed with their temporary directories.

PostgreSQL import/seed integration checks are opt-in under [the database guide](../database/README.md#kiểm-tra); they use disposable Docker containers, not an existing source database.
