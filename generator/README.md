# OLTP & Access Log Data Generator

The Data Generator (`data-generator` package, v0.8.0) creates deterministic synthetic datasets with configured Vietnamese retail seasonality and purchase patterns. It exports relational SQL inserts for MySQL OLTP and synthetic HTTP access logs using the runtime log contract. These are modeled distributions, not calibration results from a measured customer dataset.

---

## Automated Backfill (Zero-Footprint on Host)

To backfill data without leaving temporary SQL or log files on your host filesystem, use [`scripts/backfill_data.sh`](../scripts/backfill_data.sh):

```bash
# Backfill both OLTP SQL into MySQL and Access Logs to MinIO S3
./scripts/backfill_data.sh

# Or backfill individually
./scripts/backfill_data.sh --mode oltp  # MySQL database only
./scripts/backfill_data.sh --mode logs  # MinIO Landing Zone only

# With a specific scale configuration
./scripts/backfill_data.sh --config generator/configs/medium.yml
```

---

## Manual Export Workflow

### 1. Export SQL History

```bash
uv run --locked --package data-generator -- generator export-sql \
  --config generator/configs/small.yml \
  --output data/generator/small.sql
```

The output file is written to `data/generator/small.sql`.

### 2. Import into MySQL

Once MySQL is running and its 28-table schema has been initialized from `database/schema.sql`:

```bash
./scripts/import_generated_sql.sh data/generator/small.sql
```

> [!IMPORTANT]
> The generated SQL runs within a single atomic transaction without disabling foreign key checks. Do not import the same SQL file twice into the same database.

### Synthetic Customer Accounts
Generated active accounts use `customer.<index>.<logical-prefix>@web.local` and the synthetic-data password `Synthetic@12345`. The traffic generator uses the same mapping. No featured customer receives a reserved order quota or a special timeline; order allocation follows the configured customer classes.

These are development data-source accounts. SQL and access logs are generated data, not measurements of real customer activity.

Version 0.8 changes the generator logical identity because payment, return and stock semantics changed. Generate/import matching SQL before running traffic for that configuration. Existing datasets are not migrated or repaired automatically; exporting a new version does not modify MySQL.

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

### 4. Upload to MinIO Landing Zone

When MinIO and Docker services are active, upload generated logs to the S3 Landing Zone:

```bash
./scripts/upload_generated_logs.sh data/generator/access-logs
```

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

- **Surrogate PKs:** Uses `BIGINT UNSIGNED` keys for internal joins and performant bulk imports.
- **Public Identifiers:** Uses deterministic UUIDv5 for `public_id`, `logical_identity`, `generation_run_id`, and transaction idempotency keys.
- **Business Keys:** Keeps human-readable `order_number`, SKU, slug, and coupon code identifiers.

## Live Access Log Source

After importing the matching SQL dataset, create API traffic with:

```bash
uv run --locked --package data-generator -- python scripts/simulate_web_traffic.py \
  --config generator/configs/small.yml --duration 300 --concurrency 5
```

The API publishes these access events to Kafka for the Flink stream. Historical `.jsonl.gz` files uploaded to MinIO remain raw Landing files; the current Flink job reads Kafka and does not automatically process those uploaded files into Gold.

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

The checks generate temporary datasets without contacting MySQL, Kafka or external APIs. They cover payment/paid timestamps, return lineage/refunds, stock balances and nonnegative stock history, status continuity, single-variant/free-item edges, multiple seeds and INSERT batches, preset loading, deterministic SQL output and historical-log provenance. Generated SQL/log files are removed with their temporary directories.
