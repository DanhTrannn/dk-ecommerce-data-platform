# OLTP Data Generator

The Data Generator (`data-generator` package, v0.8.0) creates deterministic synthetic datasets with configured Vietnamese retail seasonality and purchase patterns. It exports relational SQL inserts for MySQL OLTP. Synthetic access-log generation is currently removed. These are modeled distributions, not calibration results from a measured customer dataset.

---

## Manual Export Workflow

### Machine-readable summaries

`run` and `export-sql` accept `--json` for job runners and other tools. The JSON summary is printed to stdout; the existing human-readable lines remain the default.

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

### 2. Import into MySQL

Once MySQL is running and its 28-table schema has been initialized from `database/schema.sql`:

```bash
./scripts/import_generated_sql.sh data/generator/small.sql
```

> [!IMPORTANT]
> The generated SQL runs within a single atomic transaction without disabling foreign key checks. Do not import the same SQL file twice into the same database.

### Synthetic Customer Accounts
Generated active accounts use `customer.<index>.<logical-prefix>@web.local` and the synthetic-data password `Synthetic@12345`. No featured customer receives a reserved order quota or a special timeline; order allocation follows the configured customer classes.

These are development data-source accounts. SQL records are generated data, not measurements of real customer activity.

Version 0.8 changes the generator logical identity because payment, return and stock semantics changed. Existing datasets are not migrated or repaired automatically; exporting a new version does not modify MySQL.

### Business consistency in 0.8

- COD starts confirmed with a pending payment. Payment succeeds and `paid_at` is set on completion; cancelled or failed-delivery COD has no successful payment or cash refund. VietQR cancellation retains the original paid timestamp and records its refund.
- Return lines reference the selected order item and variant. Refunds use the discounted unit price rounded to VND, matching the current API formula; completed monetary refunds have payment-linked records. A full refund of all ordered units marks the order returned. An exchange without another variant becomes a refund.
- Each order has a checkout stock withdrawal; cancellations and failed deliveries have compensating movements. Passed returns add stock; modeled exchanges also withdraw the replacement SKU. Opening balance plus net movements equals closing stock. The arbitrary 500-unit inbound rows have been removed: opening stock is an explicit assumption sized from total withdrawals plus an 80–200 unit buffer, not a simulated receipt or measured replenishment policy.
- Return creation/resolution fits between completion and the anchor. Order status history follows COD/VietQR paths.

These checks establish internal consistency within the covered scenarios. Product sampling, customer order rescaling, COD/return selection and seasonality still use configured assumptions. POS, actual inbound receipts, reference calibration and real API exchange execution are not supplied by these SQL fixes. A modeled exchange does not certify that an equivalent API workflow exists.

---

## Dataset Scale Configurations

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

## Vietnamese E-Commerce Behavioral Distributions

The generator uses the following retail scenario assumptions:

- **Timezone & Seasonality:** Uses `Asia/Ho_Chi_Minh` (stored as UTC). Models a configured Tet window, double-day campaigns (e.g. 9/9, 11/11, 12/12), and Black Friday. The fixed Tet window is an assumption, not a year-specific lunar-calendar lookup.
- **Diurnal Rhythm:** Normal day peaks occur at 19:00–22:00. Campaign days feature 00:00–02:00 midnight spikes, lunch rushes (12:00), and evening surges.
- **Customer Segmentation:** Customers are segmented into `loyal`, `regular`, and `one_off` cohorts with distinct purchase intervals, coupon affinity, review propensities, and cancellation rates.
- **Review Behavior:** Reviews originate exclusively from verified `completed` order items. Default synthetic ratio features 94% published immediately and 6% post-moderated (`rejected`) with Vietnamese audit reasons.
- **Soft Archive:** Simulates product and coupon lifecycle archives without breaking historical relational lineage.

---

## Deterministic Identity Strategy

- **Surrogate PKs:** Uses `BIGINT UNSIGNED` keys for internal joins and performant bulk imports.
- **Public Identifiers:** Uses deterministic UUIDv5 for `public_id`, `logical_identity`, `generation_run_id`, and transaction idempotency keys.
- **Business Keys:** Keeps human-readable `order_number`, SKU, slug, and coupon code identifiers.

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

## Reference Dataset EDA (local notebook)

The [EDA notebook](notebooks/reference_datasets_eda.ipynb) profiles all three sources with `describe()` and `null_count()`. Olist 2017 covers baskets/order values, status/payment mix, delivery duration/lateness and ratings by lateness. H&M 2019 covers purchase days, observed repurchase gaps, product concentration, product groups and normalized monthly demand. The behavior section measures observed session spans, event mix, raw cart/purchase presence, a strictly ordered same-product funnel, timestamp ties and completed-path timings. This analysis remains reference material; log generation is outside the current generator scope. See the [plan](../docs/project/GENERATOR_EDA_PLAN.md).

Behavior analysis uses October 2019 with November 1 as boundary context. It excludes sessions touching October 1 or November 1, keeps the source `(user_id, user_session)` key and applies no inactivity timeout. The main funnel denominator is sessions with a view; timing statistics use completed session/product paths. No HTTP latency or order count is inferred from these events.

Each business analysis ends with a short Markdown finding and proposed generation rule. Section 4 summarizes the rules and evidence gaps. Code cells only load data, calculate statistics and plot; the notebook writes no CSV, figures or other files. Results and commentary stay in the notebook.

Generator code and parameters remain unchanged. Before claiming improved generation quality, compare baseline and calibrated output using matching definitions, seed, scale and time window, then evaluate on held-out reference periods. Calibration and that comparison have not been performed.

From the repository root:

```bash
uv sync --locked --group eda
```

Open the notebook in VS Code and select the repository's `.venv/bin/python` kernel, or use an existing local Jupyter installation with that kernel. No containers or running services are required. EDA dependencies are in the optional `eda` group; Seaborn brings Pandas as a transitive dependency, while analysis uses Polars.

Sources and local CSV locations:

- [H&M](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data): `data/generator/eda/raw/hm/transactions_train.csv` and `articles.csv`; images are not needed. The original competition download requires Kaggle authentication and applicable competition terms.
- [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), CC BY-NC-SA 4.0: CSV files under `data/generator/eda/raw/olist/`.
- [eCommerce Behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): `2019-Oct.csv` and `2019-Nov.csv` under `data/generator/eda/raw/ecommerce_behavior/`. Follow the author's source-attribution requirements on the dataset page.

The user-provided H&M mirror is [einrafh/hnm-fashion-recommendations-data](https://huggingface.co/datasets/einrafh/hnm-fashion-recommendations-data/tree/acde210b0090be673abdba5f989c48925c856f78), pinned to commit `acde210b0090be673abdba5f989c48925c856f78`. Downloaded CSV sizes and SHA-256 were checked against its Git LFS pointers; this verifies the mirror revision, not independent equality with the original Kaggle download. The data directory retains only source CSV files; downloaded archives, the duplicate clone, download metadata and previous EDA exports have been removed.

The notebook contains analysis and minimal data-loading code. Large sources use Polars lazy scans; `describe()` collects only the selected price/date columns, and null counts use streaming aggregation. Setup instructions, source provenance and implementation progress stay outside the notebook. Edit `data_dir` if files are stored elsewhere.

Analysis references, read through Kaggle's public notebook API:

- [Relational Database — EDA & Data Preparation](https://www.kaggle.com/code/khusheekapoor/relational-database-eda-data-preparation): table-level missingness and relational structure.
- [H&M EDA FIRST LOOK](https://www.kaggle.com/code/vanguarde/h-m-eda-first-look): separation of product/customer/transaction analysis and product-group exploration.
- [ECommerce behavior data from multi category store](https://www.kaggle.com/code/varshnidevi/ecommerce-behavior-data-from-multi-category-store): missing category/brand, event counts and session journeys.

These references guide questions and presentation. All results are recomputed locally. Basic profiling retains source rows and does not apply cleaning rules.

To execute from a fresh kernel and save results in the same notebook:

```bash
uv run --locked --group eda jupyter nbconvert --to notebook --execute \
  generator/notebooks/reference_datasets_eda.ipynb \
  --inplace \
  --ExecutePreprocessor.timeout=1800
```

Raw CSV files stay under the ignored `data/generator/eda/raw/` directory. Sources are documented above. Analysis results stay in notebook cells; clear outputs and execution counts before committing.

## Regression Checks

Run from the repository root, using Python's standard-library unittest runner:

```bash
uv run --locked --package data-generator -- python -m unittest discover -s generator/tests -v
```

The checks generate temporary datasets without contacting MySQL, Kafka or external APIs. They cover payment/paid timestamps, return lineage/refunds, stock balances and nonnegative stock history, status continuity, single-variant/free-item edges, multiple seeds and INSERT batches, preset loading, deterministic SQL output and CLI summaries. Generated SQL files are removed with their temporary directories.
