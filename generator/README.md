# OLTP Data Generator

The Data Generator (`data-generator` package, v0.8.0) exports deterministic SQL data for the current MySQL [schema](../database/schema.sql). The rewritten sampler uses H&M 2019 purchase counts by product-rank group; transferring those groups onto the synthetic catalog is a modeling assumption. Other behavioral values retain the previous scenario settings and are explicitly documented as assumptions in [GENERATOR_METRICS](../docs/project/GENERATOR_METRICS.md).

Runtime responsibilities are grouped in `config.py`/`validation.py`, `catalog.py`, `transactions.py`, `sql_export.py` and `cli.py`. The runtime does not read research CSVs or execute the notebook. Synthetic access-log generation and the old UCI profile modules remain removed. PostgreSQL infrastructure conversion does not change this generator's MySQL target.

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

Initialize an empty MySQL schema from `database/schema.sql` and import the generated file with your MySQL client. The current Compose stack provides PostgreSQL, so it cannot execute these MySQL files directly. Generating SQL does not initialize a database, import data or trigger pipelines.

> [!IMPORTANT]
> The generated SQL runs within a single atomic transaction without disabling foreign key checks. Do not import the same SQL file twice into the same database.

### Synthetic Customer Accounts
Generated active accounts use `customer.<index>.<logical-prefix>@web.local` and the synthetic-data password `Synthetic@12345`. No featured customer receives a reserved order quota or a special timeline; order allocation follows the configured customer classes.

These are development data-source accounts. SQL records are generated data, not measurements of real customer activity.

The rewritten generator uses a `dk-generator-v2` identity namespace so older configurations cannot accidentally reuse the previous dataset identity. Identity includes the full configuration and generator version. Existing datasets are not migrated or repaired automatically.

### Business consistency in 0.8

- COD starts confirmed with a pending payment. Payment succeeds and `paid_at` is set on completion; cancelled or failed-delivery COD has no successful payment or cash refund. VietQR cancellation retains the original paid timestamp and records its refund.
- Return lines reference the selected order item and variant. Refunds use the discounted unit price rounded to VND, matching the current API formula; completed monetary refunds have payment-linked records. A full refund of all ordered units marks the order returned. An exchange without another variant becomes a refund.
- Each order has a checkout stock withdrawal; cancellations and failed deliveries have compensating movements. Passed returns add stock; modeled exchanges also withdraw the replacement SKU. Opening balance plus net movements equals closing stock. The arbitrary 500-unit inbound rows have been removed: opening stock is an explicit assumption sized from total withdrawals plus an 80–200 unit buffer, not a simulated receipt or measured replenishment policy.
- Return creation/resolution fits between completion and the anchor. Order status history follows COD/VietQR paths.

These checks establish internal consistency within the covered scenarios. Product demand now uses measured H&M rank-group weights; catalog transfer, customer order rescaling, COD/return selection and seasonality remain documented assumptions. POS, actual inbound receipts and API exchange execution are outside the generated scenarios. The generator uses 23 of the 28 schema tables; `cities`, `stores`, `store_inventory`, `inbound_receipts` and `inbound_receipt_items` are not populated.

---

## Dataset Scale Configurations

The retained preset is:

| Config | Customers | Products | Variants | Configured Orders | History |
|---|---|---|---|---|---|
| `small.yml` | 500 | 60 | 240 | 3,000 | 12 months |

---

These are YAML inputs, not measured loaded row counts. Edit the preset or a copy to choose another scale. Dates follow `anchor_time`, not the date the command runs. `distributions.product_popularity` contains ten empirical purchase-count weights in descending rank groups; other behavioral values remain unchanged assumptions.

## Vietnamese E-Commerce Behavioral Distributions

The generator uses the following retail scenario assumptions:

- **Timezone & Seasonality:** Uses `Asia/Ho_Chi_Minh` (stored as UTC). Models a configured Tet window, double-day campaigns (e.g. 9/9, 11/11, 12/12), and Black Friday. The fixed Tet window is an assumption, not a year-specific lunar-calendar lookup.
- **Diurnal Rhythm:** Normal day peaks occur at 19:00–22:00. Campaign days feature 00:00–02:00 midnight spikes, lunch rushes (12:00), and evening surges.
- **Customer Segmentation:** Customers are segmented into `loyal`, `regular`, and `one_off` cohorts with distinct purchase intervals, coupon affinity, review propensities, and cancellation rates.
- **Review Behavior:** Reviews originate from completed order items, within the observation window and before a full return is resolved. Weights for approved/rejected status are 94/6; they are assumptions, not guaranteed final proportions.
- **Soft Archive:** Simulates product and coupon lifecycle archives without breaking historical relational lineage.

---

## Deterministic Identity Strategy

- **Surrogate PKs:** Uses `BIGINT UNSIGNED` keys for internal joins and performant bulk imports.
- **Public Identifiers:** Uses deterministic UUIDv5 for `public_id`, `logical_identity`, `generation_run_id`, and transaction idempotency keys.
- **Business Keys:** Keeps human-readable `order_number`, SKU, slug, and coupon code identifiers.

## Reference Dataset EDA (local notebook)

The [EDA notebook](notebooks/reference_datasets_eda.ipynb) is organized by the behavior the generator needs to model: customers/repurchase, baskets, product demand, purchase timing, and transactions/after-sales. Each group follows **business question → research evidence → EDA → assessment → generation rule → project application**. Data preparation retains `describe()` and `null_count()`. The [Vietnam research report](../docs/project/GENERATOR_VIETNAM_RESEARCH.md) documents the source populations, measurements and limits; relevant findings are placed beside the notebook analysis. See the [plan](../docs/project/GENERATOR_EDA_PLAN.md).

UCI supplies invoice-level quantities and repurchase metrics; H&M supplies purchase-day patterns and product demand; Olist supplies baskets and delivery/review relationships. Their distributions remain separate: purchase days differ from orders, source product codes differ from project variants, and overseas measurements are not estimates for Vietnamese shoppers. eCommerce Behavior is retained in an appendix; log generation remains outside scope.

Behavior analysis uses October 2019 with November 1 as boundary context. It excludes sessions touching October 1 or November 1, keeps the source `(user_id, user_session)` key and applies no inactivity timeout. The main funnel denominator is sessions with a view; timing statistics use completed session/product paths. No HTTP latency or order count is inferred from these events.

Each result has a short Markdown finding. Group conclusions assess differences between sources, identify the proposed sampling rule and map it to business tables. Section 6 consolidates rules R01–R07 with empirical findings, Vietnam evidence, remaining assumptions and application targets. It distinguishes usable relationships from reference distributions and parameters still awaiting a decision. Code cells only load data, calculate statistics and plot; the notebook writes no CSV, figures or other files. Results and commentary stay in the notebook.

UCI uses UK invoices in `[2010-12-01, 2011-12-01)`, preserving source time. Invoice metadata must have one timestamp/country and at most one known customer. Cancellations, nonpositive quantity/price and codes outside five digits plus up to two letters are excluded. Duplicate source rows and bulk purchases are retained; lines aggregate by invoice/stock code. Missing customer IDs remain in baskets but are excluded from repurchase metrics. Multiple invoices on one day remain distinct. This gift-retail source includes wholesalers; its distributions are not Vietnamese fashion-retail estimates.

R03 is now implemented through ten H&M product-rank groups in the YAML. We integrate group weights over the target rank intervals, shuffle the rank-to-product assignment with the scenario seed, then select a product before selecting one of its remaining variants. This avoids giving a product extra demand solely because it has more variants. Uniform demand inside each rank group and transfer to a smaller catalog are explicit assumptions. Other rates remain the previous scenario assumptions; the Olist/UCI distributions and Vietnam surveys are not copied into unsupported local probabilities. A generator before/after comparison or holdout benchmark remains outside scope.

From the repository root:

```bash
uv sync --locked --group eda
```

Open the notebook in VS Code and select the repository's `.venv/bin/python` kernel, or use an existing local Jupyter installation with that kernel. No containers or running services are required. EDA dependencies are in the optional `eda` group; Seaborn brings Pandas as a transitive dependency, while analysis uses Polars.

Sources and local CSV locations:

- [H&M](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data): `data/generator/eda/raw/hm/transactions_train.csv` and `articles.csv`; images are not needed. The original competition download requires Kaggle authentication and applicable competition terms.
- [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), CC BY-NC-SA 4.0: CSV files under `data/generator/eda/raw/olist/`.
- [eCommerce Behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): `2019-Oct.csv` and `2019-Nov.csv` under `data/generator/eda/raw/ecommerce_behavior/`. Follow the author's source-attribution requirements on the dataset page.
- [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail), Chen, D. (2015), DOI `10.24432/C5BW33`, CC BY 4.0: `data/generator/eda/raw/uci/online_retail.csv`. The 541,909 rows were converted once from the official XLSX with Python's standard library; source columns/rows are retained and Excel dates become naive ISO timestamps. No spreadsheet dependency or runtime profile mechanism is added.

UCI official archive SHA-256: `f5385cbb54bbebf7196389109c6b0621faab0c304e3702548165e71c84aede8b`; prepared CSV SHA-256: `9d6061248a4783cbd8aa464bdb3ff355efe60cf1b1924d6b8414f5980987c20f`. The archive/conversion tooling stays outside the repository; only the CSV remains under `data/`.

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
