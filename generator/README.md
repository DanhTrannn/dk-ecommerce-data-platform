# Synthetic Data Generator

This directory holds the research, the presets and the generator for synthetic data. The `datagen` command loads 12 months of synthetic OLTP history (customers, orders, payments, shipments, returns, reviews, carts, wishlists, POS sales and the inventory ledger) into the D&K PostgreSQL database. `datagen logs` then writes the matching access-log history (sale season 1 November – 15 December 2025) as daily Parquet files for the lakehouse backfill.

```text
generator/
├── pyproject.toml         workspace package `data-generator`, command `datagen`
├── src/datagen/
│   ├── cli.py             generate, verify, logs, reset
│   ├── presets.py         typed, validated preset YAML
│   ├── randomness.py      one random stream per batch, derived from the seed
│   ├── distributions.py   closed-form expectations shared by generate and verify
│   ├── postgres.py        the only module that talks to PostgreSQL (schema check, COPY)
│   ├── oltp/              one module per group of tables; run.py orchestrates a run
│   ├── logs/              access-log history: inputs.py (PostgreSQL), sessions.py, events.py, run.py
│   └── verify/            ledger.py, rules.py, report.py
├── oltp/
│   ├── configs/     1m.yml, 10m.yml, 50m.yml   purchase-behavior presets
│   └── notebooks/   purchase_behavior_eda.ipynb
└── logs/
    ├── configs/     1m.yml, 10m.yml, 50m.yml   browsing-behavior presets
    └── notebooks/   browsing_behavior_eda.ipynb
```

Every YAML block carries a `source` label: `measured` (measured in the notebook), `transfer` (measured on a foreign source and applied to a Vietnamese fashion shop as an assumption) or `assumption` (scenario parameter without data).

## OLTP presets

| Preset | Buying customers | Products | Expected orders (12 months) |
|---|---|---|---|
| `1m.yml` | 260,000 | 500 | ≈ 1,000,000 |
| `10m.yml` | 2,600,000 | 1,000 | ≈ 10,000,000 |
| `50m.yml` | 13,000,000 | 2,000 | ≈ 50,000,000 |

Total orders are an outcome, not an input: expected orders = buying customers × 3.846 for 12 months of history (log-series purchase frequency). Changing `history_months` changes that factor. The three presets share all behavior rules and differ only in `scenario_id` and `scale`.

Rules measured in [purchase_behavior_eda.ipynb](oltp/notebooks/purchase_behavior_eda.ipynb) (H&M online 2019, Olist 2017):

| Rule | Behavior | Parameter |
|---|---|---|
| R01 | Orders per customer: log-series, θ = T/(α+T) | α = 0.1141 years |
| R02 | Gap between orders: exponential, mean T(1 − c/k)/(k+1) | c = 0.518 |
| R03 | Product demand: LogNormal(0, σ) × category multiplier | σ = 1.35 |
| R04 | Basket: geometric products per order; variant/quantity tables | p = 0.296 |
| R05 | Sale days ×6.39, three days after ×2.6; flat weekdays; Olist hour profiles | — |
| R06 | Review stars by delivery lateness (Olist table) | — |

Section 7 lists scenario assumptions (COD, cancellations, returns, coupons, delivery days, Tết) and section 7.1 cross-checks them against public Vietnamese figures; those figures are not used for calibration.

Blocks without a notebook measurement are scenario assumptions: `scale.stores` (5 / 20 / 50 stores), `locations` (cities and their share of customers, shipping addresses and stores), `pos` (POS orders per online order, walk-in rate, payment methods, opening hours, staff per store) and `policies` (operational ranges such as `timing.payment_delay_minutes`, `timing.dispatch_delay_hours`, `timing.completion_delay_hours`, `carts.active_cart_rate`, `inventory.store_buffer_units` and `shipping.delivery_staff`).

## Running the OLTP generator

The generator needs only PostgreSQL with the schema from `database/schema.sql` and the catalog seed (see [docs/setup.md](../docs/setup.md)). It reads the connection from `GENERATOR_DATABASE_URL` in `.env`; `.env.example` derives it from the source database owner.

```bash
make generate PRESET=1m          # datagen generate --preset 1m
make generate-verify PRESET=1m   # datagen verify --preset 1m
uv run --locked --env-file .env --package data-generator -- datagen reset --yes
```

- `generate` is a one-time history load. It checks the live schema against its own column list, refuses to run when synthetic rows or API-created orders already exist, and loads batch by batch with `COPY`, all in one transaction. Before the first batch it drops the foreign keys, unique constraints and secondary indexes of the loaded tables (primary keys, CHECK and NOT NULL stay) and rebuilds them from the live catalog after the last one. Any failure rolls back the whole run: no row remains and every index and constraint is back. The rolled-back rows still take disk space, though (tens of GB for `10m`), so before rerunning run `datagen reset --yes` and `make seed`; the error message says so too. The 27 loaded tables stay locked until the run commits, so the API, Streamlit and source readers wait. Rows carry `data_origin = 'synthetic'` and `generation_run_id = <scenario_id>:<seed>`. The same preset and seed give the same rows.
- `verify` only reads. It prints expected value, measured value, tolerance and verdict for each check and exits non-zero when a check fails.
- `reset --yes` truncates all 28 source tables, including the seed, so run `make seed` again afterwards. It refuses when the database holds orders created through the API (`data_origin = 'manual'`).

History ends at `anchor_time`; orders created shortly before it are still in flight (paid, confirmed, shipping). POS orders are completed at the counter. Stock for a month is received and transferred to stores before that month starts, so no running balance goes negative.

### Verify checks

Tolerances live in one table, `verify/report.py`. "Old" orders are online orders created more than 45 days before the anchor, old enough that every event has happened.

| Check | Expected |
|---|---|
| `ledger.central_balance_mismatches`, `ledger.store_balance_mismatches` | every balance = `opening_on_hand` + sum of its ledger rows |
| `ledger.negative_running_balances` | no (variant, location) ever below zero over time |
| `ledger.rows_without_required_document` | every ledger row names the right order, receipt, return or transfer code |
| `r01.orders_per_buyer`, `r01.single_order_share` | log-series mean and P(1 order) |
| `r02.mean_gap_days_two_orders` | T(1 − c/2)/3 for customers with exactly two orders |
| `r03.top_decile_unit_share` | share of units sold by the top 10% of products under LogNormal(0, σ) |
| `r04.products_per_order` | 1/p |
| `r05.sale_day_ratio` | mean orders on sale days ÷ median daily orders = sale-day multiplier |
| `outcome.cod_share`, `outcome.cancellation_rate`, `outcome.cod_failure_rate`, `outcome.return_rate`, `outcome.exchange_share` | the `outcomes` rates |
| `coupon.usage_rate` | `coupons.usage_rate` on eligible ordinary-day orders |
| `r06.review_write_rate`, `r06.on_time_five_star_share` | review rate; five-star share of on-time deliveries |
| `pos.orders_per_online_order`, `pos.walk_in_rate`, `pos.cash_share` | the `pos` block |
| `location.largest_city_customer_share` | largest `locations.cities` share |

### Measured runs

Local PostgreSQL 16.8 with the Compose defaults, 8 cores, 15 GB RAM:

| Preset | Result | Wall time | Peak memory | Rows |
|---|---|---|---|---|
| `1m` | `verify` 22/22 pass | 210 s (≈ 3.5 min), of which rebuilding 115 indexes and 55 foreign keys takes 73 s; `verify` 26 s | 1.8 GB | 286,016 customers, 1,147,317 orders, 4,218,889 order items, 4,929,285 ledger rows; database 6.9 GB |
| `10m` | `verify` 22/22 pass | 3,651 s (≈ 61 min): loading 20 min, rebuilding 115 indexes 27 min and 55 foreign keys 5.5 min; `verify` 16 min | 2.0 GB | 2,860,061 customers, 11,495,069 orders, 42,338,226 order items, 49,073,756 ledger rows; database 67 GB |

Without indexes during the load, equal-sized online batches (≈ 3.9 million rows) load in 10–20 s each and no longer slow down as the tables grow. With every index in place, the same batches took 77 s at first and 184 s by the sixth, the whole `1m` run took 1,126 s, and `10m` was stopped after 10 of 58 online batches because its projection passed 11 hours.

## Access-log rules

[browsing_behavior_eda.ipynb](logs/notebooks/browsing_behavior_eda.ipynb) measures four rules on eCommerce Behavior (`apparel` sessions; L1–L3 on 2–31 October 2019, L4 on November 2019) and writes them to `logs/configs/1m.yml`, `10m.yml` and `50m.yml` (same rules; each names the OLTP preset it follows and carries its log volume):

| Rule | Behavior | Parameter |
|---|---|---|
| L1 | Non-buying sessions = r × buying sessions | r = 41.39 (plausible 31–52 by session definition) |
| L2 | Product views per session: geometric (buying), log-series (non-buying) | p = 0.1044; θ = 0.9239 |
| L3 | Seconds between requests: log-normal | median 36.2 s, σ = 0.938 |
| L4 | On a sale-event day r is multiplied: the data's busiest day (17 November 2019) had r = 16.2 against a median of 45.9, so a sale-day visit converts about 2.8 times as often | × 0.35 (one sale event) |

The cart rate of browsing sessions, product-view weights, request steps and sale-day traffic following orders are assumptions (section 5). The last notebook cell asserts that all three YAML files match the notebook values. Multi-session journeys (browse today, buy days later) are not modeled.

## Running the access-log generator

```bash
uv run --locked --env-file .env --package data-generator -- datagen logs --preset 1m --out <new or empty directory>
```

- It reads the OLTP history of the same preset from PostgreSQL and refuses when the database holds another preset's run, when the window has no online order, or when `--out` already has files.
- Every online order of the window gets one logged-in session that ends with `checkout_submit` at the order's `created_at`, carrying its `order_number`. Browsing sessions per day = r × buying sessions (× L4 on 11-11, Black Friday and 12-12); 7.5% of them log in and add one product to the cart, the rest stay anonymous.
- Each request is one JSON event of contract `ecommerce.access` 1.1.0 (adds `session.id` and `ecommerce.order_number` to 1.0.0) with `data_origin = "synthetic"`, `status_code = 200` and `duration_ns = 0` (latency is not simulated).
- Output: `session_date=YYYY-MM-DD.parquet` per day with `raw_payload` (the JSON text) and `event_at` (UTC). A session that crosses midnight stays in its own day's file; the lakehouse partitions by `event_at`. Each day has its own random stream, so the same preset, seed and OLTP data give the same events.
- The lakehouse loader for these files is not built yet; until then, check them by reading the Parquet files directly.

Measured `1m` run on the same machine: 45 files, 206,632 buying and 7,147,494 browsing sessions, 45,083,918 requests, 2.4 GB of Parquet, 86 s, 2.9 GB peak memory. Every online order of the window appears once as a `checkout_submit`; the sale-event days carry 6.4 times the buying sessions of an ordinary day with r = 14.44 (41.39 × 0.35).

## Running the notebooks

```bash
uv sync --locked --group eda
uv run --locked --group eda jupyter nbconvert --to notebook --execute --inplace \
  generator/oltp/notebooks/purchase_behavior_eda.ipynb
uv run --locked --group eda jupyter nbconvert --to notebook --execute --inplace \
  generator/logs/notebooks/browsing_behavior_eda.ipynb
```

Or open them in VS Code with the repository `.venv/bin/python` kernel. No services are required for the notebooks. Notebooks locate the repository root through `.git` and read CSVs from `data/generator/eda/raw/`.

## Sources

- [H&M](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data): `data/generator/eda/raw/hm/transactions_train.csv` and `articles.csv`. The original download requires Kaggle authentication and the competition terms. The local copy comes from the mirror [einrafh/hnm-fashion-recommendations-data](https://huggingface.co/datasets/einrafh/hnm-fashion-recommendations-data/tree/acde210b0090be673abdba5f989c48925c856f78) pinned at commit `acde210b`; sizes and SHA-256 were checked against its Git LFS pointers.
- [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), CC BY-NC-SA 4.0: CSV files under `data/generator/eda/raw/olist/`.
- [eCommerce Behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): `2019-Oct.csv` and `2019-Nov.csv` under `data/generator/eda/raw/ecommerce_behavior/` (browsing notebook only; November detects sessions crossing midnight on 31 October and measures L4). Follow the author's attribution requirements.
