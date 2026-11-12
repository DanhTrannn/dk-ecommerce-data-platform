# Synthetic Data Generator

This directory holds the research, the presets and the generator for synthetic data. The `datagen` command loads 12 months of synthetic OLTP history (customers, orders, payments, shipments, returns, reviews, carts, wishlists, POS sales and the inventory ledger) into the D&K PostgreSQL database. A historical access-log generator that reads this OLTP data will consume the log presets later; it is not built yet.

```text
generator/
├── pyproject.toml         workspace package `data-generator`, command `datagen`
├── src/datagen/
│   ├── cli.py             generate, verify, reset
│   ├── presets.py         typed, validated preset YAML
│   ├── randomness.py      one random stream per batch, derived from the seed
│   ├── distributions.py   closed-form expectations shared by generate and verify
│   ├── postgres.py        the only module that talks to PostgreSQL (schema check, COPY)
│   ├── oltp/              one module per group of tables; run.py orchestrates a run
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

- `generate` is a one-time history load. It checks the live schema against its own column list, refuses to run when synthetic rows or API-created orders already exist, and loads batch by batch with `COPY`; every batch is one transaction, so a failed batch leaves none of its rows. Rows carry `data_origin = 'synthetic'` and `generation_run_id = <scenario_id>:<seed>`. The same preset and seed give the same rows.
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
| `1m` | `verify` 22/22 pass | 1,126 s (≈ 19 min); `verify` 24 s | 1.8 GB | 286,016 customers, 1,147,317 orders, 4,218,889 order items, 4,929,285 ledger rows; database 7.6 GB |
| `10m` | not completed | stopped after 10 of 58 online batches (31 min) | — | — |

About 96% of the 1m run is PostgreSQL loading the `COPY` data; generating the rows takes under a minute. Load time grows as the tables and their indexes grow: equal-sized online batches (≈ 3.9 million rows) took 77 s at first and 184 s by the sixth in `1m`, and 291 s by the tenth in `10m`. Projected linearly, the online part of `10m` alone needs about 11 hours, so `10m` and `50m` are not practical with the current load path. A larger `max_wal_size` and `shared_buffers` did not change the first five batch times.

## Access-log rules

[browsing_behavior_eda.ipynb](logs/notebooks/browsing_behavior_eda.ipynb) measures three rules on eCommerce Behavior (2–31 October 2019, `apparel` sessions) and writes them to `logs/configs/1m.yml`, `10m.yml` and `50m.yml` (same rules; each names the OLTP preset it follows and carries its own volume estimate):

| Rule | Behavior | Parameter |
|---|---|---|
| L1 | Non-buying sessions = r × buying sessions | r = 41.39 (plausible 31–52 by session definition) |
| L2 | Product views per session: geometric (buying), log-series (non-buying) | p = 0.1044; θ = 0.9239 |
| L3 | Seconds between requests: log-normal | median 36.2 s, σ = 0.938 |

Cart abandonment, product-view weights, request steps, anonymous share and sale-day traffic are assumptions (section 4). The last notebook cell asserts that all three YAML files match the notebook values. The log contract has no session id, so multi-session journeys are not modeled.

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
- [eCommerce Behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): `2019-Oct.csv` and `2019-Nov.csv` under `data/generator/eda/raw/ecommerce_behavior/` (browsing notebook only; November is read only to detect sessions crossing midnight on 31 October). Follow the author's attribution requirements.
