# Synthetic Data Rules

This directory holds the research and configuration for synthetic data. It does not contain a runtime generator: the previous `data-generator` package (SQL export, importer, Streamlit console) was removed. A new OLTP generator, and a historical access-log generator that reads OLTP data from PostgreSQL, will consume these presets later.

```text
generator/
├── oltp/
│   ├── configs/     1m.yml, 10m.yml, 50m.yml   purchase-behavior presets
│   └── notebooks/   purchase_behavior_eda.ipynb
└── logs/
    ├── configs/     access_logs.yml             browsing-behavior rules
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

## Access-log rules

[browsing_behavior_eda.ipynb](logs/notebooks/browsing_behavior_eda.ipynb) measures three rules on eCommerce Behavior (2–31 October 2019, `apparel` sessions) and writes them to `access_logs.yml`:

| Rule | Behavior | Parameter |
|---|---|---|
| L1 | Non-buying sessions = r × buying sessions | r = 41.39 (plausible 31–52 by session definition) |
| L2 | Product views per session: geometric (buying), log-series (non-buying) | p = 0.1044; θ = 0.9239 |
| L3 | Seconds between requests: log-normal | median 36.2 s, σ = 0.938 |

Cart abandonment, product-view weights, request steps, anonymous share and sale-day traffic are assumptions (section 4). The last notebook cell asserts that the YAML matches the notebook values. The log contract has no session id, so multi-session journeys are not modeled.

## Running the notebooks

```bash
uv sync --locked --group eda
uv run --locked --group eda jupyter nbconvert --to notebook --execute --inplace \
  generator/oltp/notebooks/purchase_behavior_eda.ipynb
uv run --locked --group eda jupyter nbconvert --to notebook --execute --inplace \
  generator/logs/notebooks/browsing_behavior_eda.ipynb
```

Or open them in VS Code with the repository `.venv/bin/python` kernel. No services are required. Notebooks locate the repository root through `.git` and read CSVs from `data/generator/eda/raw/`.

## Sources

- [H&M](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data): `data/generator/eda/raw/hm/transactions_train.csv` and `articles.csv`. The original download requires Kaggle authentication and the competition terms. The local copy comes from the mirror [einrafh/hnm-fashion-recommendations-data](https://huggingface.co/datasets/einrafh/hnm-fashion-recommendations-data/tree/acde210b0090be673abdba5f989c48925c856f78) pinned at commit `acde210b`; sizes and SHA-256 were checked against its Git LFS pointers.
- [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), CC BY-NC-SA 4.0: CSV files under `data/generator/eda/raw/olist/`.
- [eCommerce Behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): `2019-Oct.csv` and `2019-Nov.csv` under `data/generator/eda/raw/ecommerce_behavior/` (browsing notebook only; November is read only to detect sessions crossing midnight on 31 October). Follow the author's attribution requirements.
