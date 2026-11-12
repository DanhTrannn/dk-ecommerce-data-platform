<!-- prettier-ignore -->
<div align="center">

# D&K E-Commerce Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.8-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.8-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-1.19-E6526F?style=flat-square&logo=apacheflink&logoColor=white)](https://flink.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.9-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.10.1-blue?style=flat-square&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![Apache Polaris](https://img.shields.io/badge/Apache_Polaris-1.8.0-teal?style=flat-square&logo=apache&logoColor=white)](https://polaris.apache.org)
[![Trino](https://img.shields.io/badge/Trino-483-DD00A1?style=flat-square&logo=trino&logoColor=white)](https://trino.io)
[![Apache Superset](https://img.shields.io/badge/Apache_Superset-6.1.0-20A6C9?style=flat-square&logo=apachesuperset&logoColor=white)](https://superset.apache.org)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)](.github/workflows/ci.yml)

:star: If you find this project useful, consider giving it a star!

[Overview](#overview) • [Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Development](#development-commands) • [Documentation](#documentation-index)

</div>

## Overview

**D&K E-Commerce Data Platform** is a local Data Engineering project for a Vietnamese retailer with online and in-store sales. It combines configurable synthetic transaction data, a FastAPI source application, batch processing, PostgreSQL change capture, and real API access logs. Analytical tables use Apache Iceberg on MinIO, an Apache Polaris catalog, and Trino for SQL access.

The current interface is a **Streamlit Platform Console** for running online purchase scenarios against the API and managing existing Lakehouse access bindings. An optional **Apache Superset sales PoC** reads two published sales marts. The legacy Next.js application remains in source and CI, but is not started by Compose; its retired business BI endpoints are no longer available.

> **Three separate data paths:** Airflow/Spark batch is the only writer of the serving OLTP Silver/Gold namespaces. PostgreSQL CDC records Bronze audit events. API access logs flow through Kafka/Flink to Landing, then through a separate Spark pipeline and publication. CDC capture alone does not refresh sales dashboards.

## Features

- **Synthetic data rules:** EDA notebooks derive purchase rules (H&M, Olist) and browsing rules (eCommerce Behavior) into YAML presets for OLTP (`1m`/`10m`/`50m` orders per year) and access logs. The SQL generator that consumes them is not implemented yet; see [generator/README.md](generator/README.md).
- **Source workflows:** Retained APIs cover authentication, products, cart/checkout, POS, stock, inbound receipts, payments, delivery, returns and reviews. Streamlit currently automates online purchase attempts; it does not automate every retained API workflow.
- **Incremental batch:** Fixed Airflow intervals, paginated JDBC staging, Bronze history, Silver typing/deduplication/MERGE and PII pseudonymization, followed by Gold rebuilds and validation.
- **Open Lakehouse:** Iceberg snapshots, Polaris access control, MinIO workload accounts, Trino read-only access, and scheduled maintenance for an explicit table subset.
- **Data quality and publication:** Reconciliation, quality results and publication manifests keep analytical reads tied to validated snapshots. There are 15 OLTP marts and three access-log marts.
- **CDC audit:** Debezium captures changes from 27 PostgreSQL source tables; Flink preserves before/after payloads and transport/source metadata in Bronze. A separate manual batch DAG handles a bounded cart/wishlist hard-delete case.
- **HTTP observability:** Real request capture, local delivery DLQ, Landing retention, Spark deduplication and three HTTP/product/funnel marts. Published and provisional HTTP analytics retain separate contracts.
- **Sales BI:** Superset provides four KPI cards and four charts over two publication-pinned sales marts. Viewer/store permissions and the remaining BI areas are pending.

## Tech Stack

| Area | Technologies |
|---|---|
| Source and console | Python 3.11, FastAPI, SQLAlchemy, Streamlit, PostgreSQL 16.8 |
| Batch and orchestration | Apache Spark 3.5.9, Apache Airflow 3.3.2 |
| Change and event capture | Debezium 3.0.8, Apache Kafka 3.8, Apache Flink 1.19 |
| Storage and catalog | MinIO, Apache Iceberg 1.10.1, Apache Polaris 1.8.0 |
| Analytics and BI | Trino 483, Apache Superset 6.1.0 |
| Tooling | Docker Compose, uv workspace/lockfile, Ruff, unittest, GitHub Actions |
| Legacy frontend | Next.js 15, React 19, Apache ECharts 6; retained outside the Compose runtime |

## Quick Start

### Prerequisites and configuration

Use Docker Engine with Compose v2, Python 3.11 and uv. Node.js 22/npm are required only for legacy frontend development and its CI checks.

```bash
# Only when .env does not exist:
cp .env.example .env
uv sync --locked
```

Configure `.env` before starting services. It includes source/metadata database credentials, API auth settings, MinIO workload accounts, Polaris bootstrap credentials, Airflow keys and a stable `SILVER_PSEUDONYMIZE_SALT`. Superset additionally requires its metadata password, secret key and admin password. Example credentials are local placeholders; never commit real credentials. Host settings and Compose container settings differ; use [setup.md](docs/setup.md) for the complete variable groups and bootstrap sequence.

```bash
# Validate without printing resolved credentials:
docker compose --env-file .env.example --profile '*' config --quiet
make build

# Start storage/catalog services first:
docker compose --profile core up -d postgres minio polaris
make provision-lakehouse
```

### Initialize the source

Service startup does not apply the source schema, seeds, reader grants or CDC configuration. For a fresh business database:

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_ECOMMERCE_PASSWORD" exec psql -X -v ON_ERROR_STOP=1 --host=127.0.0.1 --username="$POSTGRES_ECOMMERCE_USER" --dbname="$POSTGRES_ECOMMERCE_DB"' \
  < database/schema.sql

make seed
make source-reader
make up PROFILES=core,batch
```

There is currently no bulk dataset import: the previous generator and its SQL importer were removed, and the new generator is pending. `make seed` supplies application catalog/bootstrap accounts, with optional admin credentials supplied to the host process; it does not create cities or stores. Existing databases are not migrated automatically. See [database/README.md](database/README.md) before importing into a nonempty database.

### Run data pipelines

```bash
# First full OLTP publication; monitor completion in Airflow:
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp

# Enable daily incremental runs after initialization succeeds:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
```

DAGs start paused. Do not overlap full initialization and incremental runs. For streaming, enable the additional profile and run source CDC setup, connector registration and Flink submission explicitly. Real HTTP capture also requires `KAFKA_ENABLED=true`; access-log processing has its own DAG. The exact startup/recovery steps are in [setup.md](docs/setup.md) and [operations.md](docs/operations.md).

### Optional Superset

After a successful OLTP publication and Superset environment configuration:

```bash
# Apply PostgreSQL BI environment/mount changes while jobs are idle:
docker compose --profile core up -d postgres
make superset-db
make superset-init
docker compose --profile core --profile bi up -d superset
make superset-sync
```

Open the dashboard URL returned by sync. Each dashboard is pinned to one publication's Iceberg snapshots. After a new batch publication, run sync again and open its new URL; an existing URL stays on its earlier snapshots. The current PoC is admin-only and does not inherit source API roles or store permissions.

### Profiles and service endpoints

| Profile | Responsibility |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris, Airflow, Trino, platform-app and internal lakehouse-access; explicit provisioning jobs |
| `batch` | Local Spark client, used together with core |
| `streaming` | Kafka, Connect and Flink; connector/job submission services run manually |
| `bi` | Superset web and explicit initialization/publication-sync jobs |

| Interface | Default local address |
|---|---|
| Streamlit Platform Console | <http://127.0.0.1:8501> |
| FastAPI / OpenAPI UI | <http://127.0.0.1:8000/docs> |
| Airflow | <http://localhost:8080> |
| Trino | <http://127.0.0.1:8084> |
| Superset, when enabled | <http://127.0.0.1:8088> |
| MinIO API / console | <http://127.0.0.1:9000> / <http://127.0.0.1:9001> |
| Polaris catalog / health | <http://127.0.0.1:8181/api/catalog> / <http://127.0.0.1:8182/q/health> |
| Flink / Debezium Connect | <http://localhost:8085> / <http://127.0.0.1:8086> |

Kafka host clients use `localhost:9094`; containers use `kafka:9092`. PostgreSQL defaults to host port 5432. `lakehouse-access:8010` is internal and has no published host port. Bind addresses/ports are configurable; see setup before using this local stack on an externally reachable machine.

## Architecture

| Path | Processing | Result |
|---|---|---|
| OLTP batch | PostgreSQL → Spark staging/Bronze → Silver → Gold → quality gate/publication | Six dimensions, six facts and 15 business marts; Trino/Superset read published snapshots |
| PostgreSQL CDC | WAL → Debezium → Kafka → Flink | Immutable Bronze change audit; no continuous Gold materialization |
| API access logs | FastAPI → Kafka → Flink Landing → Spark Bronze/Silver/Gold | HTTP fact and three marts, with an independent publication |

The source has 28 tables; 27 are configured for analytical extraction and CDC, excluding `customer_credentials`. Mutable and append-only sources use different cursors. Currency is VND; business time uses Asia/Ho_Chi_Minh, with UTC handling defined per pipeline. Generated transactions, API-created transactions and observed request logs have distinct provenance.

Publication gates prevent a failed refresh from replacing the last accepted manifest. Superset SQL pins snapshot IDs; expired snapshots require operational coordination. Current CDC and log acceptance results establish bounded integration behavior, not production availability or latency guarantees. Details: [architecture](docs/architecture.md), [data model and KPI contracts](docs/data.md), [operational evidence and limits](docs/operations.md).

### Repository structure

```text
.
├── services/
│   ├── platform-app/       # FastAPI, Streamlit, workers, access administration, provisioning
│   └── storefront/         # Legacy Next.js application
├── generator/              # EDA notebooks and YAML presets: oltp/ (purchases), logs/ (browsing)
├── database/               # PostgreSQL schema and explicit seeds
├── pipelines/              # Spark batch, Flink streaming, source contracts and tests
├── airflow/                # DAGs and Airflow runtime
├── infrastructure/         # Storage, catalog, streaming, Trino and Superset configuration
├── docs/                   # Four technical references and the retained final report
├── assets/report/          # Local report figures and diagram sources (gitignored)
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── uv.lock
```

## Data Generation and Source APIs

OLTP presets live in `generator/oltp/configs/`: `1m.yml`, `10m.yml` and `50m.yml` target about 1, 10 and 50 million orders over 12 months (260,000 / 2,600,000 / 13,000,000 buying customers). Total orders are an outcome of the purchase-frequency rule, not an input. Browsing rules for synthetic access logs live in `generator/logs/configs/access_logs.yml`. No runtime reads these presets yet. See [generator/README.md](generator/README.md) and the [synthetic data notes](docs/data.md#8-dữ-liệu-sinh-và-căn-cứ).

Streamlit supports online purchase scenarios and existing Polaris/MinIO binding administration. Scenario runs are persisted under `data/generator/scenarios/`. Purchase attempts create real source API records; uncertain writes are recorded for inspection instead of being blindly replayed. Runtime logs are observed HTTP requests, not historical synthetic sessions.

FastAPI retains source write operations and the lookups needed to supply their IDs. Six former business BI aggregation endpoints and ten administrative list/duplicate GET endpoints are retired. Inspect the current route contract at `/docs` or in the [API guide](services/platform-app/src/platform_app/api/README.md). The two retained administrative HTTP analytics endpoints are:

- `/api/v1/admin/analytics/http-metrics`: published HTTP mart, bounded cached fallback, 1–30 UTC dates.
- `/api/v1/admin/analytics/http-realtime`: provisional Landing analysis, 5–60-minute event-time window.

Both require `admin`/`system_admin`. Streamlit access administration uses the internal service to inspect and assign/revoke supported existing bindings; account creation, arbitrary policy editing and secret rotation are outside its current scope.

The optional **Polars + PyIceberg migration runtime** includes the categories pilot and an all-27-source Bronze/Silver CLI with validation, PII pseudonymization and quarantine. It also builds all 27 Gold tables, persists 55 quality checks and publishes validated snapshot manifests in isolated migration namespaces. It stays outside scheduled serving and the current BI readers. Setup, opt-in checks and limitations are in the [pipeline guide](pipelines/README.md#polars--pyiceberg-migration-pilot). An additional native CLI builds the three log marts from the persisted serving fact and a pinned published catalog. Spark still performs log ingestion and scheduled serving; the native outputs have separate publication and are not connected to current BI readers.

## Development Commands

```bash
uv lock --check
python3 -m compileall -q airflow/dags/
uvx ruff check .

# Tests are kept local only (every tests/ directory is gitignored, except pipelines/dbt/tests data tests).
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v

make ps
make logs
# Stop containers, retain them and their volumes:
docker compose --profile '*' stop
# Remove containers/networks, retain named volumes:
make down
```

`make stop` is an alias for `make down`. It is not the container-retaining stop command above. Pipeline integration checks use opt-in flags and isolated resources; see [operations.md](docs/operations.md). Do not assume ordinary unit tests run Docker acceptance checks.

The [CI workflow](.github/workflows/ci.yml) has two jobs: repository validation (six Compose profile combinations, DAG compilation and Ruff) and legacy storefront validation (`npm ci`, lint, typecheck and production build). Documentation-only changes are skipped. Unit and opt-in integration suites are separate local checks. Next.js remains in CI until its removal is implemented.

## Documentation Index

| Document | Responsibility |
|---|---|
| [architecture.md](docs/architecture.md) | System components, deployment boundaries and data flows |
| [data.md](docs/data.md) | Sources, schema, Bronze/Silver/Gold, ETL/ELT, marts, KPIs and generator assumptions |
| [setup.md](docs/setup.md) | Installation, environment configuration, explicit bootstrap and startup |
| [operations.md](docs/operations.md) | Monitoring, logging, troubleshooting, recovery, maintenance and acceptance evidence |
| [FINAL_REPORT.md](docs/FINAL_REPORT.md) | Retained academic report; historical content is not the current runtime specification |

Component usage remains beside its source: [platform-app](services/platform-app/README.md), [generator](generator/README.md), [database](database/README.md) and [pipelines](pipelines/README.md).

Report figures and diagram sources are kept locally in `assets/report/`, which is excluded by `.gitignore`. Copy that directory separately when sharing or rendering `docs/FINAL_REPORT.md`; a fresh clone does not include it.
