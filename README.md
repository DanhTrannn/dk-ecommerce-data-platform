<!-- prettier-ignore -->
<div align="center">

# D&K E-Commerce Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Node.js](https://img.shields.io/badge/Node.js-22-339933?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.4-4479A1?style=flat-square&logo=mysql&logoColor=white)](https://mysql.com)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.8-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-1.19-E6526F?style=flat-square&logo=apacheflink&logoColor=white)](https://flink.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.9-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.10.1-blue?style=flat-square&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![Apache Polaris](https://img.shields.io/badge/Apache_Polaris-1.8.0-teal?style=flat-square&logo=apache&logoColor=white)](https://polaris.apache.org)
[![Trino](https://img.shields.io/badge/Trino-483-DD00A1?style=flat-square&logo=trino&logoColor=white)](https://trino.io)
[![Apache ECharts](https://img.shields.io/badge/Apache_ECharts-6.1-AA344D?style=flat-square&logo=apacheecharts&logoColor=white)](https://echarts.apache.org)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)](.github/workflows/ci.yml)

:star: If you find this project useful, consider giving it a star!

[Overview](#overview) • [Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Development](#development-commands) • [Documentation](#documentation-index)

</div>

> **Infrastructure transition:** Compose uses one PostgreSQL service with separate `ecommerce`, `airflow` and `polaris` databases. The standalone API/frontend containers and old Python bootstrap services are excluded. FastAPI and Streamlit share platform-app. Application schema/ORM, generator source integration, JDBC/source handling and CDC consumer conversion remain pending. Existing data is not migrated; infrastructure image builds and service health checks do not verify business pipelines.

**Current infrastructure-only scope:** Compose excludes standalone API/frontend services. FastAPI and the Streamlit Generator Console run together in the platform-app Python slim image. The `core` profile starts PostgreSQL, MinIO, Polaris, Airflow, Trino and platform-app. Airflow has service startup configuration and the Python path required to parse mounted DAGs; source credentials, Spark connections, pipeline configuration and PII salt must be supplied later before running business DAGs. Existing application/startup instructions below describe the broader project and are outside this temporary scope.

Application code is grouped under `services/`: `services/platform-app` owns the shared Python package `platform_app` (`api/` and `ui/`), Dockerfile and startup script; `services/storefront` retains Next.js. `generator/` stays a separate reusable CLI/package. Compose service and image are named `platform-app`; host settings use `PLATFORM_APP_*`.

`platform-app` builds from `services/platform-app/Dockerfile` and uses the `PLATFORM_APP_*` host port/user settings. The Python distribution and UI command are named `platform-app`. Make starts services only; it no longer initializes Gold tables. CDC registration and Flink submission jobs belong to streaming with `scale: 0`; run them manually with Compose run.

The shared container exposes Streamlit on `PLATFORM_APP_PORT` (8501) and FastAPI on `API_PORT` (8000). Both use `PLATFORM_APP_BIND_ADDRESS`. Its healthcheck verifies both HTTP processes; PostgreSQL readiness is checked separately at `/health/ready`. No source schema or seed is applied automatically. The PostgreSQL driver is available, but migration of business schema/SQL remains pending. Both processes shut down when either exits.

MinIO runs only its storage server image; there is no separate Python init image/container. FastAPI and Streamlit run in `platform-app`; future bootstrap tools will use the same container. MinIO policies remain as reference files; bootstrap will be redesigned separately. Airflow and PyFlink retain their engine runtimes.

Infrastructure images remain separate per service. `ICEBERG_VERSION` is passed to Airflow, Spark and Flink builds. Flink configuration and launcher scripts are baked into its image; rebuild after editing `infrastructure/flink/conf/` or `infrastructure/flink/bin/`. Airflow installs runtime dependencies with pip independently of the business workspace. MinIO/Polaris account and catalog bootstrap will be redesigned in `platform-app`.

The **D&K E-Commerce Data Platform** is a local Data Engineering project combining an e-commerce source application, batch data processing, MySQL change capture, and access-log streaming. MySQL contains 28 OLTP tables; 27 are configured for analytical ingestion. Data is stored in Apache Iceberg on MinIO, cataloged by Polaris, and queried through Trino. A Next.js/FastAPI application provides Storefront, POS, administration, and ECharts dashboards.

> [!NOTE]
> The platform has three data paths: **OLTP batch**, **MySQL binlog CDC**, and **access-log streaming**. Airflow batch is the only OLTP Silver/Gold writer. CDC continuously records Bronze audit events; it does not apply changes to analytical tables.

The planned transition replaces Next.js with a Streamlit generator console and Apache Superset over Trino. Phase 1 records [scenario/API scope](docs/project/refactor_v2.md) and a [local data baseline](docs/project/DATA_BASELINE.md). Next.js remains the implemented UI; Streamlit and Superset are not installed yet.

## Overview

The project covers data collection, incremental extraction, Medallion transformations, data quality, reconciliation, and SQL analytics. The website and POS provide transactional source data; the generator supplies configurable synthetic datasets and simulated API traffic.

Published reports for all seven analytics roles, admin overview, store overview, and sales trends query Trino over pinned Iceberg publications. System HTTP Realtime separately observes a current Landing snapshot. Marketing has independent log and OLTP publications, with separate availability and cache metadata. MySQL remains the transactional source for checkout, POS, inventory operations, authentication, RBAC, and source reconciliation. If Trino is unavailable, the API serves a bounded last-good OLAP response or returns 503; it does not switch analytical aggregates to MySQL. Machine learning training and scoring remain outside the implemented scope.

## Features

- **Hybrid Ingestion:** Spark batch extraction, Debezium MySQL CDC, and Kafka/Flink access-log processing.
- **Incremental Batch:** Airflow time intervals, sequential Spark JDBC extraction in 10,000-row chunks, reusable Parquet staging, retry-safe Bronze history, and incremental Silver MERGE; no lookback, cursor files or manifests. Source tables and mappings are declared in `pipelines/src/lakehouse/utils/config.py`. A separate manual DAG synchronizes CDC hard deletes only for removed cart/wishlist items with soft-delete evidence and source deletions at least 30 days old, while retaining Bronze/audit history. Both OLTP DAGs expose one task per table in five TaskGroups: staging, Bronze, Silver, Gold and mart.
- **Medallion Processing:** Bronze source history, Silver typing/deduplication/MERGE and PII pseudonymization, and Gold dimensions/facts/marts.
- **Data Quality and Reconciliation:** Source-to-Silver reconciliation and post-Gold checks, with audit results in `lakehouse.system.data_quality_results`.
- **Open Lakehouse Storage:** MinIO object storage, Iceberg tables/snapshots, Polaris catalog, and scheduled maintenance.
- **Role-Based Analytics:** Seven dashboard views with API-level role and store filtering, Trino/Iceberg publications, bounded last-good OLAP cache, and API-level role/store filtering.
- **Access-Log Analytics:** Request volume, duration percentiles, error rates, product interactions, and event-based marketing funnels.
- **Synthetic Data Generation:** Version 0.8 generates deterministic SQL history with corrected payment/return/stock consistency from YAML configurations. Synthetic log generation is removed; runtime API access logging remains separate. The reference-profiling CLI records measured UCI distributions and audit counts; applying them to generation remains pending.
- **Operational Source Application:** Storefront, cart/checkout, POS, store inventory, in-house delivery, COD failure tracking, returns/refunds, and product reviews.
- **Inventory Costing:** Inbound receipts, Moving Weighted Average costs, and order-item cost snapshots for analytical COGS calculations.

## Quick Start

### Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/) with Docker Compose v2 and BuildKit support.
- [Python 3.11](https://www.python.org/downloads/) and [uv](https://docs.astral.sh/uv/) for host-side data generation and Python development.
- [Node.js 22.x](https://nodejs.org/) and npm 10+ for host-side frontend development.

### Quick Start

| Profile | Services |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris, Airflow, Trino, platform-app; associated initialization services |
| `batch` | Local Spark client (`spark-client`), used with core; idle until a manual command |
| `streaming` | Kafka, Debezium Connect, Flink; manual jobs default to zero instances |


Makefile is the entrypoint for Compose. `make up` creates `.env` from `.env.example` only when missing, selects `PROFILES` (`core` by default), and waits for service readiness. Existing `.env` files are preserved.

```bash
# Build all project images
make build

# Core services
make up

# Add local Spark client
make up PROFILES=core,batch

# Full infrastructure; data jobs are manual
make up PROFILES=core,batch,streaming

make ps
make logs
make down
```

`make up` reports Compose failures directly. `make down` stops all groups and retains data volumes. Starting fewer profiles does not stop containers from other groups; stop the stack first when switching to a smaller selection. Make overrides `COMPOSE_PROFILES` from `.env` with the selected `PROFILES` value.

Image builds and service readiness do not initialize business schemas, seed data, run DAGs or verify analytical results. Fresh MinIO/Polaris provisioning remains pending redesign. See the [setup guide](docs/runbook/SETUP.md).

#### 3. Service Access Endpoints

Ports and credentials below follow the default configuration; environment overrides may change them.

| Service | Port | URL | Credentials / Notes |
|---|---|---|---|
| **Storefront** | 3000 | `http://localhost:3000` | Public browsing |
| **Admin Console** | 3000 | `http://localhost:3000/admin` | Bootstrap admin from `API_BOOTSTRAP_ADMIN_*` |
| **BI Analytics Hub** | 3000 | `http://localhost:3000/admin/analytics` | Staff role or admin |
| **POS** | 3000 | `http://localhost:3000/store/pos` | Store manager / admin UI; store scope checked by API |
| **Backend API Docs** | 8000 | `http://localhost:8000/docs` | Swagger UI |
| **MinIO Console** | 9001 | `http://localhost:9001` | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| **Polaris Console** | 8183 | `http://localhost:8183` | `POLARIS_ROOT_CLIENT_ID` / `POLARIS_ROOT_CLIENT_SECRET`; realm `POLARIS` |
| **Airflow UI** | 8080 | `http://localhost:8080` | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| **Trino** | 8084 | `http://localhost:8084` | Query engine / Web UI |
| **Flink Dashboard** | 8085 | `http://localhost:8085` | Job status |
| **Kafka** | 9092, 9094 | `localhost:9094` | External listener; Docker clients use `kafka:9092` |
| **Debezium Connect** | 8086 | `http://127.0.0.1:8086` | Connector lifecycle/status |
| **Generator Console (optional)** | 8501 | `http://localhost:8501` | Optional `tools` profile; SQL jobs stored under `data/generator/jobs/` |

Additional staff accounts and store assignments are managed through the application.

### Service Identities

The retained policy templates describe five scoped MinIO identities and three Polaris principals: `spark_writer`, `flink_writer`, and `trino_reader`. The standalone `trino-admin` service and its seed identity/credential output are removed, as is the unused MinIO Airflow identity. Polaris uses YAML only for first-run bootstrap; later access changes are managed through Console/API and preserved when init runs again. The old MinIO Python initializer has been removed; account/policy initialization is pending redesign in `platform-app`. This configuration cleanup does not delete existing remote identities or Docker volumes. Details are in the [setup guide](docs/runbook/SETUP.md).

---

## Architecture

`customer_credentials` is excluded from both the 27-table batch configuration and CDC capture. The three ingestion paths have different processing and serving boundaries:

```text
                    Customer / Staff / Admin
                              |
                       Next.js Storefront
                              |
                           FastAPI
                              |
                       MySQL (28 tables)

OLTP batch:
  MySQL interval -> Spark -> reusable staging -> Bronze source versions -> Silver MERGE -> Gold/marts
  Validated Gold -> published Iceberg snapshots -> Trino/API

MySQL CDC:
  MySQL binlog -> Debezium -> Kafka -> Flink -> Bronze CDC events
  Bronze CDC events -> audit/replay inspection (no automatic Silver/Gold writes)

Access logs:
  FastAPI middleware -> Kafka -> Flink -> append-only Landing
  Landing -> Trino -> authorized HTTP Realtime API/dashboard (10-second UI refresh)
  Hourly Spark: Landing -> Bronze -> Silver -> Gold fact -> three marts -> publication

Storage / catalog:
  Iceberg data and metadata files on MinIO + Polaris REST catalog

Serving:
  Iceberg -> Trino -> FastAPI analytics -> Admin BI Hub / ECharts
  MySQL -----------> FastAPI transactions, auth/RBAC, source reconciliation
```

See the [system architecture](docs/architecture/SYSTEM_ARCHITECTURE_VISUAL.md) and [editable draw.io diagram](docs/diagram/08-system-architecture.drawio) for component boundaries.

### Technology Stack

| Layer | Component | Version / Image Tag | Responsibility |
|---|---|---|---|
| **Presentation** | Next.js / React | 15.5.21 / 19.1.8 | Storefront, POS, administration, BI Hub |
| **Visualization** | Apache ECharts | 6.1.0 | Dashboard charts |
| **Application** | FastAPI / SQLAlchemy | 0.116.1 / 2.0.41 | Transactions, auth/RBAC, logging, analytics API |
| **Operational DB** | MySQL | 8.4.5 | 28 OLTP tables; 27 analytical source tables |
| **Change Capture** | Debezium | 3.0.8 | Initial snapshot and row-binlog capture |
| **Event Streaming** | Apache Kafka | 3.8.0 | CDC and access-log topics |
| **Stream Processing** | Apache Flink | 1.19 | Raw access-log Landing and Bronze CDC ingestion |
| **Object Storage** | MinIO | `latest-dev` | Landing, Iceberg files, and processing state |
| **Table Format** | Apache Iceberg | 1.10.1 | Tables, snapshots, and commits |
| **Catalog** | Apache Polaris | 1.8.0 | REST catalog and access control |
| **Batch Processing** | Apache Spark | 3.5.9 | Local execution inside job containers; extraction, transforms, gates and maintenance |
| **Orchestration** | Apache Airflow | 3.3.2 | API server/UI, scheduler, standalone DAG processor; LocalExecutor |
| **Metadata DB** | PostgreSQL | 16.8 | Polaris and Airflow metadata |
| **Query Engine** | Trino | 483 | SQL over Iceberg |

The OLTP Gold model defines **6 dimensions, 6 facts (including current inventory), and 15 marts**. The log model defines **1 fact and 3 marts**. Row counts depend on the loaded dataset and completed pipeline runs.

The OLTP batch DAG is scheduled daily at 02:00 Asia/Ho_Chi_Minh; OLTP maintenance runs at 03:00 and streaming maintenance every two hours. The independent `lakehouse_logs_marts` DAG processes Landing → Bronze → Silver → Gold fact, then validates and publishes all three log marts hourly; maintenance failures do not block that DAG. Raw payloads, including malformed events, stay in Landing/Bronze. Missing-key inserts make each layer retryable without rewriting existing Silver/fact history. DAGs are paused at creation and must be enabled in Airflow. Flink runs continuously with a configured 10-second checkpoint interval. That interval is not a measured end-to-end latency guarantee.

Source–Silver reconciliation runs before Gold processing; a second gate checks Gold counts, monetary parity, keys, references, lineage, and PII tokens. Failures mark the task/DAG unsuccessful and results are recorded in `lakehouse.system.data_quality_results`. These runtime gates remain part of data processing.

OLTP batch is the only Silver/Gold writer. Airflow supplies a fixed interval, Spark overwrites staging, Bronze retains each run, and Silver MERGEs only that delta. Gold is rebuilt and validated before publication. CDC remains Bronze audit only. Maintenance targets an explicit subset of tables. Current limits and recovery commands are in [scope](docs/project/SCOPE.md), [pipelines](pipelines/README.md) and [setup](docs/runbook/SETUP.md).

---

## Data Generation and Simulation

Data generation is configurable through `generator/configs/`. For example, `small.yml` specifies 500 customers, 60 products, 240 variants, 3,000 orders, and 12 months of history. Dates are anchored by `anchor_time` in the configuration.

### Streamlit Generator Console

The optional console drives the same generator CLI in a detached worker, keeps one job at a time, and persists config/state/artifacts under `data/generator/jobs/<job_id>/`:

```bash
# Local process
uv run --locked --package platform-app platform-app
# Or with the tools profile
make build
make up PROFILES=core
```

Open `http://localhost:8501`, configure a SQL generation job, then monitor the per-stage results and download outputs. The console does not import SQL, run traffic, upload logs or trigger Airflow. See [services/platform-app/README.md](services/platform-app/README.md) for job layout and recovery behavior.

### Real-Time Traffic Simulation

Import the matching SQL dataset first, then run virtual users against the API to produce live access-log events:

```bash
uv run --locked --package data-generator -- python scripts/simulate_web_traffic.py \
  --config generator/configs/small.yml \
  --duration 60 \
  --concurrency 5 \
  --error-rate 0.05
```

### Historical Data Backfill

Export OLTP history with the generator CLI:

```bash
uv run --locked --package data-generator -- generator export-sql \
  --config generator/configs/small.yml \
  --output data/generator/small.sql
```

Synthetic access-log export is currently removed. Runtime API middleware remains the source for the Kafka access-log stream. Generated SQL is synthetic and does not establish real customer behavior or production performance.

### Run the OLTP Batch Pipeline

DAGs start paused by default. After importing source data:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_pipeline
```

Importing the same SQL dataset twice is rejected by unique keys. See the [generator guide](generator/README.md) for configuration and account mapping.

---

### Operational Signals and Analytical Metrics

The BI Hub reads a pinned Iceberg snapshot set through Trino. Responses identify the publication ID/time and whether the bounded last-good cache is serving stale data. System reconciliation compares published order-mart counts/placed value with a single current MySQL observation.

| Role | Implemented Indicators | Primary Source | Interpretation |
|---|---|---|---|
| **Executive** | Order totals, revenue, COGS, profit, AOV, COD in transit | Published order mart | Financial recognition follows the documented Gold formula |
| **Sales** | Store/channel contributions, top products, category shares | Published order, product-sales and category-sales marts | Order totals use the order mart; category order counts are not additive |
| **Store** | Today's revenue/orders, low stock, stockout SKUs | Published store/order/current-inventory marts, restricted by store | Today's boundary uses Asia/Ho_Chi_Minh |
| **Inventory** | On-hand units/value, inbound batches, stock alerts and daily history | Published current/summary/history inventory marts | Current balances are separate from daily observations |
| **Operations** | Pending orders at observation time, shipment performance, COD regions, return requests and successful refunds | Published operations/COD/logistics/return marts | Requests, lines, units and paid refunds use separate definitions |
| **Marketing** | 30-day event funnel, product interest, coupon usage, recent approved negative reviews | Two published log marts and three published OLTP marketing marts | Independent source timestamps/cache; request events are not unique people/orders |
| **System** | Batch publication age, order-count/value variance; HTTP request/errors/latency | Published order mart compared with current MySQL; hourly HTTP mart and separate realtime Landing snapshot | Source may lead batch; published and realtime HTTP windows/availability are independent |

Dashboard refresh reads the latest complete publication; new OLTP data appears after a successful batch extraction, Silver MERGE, Gold refresh and publication. Definitions, windows, denominators, and current limitations are detailed in the [Metrics Catalog](docs/architecture/METRICS_CATALOG.md).

Executive, Sales and sales trends read only published Data Marts. `mart_order_daily` groups by Vietnam order date, channel and store so order counts are additive; `mart_product_sales_daily` supplies product rankings, while `mart_sales_daily` retains category detail. Missing Executive data and undefined ratios return `null`; a verified zero amount remains zero. Store, Inventory, store overview and store selection also use published marts. `mart_inventory_current` stores one current SKU/location balance; `mart_inventory_summary` holds the overall counts/amounts and inbound count; `mart_store_overview` holds store totals/staff and stock coverage. The existing `mart_inventory_health` supplies daily history. Undefined stock coverage is null, and SKU stockouts are counted distinctly across locations. Depletion/runway estimates have been removed. Operations uses `mart_operations_summary`, `mart_cod_region_daily`, `mart_logistics_performance` and `mart_product_returns`. Waiting time is frozen at the declared observation timestamp. Missing shipment deadlines produce unknown on-time rates. Return counts distinguish unique requests and lines; requested refund amounts are separate from successful refund payments. Marketing uses `mart_marketing_funnel_daily` and `mart_daily_product_demand` through a separate log publication, and `mart_marketing_summary`, `mart_coupon_usage` and `mart_product_review_summary` through the OLTP publication. Its 15/45-minute commerce windows freeze at the observation time; the log window covers 30 UTC dates ending at the log observation date. Each source has independent availability/cache metadata. Admin overview reads `mart_admin_overview` for independent domain/status counters and `mart_order_daily` for finance. Its counts freeze at `admin_observed_at`; missing domains remain null. System reads only the order mart on the Lakehouse side, observing current MySQL count/value together for reconciliation. It reports matched/mismatch/unavailable/cached with source observation time and publication age, rather than inferring pipeline health or an SLA. A zero source denominator leaves percentage null while absolute differences still identify mismatches.

System adds **HTTP Realtime** from `landing.access_logs` through Trino, with a rolling 5–60-minute event-time window (UI: 5/15/60), request/error trends by minute, approximate p95 and a Top 20 route table. Admin/System Admin only. Each read pins one Landing snapshot, deduplicates replayed event IDs, excludes conflicting payloads and reports invalid/late/future observations separately. It returns aggregates without raw payloads and does not require an hourly publication. The view refreshes every 10 seconds while visible; failures return 503 and the UI labels the retained previous observation. Landing figures are provisional; refresh/checkpoint intervals are not latency guarantees. Business-event realtime remains pending.

System also has an independent HTTP Metrics report backed by `mart_hourly_route_metrics` and the hourly log publication. Admin and System Admin can select 1/7/30 UTC calendar dates, inspect request/error trends and route breakdowns, and view approximate percentiles only at their original hour/route/method grain. HTTP availability and cache metadata are independent of order reconciliation.

Dashboard presentation prioritizes KPI summaries and charts, with collapsed detail tables and separate views for independent sources/windows. All 18 built marts are mapped to visible measures or drill-downs in the [dashboard design](docs/design-analysis/DASHBOARD_DESIGN.md), based on official Power BI, Tableau and W3C guidance.

System reconciliation renders one chart per metric so order counts and VND use independent scales. Cached OLAP responses are marked degraded and are not presented as a new source comparison.

---

## Development Commands

```bash
# Validate all active Compose profiles with the example environment
docker compose --env-file .env.example --profile "*" config --quiet

# Python static checks
uv lock --check
python3 -m compileall -q airflow/dags/
uvx ruff check .

# Generator regression checks (temporary data, no running services required)
uv run --locked --package data-generator -- python -m unittest discover -s generator/tests -v

# Generator Console checks (job store, worker, config builder, UI smoke)
uv run --locked --package platform-app -- python -m unittest discover -s services/platform-app/tests -v

# Frontend static checks and production build
npm --prefix services/storefront ci
npm --prefix services/storefront run lint
make typecheck
npm --prefix services/storefront run build
```

### Continuous Integration

The [GitHub Actions workflow](.github/workflows/ci.yml) runs on pushes to `dev`, `main`, or `master`, pull requests, and manual dispatch. Pushes and pull requests that only change `docs/**`, `**.md`, `.gitignore`, or `LICENSE` are skipped.

| Job | Checks |
|---|---|
| **Repository validation** | Validate each Compose service group and supported combinations with `.env.example`; compile Airflow DAG Python files; run Ruff |
| **Storefront** | Install dependencies with `npm ci`; run lint, TypeScript typecheck, and the Next.js production build |

CI uses `--env-file .env.example` because `.env` is not committed and Compose requires the service-account credentials. `uv lock --check` is an additional local check in the commands above.

---

## Documentation Index

| Topic | Document | Purpose |
|---|---|---|
| **Documentation Map** | [docs/README.md](docs/README.md) | Current implementation references and retained thesis documents |
| **System Scope** | [SCOPE.md](docs/project/SCOPE.md) | DE goals, data boundaries, and implementation limits |
| **Transition Plan** | [refactor_v2.md](docs/project/refactor_v2.md) | Target responsibilities, retained scenarios, mart coverage, and phased acceptance criteria |
| **API Transition Scope** | [API_TRANSITION_SCOPE.csv](docs/project/API_TRANSITION_SCOPE.csv) | Inventory of 86 registered endpoints and proposed retention decisions; no endpoints removed |
| **Local Data Baseline** | [DATA_BASELINE.md](docs/project/DATA_BASELINE.md) | Read-only source observations, published snapshot IDs, and comparison limits |
| **System Architecture** | [SYSTEM_ARCHITECTURE_VISUAL.md](docs/architecture/SYSTEM_ARCHITECTURE_VISUAL.md) | Components, batch/CDC/log flows, serving, and deployment |
| **Repository Layout** | [PROJECT_STRUCTURE.md](docs/architecture/PROJECT_STRUCTURE.md) | Source directories and pipeline entry points |
| **Lakehouse Model** | [LAKEHOUSE_DESIGN_PLAN.md](docs/project/LAKEHOUSE_DESIGN_PLAN.md) | Storage layout and dimension/fact/mart grains |
| **Metric Definitions** | [METRICS_CATALOG.md](docs/architecture/METRICS_CATALOG.md) | Formulas, sources, windows, and interpretation limits |
| **OLTP Schema** | [OLTP_SCHEMA.md](docs/architecture/OLTP_SCHEMA.md) | 28 source tables, relationships, and invariants |
| **Physical SQL Schema** | [database/schema.sql](database/schema.sql) | MySQL table definitions for empty-database initialization |
| **CDC** | [CDC_DASHBOARD_DATA_SPEC.md](docs/architecture/CDC_DASHBOARD_DATA_SPEC.md) | Capture contract, replay metadata, and recovery |
| **Access Logs** | [ACCESS_LOG_DESIGN.md](docs/architecture/ACCESS_LOG_DESIGN.md) | Event contract, privacy rules, and streaming/raw-file boundaries |
| **Data Generator** | [generator/README.md](generator/README.md) | Synthetic SQL/log generation and live traffic |
| **Generator Reference Profile** | [GENERATOR_REFERENCE_PROFILE.md](docs/project/GENERATOR_REFERENCE_PROFILE.md) | Pinned UCI source, feature definitions, cohort/quality policy, measured results and calibration limits |
| **Generator EDA** | [Notebook](generator/notebooks/reference_datasets_eda.ipynb) · [Plan](docs/project/GENERATOR_EDA_PLAN.md) | Local Polars EDA for H&M, Olist and eCommerce Behavior; describe, missing values and date coverage |
| **Generator Console** | [GENERATOR_CONSOLE.md](docs/project/GENERATOR_CONSOLE.md) | Streamlit job workflow, validation, persisted job layout, statuses and scope limits |
| **Diagrams** | [docs/diagram/README.md](docs/diagram/README.md) | Chapter 3 draw.io sources, SVG figures, and offline HTML viewer |
| **Business Analysis (B)** | [BUSINESS_DOMAIN.md](docs/architecture/BUSINESS_DOMAIN.md) | Implemented role views, business questions, sources, and KPI limits |
| **Business Requirements (C)** | [BUSINESS_REQUIREMENTS.md](docs/project/BUSINESS_REQUIREMENTS.md) | Retail, POS, inventory, delivery, and returns |
| **Object-Oriented Analysis (A)** | [OOSAD_ANALYSIS.md](docs/design-analysis/OOSAD_ANALYSIS.md) | Actors, Use Cases, Activity, and Sequence analysis |
| **Web Design (D)** | [WEB_DESIGN_PLAN.md](docs/project/WEB_DESIGN_PLAN.md) | Source application design |
| **Local Setup** | [SETUP.md](docs/runbook/SETUP.md) | Startup dependencies, identities, data operations, catalog inspection, and troubleshooting |
