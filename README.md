<!-- prettier-ignore -->
<div align="center">

# D&K E-Commerce Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.8-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.8-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-1.19-E6526F?style=flat-square&logo=apacheflink&logoColor=white)](https://flink.apache.org)
[![Polars](https://img.shields.io/badge/Polars-2.1.0-CD792C?style=flat-square&logo=polars&logoColor=white)](https://pola.rs)
[![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.10.1-blue?style=flat-square&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![Apache Polaris](https://img.shields.io/badge/Apache_Polaris-1.8.0-teal?style=flat-square&logo=apache&logoColor=white)](https://polaris.apache.org)
[![Trino](https://img.shields.io/badge/Trino-483-DD00A1?style=flat-square&logo=trino&logoColor=white)](https://trino.io)
[![Apache Superset](https://img.shields.io/badge/Apache_Superset-6.1.0-20A6C9?style=flat-square&logo=apachesuperset&logoColor=white)](https://superset.apache.org)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)](.github/workflows/ci.yml)

:star: If you find this project useful, consider giving it a star!

[Overview](#overview) • [Features](#features) • [Quick Start](#quick-start) • [Architecture](#architecture) • [Development](#development-commands) • [Documentation](#documentation-index)

</div>

## Overview

**D&K E-Commerce Data Platform** is a local data engineering project for a Vietnamese retailer with online and in-store sales. A FastAPI application and a synthetic data generator fill a PostgreSQL source; Airflow loads it into an Apache Iceberg lakehouse (MinIO storage, Apache Polaris catalog) and dbt on Trino builds the analytical Gold layer.

| Part | What it does |
|---|---|
| Source | FastAPI + PostgreSQL (28 tables): catalog, accounts, cart/checkout, POS, stock, delivery, returns, reviews |
| Generator | `datagen` loads 12 months of synthetic history from YAML presets (`1m`/`10m`/`50m` online orders per year) |
| Console | Streamlit: purchase scenarios against the API, and MinIO/Polaris access administration |
| OLTP pipeline | Airflow: PostgreSQL → staging → Bronze (PyIceberg) → Silver → Gold (dbt on Trino); see [airflow/README.md](airflow/README.md) |
| Access governance | Per-workload MinIO users, Polaris principals and roles, vended storage credentials, Trino SQL access rules |
| BI | Optional Superset dashboard over three Gold sales marts |

The access-log path (API → Kafka → Flink → lakehouse) is being redesigned: the API can still publish to Kafka, but the Flink job and log DAGs are removed. The table designs are kept in [`streaming/sql`](streaming/sql) and [`dbt/models/logs`](dbt/models/logs).

## Tech Stack

| Area | Technologies |
|---|---|
| Source and console | Python 3.11, FastAPI, SQLAlchemy, Streamlit, PostgreSQL 16.8 |
| Pipeline | Apache Airflow 3.3.2, PyIceberg 0.12, PyArrow, Polars 2.1, dbt 1.11 (dbt-trino) |
| Storage and catalog | MinIO, Apache Iceberg 1.10.1, Apache Polaris 1.8.0 |
| Query and BI | Trino 483, Apache Superset 6.1.0 |
| Events | Apache Kafka 3.8, Apache Flink 1.19 |
| Tooling | Docker Compose, uv workspace, Ruff, unittest, GitHub Actions |

## Quick Start

Requires Docker Engine with Compose v2, Python 3.11 and uv. Full steps and variables: [docs/SETUP.md](docs/SETUP.md).

```bash
cp .env.example .env            # only if .env does not exist; then fill in the secrets
uv sync --locked
make build && make up           # API, Streamlit, PostgreSQL, MinIO, Polaris
```

1. Open Streamlit at <http://127.0.0.1:8501> → **Quản trị quyền**, sign in as the bootstrap admin (`API_BOOTSTRAP_ADMIN_*`). Create the MinIO users, bucket and policies, the Polaris catalog, principals and roles, then assign principal credentials to workloads in **Cấp cho workload**.
2. Load source data and the batch reader:

   ```bash
   make seed
   make generate PRESET=1m
   make source-reader
   ```

3. Start Airflow and Trino, then run the first load:

   ```bash
   make core
   docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
   docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
   # After it succeeds:
   docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
   ```

| Profile | Services |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris, Airflow, Trino, platform-app, admin bootstrap, credential exporter |
| `streaming` | Kafka, Flink |
| `bi` | Superset |

| Interface | Address |
|---|---|
| Streamlit / FastAPI docs | <http://127.0.0.1:8501> / <http://127.0.0.1:8000/docs> |
| Airflow | <http://localhost:8080> |
| Trino (HTTPS, generated CA) | <https://127.0.0.1:8084> |
| MinIO API / console | <http://127.0.0.1:9000> / <http://127.0.0.1:9001> |
| Polaris catalog | <http://127.0.0.1:8181/api/catalog> |
| Superset | <http://127.0.0.1:8088> |

## Repository Structure

```text
.
├── airflow/                # OLTP pipeline: DAGs, tasks, lakehouse library, tests
├── dbt/                    # dbt project: Silver, Gold, seeds, data tests
├── streaming/sql/          # access-log and Flink SQL designs (not running)
├── services/platform-app/  # FastAPI, Streamlit, access administration, credential exporter
├── generator/              # datagen CLI, EDA notebooks, YAML presets
├── database/               # PostgreSQL schema and seeds
├── infrastructure/         # Airflow, Flink, Trino, MinIO, PostgreSQL, Superset configuration
├── docs/                   # architecture, data, setup, operations, final report
├── docker-compose.yml
└── Makefile
```

## Development

```bash
uv lock --check
uvx ruff check .
make test                       # every test suite; pipeline tests run in the Airflow container
make ps && make logs
make down                       # remove containers, keep volumes
```

CI ([workflow](.github/workflows/ci.yml)) validates the Compose profiles, compiles the DAGs and runs Ruff; it does not run tests. Every test suite is in git and runs offline, without a live stack.

## Documentation

| Document | Content |
|---|---|
| [airflow/README.md](airflow/README.md) | OLTP pipeline: DAGs, layers, connections, running and testing |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, access control, deployment |
| [docs/DATA.md](docs/DATA.md) | Source, lakehouse layers, Gold models and KPIs, generator assumptions |
| [docs/SETUP.md](docs/SETUP.md) | Environment, bootstrap, startup |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Monitoring, troubleshooting, recovery, backups |
| [docs/LAKEHOUSE_ACCESS_SETUP.md](docs/LAKEHOUSE_ACCESS_SETUP.md) | Step-by-step access setup in Streamlit |
| [CHANGELOG.md](CHANGELOG.md) | Notable changes by milestone |
| [docs/FINAL_REPORT.md](docs/FINAL_REPORT.md) | Thesis report; historical, not the current specification |

Components: [platform-app](services/platform-app/README.md), [API](services/platform-app/src/platform_app/api/README.md), [generator](generator/README.md), [database](database/README.md).
