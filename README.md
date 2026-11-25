<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![CI][ci-shield]][ci-url]
[![Last commit][last-commit-shield]][last-commit-url]

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">D&amp;K E-Commerce Data Platform</h3>

  <p align="center">
    A data engineering graduation project: an open data lakehouse with self-managed access control for a Vietnamese fashion retailer.
    <br />
    <a href="docs/ARCHITECTURE.md"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="#getting-started">Getting Started</a>
    &middot;
    <a href="docs/FINAL_REPORT.md">Final Report</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#project-structure">Project Structure</a></li>
    <li><a href="#documentation">Documentation</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#authors">Authors</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

[![Architecture walkthrough][architecture-gif]](assets/report/architecture-overview.svg)

**D&K E-Commerce Data Platform** is a data engineering graduation project, run locally, for a Vietnamese retailer with online and in-store sales. A FastAPI application and a synthetic data generator fill a PostgreSQL source. Airflow loads it into an Apache Iceberg lakehouse (MinIO storage, Apache Polaris catalog), and dbt on Trino builds the analytical Gold layer.

| Part | What it does |
|---|---|
| Source | FastAPI + PostgreSQL (28 tables): catalog, accounts, cart/checkout, POS, stock, delivery, returns, reviews |
| Generator | `datagen` loads 12 months of synthetic history from YAML presets (`100k`/`1m`/`10m`/`50m` online orders per year) |
| Console | Streamlit: purchase scenarios against the API, and Polaris access administration |
| OLTP pipeline | Airflow: PostgreSQL → staging → Bronze (PyIceberg) → Silver → Gold (dbt on Trino); see [airflow/README.md](airflow/README.md) |
| Access governance | Polaris principals and roles per workload, per-table storage credentials vended by Polaris (one MinIO account, Polaris's), Trino SQL access rules |
| BI | Four Superset dashboards for sales, operations, POS, and customer and marketing analysis. They read all 14 OLTP Gold marts. |

FastAPI writes structured access logs to stdout, and the generator can write synthetic access logs to Parquet. Their ingestion into the lakehouse is designed in [the access-log design](docs/ACCESS_LOG_DESIGN.md) but not built yet.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

[![Python][Python-shield]][Python-url]
[![FastAPI][FastAPI-shield]][FastAPI-url]
[![Streamlit][Streamlit-shield]][Streamlit-url]
[![PostgreSQL][PostgreSQL-shield]][PostgreSQL-url]
[![Airflow][Airflow-shield]][Airflow-url]
[![Polars][Polars-shield]][Polars-url]
[![dbt][dbt-shield]][dbt-url]
[![Iceberg][Iceberg-shield]][Iceberg-url]
[![Polaris][Polaris-shield]][Polaris-url]
[![MinIO][MinIO-shield]][MinIO-url]
[![Trino][Trino-shield]][Trino-url]
[![Superset][Superset-shield]][Superset-url]
[![Docker][Docker-shield]][Docker-url]

| Area | Technologies |
|---|---|
| Source and console | Python 3.11, FastAPI, SQLAlchemy, Streamlit, PostgreSQL 16.8 |
| Pipeline | Apache Airflow 3.3.2, PyIceberg 0.12, PyArrow, Polars 2.1, dbt 1.11 (dbt-trino) |
| Storage and catalog | MinIO, Apache Iceberg, Apache Polaris 1.8.0 |
| Query and BI | Trino 483, Apache Superset 6.1.0 |
| Tooling | Docker Compose, uv workspace, Ruff, unittest, GitHub Actions |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

The first start provisions access in Streamlit, then the full stack runs with `make up`. Environment variables and every setup step are in [docs/SETUP.md](docs/SETUP.md).

### Prerequisites

* Docker Engine with Compose v2
* Python 3.11
* [uv](https://docs.astral.sh/uv/)

### Installation

1. Clone the repository and create the environment file. Fill in the secrets before startup.

   ```bash
   git clone https://github.com/DanhTrannn/dk-ecommerce-data-platform.git
   cd dk-ecommerce-data-platform
   [ -f .env ] || cp .env.example .env
   uv sync --locked --all-packages
   ```

2. Start the platform app for first-time setup.

   ```bash
   make up SERVICES=platform-app
   ```

3. Open Streamlit at <http://127.0.0.1:8501> → **Quản trị quyền** and sign in as the bootstrap admin (`API_BOOTSTRAP_ADMIN_*`). `minio-init` already created the bucket and the storage account Polaris uses. Create the Polaris catalog, principals and roles, then assign principal credentials to workloads in **Cấp cho workload**. [Step-by-step guide](docs/LAKEHOUSE_ACCESS_SETUP.md)

4. Configure the PostgreSQL batch reader. For a fresh demo database, seed it and generate the `100k` history.

   ```bash
   make source-reader
   make seed
   make generate PRESET=100k
   ```

5. Start the full stack and run the first load. Compose initializes Superset metadata during startup.

   ```bash
   make up
   docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
   docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
   # After it succeeds:
   docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
   docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
   ```

6. After the first load succeeds, create or update the four dashboards.

   ```bash
   make superset-sync
   ```

   Open the URLs in `SUPERSET_RESULT.dashboards` or the Superset **Dashboards** list. [BI setup and date filters](docs/SETUP.md#6-dashboard-superset)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- USAGE -->
## Usage

| Interface | Address |
|---|---|
| Streamlit / FastAPI docs | <http://127.0.0.1:8501> / <http://127.0.0.1:8000/docs> |
| Airflow | <http://localhost:8080> |
| Trino (HTTPS, generated CA) | <https://127.0.0.1:8084> |
| MinIO API / console | <http://127.0.0.1:9000> / <http://127.0.0.1:9001> |
| Polaris catalog | <http://127.0.0.1:8181/api/catalog> |
| Superset | <http://127.0.0.1:8088> |

`lakehouse_oltp_incremental` runs daily at 03:00 and `lakehouse_oltp_sync` follows it; existing dashboards read the current Gold tables as the pipeline updates them. Running DAGs by hand, Trino checks and recovery are in [docs/OPERATIONS.md](docs/OPERATIONS.md).

Development commands:

```bash
uv lock --check
uvx ruff check .
make test                       # every test suite; pipeline tests run in the Airflow container
make ps
make logs
make down                       # remove containers, keep volumes
```

CI validates the Compose configuration, compiles the DAGs and runs Ruff. `make test` uses the running Airflow scheduler container for pipeline tests and the host workspace for the other suites. [CI workflow](.github/workflows/ci.yml)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- PROJECT STRUCTURE -->
## Project Structure

```text
.
├── airflow/                # OLTP pipeline: DAGs, tasks, lakehouse library, tests
├── dbt/                    # dbt project: Silver, Gold, seeds, data tests
├── services/platform-app/  # FastAPI, Streamlit, access administration, credential exporter
├── generator/              # datagen CLI, EDA notebooks, YAML presets
├── database/               # PostgreSQL schema and seeds
├── infrastructure/         # Airflow, Trino, MinIO, PostgreSQL, Superset configuration
├── assets/                # report diagrams, README animation
├── docs/                   # architecture, data, setup, operations, final report
├── docker-compose.yml
└── Makefile
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- DOCUMENTATION -->
## Documentation

| Document | Content |
|---|---|
| [airflow/README.md](airflow/README.md) | OLTP pipeline: DAGs, layers, connections, running and testing |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, access control, deployment |
| [docs/DATA.md](docs/DATA.md) | Source, lakehouse layers, Gold models and KPIs, generator assumptions |
| [docs/SETUP.md](docs/SETUP.md) | Environment, bootstrap, startup |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Monitoring, troubleshooting, recovery, backups |
| [docs/LAKEHOUSE_ACCESS_SETUP.md](docs/LAKEHOUSE_ACCESS_SETUP.md) | Step-by-step access setup in Streamlit |
| [docs/SYNC_DESIGN.md](docs/SYNC_DESIGN.md) | Source schema and deletion synchronization |
| [docs/ACCESS_LOG_DESIGN.md](docs/ACCESS_LOG_DESIGN.md) | Proposed log ingestion and session funnel |
| [CHANGELOG.md](CHANGELOG.md) | Notable changes by milestone |
| [docs/FINAL_REPORT.md](docs/FINAL_REPORT.md) | Thesis report, architecture diagrams, and experimental results |

Components: [platform-app](services/platform-app/README.md), [API](services/platform-app/src/platform_app/api/README.md), [generator](generator/README.md), [database](database/README.md).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ROADMAP -->
## Roadmap

- [x] Transactional source: FastAPI + PostgreSQL, 28 tables
- [x] Synthetic history generator with research-backed presets
- [x] OLTP pipeline: staging → Bronze → Silver → Gold, idempotent at every layer
- [x] Soft-delete sync and approved source column changes
- [x] Polaris access administration and per-workload credential vending
- [x] Four Superset dashboards over the 14 OLTP Gold marts
- [ ] Access-log ingestion and session funnel ([design](docs/ACCESS_LOG_DESIGN.md))
- [ ] Store-level permissions in Superset
- [ ] Scenario runners beyond online purchase
- [ ] HA, backup/restore and hardening for public deployment

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

This is a personal academic project and is not open for outside contributions. No license is granted; all rights are reserved by the authors.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- AUTHORS -->
## Authors

* [DanhTrannn](https://github.com/DanhTrannn)
* [leekhoa0409](https://github.com/leekhoa0409)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* The market reports and public datasets behind the generator presets, listed in [docs/DATA.md](docs/DATA.md#8-hồ-sơ-nguồn-nghiên-cứu-generator)
* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)
* [Shields.io](https://shields.io)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[ci-shield]: https://img.shields.io/github/actions/workflow/status/DanhTrannn/dk-ecommerce-data-platform/ci.yml?branch=dev&style=for-the-badge&label=CI
[ci-url]: https://github.com/DanhTrannn/dk-ecommerce-data-platform/actions/workflows/ci.yml
[last-commit-shield]: https://img.shields.io/github/last-commit/DanhTrannn/dk-ecommerce-data-platform/dev.svg?style=for-the-badge
[last-commit-url]: https://github.com/DanhTrannn/dk-ecommerce-data-platform/commits/dev
[architecture-gif]: assets/readme/architecture.gif
[Python-shield]: https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org
[FastAPI-shield]: https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white
[FastAPI-url]: https://fastapi.tiangolo.com
[Streamlit-shield]: https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white
[Streamlit-url]: https://streamlit.io
[PostgreSQL-shield]: https://img.shields.io/badge/PostgreSQL-16.8-4169E1?style=for-the-badge&logo=postgresql&logoColor=white
[PostgreSQL-url]: https://www.postgresql.org
[Airflow-shield]: https://img.shields.io/badge/Apache_Airflow-3.3.2-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white
[Airflow-url]: https://airflow.apache.org
[Polars-shield]: https://img.shields.io/badge/Polars-2.1-CD792C?style=for-the-badge&logo=polars&logoColor=white
[Polars-url]: https://pola.rs
[dbt-shield]: https://img.shields.io/badge/dbt-1.11-FF694B?style=for-the-badge&logo=dbt&logoColor=white
[dbt-url]: https://www.getdbt.com
[Iceberg-shield]: https://img.shields.io/badge/Apache_Iceberg-1F6FEB?style=for-the-badge&logo=apache&logoColor=white
[Iceberg-url]: https://iceberg.apache.org
[Polaris-shield]: https://img.shields.io/badge/Apache_Polaris-1.8.0-008080?style=for-the-badge&logo=apache&logoColor=white
[Polaris-url]: https://polaris.apache.org
[MinIO-shield]: https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white
[MinIO-url]: https://min.io
[Trino-shield]: https://img.shields.io/badge/Trino-483-DD00A1?style=for-the-badge&logo=trino&logoColor=white
[Trino-url]: https://trino.io
[Superset-shield]: https://img.shields.io/badge/Apache_Superset-6.1.0-20A6C9?style=for-the-badge&logo=apachesuperset&logoColor=white
[Superset-url]: https://superset.apache.org
[Docker-shield]: https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white
[Docker-url]: https://docs.docker.com/compose/
