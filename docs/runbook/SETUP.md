# Local Lakehouse Setup

FastAPI and Streamlit now share the `platform-app` Python slim image. Start with `docker compose --profile core up -d --build platform-app`; PostgreSQL is its required dependency. Streamlit uses `PLATFORM_APP_PORT` (8501), FastAPI uses `API_PORT` (8000). Healthchecks cover both processes; business schema bootstrap remains an explicit manual step; storage provisioning remains separate.

> **Infrastructure-only transition (2026-11-06):** PostgreSQL now hosts separate empty databases `ecommerce`, `airflow` and `polaris` on first initialization. MySQL/CDC-user-init services, MySQL JDBC JARs and the MySQL connector plugin are removed from infrastructure. Debezium now targets PostgreSQL `pgoutput` with a filtered publication. Each service keeps its own Dockerfile/image.
>
> No old data migration or runtime reset is performed in this change. Build alone does not empty Docker volumes: database init scripts run only on a fresh PostgreSQL volume. At that infrastructure-only stage the application and ingestion conversions were deferred; the PostgreSQL batch update below records subsequent work; do not treat the older MySQL application/ingestion instructions below as working against the new infrastructure. Source tables and their replica identity must be prepared before enabling CDC. Airflow 3.3.2 build/migration verification is still paused.

> **PostgreSQL batch update — 07/11/2026:** schema/ORM, generator, importer and API purchase runner have already been converted. Batch extraction and manual source reconciliation now use PostgreSQL JDBC and a dedicated reader. Configure `POSTGRES_ECOMMERCE_READER_USER`/`POSTGRES_ECOMMERCE_READER_PASSWORD`, recreate PostgreSQL to receive the new script mount/environment, then run `make source-reader` after schema bootstrap. Configure a stable `SILVER_PSEUDONYMIZE_SALT` and existing MinIO/Polaris prerequisites, then rebuild/recreate Spark/Airflow. See the [pipeline guide](../../pipelines/README.md#postgresql-batch-source--07112026). Older MySQL operational commands below are historical and do not configure this reader. MinIO/Polaris provisioning and PostgreSQL CDC remain pending; service startup does not initialize them.

This runbook covers local startup dependencies, data operations, RBAC bootstrapping, catalog inspection, and troubleshooting. Run commands from the repository root. See the [root README](../../README.md#development-commands) for development commands and CI.

## 1. Component Boundaries

The local stack implements the decoupled architecture defined in the lakehouse plan:

- **MinIO S3:** Stores Landing zone files, Iceberg metadata, and Parquet data files under bucket `lakehouse`.
- **Apache Polaris (v1.8.0):** Acts as the Iceberg REST Catalog, backed by PostgreSQL 16.8 for realm and RBAC metadata.
- **Apache Spark (v3.5.9 + Iceberg v1.10.1):** The batch writer engine responsible for ETL and table commits; Flink independently handles streaming writes.
- **Trino (v483):** The distributed SQL query engine for read-only serving.
- **Debezium Connect (v3.0.8):** Captures committed changes from the MySQL row binlog and publishes per-table Kafka topics (`:8086`, localhost only).
- **Polaris Web Console:** Catalog and RBAC management UI (`:8183`).
- **Generator Console:** Optional Streamlit UI (`:8501`) that creates and monitors background SQL/access-log jobs; it does not load MySQL, upload logs, or trigger pipelines.

*Note: Polaris does not store Parquet files or execute queries. PostgreSQL holds only catalog/RBAC metadata, not actual Iceberg data.*

---

## 2. Environment Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

If exposing services beyond `localhost`, update the following variables to avoid CORS issues with the Polaris Console:
- `POLARIS_PUBLIC_API_URL=http://<host>:8181`
- `POLARIS_CONSOLE_ORIGIN=http://<host>:8183`

Review browser-facing NEXT_PUBLIC_API_BASE_URL and API_CORS_ORIGINS together. NEXT_PUBLIC_* values are frontend build arguments; rebuild the image after changing them. Compose forwards only the listed API variables; adding API_COOKIE_SECURE or API_INTERNAL_SECRET to the root .env alone does not inject them into the API container.

MinIO starts its server directly. Policy JSON files are retained for future bootstrap tools in `platform-app`; there is no separate MinIO Python init image or automatic user/policy provisioning in the current Compose configuration.

Validate interpolation without displaying rendered secrets:

```bash
docker compose --env-file .env.example --profile "*" config --quiet
```

This validates the example configuration, not running services or production security. Several Compose secrets still have development defaults; secret-manager injection, TLS, enforced Secure cookies and rate limiting are not currently provisioned. Several database/compute/broker ports are host-published without a loopback-only bind; the default network is not fully isolated from the host's public interfaces.

---

## 3. Startup Sequence

Makefile calls Compose directly. `PROFILES` defaults to `core` and overrides `COMPOSE_PROFILES` from `.env`. Build all images separately, then start the selected groups:

```bash
make build
make up                                  # Core services
make up PROFILES=core,batch               # Add local Spark client
make up PROFILES=core,streaming           # Add streaming infrastructure
make up PROFILES=core,batch,streaming
make ps
make logs
make down                                # All profiles; retain volumes
```

`make up` waits for readiness with a 180-second timeout and returns Compose errors directly. It creates `.env` from the example only when absent, preserving existing settings. There is no shell argument parser, degraded-success mode, automatic Gold initialization or dynamic endpoint listing. `services/platform-app/start.sh` remains the container entrypoint that manages its two Python processes.

To switch to fewer running groups, use `make down` before the new `make up`. Compose can also be called directly with an explicit `COMPOSE_PROFILES` selection. Source schema, MinIO/Polaris provisioning, Spark task settings and pipeline readiness remain separate from service startup.

### Airflow 3.3.2 upgrade

Airflow now runs as `airflow-api-server` (UI/API on the same port 8080), `airflow-scheduler` and `airflow-dag-processor`. The former `airflow-webserver` command/service is replaced. LocalExecutor, Spark 3.5.9 and FAB users are retained. FAB and Standard providers are installed explicitly; DAGs use the Airflow SDK imports.

The current decision is a clean installation: do not migrate or reuse old application/metadata data. `airflow-init` still uses `airflow db migrate` to create the Airflow schema in its new empty database; this is not a data migration from MySQL or an upgrade of the old metadata volume.

```bash
docker compose --profile core build airflow-init
# After a fresh PostgreSQL volume has been initialized:
docker compose --profile core run --rm airflow-init
docker compose --profile core up -d airflow-api-server airflow-scheduler airflow-dag-processor
```

Set `AIRFLOW_JWT_SECRET` in `.env` to a stable private value shared by all Airflow components; API secret signing still uses `AIRFLOW_SECRET_KEY`. Health is exposed at `/api/v2/monitor/health`; the public API is `/api/v2`, with FAB token login. `AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=true` preserves batch extraction intervals. No triggerer or separate worker service is needed by the current DAGs.

Official migration reference: [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/upgrading_to_airflow3.html).

### Updating an existing installation

Rebuild the Spark utility/CDC image and recreate the changed execution services. Preserve all named volumes:

```bash
docker compose build spark-client
make up PROFILES=core,batch,streaming
```

Old `SPARK_MASTER_UI_PORT`, `SPARK_WORKER_UI_PORT`, `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` entries are unused and can be removed from existing `.env` files. Old master/worker containers are now orphans; after stopping old jobs, remove only those two stateless containers. Their old `spark-work` volume is unused; database, object-store, broker and credential volumes must be retained. An older `.env` with `COMPOSE_PROFILES=core,batch,streaming` still starts those groups when calling Compose directly; the launcher overrides it.

### MySQL Initialization

On an empty MySQL volume, the official entrypoint creates the configured database/user and imports [`database/schema.sql`](../../database/schema.sql) from `/docker-entrypoint-initdb.d/10-ecommerce-schema.sql`. The database uses `utf8mb4_0900_ai_ci` and UTC. Existing volumes keep their schema and records; see the [database guide](../../database/README.md).

The current shared `platform-app` container starts FastAPI and Streamlit after PostgreSQL becomes healthy. It does not apply business schema or seeds. The MySQL initialization description above applies to the legacy source design, pending conversion.

### Infrastructure startup and deferred bootstrap

`core` starts PostgreSQL, MinIO, Polaris, Airflow, Trino and platform-app. The other infrastructure profiles start service runtimes without submitting data jobs. `platform-app` builds from `services/platform-app/Dockerfile`; its Python package/CLI is named `platform-app`, and its host settings use `PLATFORM_APP_*`.

1. On a fresh PostgreSQL volume, its entrypoint creates `ecommerce`, `airflow` and `polaris` databases and the Debezium replication account. Business tables remain pending.
2. `polaris-bootstrap` is the native Java tool that creates the Polaris realm; `polaris` then waits for that tool and MinIO server readiness.
3. Old MinIO/Polaris Python init services are removed. Bucket, IAM, catalog, namespaces and client credential provisioning will be redesigned in `platform-app`; no replacement bootstrap is implemented in this change.
4. Existing data and credential volumes are retained. Starting compute/query consumers on new empty volumes still requires the deferred bootstrap. Airflow startup depends only on its PostgreSQL metadata database.

There are only three profiles: core, batch and streaming. CDC registration (`debezium-init`) and Flink submissions (`flink-submit`, `flink-cdc-submit`) belong to streaming with `scale: 0`, so normal startup creates no data jobs. After source/schema/bootstrap configuration is ready and core/streaming engines are running, invoke each manually:

```bash
COMPOSE_PROFILES=core,streaming docker compose run --rm --no-deps debezium-init
COMPOSE_PROFILES=core,streaming docker compose run --rm --no-deps flink-submit
COMPOSE_PROFILES=core,streaming docker compose run --rm --no-deps flink-cdc-submit
```

Register CDC successfully before submitting the CDC job. Spark remains a local SQL client in batch, not a master/worker cluster. `make up PROFILES=core,batch` keeps the client container idle without running SQL. Run a query manually with `COMPOSE_PROFILES=core,batch docker compose run --rm spark-client /opt/spark/bin/spark-sql -e "SHOW NAMESPACES IN lakehouse"`.

### MinIO initialization

MinIO no longer builds or runs a dedicated Python init image. Keep policy templates in `infrastructure/minio/policies/`; future Python bootstrap and administration tools will run in `platform-app`. This change does not delete existing users, policies, buckets or data. Fresh account/policy provisioning and the historical upload helper that depended on `minio-init` are deferred until that tooling is redesigned.

### Service Dependencies and Profiles

Compose waits for health checks or successful completion of initialization jobs where configured. The main dependencies are:

| Group / profile | Startup dependencies |
|---|---|
| Core storage/catalog | PostgreSQL → native Polaris realm bootstrap; MinIO + bootstrap → Polaris → Trino |
| Core Airflow | PostgreSQL → airflow-init → API server and DAG processor → scheduler |
| Core application | PostgreSQL → platform-app (FastAPI and Streamlit) |
| Batch | Core Polaris/credentials → local Spark client |
| Streaming | Core PostgreSQL + Kafka → Debezium Connect; Kafka + core Polaris → Flink JobManager → TaskManager |
| Streaming manual jobs | Connector registration and job submissions use scale 0; explicit Compose run only |

Use `make up`, `make up PROFILES=core,batch`, `make up PROFILES=core,streaming` or `make up PROFILES=core,batch,streaming`. Include core when selecting batch or streaming because their dependencies belong to core. Existing credentials/metadata are retained; fresh bootstrap is still pending redesign.

### Trino query engine

`trino` is the only Trino service and uses the read-only `trino_reader` principal on host port 8084. The standalone `trino-admin` engine and its port setting have been removed. Its dedicated MinIO identity, Polaris bootstrap roles/principal and credential output are no longer configured. Unused MinIO Airflow credentials and the batch `pipeline-run-data` volume are also removed from Compose. This configuration cleanup does not delete existing remote identities, saved Polaris bootstrap state or Docker volumes; the new seed applies to a fresh installation.

### Starting the Optional Generator Console

The console runs the existing generator CLI in a detached worker and keeps job state under `data/generator/jobs/<job_id>/`. It is available in the `core` profile:

```bash
make up PROFILES=core
# or
docker compose --profile core up -d --build platform-app
```

Open `http://localhost:8501` (`PLATFORM_APP_PORT` overrides the host port). For local development without Compose:

```bash
uv run --locked --package platform-app platform-app
```

Only one job can be `queued` or `running` per console. Refresh or tab changes do not create jobs; closing the tab does not stop the worker. If a console/worker restart cannot verify the worker PID, the job is marked `interrupted` and existing artifacts are retained. The console does not import SQL into MySQL, run traffic, upload logs, or trigger Airflow. See [services/platform-app/README.md](../../services/platform-app/README.md) for the job layout and download behavior.

---

## 5. Inspect Catalog and Data

Use the Trino CLI to browse the catalog and available Gold tables:

```bash
docker compose exec trino trino --execute "SHOW SCHEMAS FROM lakehouse;"
docker compose exec trino trino --execute "SHOW TABLES FROM lakehouse.gold;"
```

Airflow and Flink dashboards show the state of batch and streaming jobs.

### Load Source History and Run Batch

The [root README](../../README.md#historical-data-backfill) contains automated and manual SQL/log backfill commands; the [generator guide](../../generator/README.md) describes dataset configuration and account mapping. Uploaded historical JSONL files remain raw Landing assets because the current Flink job reads Kafka.

After loading source data, use the separate manual init DAG. It reads the entire source without a time filter:

```bash
docker compose exec -T airflow-scheduler airflow dags pause lakehouse_oltp_pipeline
docker compose exec -T airflow-scheduler airflow dags unpause init_lakehouse_oltp
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp
```

Wait for validation/publication before starting interval runs. Init and batch share staging and analytical targets; there is no cross-DAG concurrency check. Init preserves existing history; it does not truncate Bronze or Silver. The normal DAG no longer accepts a full-extract flag.

Scheduled runs read only their Airflow interval, with no lookback. Staging is overwritten, Bronze merges source versions by primary key/change timestamp; Silver uses incremental MERGE by primary key. Extract retries refetch from the beginning; downstream retries reuse their existing input without checkpoints. Retry failed tasks in the same DAG run; downstream retries read existing input from the beginning, without re-extracting source. Gold uses the fixed DAG start time for time-based calculations. Recover missed intervals explicitly or run the init DAG. Source timestamps must change on updates; source hard deletes are not propagated automatically. See [pipeline details](../../pipelines/README.md).

Maintenance DAGs are also initially paused. Enable them explicitly when required:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_maintenance
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_streaming_maintenance
```

OLTP maintenance targets 16 Gold and 8 Silver tables; log maintenance targets four log tables. Bronze CDC and the remaining tables are not automatically covered. The streaming monitor accepts a RUNNING job, so verify both named streams separately rather than treating this task alone as proof of their health.

### Inspect Streaming and CDC

With `core` and `streaming` enabled, live API traffic feeds the access-log pipeline. Check Flink and the CDC connector:

```bash
docker compose --profile streaming logs --tail=200 flink-jobmanager
curl -fsS http://127.0.0.1:8086/connectors/mysql-ecommerce-cdc/status
curl -fsS http://localhost:8085/jobs/overview
```

CDC appends Bronze audit events only; batch owns Silver/Gold publication. No persistent Spark CDC consumer or transport offsets are required for analytics. See the [CDC specification](../architecture/CDC_DASHBOARD_DATA_SPEC.md).

---

## 6. Polaris bootstrap pending redesign

Server and native admin tool both use tag `1.8.0`. PostgreSQL's first-init script creates `polaris_schema` owned by the configured Polaris user before realm bootstrap, as required by [Polaris 1.8](https://polaris.apache.org/downloads/1.8.0/). This init change applies only to empty PostgreSQL volumes.

For an existing 1.6 installation, preserve metadata, credentials and the existing schema. The current JDBC URL has no `currentSchema` override, so it retains the default schema. Polaris does not run automatic schema migrations: schema-v6 index improvements require a separate operator-reviewed upgrade; see the release notes above. Changing the image tag does not reset volumes, migrate metadata or verify runtime compatibility.

The old Python `polaris-init` service, image and entrypoint are removed. `infrastructure/polaris/rbac.yml` remains a reference template for the three service principals and four private consumer credential outputs. Provisioning will be implemented later in `platform-app`; this refactor does not recreate credentials, alter grants or implement YAML synchronization.

Existing PostgreSQL metadata and private credential volumes remain on Docker. The four consumer credential volumes are still mounted into Spark, Airflow, Flink and Trino. Their clients require valid credential files; a fresh installation is not fully provisioned until the replacement bootstrap is implemented. The native Java `polaris-bootstrap` only initializes the realm, not the analytical catalog or IAM accounts.

---

## 7. Troubleshooting

### Container Logs

```bash
# Check container status
docker compose --profile core --profile batch --profile streaming ps

# Polaris and Database logs
docker compose --profile batch logs --tail=200 postgres polaris-bootstrap polaris

# Spark job logs (local execution)
docker compose --profile batch logs --tail=200 airflow-scheduler
docker compose --profile streaming logs --tail=200 flink-cdc-submit

# Trino query engine logs
docker compose logs --tail=200 trino
```

### Common Issues

- **CORS Errors in Polaris Console:** Ensure `POLARIS_CONSOLE_ORIGIN` matches the exact browser URL origin.
- **Authentication Failures in Spark/Trino:** Until bootstrap is redesigned, provision valid credentials and check that your consumer mounts its own private credential volume (`polaris-spark-client-credentials`, `polaris-spark-airflow-credentials`, `polaris-flink-credentials`, or `polaris-trino-reader-credentials`) — there is no shared credential volume. Each `clients.env` is written `0600` and owned by the consumer UID; a service that cannot read its file **fails startup** rather than falling back, so a startup failure usually means a UID/permission mismatch, not missing data.
- **Upgrading an Existing Deployment (Polaris root secret):** The Polaris `root` client secret is baked into the Polaris metadata database at first bootstrap. On an existing `postgres-data` volume, keep your current `POLARIS_ROOT_CLIENT_SECRET` in `.env` even if `.env.example` now ships a new default — the native realm bootstrap does not auto-rotate the root secret, and using the new default against an existing realm fails `root` authentication and blocks startup. Only brand-new installs (no `postgres-data` volume) pick up the `.env.example` value.
- **S3 Connectivity Errors:** Ensure container network connectivity to `http://minio:9000`. Each consumer must use its own dedicated MinIO service user (`lakehouse-*`); MinIO root credentials are not mounted into any workload.
- **UI/API role mismatch:** operations_manager can open order/logistics/return UI routes but those APIs require admin. A 403 is not a Trino/Iceberg failure. Store confirm/cancel routes also lack the explicit CSRF dependency.
- **Branch stock changes return 204 but do not persist:** branch-inventory PATCH and inventory/refill POST currently only flush; get_db closes the session without committing. This is an application transaction limitation, not a CDC lag symptom. See [API limits](../../services/platform-app/README.md#current-limits).
- **COD payment changes missing in Silver:** payments uses updated_at. Existing databases require the explicit timestamp upgrade and one-time Silver refresh described below. A created_at-only legacy schema cannot capture later COD updates.
- **Flink recovery/replay:** submission does not automatically pass a checkpoint/savepoint restore path. Access capture and CDC start from their committed group offsets with earliest fallback. Stop the old access graph cleanly before replacing it; keep Kafka retention, offsets and checkpoint/savepoint volumes. See the Landing-first upgrade below. The access producer has a local DLQ without automatic replay.
- **Dashboard data is behind OLTP:** check the OLTP batch DAG, its `validate_and_publish` task and the latest `system.analytics_publications` row. Browser refresh polls every 15 seconds and never starts a pipeline.

### Rebuild a changed frontend

The production image does not mount local source. Apply UI/source or NEXT_PUBLIC_* changes without rebuilding other services:

```bash
docker compose up -d --no-deps --build storefront
curl -fsS http://localhost:3000/admin/analytics -o /dev/null
```

HTTP success only verifies that the page is served. Sign in with an appropriate role to inspect KPI requests, source/degraded labels and system reconciliation. Order counts and VND use separate metric charts; full monetary values are available in tooltips/table cells.

---

## 7.1. Move an existing CDC-written installation to batch

Pause the old OLTP DAG and finish/cancel any unfinished runs before deploying the changed task graph. Stop the retired `spark-cdc` container before the first batch so it cannot write concurrently. If it is still present after updating Compose, stop the stack with `docker compose --profile '*' down --remove-orphans` (without `-v`), then start `core,batch` and optionally `streaming`. This preserves data volumes; no bootstrap state, source data, Bronze or old publications should be deleted.

Rebuild affected API/Airflow images as needed. Check that the source schema includes `payments.updated_at` (older databases need the explicit migration `python -m platform_app.api.db.upgrade_payment_timestamp` in the API container; starting the API does not run it automatically). Unpause and trigger `init_lakehouse_oltp`, wait for validation/publication, then enable the interval DAG as needed. The reader contract remains publication schema 3 and dashboard cache/RBAC are preserved. Historical source deletes cannot be inferred by the full extract.

Do not delete CDC offsets, audit tables or historical inventory snapshots to force an upgrade. Source-to-Silver reconciliation is an optional diagnostic while source writes are paused, not a required stage of every batch.

## 7.2. Kafka data directory

Kafka sets `KAFKA_LOG_DIRS=/var/lib/kafka/data`, matching the `kafka-data` volume. Existing installs that used the image default `/tmp/kafka-logs` require a one-time copy before recreating the broker: stop API/CDC/Connect and Kafka, back up the stopped broker's log directory, copy its complete contents (including `meta.properties` and cluster metadata) into the empty named volume, preserve UID/GID 1000, then recreate Kafka and resume consumers. Changing the environment setting alone does not move existing logs.

The full-stack check migrated this environment and verified that topic/partition log end offsets did not move backwards across broker recreation. The private recovery copy and test logs are stored outside the repository.

---

## 8. Teardown and Environment Reset

To stop services while keeping data intact:

```bash
docker compose --profile core --profile batch --profile streaming down
```

To perform a complete factory reset (erasing all MySQL records, MinIO objects, Polaris metadata, and Airflow state):

```bash
docker compose --profile "*" down -v --remove-orphans
```


## System/Admin mart upgrade

The OLTP Gold registry now contains 15 marts (27 Gold tables total). Both OLTP DAGs automatically include mart_admin_overview in their mart TaskGroup, bringing each DAG to 109 tasks. The new singleton is additive; no source schema migration, environment variable or dependency is required. Its observation time uses the fixed DAG --as-of. Existing DAG concurrency/staging rules still apply.

On existing volumes, run the normal batch/init pipeline to build and validate the new mart and publish its snapshot before deploying the updated API/frontend. Keep batch/init/manual-delete jobs from overlapping; there is no shared writer lock. A publication without the new admin mart makes admin overview unavailable (or bounded cache), rather than aggregating Fact/Silver as a replacement. The two overview routes continue to require admin.

Admin count/financial fields now accept null, add total_orders/pending_payment_orders and admin_observed_at, and distinguish returned orders from requests. System responses replace legacy CDC heartbeat/pending/lag, pipeline_status and SLA fields with source_mode, publication_age_minutes, source_observed_at and reconciliation_status. Variance rows preserve integer values/absolute_difference and allow null sides/percentages. A nonzero difference with zero MySQL denominator is mismatch with a null percentage. A cached response does not perform a new source comparison. Only System's source reconciliation queries MySQL; all Lakehouse business aggregates for these views read pinned marts.

### Cập nhật HTTP Metrics trong góc nhìn System

Không thêm biến môi trường, dependency hoặc thay đổi schema cho đợt HTTP Metrics. Dùng publication log có sẵn của `lakehouse_logs_marts`; nếu chưa có publication thì bật/chạy DAG này theo hướng dẫn log hiện có. Không cần chạy lại batch OLTP khi đã có publication catalog hợp lệ. Build/restart API và Storefront theo quy trình hiện có, giữ cấu hình `KAFKA_ENABLED` đang dùng khi tạo lại container API.

Đăng nhập admin hoặc system_admin, mở `/admin/analytics?role=system`, xem phần HTTP Metrics và chọn 1/7/30 ngày UTC. Kiểm tra thời điểm quan sát/công bố riêng của phần HTTP; endpoint là `/api/v1/admin/analytics/http-metrics?days=7`. Báo cáo dùng mart theo giờ; poll giao diện không chạy DAG. Không có quan sát thì để trống; lỗi publication/Trino trả bản lưu có đánh dấu hoặc 503. Phần HTTP và đối soát đơn tải độc lập.

### Landing-first access-log upgrade

The access Flink job now writes only `landing.access_logs`; the hourly `lakehouse_logs_marts` task processes Bronze/Silver/fact before validating/publishing three marts. No new environment variables, dependencies or source schema changes are needed. Code is mounted into Flink/Airflow; submitting a new job is required to change the already running Flink graph.

1. Pause `lakehouse_logs_marts` and wait for any active run to finish. Do not run standalone log Spark writers concurrently. Keep the last completed log publication available to dashboards.
2. Stop **only the access-log job** with a completed savepoint, retaining its returned path. Flink REST uses `POST /jobs/{access_job_id}/stop` with `{"drain": false, "targetDirectory": "file:///opt/flink/checkpoints/savepoints"}`; poll the returned savepoint request until complete and confirm the old job has stopped. Leave the CDC job running. Inspect committed offsets for `flink-lakehouse-pure-stream`; do not reset offsets, delete topics or remove checkpoint volumes.
3. Submit the updated access job through `flink-submit` with the normal launcher/Compose procedure. It keeps the group and job name, using committed offsets with earliest fallback when no committed position exists. The old four-sink savepoint is retained for controlled recovery; old code and the additively evolved Bronze schema must be checked for compatibility before any rollback. The changed graph is not blindly restored from that state using ignored/unmapped operators. This upgrade is broker-offset startup, not a tested cross-graph savepoint restore. Raw replay may repeat Landing envelopes; downstream hashes/IDs deduplicate them.
4. Confirm the new Flink graph has only a Landing sink and a completed checkpoint. A new GET request should appear in Landing while remaining absent from downstream layers until Spark runs. No API realtime view is introduced in this phase.
5. Unpause the log DAG and let its next run execute, or trigger it when idle. Its single-active-run boundary serializes runs. The existing task ID `rollup_logs_gold_marts` is kept; its CLI now accepts a fixed `--as-of` from the DAG start. Spark adds three nullable Bronze columns (`raw_payload`, `_raw_hash`, `_validation_error`) and initializes missing log schemas. No stored table is dropped or truncated. First migration adds canonical raw observations alongside retained legacy typed Bronze rows; Bronze total rows can increase without increasing valid event counts.
6. Verify the run succeeds, the new publication still uses schema 1, and `source_snapshots_json` includes Landing/Bronze/Silver/fact. Check the request ID through Silver/fact/marts and open Marketing/HTTP Metrics. Existing Silver/fact rows and OLTP publication/business totals must remain unchanged. Retry a failed DAG run with the same fixed observation; each layer independently catches up from its persisted predecessor.

The new path scans for missing raw hashes/IDs; it has no event-time cursor or incremental file-position checkpoint. Late events stay eligible. Invalid payloads remain raw in Landing/Bronze with a generic validation marker; conflicting valid raw envelopes sharing an event ID fail publication for investigation. Raw payloads are restricted source data, not dashboard output. Existing historical VN-local timestamps are preserved, not silently rewritten. Maintenance schedules/retention are unchanged; Landing realtime serving and 10-second UI polling remain a later phase. A successful local probe does not establish a latency SLA or end-to-end exactly-once delivery.
