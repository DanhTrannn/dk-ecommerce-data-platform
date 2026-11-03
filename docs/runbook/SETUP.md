# Local Lakehouse Setup

This runbook covers local startup dependencies, data operations, RBAC bootstrapping, catalog inspection, and troubleshooting. Run commands from the repository root. See the [root README](../../README.md#development-commands) for development commands and CI.

## 1. Component Boundaries

The local stack implements the decoupled architecture defined in the lakehouse plan:

- **MinIO S3:** Stores Landing zone files, Iceberg metadata, and Parquet data files under bucket `lakehouse`.
- **Apache Polaris (v1.6.0):** Acts as the Iceberg REST Catalog, backed by PostgreSQL 16.8 for realm and RBAC metadata.
- **Apache Spark (v3.5.9 + Iceberg v1.10.1):** The batch writer engine responsible for ETL and table commits; Flink independently handles streaming writes.
- **Trino (v483):** The distributed SQL query engine for read-only serving.
- **LibreDB Studio (vlatest):** Web-based SQL IDE for MySQL, PostgreSQL, and Trino (`:3001`).
- **Debezium Connect (v3.0.8):** Captures committed changes from the MySQL row binlog and publishes per-table Kafka topics (`:8086`, localhost only).
- **Polaris Web Console:** Catalog and RBAC management UI (`:8183`).

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

The seven MinIO access-key names are validated by minio-init as the managed lakehouse-* identities; secret keys must meet its length checks. Replace development secrets, but do not arbitrarily rename these accounts without updating bootstrap/policies. The LibreDB port variable is **LIBREDB_PORT**, default 3001.

Validate interpolation without displaying rendered secrets:

```bash
docker compose --env-file .env.example --profile "*" config --quiet
```

This validates the example configuration, not running services or production security. Several Compose secrets still have development defaults; secret-manager injection, TLS, enforced Secure cookies and rate limiting are not currently provisioned. Several database/compute/broker ports are host-published without a loopback-only bind; the default network is not fully isolated from the host's public interfaces.

---

## 3. Startup Sequence

The launcher defaults to `core`, regardless of an older `COMPOSE_PROFILES` value in `.env`:

```bash
# MySQL + API + storefront
make up START_FLAGS=--build

# Website with batch/query services
make up PROFILES=core,batch

# Full processing platform, without optional management interfaces
make up PROFILES=core,batch,streaming

# Include Polaris Console, LibreDB Studio and Trino Admin
./scripts/start_all.sh --all
make ps
```

`--profiles core,batch` selects an exact list. The launcher enables Kafka access logging only when `streaming` is selected. It uses Compose readiness checks for selected services and resolves published endpoint ports from the running Compose model. `--no-wait` skips readiness and Gold checks; `--allow-degraded` reports failed readiness/schema checks as warnings rather than proving platform health.

Selecting fewer profiles does not stop existing containers from other groups. To switch to a smaller running stack, use `make down` followed by the desired `make up` command. Stop/start retains named volumes; do not pass `-v` when keeping data.

Direct Compose startup is also available. Override `COMPOSE_PROFILES` explicitly because `--profile` flags otherwise add to the value in `.env`:

```bash
COMPOSE_PROFILES=core KAFKA_ENABLED=false docker compose up -d --build
COMPOSE_PROFILES=core,batch,streaming KAFKA_ENABLED=true docker compose up -d
```

Core-only startup does not launch MinIO, Polaris, Airflow, Kafka or Trino. Transactional features remain available; BI queries require the analytical platform and a completed publication. The launcher initializes missing Gold schemas only for `batch`/`streaming`; creating tables does not populate analytical results or execute a DAG.

Service profiles do not change `LAKEHOUSE_OLTP_MODE`: the existing default is still `cdc` in this deployment-only refactor. Running the OLTP batch DAG requires the controlled writer handoff described in section 7.1 (stop `spark-cdc`, then select batch mode). Starting only `core,batch` does not automatically republish existing source data.

Spark runs with `SPARK_MASTER_URL=local[2]` inside the container executing each job. Airflow's `spark_default` connection uses the same value, including an environment-backed connection that overrides legacy metadata pointing at the retired cluster. Spark SQL and the current CDC materializer also run locally. There is no Spark master/worker service or persistent Spark cluster UI. Adjust the local thread count in `.env` if required.

### Updating an existing installation

Rebuild the Spark utility/CDC image and recreate the changed execution services. Preserve all named volumes:

```bash
docker compose build spark-client
./scripts/start_all.sh --profiles core,batch,streaming
```

Old `SPARK_MASTER_UI_PORT`, `SPARK_WORKER_UI_PORT`, `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` entries are unused and can be removed from existing `.env` files. Old master/worker containers are now orphans; after stopping old jobs, remove only those two stateless containers. Their old `spark-work` volume is unused; database, object-store, broker and credential volumes must be retained. An older `.env` with `COMPOSE_PROFILES=core,batch,streaming` still starts those groups when calling Compose directly; the launcher overrides it.

### MySQL Initialization

On an empty MySQL volume, the official entrypoint creates the configured database/user and imports [`database/schema.sql`](../../database/schema.sql) from `/docker-entrypoint-initdb.d/10-ecommerce-schema.sql`. The database uses `utf8mb4_0900_ai_ci` and UTC. Existing volumes keep their schema and records; see the [database guide](../../database/README.md).

After MySQL becomes healthy, `ecommerce-api` initializes the catalog/admin if needed and starts FastAPI. The API expects the schema to exist and does not apply schema upgrades. `storefront` waits for API readiness at `/health/ready`.

### Automated Bootstrap Workflow

1. `minio-init` builds from `infrastructure/minio/Dockerfile` and converges the private `lakehouse` bucket plus **seven explicitly managed users/policies** (`lakehouse-polaris`, `lakehouse-spark`, `lakehouse-flink`, `lakehouse-airflow`, `lakehouse-trino`, `lakehouse-trino-admin`, `lakehouse-uploader`). Correct users/policies remain unchanged; missing resources are created and drift is updated in place. Unexpected direct policies and group memberships are removed only from these users. Accounts/policies outside this exact list are preserved, including unknown `lakehouse-*` names, which produce a warning. Access keys/secrets come from `.env`; no user is deleted/recreated by synchronization.
2. On a fresh volume, `postgres` provisions databases for `polaris` and `airflow`; its initialization script also retains a legacy `hue` database definition. There is no active Hue service in this Compose stack.
3. `polaris-bootstrap` initializes the JDBC metadata store and default `POLARIS` realm.
4. `polaris-init` creates the `lakehouse` catalog mapped to `s3://lakehouse/warehouse` and registers the namespaces (`landing`, `bronze`, `silver`, `gold`, `quarantine`, `system`).
5. On a new installation, `polaris-init` bootstraps **four dedicated service principals** (`spark_writer`, `flink_writer`, `trino_reader`, `trino_admin`) from `infrastructure/polaris/rbac.yml`. Each consumer receives its **own private `clients.env`** written to `polaris-spark-client-credentials`, `polaris-spark-airflow-credentials`, `polaris-flink-credentials`, `polaris-trino-reader-credentials`, or `polaris-trino-admin-credentials` (per-consumer Docker volumes, `0600` files). Principal credentials are also cached in a **root-only state file** under `polaris-credential-state` (`/run/polaris-state/`) so subsequent init verifies identities and restores missing output files without applying YAML or resetting credentials. Existing catalogs/credentials are adopted without RBAC changes. See section 6 for the JSON bootstrap marker and recovery.
6. `mysql-cdc-init` provisions the dedicated replication account; `debezium-init` idempotently registers the MySQL connector.
7. `flink-cdc-submit` starts the continuous Kafka-to-Iceberg Bronze CDC job alongside access-log streaming.

### MinIO identity synchronization

The repository remains authoritative for the seven managed MinIO users. This differs from Polaris, whose YAML is only a first-run seed. Manual changes to a managed MinIO user's policy, group membership, enabled status or configured secret are reconciled on the next init.

The one-shot init image adds Python and Boto3 (the workspace's pinned version) to the MinIO CLI runtime. Python parses CLI JSON and compares IAM documents without relying on text formatting or action/resource order. No additional long-running service or persistent synchronization database is introduced.

The controller validates all local credentials/policy files before remote changes, validates changed IAM documents with temporary policies, then creates/updates only the required resources. Preflight policies have unique names and are removed after validation. Updating a policy used by an unmanaged user or a group containing unmanaged members fails before the real update; sharing must be resolved explicitly.

S3 `GetBucketLocation` verifies each configured credential; the seven seed policies allow it. Only authentication errors trigger an in-place secret update. Permission, network and other verification errors fail instead of rotating a secret. Secrets are passed to CLI user commands through stdin and are not printed in logs. User status is converged to enabled, matching these configured workload identities.

Once MinIO is healthy, rebuild the new init image and run it independently:

```bash
docker compose build minio-init
docker compose run --rm --no-deps minio-init
```

The final log reports counts of created users/policies, changed secrets, repaired attachments/group memberships and bucket changes. An already correct installation reports zero changes. Any failed command/verification returns nonzero; a multi-user update is not atomic, so rerun after resolving the error. The controller does not delete users or their data. The configured bucket is created only when missing, and its bucket policy is removed when needed to maintain the repository's private-bucket configuration; object contents are retained.

To rotate a managed secret deliberately, coordinate a maintenance window for its consumers, update `.env`, run `minio-init`, then recreate the consumers with the new environment. `docker compose restart` does not reload environment values. Remember that Spark in Airflow uses the Spark identity, while orchestration also receives the Airflow identity. Updating a MinIO secret does not rotate Polaris OAuth credentials.

Unknown or retired `lakehouse-*` users/policies are preserved and warned about, rather than purged by prefix. Removing a name from the managed list is not an account-deletion command. Deactivation/removal remains an explicit operator action.

### Service Dependencies and Profiles

Compose waits for health checks or successful completion of initialization jobs where configured. The main dependencies are:

| Group / profile | Startup dependencies |
|---|---|
| Shared storage/catalog (`batch`, `streaming`, `tools`, `lakehouse-tools`) | Healthy `postgres` → `polaris-bootstrap`; healthy `minio` → `minio-init`; successful initialization → `polaris` → `polaris-init` |
| Query serving (`batch`, `streaming`, `tools`) | Successful `polaris-init` → read-only `trino` |
| Airflow (`batch`) | Healthy `postgres`, initialized catalog/credentials and writer lock → `airflow-init` → webserver/scheduler with `LocalExecutor`; Spark jobs execute locally |
| Web (`core`) | Healthy `mysql` → `ecommerce-api` → `storefront`; Kafka is optional and used when `streaming` is enabled |
| CDC (`streaming`) | Healthy MySQL → `mysql-cdc-init`; healthy Kafka plus successful MySQL CDC init → `debezium-connect` → `debezium-init` |
| Flink (`streaming`) | Kafka/catalog ready → JobManager/TaskManager → separate access-log and CDC submissions |
| CDC materializer (`streaming`) | Initialized catalog/credentials, writer lock and CDC submission → local `spark-cdc`; no Spark cluster |
| Management interfaces (`tools`) | Polaris Console, LibreDB Studio and Trino Admin, with their storage/query dependencies; no Airflow or streaming jobs |
| Spark SQL utility (`lakehouse-tools`) | `spark-client` runs local Spark SQL with its own credential volume; storage/catalog dependencies are included |

`mysql` is shared by `core`, `batch`, `streaming` and `tools`. The `lakehouse-tools` profile is a one-off utility, not a normal long-running startup group. Exact membership is defined in [`docker-compose.yml`](../../docker-compose.yml).

---

## 4. Local Service Endpoints

| Service | Local URL | Port | Description |
|---|---|---|---|
| **Storefront** | `http://localhost:3000` | 3000 | Consumer shopping UI |
| **Admin Console** | `http://localhost:3000/admin` | 3000 | Store operations; bootstrap account from `API_BOOTSTRAP_ADMIN_*` |
| **Ecommerce API** | `http://localhost:8000/docs` | 8000 | Backend Swagger UI |
| **LibreDB Studio (SQL IDE)** | `http://localhost:3001` | 3001 | Web-based SQL editor for MySQL, PostgreSQL, and Trino |
| **Airflow UI** | `http://localhost:8080` | 8080 | DAG orchestration UI (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`) |
| **Trino Web UI** | `http://localhost:8084` | 8084 | Query execution status |
| **Flink Dashboard** | `http://localhost:8085` | 8085 | Streaming jobs and task status |
| **Polaris Console** | `http://localhost:8183` | 8183 | Catalog & RBAC UI (`POLARIS_ROOT_CLIENT_ID` / `POLARIS_ROOT_CLIENT_SECRET`; Realm: `POLARIS`) |
| **MinIO Console** | `http://localhost:9001` | 9001 | S3 Object browser (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| **Trino Admin (optional)** | `http://localhost:8087` | 8087 | Administrative Trino engine (profile `tools`, not started by default) |
| **Debezium Connect** | `http://127.0.0.1:8086` | 8086 | CDC connector lifecycle/status API |

### Starting the Optional Trino Admin Engine

The default startup provisions `trino` with the read-only `trino_reader` principal. An administrative Trino engine using the `trino_admin` principal (full catalog management) can be started on demand — it is **not** part of the default startup:

```bash
COMPOSE_PROFILES=tools docker compose up -d trino-admin
```

It serves on host port `8087` (`http://localhost:8087`). The read-only `trino` reader engine remains the default query engine.

---

## 5. Inspect Catalog and Data

Use the Trino CLI to browse the catalog and available Gold tables:

```bash
docker compose exec trino trino --execute "SHOW SCHEMAS FROM lakehouse;"
docker compose exec trino trino --execute "SHOW TABLES FROM lakehouse.gold;"
```

LibreDB Studio at `http://localhost:3001` also provides a SQL interface. Airflow and Flink dashboards show the state of batch and streaming jobs.

### Load Source History and Run Batch

The [root README](../../README.md#historical-data-backfill) contains automated and manual SQL/log backfill commands; the [generator guide](../../generator/README.md) describes dataset configuration and account mapping. Uploaded historical JSONL files remain raw Landing assets because the current Flink job reads Kafka.

After loading source data, unpause and trigger the OLTP DAG (DAGs start paused by default):

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_pipeline
```

Inspect the run in Airflow and query its quality audit before considering Gold ready. Batch commits cursors after Bronze; a later failure does not automatically rewind them or roll back Gold tables together. The source is live during per-table extraction/reconciliation, not frozen in one global MySQL snapshot. See [Lakehouse Design](../project/LAKEHOUSE_DESIGN_PLAN.md).

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

CDC appends Bronze events, applies them to typed Silver/affected Gold with `spark-cdc`, then publishes offsets and pinned Iceberg snapshot IDs. In CDC mode the daily OLTP DAG checks source-to-Silver parity; manual batch processing requires a controlled handoff with CDC stopped. See the [CDC specification](../architecture/CDC_DASHBOARD_DATA_SPEC.md) for transaction gates, bootstrap, lock boundaries and recovery, and [access-log design](../architecture/ACCESS_LOG_DESIGN.md) for log contracts.

---

## 6. Polaris first-run bootstrap and Console-managed RBAC

`infrastructure/polaris/rbac.yml` seeds a new installation once. After initialization, manage catalog properties, roles, grants and principal assignments through Polaris Console or the Management API. Editing YAML and rerunning `polaris-init` does not reconcile an initialized environment.

### Bootstrap template

- **`catalog` / `namespaces`**: Initial S3 catalog settings and the six namespaces.
- **`catalog_roles` / `principal_roles`**: Initial privileges and role bindings. The seed reader role has metadata/data-read privileges; the seed Spark/Flink roles manage catalog content.
- **`principals`**: Four initial identities: `spark_writer`, `flink_writer`, `trino_reader`, `trino_admin`.
- **`credential_outputs`**: Six private consumer files for Spark CDC, Spark SQL, Airflow, Flink, Trino Reader and Trino Admin. Each consumer mounts only its own `clients.env`, mode `0600`, with its configured UID/GID.

The historical entrypoint name `sync_polaris_rbac.py` remains for compatibility; it implements one-time bootstrap. It has no purge, privilege-revocation, catalog-update or credential-reset operation.

### Persistent state and restart behavior

The existing `polaris-credential-state` volume contains:

| File | Purpose |
|---|---|
| `clients.env` | Private saved client IDs/secrets; preserve this file with metadata backups |
| `ready` | Atomic JSON marker: state version, realm, catalog name/creation timestamp, original template/output mapping and principal client IDs |
| `bootstrap.lock` | Serializes initialization processes on this local volume |

A new installation writes `status=initializing` before remote changes and `status=complete` only after identities, namespaces, credentials and outputs are verified. Each issued credential is saved before proceeding to further API calls. An interrupted initialization resumes the saved plan without automatically resetting credentials.

An existing catalog with the previous empty `ready` marker or valid saved credentials is adopted without modifying remote catalog/RBAC. Missing or mismatched credentials block adoption. A completed marker uses its saved configuration; subsequent template edits, including a missing template file, are not applied. The selected realm/catalog must still match the marker.

On subsequent init, the script verifies catalog creation identity, service-principal client IDs and credential authentication. It leaves current roles, grants, namespaces and catalog properties unchanged. Missing consumer files are restored from saved credentials; existing matching files keep their content. Intentionally revoked privileges are not restored.

### Inspect or restore credential outputs

Once Polaris is healthy, rerun only its initialization service:

```bash
docker compose run --rm --no-deps polaris-init
```

`--no-deps` avoids rerunning storage initialization. This verifies the existing bootstrap and restores outputs; it does not apply RBAC edits. Use Console/Management API for access changes.

For a malformed marker, missing metadata, replaced principal or invalid secret, init returns nonzero without recreating identities or rotating credentials. Restore matching metadata/private state from backup, or explicitly update runtime credentials after an operator-managed rotation. Do not erase the marker to force YAML reconciliation. If a process dies after a server issues a credential but before it can be saved locally, recovery requires an explicit credential operation; restart does not silently reset that principal.

The ready marker detects catalog replacement by its creation timestamp and principals by client ID. This local initialization state is not a distributed coordination mechanism or a substitute for backing up PostgreSQL and credential volumes. MinIO has its separate in-place synchronization lifecycle described above; it does not use the Polaris bootstrap marker.

---

## 7. Troubleshooting

### Container Logs

```bash
# Check container status
docker compose --profile core --profile batch --profile streaming ps

# Polaris and Database logs
docker compose --profile batch logs --tail=200 postgres polaris-bootstrap polaris polaris-init

# Spark job logs (local execution)
docker compose --profile batch logs --tail=200 airflow-scheduler
docker compose --profile streaming logs --tail=200 spark-cdc

# Trino query engine logs
docker compose logs --tail=200 trino
```

### Common Issues

- **CORS Errors in Polaris Console:** Ensure `POLARIS_CONSOLE_ORIGIN` matches the exact browser URL origin.
- **Authentication Failures in Spark/Trino:** Check that `polaris-init` exited with code 0 and that your consumer mounts its own private credential volume (`polaris-spark-client-credentials`, `polaris-spark-airflow-credentials`, `polaris-flink-credentials`, `polaris-trino-reader-credentials`, or `polaris-trino-admin-credentials`) — there is no shared credential volume. Each `clients.env` is written `0600` and owned by the consumer UID; a service that cannot read its file **fails startup** rather than falling back, so a startup failure usually means a UID/permission mismatch, not missing data.
- **Upgrading an Existing Deployment (Polaris root secret):** The Polaris `root` client secret is baked into the Polaris metadata database at first bootstrap. On an existing `postgres-data` volume, keep your current `POLARIS_ROOT_CLIENT_SECRET` in `.env` even if `.env.example` now ships a new default — `polaris-init` does not auto-rotate the root secret, and using the new default against an existing realm fails `root` authentication and blocks startup. Only brand-new installs (no `postgres-data` volume) pick up the `.env.example` value.
- **S3 Connectivity Errors:** Ensure container network connectivity to `http://minio:9000`. Each consumer must use its own dedicated MinIO service user (`lakehouse-*`); MinIO root credentials are not mounted into any workload.
- **UI/API role mismatch:** operations_manager can open order/logistics/return UI routes but those APIs require admin. A 403 is not a Trino/Iceberg failure. Store confirm/cancel routes also lack the explicit CSRF dependency.
- **Branch stock changes return 204 but do not persist:** branch-inventory PATCH and inventory/refill POST currently only flush; get_db closes the session without committing. This is an application transaction limitation, not a CDC lag symptom. See [API limits](../../services/ecommerce-api/README.md#current-limits).
- **COD payment changes missing in Silver:** payments uses updated_at. Existing databases require the explicit timestamp upgrade and one-time Silver refresh described below. A created_at-only legacy schema cannot capture later COD updates.
- **Flink recovery/replay:** submission does not pass a checkpoint/savepoint restore path. A new access-log submission starts at earliest; CDC uses group offsets with earliest fallback. Inspect restore state and offsets explicitly. The access producer has a local DLQ without automatic replay.
- **Dashboard data is behind OLTP:** check Debezium, Flink Bronze, `spark-cdc` heartbeat/pending transaction state, and the latest `system.analytics_publications` row. Browser refresh polls every 15 seconds and never starts a pipeline.

### Rebuild a changed frontend

The production image does not mount local source. Apply UI/source or NEXT_PUBLIC_* changes without rebuilding other services:

```bash
docker compose up -d --no-deps --build storefront
curl -fsS http://localhost:3000/admin/analytics -o /dev/null
```

HTTP success only verifies that the page is served. Sign in with an appropriate role to inspect KPI requests, source/degraded labels and system reconciliation. Order counts and VND use separate metric charts; full monetary values are available in tooltips/table cells.

---

## 7.1. Upgrade payment timestamps and analytical publication schema

For an existing environment, keep MySQL/MinIO/Polaris/Spark/Flink/Trino available. Pause source writes (API, POS and import/generator tasks) and scheduled OLTP batch tasks. Let CDC drain completed transactions before stopping `spark-cdc`; check that `system.cdc_pipeline_state` reports `idle` with zero pending events. The payment refresh assumes this maintenance window; it is not a concurrent source snapshot protocol.

Build images containing the new Python modules and packaged SQL, then stop the source API and CDC writer:

```bash
docker compose build ecommerce-api spark-cdc airflow-scheduler
docker compose stop ecommerce-api
# Wait for CDC to report idle / pending_events=0, then:
docker compose stop spark-cdc
```

Upgrade MySQL in place using the application database connection:

```bash
docker compose run --rm --no-deps ecommerce-api python -m app.db.upgrade_payment_timestamp
```

This adds or normalizes `payments.updated_at` with `ON UPDATE CURRENT_TIMESTAMP(6)` and its delta index. Repeating the command preserves existing rows and timestamps. It does not recreate the database or add a migration framework.

In the default CDC mode, refresh existing Silver payments and rebuild all Gold outputs while retaining transport/source offsets:

```bash
docker compose run --rm --no-deps -e SPARK_MASTER_URL=local[2] spark-cdc /opt/spark/bin/spark-submit --master local[2] --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=0.0.0.0 /opt/project/pipelines/src/jobs/cdc/apply_cdc_to_lakehouse.py --once --refresh-payments --rebuild-gold
```

The job adds missing Iceberg columns without dropping tables, refreshes canonical facts/marts, validates financial parity, and publishes schema version 3. Do not manually delete publication rows or CDC checkpoints. If it reports incomplete transactions or a quality failure, resolve the issue before resuming writes. Missing historical item cost blocks publication; today's variant cost is not a replacement.

Restart the updated services after a successful publication:

```bash
docker compose up -d --no-deps ecommerce-api spark-cdc
```

For batch mode, keep CDC stopped, set `LAKEHOUSE_OLTP_MODE=batch` in the relevant services and recreate the Airflow services with that setting. Upgrade MySQL first, then run the OLTP DAG. The new timestamp on existing payments makes them eligible for the next incremental extraction. The post-Gold quality task creates a batch publication only after validation. Batch is rejected at DAG startup while `LAKEHOUSE_OLTP_MODE=cdc`.

Verify `system.analytics_publications.schema_version=3`, its `source_mode` and snapshot entry for `lakehouse.gold.fact_inventory_current`. API reads require schema 3; older publications produce a clear unavailable response until rebuilt. The inventory alert threshold is `on_hand_quantity <= 5`. Current inventory is replaced fully; daily inventory history is retained separately.

Executive/Sales and sales trends additionally require published snapshots for `mart_order_daily` and `mart_product_sales_daily`; category shares continue to use `mart_sales_daily`. These are additive schema-3 tables, not a reset of existing data. `ensure_oltp_gold_tables` creates them, batch mart processing fills them, and publication includes their snapshot IDs. CDC startup detects a publication missing either new mart and rebuilds Gold using the retained CDC offsets before publishing. Restart the updated consumer with `docker compose restart spark-cdc`, wait for a new complete publication containing both marts, then deploy the updated API/frontend. Monitor CDC errors; do not remove offsets or data to force this upgrade. API queries never substitute fact/Silver/MySQL for these marts. If a mart has no rows, Executive values are null and list endpoints return empty lists; valid zero activity remains zero.

Store/Inventory, store overview and store selection additionally require `mart_inventory_current`, `mart_inventory_summary` and `mart_store_overview`. The same additive schema-3 startup detection creates/builds them and publishes their snapshots without resetting offsets. Deploy the updated API/frontend after that publication is available. Inventory current/summary tables must use current balances, not a sum over historical snapshots; daily history remains in `mart_inventory_health`. Missing stock coverage returns null; warehouse alert lists distinguish zero from positive low stock (1–5 units). Store metrics enforce the assigned store at the API boundary. The `depletion_velocity` and `overselling_prevented` fields were removed; internal frontend types/rendering were updated together.

Operations additionally requires `mart_operations_summary` and `mart_cod_region_daily`, plus rebuilt logistics/return marts with their additive columns. Existing table data/snapshots are retained; DDL repair adds inspection status, eligibility/duration samples and return-line measures. Before deploying the updated API/frontend, rebuild Gold and publish the new snapshot set. An existing CDC consumer started with earlier code must be restarted; missing mart entries trigger its normal rebuild, or use the existing `--once --rebuild-gold` option in a controlled run while the continuous consumer is stopped. Retain offsets and restart the continuous consumer after success. Clock-based waiting measures freeze at operations_observed_at until a new build; do not interpret UI refresh as recomputation. API fields shipping_sla_violations_count, boom_orders_count, regional_boom_rates and fulfillment_bottleneck were replaced with explicit shipment/COD/fulfillment names in the Operations response; admin overview's separate fields are unchanged. No source shipment deadline exists, so on-time rates must remain unknown.


Marketing requires the additive OLTP marts `mart_marketing_summary`, `mart_coupon_usage` and `mart_product_review_summary`. Rebuild/publish Gold before deploying the new API/frontend, retaining CDC offsets as above. Coupon/review windows freeze at marketing_observed_at. The internal Marketing response now exposes request-event fields, coupon/review/product lists, and independent `log_status`/`commerce_status`; it no longer exposes visitor/purchase proxy fields or voucher burn-rate signals.

For logs, rerun `lakehouse-writer-init` to provision the sibling `logs.lock` in the existing writer volume. Enable `lakehouse_logs_marts` in Airflow for hourly rollups; DAGs start paused. The old maintenance DAG no longer refreshes marts. To build manually while no scheduled log run is active:

```bash
docker compose run --rm --no-deps lakehouse-writer-init
docker compose exec airflow-scheduler spark-submit --master 'local[2]' /opt/project/pipelines/src/jobs/logs/build_logs_gold.py --run-id marketing-log-initialization
```

The log job pins the fact and published catalog inputs, replaces all three marts, validates results and writes its own `system.logs_analytics_publications` entry (schema 1). It retains OLTP publication/offsets and does not mutate source log events. The previous `--ingest-date` option is removed: a complete rollup handles older dates and identity mappings consistently. Log publication records observation time, source-event time and source/output snapshot IDs. On failure, the prior complete publication remains readable; retry rebuilds the outputs. API sections use independent bounded caches, or unavailable if a source has no readable publication. A Marketing 200 response can be partial/degraded; inspect both statuses. No single run ID or timestamp represents both sources.

This phase keeps the existing Flink four-sink path. The later Landing-first log pipeline and realtime dashboards have not been implemented. Log maintenance remains on its existing two-hour schedule; manual-only maintenance is a later refactor step.

---

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
