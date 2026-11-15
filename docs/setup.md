# Cài đặt và khởi chạy

Lệnh bên dưới chạy từ root repo. Xem [architecture.md](architecture.md) để chọn thành phần và [operations.md](operations.md) để kiểm tra/phục hồi. Không cần Next.js để chạy nguồn dữ liệu, Streamlit hoặc Superset.

## 1. Chuẩn bị môi trường

Cần Docker Engine/Compose có BuildKit, Python 3.11 và `uv`. Node.js 22 chỉ cần nếu phát triển frontend cũ. Version image/package lấy từ [Compose](../docker-compose.yml), [workspace](../pyproject.toml) và lock files; không cài Flink/Superset vào venv API.

```bash
cp .env.example .env
uv sync --locked --all-packages
docker compose --env-file .env.example --profile '*' config --quiet
```

Chỉ copy khi chưa có `.env`; `make up` cũng chỉ tạo file nếu thiếu. `.env` thật không được commit. Khi dùng volume đã có, giữ credential đúng với tài nguyên đã khởi tạo; đổi biến env không tự đổi password role hoặc secret đã lưu.

| Nhóm | Cấu hình cần chú ý |
|---|---|
| PostgreSQL | `POSTGRES_*`, các database/user nguồn, Airflow và Polaris; password reader không để trống |
| API | `API_SECRET_KEY` ít nhất 32 ký tự; host `API_DATABASE_URL` dùng owner nguồn; schema trống và admin bootstrap được khởi tạo bởi `platform-admin-init` |
| Storage/catalog | Root chỉ dùng quản trị/provisioning; đủ `MINIO_*_ACCESS_KEY/SECRET_KEY`, bucket, realm/catalog và Polaris root secret |
| Silver | `SILVER_PSEUDONYMIZE_SALT` ổn định, không trống |
| Airflow | Secret/JWT/Fernet, admin và `AIRFLOW_UID` phù hợp; Fernet phải là key hợp lệ |
| Streamlit | Bind/port/UID/GID qua `PLATFORM_APP_*`; `./data` cần ghi được với UID đó |
| Log | `KAFKA_ENABLED=false` cho core-only; bật true khi dùng streaming; topic chung `KAFKA_ACCESS_LOG_TOPIC` |
| BI tùy chọn | `SUPERSET_SECRET_KEY` ≥32 ký tự, `SUPERSET_DB_PASSWORD`, `SUPERSET_ADMIN_PASSWORD` độc lập; không có password mặc định |

Ví dụ sinh Fernet hợp lệ bằng thư viện chuẩn Python:

```bash
python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

Compose dùng hostname nội bộ `postgres`, `kafka`, `trino`; URL host trong `.env.example` không thay các địa chỉ đó. API và Streamlit chạy cùng container; `SCENARIO_API_BASE_URL` mặc định tới API trong container. Các biến host không được forward vào service chỉ vì xuất hiện trong `.env`: kiểm tra `environment` trong Compose khi cần thay cấu hình runtime.

## 2. Build và chọn profile

```bash
make build  # chỉ build image platform-app dùng chung
make up     # khởi động platform-app và các dependency quản trị
```

`platform-app` chạy FastAPI cổng 8000 và Streamlit cổng 8501. API quản trị MinIO/Polaris dùng chung FastAPI tại `/api/v1/access`; không có container quản trị riêng. Dependency gồm PostgreSQL, MinIO, Polaris/bootstrap, `platform-admin-init` và `credential-exporter`. `make up` không khởi động toàn bộ Airflow/Trino hoặc streaming/BI chỉ vì profile đã được bật.

### Tạo quyền và credential trên Streamlit

1. Mở `http://127.0.0.1:8501` → **Quản trị quyền**. Đăng nhập bằng admin cấu hình qua `API_BOOTSTRAP_ADMIN_EMAIL`/`API_BOOTSTRAP_ADMIN_PASSWORD`; admin khác và `system_admin` không được quản trị hạ tầng.
2. Trong MinIO, tạo bucket, user và policy cần cho Polaris/batch/Flink/Trino; gán policy cho user. Dùng đúng access key/secret của các biến `MINIO_*` mà consumer đang nhận từ `.env`. Thay đổi trên Streamlit không tự sửa các biến này.
3. Trong Polaris, tạo catalog với bucket tương ứng, principal, principal role và catalog role. Gán principal → principal role → catalog role và các privilege cần thiết. Tên catalog phải khớp `POLARIS_CATALOG_NAME` của consumer.
4. Mở **Cấp cho workload**, chọn principal có credential đã lưu và cấp cho consumer cần dùng:

| Workload | Volume | File consumer đọc |
|---|---|---|
| `batch-airflow` | `polaris-batch-airflow-credentials` | `/run/polaris/clients.env` trong Airflow |
| `flink` | `polaris-flink-credentials` | `/run/polaris/clients.env` trong Flink |
| `trino-reader` | `polaris-trino-reader-credentials` | `/run/polaris/clients.env` trong Trino |
| `trino-writer` | `polaris-trino-writer-credentials` | `/run/polaris-writer/clients.env` trong Trino |

API gửi yêu cầu đã ký tới `credential-exporter`; exporter ghi file với UID/GID của consumer và xác minh kết quả. Tạo principal chưa tự cấp file. Thay credential đã có cần xác nhận; gỡ file cần nhập đúng workload. Sau thay đổi, khởi động lại consumer hoặc submit lại job nếu tiến trình giữ credential trong bộ nhớ. Chi tiết CRUD và giới hạn ở [platform-app](../services/platform-app/README.md#quản-trị-quyền-trên-streamlit).

### Bật workload sau khi cấu hình quyền

```bash
make build-all  # build image cho các nhóm cần dùng
make up-all PROFILES=core
# Chọn khi cần thêm streaming hoặc BI:
make up-all PROFILES=core,streaming
make up-all PROFILES=core,bi
```

Bật ít profile hơn không tự dừng các service thuộc profile đã bật trước đó. `make build` chỉ build image platform-app; `make build-all` build image của mọi profile. Không còn luồng provisioning tài khoản/role mẫu. Polaris bootstrap chỉ khởi tạo realm/root; các quyền workload được tạo chủ động trên Streamlit.

Khi nâng cấp từ bản có service quản trị riêng, áp dụng cấu hình và dọn container cũ bằng:

```bash
docker compose --profile core up -d --build --wait --remove-orphans platform-app
```

Lệnh giữ named volumes; người dùng đang mở console cần đăng nhập lại sau khi API được thay thế.

## 3. Khởi tạo schema và nạp dữ liệu

PostgreSQL init tạo database/user `ecommerce`, `airflow`, `polaris`. Sau đó `platform-admin-init` áp dụng `database/schema.sql` nếu schema nguồn trống, tạo admin bootstrap và chuẩn bị thư mục secret/audit. Database mới chỉ có tài khoản admin, chưa có product/catalog mẫu. Khi schema đã đủ, startup giữ dữ liệu và password; schema dở dang bị từ chối thay vì tự migrate/reset. Metadata Superset cần bước riêng.

Nếu chỉ chạy PostgreSQL mà không chạy bootstrap của ứng dụng, có thể nạp DDL thủ công vào database nguồn trống theo [database guide](../database/README.md#bootstrap-database-trống). Không nạp lại DDL sau khi `make up` đã tạo schema.

Schema được render từ ORM và bổ sung trigger UTC. Kiểm tra/ghi lại DDL từ code:

```bash
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Không dùng DDL bootstrap để nâng cấp volume đang có. Đổi schema thực cần thao tác/migration được review riêng.

### Seed và dữ liệu lịch sử

```bash
make seed
```

Seed đọc `API_DATABASE_URL` và `API_SECRET_KEY` trong môi trường host; URL phải tới PostgreSQL qua port host và dùng owner nguồn. Seed bổ sung catalog/admin còn thiếu và giữ record/credential đã có; **không tạo `cities`/`stores`**.

Nạp 12 tháng lịch sử tổng hợp (khách, đơn online/POS, giao vận, trả hàng, sổ kho, kèm `cities`/`stores`) sau khi seed. Lệnh đọc `GENERATOR_DATABASE_URL` trong `.env` và chỉ cần PostgreSQL:

```bash
make generate PRESET=1m          # nạp một lần; từ chối nếu đã có dữ liệu sinh
make generate-verify PRESET=1m   # chỉ đọc; mã lỗi khác 0 nếu có kiểm tra không đạt
```

Muốn nạp lại: `uv run --locked --env-file .env --package data-generator -- datagen reset --yes` (xóa trắng 28 bảng nguồn, từ chối nếu có đơn do API tạo), rồi `make seed` và `make generate`. Preset, kiểm tra và số đo: [generator](../generator/README.md).

### API và Streamlit

```bash
docker compose --profile core up -d --build platform-app
```

Mở Streamlit tại `http://127.0.0.1:8501`; FastAPI `/docs` tại port 8000 nếu docs được bật. Console có các phần:

- Mô phỏng API: khách mới → catalog → giỏ → quote → checkout → hậu kiểm; chưa tự fulfillment/POS/đổi trả.
- Quản trị quyền: admin bootstrap tạo/xem/sửa/xóa tài nguyên MinIO/Polaris, gán/gỡ quyền và cấp credential Polaris cho workload.

Nguồn cần catalog active và tồn kho đủ để runner mua hàng. Không tự replay thao tác ghi có kết quả chưa rõ. Run mô phỏng được ghi dưới `data/scenarios/` để tiếp tục xem sau restart.

Chạy local thay Docker:

```bash
uv run --locked --package platform-app -- \
  uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log
# Terminal khác:
uv run --locked --package platform-app platform-app
```

Thay bootstrap admin trong `.env` phục vụ lệnh seed host; Compose không forward các biến bootstrap admin vào platform-app. Seed trước khi đăng nhập phần quản trị quyền; Platform Console local không phải hệ thống auth public đã harden.

## 4. Cấp quyền và chạy batch

Sau schema/provisioning, áp dụng env/mount hiện tại của PostgreSQL và cấu hình reader:

```bash
make source-reader
docker compose --profile core up -d \
  airflow-api-server airflow-scheduler airflow-dag-processor trino
```

Batch reader chỉ SELECT 27 bảng; `customer_credentials` bị loại. Airflow (môi trường batch Polars/PyIceberg/dbt) cần reader password, MinIO batch key, Polaris credential file và salt PII. Chạy lần đầu bằng DAG thủ công:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp
```

Theo dõi trong Airflow UI tới khi mọi task/gate và publication hoàn tất. Sau đó bật DAG batch:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
```

DAG định kỳ chạy 03:00 Asia/Ho_Chi_Minh, đọc interval `[start,end)`. Không chạy init và interval DAG đồng thời; giới hạn `max_active_runs=1` chỉ áp dụng trong từng DAG. Các DAG mới được tạo ở trạng thái paused theo Compose.

Nếu kích hoạt interval DAG qua Airflow API v2, phải cung cấp interval hợp lệ. Fixture đã dùng endpoint `/api/v2/dags/lakehouse_oltp_pipeline/dagRuns` với body như sau (chọn thời gian đúng với dữ liệu cần nạp):

```json
{
  "dag_run_id": "manual_interval_example",
  "logical_date": "2026-11-09T00:00:00+00:00",
  "data_interval_start": "2026-11-09T00:00:00+00:00",
  "data_interval_end": "2026-11-10T00:00:00+00:00"
}
```

Đây không thay thế full init/backfill và không khẳng định mọi manual trigger không có interval đều chạy được. Auth/API trigger được dùng trong stack helper `pipelines/tests/stack/runtime.py` (test chỉ có ở local).

## 5. Access log

### Log API thực

Đặt `KAFKA_ENABLED=true` trong `.env`, áp dụng lại cấu hình platform-app, rồi submit job log:

```bash
docker compose --profile core --profile streaming up -d \
  kafka flink-jobmanager flink-taskmanager platform-app
docker compose --profile core --profile streaming run --rm flink-submit
```

`kafka-init` tạo topic dùng chung trước submit. API trong Compose dùng `kafka:9092`, DLQ `/data/events/dlq_access_logs.jsonl` được mount ra host. API chạy host dùng các giá trị host `localhost:9094`/đường dẫn tương đối trong `.env.example`.

Gọi API/runner và đối chiếu `X-Request-ID` với Landing. Sau publication catalog OLTP đầu tiên, trigger DAG log khi idle:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_logs_marts
```

Bật lịch hourly trong Airflow khi cần. Flink chỉ append Landing; job `build_logs_gold` làm Bronze/Silver/fact, dbt dựng mart rồi publication. Health probes không sinh event. DLQ cũ chưa được replay tự động khi Kafka phục hồi.

## 6. Superset tùy chọn

PoC dành cho admin, hai mart doanh thu, bốn KPI/bốn chart; không kế thừa role/cửa hàng FastAPI. Cần Trino và publication OLTP đã hoàn tất. Điền ba secret Superset độc lập trong `.env`, rồi:

```bash
docker compose --profile core --profile bi build superset
# Nếu PostgreSQL chưa nhận env/mount mới, áp dụng khi các job rảnh:
docker compose --profile core up -d postgres
make superset-db
make superset-init
docker compose --profile core --profile bi up -d superset
make superset-sync
```

Mở `http://127.0.0.1:8088`, đăng nhập bằng `SUPERSET_ADMIN_USER`/`SUPERSET_ADMIN_PASSWORD`. Sync in đường dẫn dashboard trong `SUPERSET_RESULT`; mở URL tương ứng hoặc danh sách Dashboards.

Init tạo metadata/migration/role/admin nếu thiếu, không reset password. Script metadata DB giữ password của role đang có. Sau batch mới, sync lại và mở URL mới; URL cũ giữ snapshot cũ. Không tự chạy sync theo Airflow hoặc tự refresh sang publication mới.

Superset dùng Python/runtime và driver lock riêng trong [infrastructure/superset](../infrastructure/superset/), một web worker/bốn thread, không Redis/Celery. Chưa có RBAC viewer/store hoặc hardening public. Cookie/proxy flags không tự bật HTTPS. Lưu `SUPERSET_SECRET_KEY` ổn định cùng backup metadata; lifecycle snapshot ở [operations.md](operations.md#6-bảo-trì-và-lưu-trữ).

## 7. Endpoint và dừng dịch vụ

Port dưới đây là mặc định, cần xem override/bind address trong `.env` và Compose:

| Service | Host endpoint |
|---|---|
| Streamlit / FastAPI | `127.0.0.1:8501` / `127.0.0.1:8000` |
| Airflow | `localhost:8080` |
| Trino | `127.0.0.1:8084` |
| Superset khi bật BI | `127.0.0.1:8088` |
| MinIO API / console | `127.0.0.1:9000` / `127.0.0.1:9001` |
| Polaris catalog / health | `127.0.0.1:8181/api/catalog` / `127.0.0.1:8182/q/health` |
| Flink dashboard / Connect | `localhost:8085` / `127.0.0.1:8086` |
| Kafka host listener | `localhost:9094`; Docker clients dùng `kafka:9092` |
| PostgreSQL | `localhost:5432` |
| Lakehouse access admin | `127.0.0.1:8000/api/v1/access`, chung FastAPI; yêu cầu admin bootstrap |
| Credential exporter | Nội bộ `credential-exporter:8011`, không publish port |

Không liệt kê Storefront/Polaris Console như service đang được Compose chạy. Một số port PostgreSQL/Kafka/Flink trong Compose không bind localhost; kiểm tra trước khi dùng trên máy có public interface. Đây là cấu hình local, không phải production recipe.

```bash
make ps
# Dừng, giữ container và volume:
docker compose --profile '*' stop
# Gỡ container/network, vẫn giữ named volumes:
make down
```

`make stop` là alias của thao tác down trong Makefile, không phải `docker compose stop`. Không dùng xóa volume như bước khắc phục mặc định.

## 8. Chạy job batch bằng CLI

DAG gọi các job dưới đây bằng `PIPELINE_PYTHON` (venv Polars/PyIceberg/dbt trong image Airflow). Chạy tay trong container scheduler, vốn đã có reader, Polaris credential file, MinIO batch key và salt PII:

```bash
# Bronze/Silver cho 27 nguồn (thêm --interval-start/--interval-end cho interval [start,end); --table để chọn nhóm)
docker compose exec -T airflow-scheduler sh -c '"$PIPELINE_PYTHON" -m jobs.oltp.run_polars_batch \
  --namespace-prefix serving --run-id <run_id>'

# Gold bằng dbt/Trino, quality gate và publication (--verify-only để chỉ audit)
docker compose exec -T airflow-scheduler sh -c '"$PIPELINE_PYTHON" -m jobs.oltp.build_oltp_gold \
  --namespace-prefix serving --run-id <run_id> --as-of 2026-11-10T10:00:00Z'

# Log: Landing → Bronze/Silver/fact → ba mart → publication log
docker compose exec -T airflow-scheduler sh -c '"$PIPELINE_PYTHON" -m jobs.logs.build_logs_gold \
  --namespace-prefix serving --run-id <run_id> --as-of 2026-11-10T10:00:00Z'
```

Prefix khác `serving` phải có dạng `polars_batch_*` để chạy thử trong namespace riêng. Giữ nguyên `--as-of` khi retry. Gate thất bại làm job lỗi sau khi lưu audit; publication trước đó được giữ nguyên.
