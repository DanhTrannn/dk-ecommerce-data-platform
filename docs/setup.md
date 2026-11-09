# Cài đặt và khởi chạy

Lệnh bên dưới chạy từ root repo. Xem [architecture.md](architecture.md) để chọn thành phần và [operations.md](operations.md) để kiểm tra/phục hồi. Không cần Next.js để chạy nguồn dữ liệu, Streamlit hoặc Superset.

## 1. Chuẩn bị môi trường

Cần Docker Engine/Compose có BuildKit, Python 3.11 và `uv`. Node.js 22 chỉ cần nếu phát triển frontend cũ. Version image/package lấy từ [Compose](../docker-compose.yml), [workspace](../pyproject.toml) và lock files; không cài Spark/Flink/Superset vào venv API.

```bash
cp .env.example .env
uv sync --locked --all-packages
docker compose --env-file .env.example --profile '*' config --quiet
```

Chỉ copy khi chưa có `.env`; `make up` cũng chỉ tạo file nếu thiếu. `.env` thật không được commit. Khi dùng volume đã có, giữ credential đúng với tài nguyên đã khởi tạo; đổi biến env không tự đổi password role hoặc secret đã lưu.

| Nhóm | Cấu hình cần chú ý |
|---|---|
| PostgreSQL | `POSTGRES_*`, các database/user nguồn, Airflow và Polaris; password reader không để trống |
| API | `API_SECRET_KEY` ít nhất 32 ký tự; host `API_DATABASE_URL` dùng owner nguồn; bootstrap admin được seed chủ động |
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
make build
make up PROFILES=core
make up PROFILES=core,batch
# Chỉ bật khi cần streaming hoặc BI:
make up PROFILES=core,batch,streaming
make up PROFILES=core,bi
```

`make build` build các image của mọi profile. Có thể build riêng để tiết kiệm thời gian:

```bash
docker compose --profile core build platform-app
docker compose --profile core --profile bi build superset
```

Bật ít profile hơn không tự dừng các service thuộc profile đã bật trước đó. `core` vẫn gồm Airflow/Trino; muốn nhẹ hơn, chỉ định service cần thiết thay vì `up` toàn core. Ví dụ khởi động storage/catalog trước:

```bash
docker compose --profile core up -d postgres minio polaris
make provision-lakehouse
```

MinIO/Polaris bootstrap là lệnh chủ động, không phải tác dụng phụ của healthcheck. `make provision-lakehouse` chạy MinIO bằng `mc` có sẵn rồi chạy Polaris provisioner one-off trong image platform-app; không tạo thêm image init Python.

## 3. Khởi tạo schema và nạp dữ liệu

PostgreSQL init tạo database/user `ecommerce`, `airflow`, `polaris` và role CDC. Nó **không** tạo bảng nghiệp vụ hoặc seed; metadata Superset cần bước riêng. DDL sau chỉ áp dụng lên source database trống:

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_ECOMMERCE_PASSWORD" exec psql -X -v ON_ERROR_STOP=1 --host=127.0.0.1 --username="$POSTGRES_ECOMMERCE_USER" --dbname="$POSTGRES_ECOMMERCE_DB"' \
  < database/schema.sql
```

Schema được render từ ORM và bổ sung trigger UTC. Kiểm tra/ghi lại DDL từ code:

```bash
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Không dùng DDL bootstrap để nâng cấp volume đang có, và không nhập SQL MySQL cũ. Đổi schema thực cần thao tác/migration được review riêng.

### Sinh và import SQL

```bash
uv run --locked --package data-generator -- generator export-sql \
  --config generator/configs/small.yml --output data/generator/small.sql
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.import_dataset data/generator/small.sql
make seed
```

Host importer/seed đọc `API_DATABASE_URL` và `API_SECRET_KEY` trong môi trường host; URL phải tới PostgreSQL qua port host và dùng owner nguồn. Có thể dùng Streamlit để sinh/nạp job thay CLI. Seed bổ sung catalog/admin còn thiếu; địa điểm thuộc generator, không còn seed riêng `0010_multi_city_stores.py`.

Importer chỉ nhận SQL generator tin cậy: schema/checksum/count/lineage gate, khóa bảng, transaction, đối soát tiền/kho, restart sequence và chặn dataset đã tồn tại. Không tự xóa dữ liệu hoặc upsert một dataset khác; seed chạy lại giữ các record/credential đã có. Sinh file không tự nạp file hoặc chạy pipeline.

### API và Streamlit

```bash
docker compose --profile core up -d --build platform-app lakehouse-access
```

Mở Streamlit tại `http://127.0.0.1:8501`; FastAPI `/docs` tại port 8000 nếu docs được bật. Console có các phần:

- Tạo job SQL từ preset/form/YAML, theo dõi stage và tải artifact.
- Kết quả/lịch sử: chọn job hoàn tất để nạp PostgreSQL bằng thao tác riêng.
- Mô phỏng API: khách mới → catalog → giỏ → quote → checkout → hậu kiểm; chưa tự fulfillment/POS/đổi trả.
- Quản trị quyền: đăng nhập bằng admin/system_admin nguồn, xem và gán/gỡ binding đã hỗ trợ.

Nguồn cần catalog active và tồn kho đủ để runner mua hàng. Không tự replay thao tác ghi có kết quả chưa rõ. Job/scenario/import được ghi dưới `data/generator/` để tiếp tục xem sau restart.

Chạy local thay Docker:

```bash
uv run --locked --package platform-app -- \
  uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log
# Terminal khác:
uv run --locked --package platform-app platform-app
```

Thay bootstrap admin trong `.env` phục vụ lệnh seed host; Compose không forward các biến bootstrap admin vào platform-app. Seed trước khi đăng nhập phần quản trị quyền; console generator local không phải hệ thống auth public đã harden.

## 4. Cấp quyền và chạy batch

Sau schema/provisioning, áp dụng env/mount hiện tại của PostgreSQL và cấu hình reader:

```bash
make source-reader
docker compose --profile core --profile batch up -d \
  airflow-api-server airflow-scheduler airflow-dag-processor trino
```

Batch reader chỉ SELECT 27 bảng; `customer_credentials` bị loại. Spark/Airflow cần reader password, MinIO Spark key, Polaris credential file và salt PII. Chạy lần đầu bằng DAG thủ công:

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

Đây không thay thế full init/backfill và không khẳng định mọi manual trigger không có interval đều chạy được. Auth/API trigger được dùng trong [stack helper](../pipelines/tests/stack/runtime.py).

## 5. CDC và access log

### CDC PostgreSQL

```bash
make source-cdc
docker compose --profile core --profile streaming up -d \
  kafka debezium-connect flink-jobmanager flink-taskmanager
docker compose --profile core --profile streaming run --rm debezium-init
docker compose --profile core --profile streaming run --rm flink-cdc-submit
```

Source setup yêu cầu đủ 27 bảng trước khi quản lý role/publication/FULL identity; connector dùng publication có sẵn. Entrypoint là `postgres_cdc_to_bronze.py`, tên job `oltp_cdc_to_bronze`. Job submit detached trả về trong khi stream vẫn chạy. Không reset slot/topic/group khi chỉ muốn chạy lại.

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

Bật lịch hourly trong Airflow khi cần. Flink chỉ append Landing; Spark mới làm Bronze/Silver/fact/mart và publication. Health probes không sinh event. DLQ cũ chưa được replay tự động khi Kafka phục hồi.

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
| Lakehouse access admin | Nội bộ `lakehouse-access:8010`, không publish port |

Không liệt kê Storefront/Polaris Console như service đang được Compose chạy. MySQL đã được thay bởi PostgreSQL ở stack hiện tại. Một số port PostgreSQL/Kafka/Flink trong Compose không bind localhost; kiểm tra trước khi dùng trên máy có public interface. Đây là cấu hình local, không phải production recipe.

```bash
make ps
# Dừng, giữ container và volume:
docker compose --profile '*' stop
# Gỡ container/network, vẫn giữ named volumes:
make down
```

`make stop` là alias của thao tác down trong Makefile, không phải `docker compose stop`. Không dùng xóa volume như bước khắc phục mặc định.
