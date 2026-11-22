# Cài đặt và khởi chạy

Lệnh bên dưới chạy từ root repo. Xem [ARCHITECTURE.md](ARCHITECTURE.md) để chọn thành phần và [OPERATIONS.md](OPERATIONS.md) để kiểm tra/phục hồi.

## 1. Chuẩn bị môi trường

Cần Docker Engine/Compose có BuildKit, Python 3.11 và `uv`. Version image/package lấy từ [Compose](../docker-compose.yml), [workspace](../pyproject.toml) và lock files; không cài Flink/Superset vào venv API.

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

`platform-app` chạy FastAPI cổng 8000 và Streamlit cổng 8501. API quản trị Polaris dùng chung FastAPI tại `/api/v1/access`; không có container quản trị riêng. Dependency gồm PostgreSQL, MinIO, `minio-init`, Polaris/bootstrap, `platform-admin-init` và `credential-exporter`. `make up` không khởi động toàn bộ Airflow/Trino hoặc streaming/BI chỉ vì profile đã được bật.

### Tạo quyền và credential trên Streamlit

1. Mở `http://127.0.0.1:8501` → **Quản trị quyền**. Đăng nhập bằng admin cấu hình qua `API_BOOTSTRAP_ADMIN_EMAIL`/`API_BOOTSTRAP_ADMIN_PASSWORD`; admin khác và `system_admin` không được quản trị hạ tầng.
2. MinIO không cần cấu hình tay: `minio-init` tạo bucket `MINIO_LAKEHOUSE_BUCKET` và account `MINIO_POLARIS_ACCESS_KEY` mà Polaris dùng để cấp credential lưu trữ tạm. Workload không có account MinIO riêng.
3. Trong Polaris, tạo catalog với bucket tương ứng, principal, principal role và catalog role. Gán principal → principal role → catalog role và các privilege cần thiết. Tên catalog phải khớp cấu hình consumer. Hai file Trino `infrastructure/trino/catalog/lakehouse*.properties` hiện đặt warehouse là `lakehouse`; đổi `POLARIS_CATALOG_NAME` trong `.env` không tự sửa các file này.
4. Mở **Cấp cho workload**, chọn principal có credential đã lưu và cấp cho consumer cần dùng:

| Workload | Volume | File consumer đọc |
|---|---|---|
| `batch-airflow` | `polaris-batch-airflow-credentials` | `/run/polaris/clients.env` trong Airflow |
| `flink` | `polaris-flink-credentials` | `/run/polaris/clients.env` trong Flink |
| `trino-reader` | `polaris-trino-reader-credentials` | `/run/polaris/clients.env` trong Trino |
| `trino-writer` | `polaris-trino-writer-credentials` | `/run/polaris-writer/clients.env` trong Trino |

Sau khi cấp hoặc thay, khởi động lại consumer hoặc submit lại job Flink. Luồng cấp và các ràng buộc ở [platform-app](../services/platform-app/README.md#cấp-credential-polaris-cho-workload).

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

Log lịch sử không được tạo từ seed. Sau khi có lịch sử OLTP đúng preset, có thể xuất log tổng hợp riêng:

```bash
uv run --locked --env-file .env --package data-generator -- \
  datagen logs --preset 1m --out data/generator/logs/1m
```

Thư mục đích phải mới/rỗng; lệnh ghi Parquet theo ngày, không upload MinIO/Kafka hay cập nhật mart. Chi tiết dữ liệu và số đo lịch sử ở [generator](../generator/README.md).

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
uv run --locked --env-file .env --package platform-app -- \
  uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log
# Terminal khác:
uv run --locked --env-file .env --package platform-app platform-app
```

Compose truyền `API_BOOTSTRAP_ADMIN_EMAIL` vào API để xác định người được quản trị hạ tầng; `platform-admin-init` nhận cả email/password để tạo tài khoản lần đầu. Đổi password trong `.env` không rotate password đã lưu. Chạy host cần schema/admin đã khởi tạo và các đường dẫn credential/exporter phù hợp; API/Streamlit trên host không tự mount volume Docker. `--env-file .env` đưa cả biến Kafka/Trino/UI vào môi trường tiến trình, ngoài nhóm API Settings đọc `.env`.

## 4. Chạy pipeline OLTP

Sau khi có schema nguồn, dữ liệu và quyền workload (`batch-airflow`, `trino-reader`, `trino-writer`):

```bash
make source-reader   # role chỉ SELECT 27 bảng, cần POSTGRES_ECOMMERCE_READER_PASSWORD
make core            # bật Airflow và Trino
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
# Khi lần init xong:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
```

`airflow-init` tạo connection `oltp_postgres` từ `POSTGRES_ECOMMERCE_DB` và `POSTGRES_ECOMMERCE_READER_*` ở lần khởi động đầu; sau đó sửa trong Airflow UI (**Admin → Connections**). DAG, tầng dữ liệu và cách chạy lại: [airflow/README.md](../airflow/README.md).

## 5. Access log

Luồng access log chưa được xây lại ([thiết kế](ACCESS_LOG_DESIGN.md)). Đặt `KAFKA_ENABLED=true` thì API vẫn gửi log request vào topic `KAFKA_ACCESS_LOG_TOPIC` (profile `streaming`), nhưng chưa có job Flink hay DAG nào đưa log vào lakehouse. `KAFKA_ENABLED=false` tắt gửi Kafka; API vẫn ghi structured log.

`datagen logs` xuất log tổng hợp ra Parquet theo ngày và không nạp vào lakehouse:

```bash
uv run --locked --env-file .env --package data-generator -- \
  datagen logs --preset 1m --out data/generator/logs/1m
```

## 6. Superset tùy chọn

PoC dành cho admin, ba mart doanh thu, bốn KPI/bốn chart; không kế thừa role/cửa hàng FastAPI. Cần Trino và một lần chạy `lakehouse_oltp_init` đã xong. Điền ba secret Superset độc lập trong `.env`, rồi:

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

Init tạo metadata/migration/role/admin nếu thiếu, không reset password. Script metadata DB giữ password của role đang có. Dataset đọc bảng Gold hiện tại nên batch mới hiện ngay trên dashboard; chỉ cần sync lại khi định nghĩa dataset/chart thay đổi.

Superset dùng Python/runtime và driver lock riêng trong [infrastructure/superset](../infrastructure/superset/), một web worker/bốn thread, không Redis/Celery. Chưa có RBAC viewer/store hoặc hardening public. Cookie/proxy flags không tự bật HTTPS. Lưu `SUPERSET_SECRET_KEY` ổn định cùng backup metadata; lifecycle snapshot ở [OPERATIONS.md](OPERATIONS.md#6-bảo-trì-và-lưu-trữ).

## 7. Endpoint và dừng dịch vụ

Port dưới đây là mặc định, cần xem override/bind address trong `.env` và Compose:

| Service | Host endpoint |
|---|---|
| Streamlit / FastAPI | `127.0.0.1:8501` / `127.0.0.1:8000` |
| Airflow | `localhost:8080` |
| Trino HTTPS | `https://127.0.0.1:8084`, cần CA được tạo bởi `trino-auth-init` |
| Superset khi bật BI | `127.0.0.1:8088` |
| MinIO API / console | `127.0.0.1:9000` / `127.0.0.1:9001` |
| Polaris catalog / health | `127.0.0.1:8181/api/catalog` / `127.0.0.1:8182/q/health` |
| Flink dashboard | `localhost:8085`; không có Kafka Connect service |
| Kafka host listener | `localhost:9094`; Docker clients dùng `kafka:9092` |
| PostgreSQL | `localhost:5432` |
| Lakehouse access admin | `127.0.0.1:8000/api/v1/access`, chung FastAPI; yêu cầu admin bootstrap |
| Credential exporter | Nội bộ `credential-exporter:8011`, không publish port |

Polaris Console không được Compose chạy. Một số port PostgreSQL/Kafka/Flink trong Compose không bind localhost; kiểm tra trước khi dùng trên máy có public interface. Đây là cấu hình local, không phải production recipe.

```bash
make ps
# Dừng, giữ container và volume:
docker compose --profile '*' stop
# Gỡ container/network, vẫn giữ named volumes:
make down
```

`make stop` là alias của thao tác down trong Makefile, không phải `docker compose stop`. Không dùng xóa volume như bước khắc phục mặc định.
