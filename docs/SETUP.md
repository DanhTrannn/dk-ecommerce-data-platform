# Cài đặt và khởi chạy

Lệnh bên dưới chạy từ root repo. Xem [ARCHITECTURE.md](ARCHITECTURE.md) để chọn thành phần và [OPERATIONS.md](OPERATIONS.md) để kiểm tra/phục hồi.

## 1. Chuẩn bị môi trường

Cần Docker Engine/Compose có BuildKit, Python 3.11 và `uv`. Version image/package lấy từ [Compose](../docker-compose.yml), [workspace](../pyproject.toml) và lock files.

```bash
cp .env.example .env
uv sync --locked --all-packages
docker compose --env-file .env.example config --quiet
```

Chỉ copy khi chưa có `.env`; `make up` cũng chỉ tạo file nếu thiếu. `.env` thật không được commit. Khi dùng volume đã có, giữ credential đúng với tài nguyên đã khởi tạo; đổi biến env không tự đổi password role hoặc secret đã lưu.

| Nhóm | Cấu hình cần chú ý |
|---|---|
| PostgreSQL | `POSTGRES_*`, các database/user nguồn, Airflow và Polaris; password reader không để trống |
| API | `API_SECRET_KEY` ít nhất 32 ký tự; host `API_DATABASE_URL` dùng owner nguồn; schema trống và admin bootstrap được khởi tạo bởi `platform-admin-init` |
| Storage/catalog | Root chỉ dùng quản trị/provisioning; đủ `MINIO_*_ACCESS_KEY/SECRET_KEY`, bucket, realm/catalog và Polaris root secret |
| Airflow | Secret/JWT/Fernet, admin và `AIRFLOW_UID` phù hợp; Fernet phải là key hợp lệ |
| Streamlit | Bind/port/UID/GID qua `PLATFORM_APP_*`; `./data` cần ghi được với UID đó |
| Superset | Đặt riêng `SUPERSET_SECRET_KEY` (ít nhất 32 ký tự), `SUPERSET_DB_PASSWORD` và `SUPERSET_ADMIN_PASSWORD` |

Ví dụ sinh Fernet hợp lệ bằng thư viện chuẩn Python:

```bash
python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

Compose dùng hostname nội bộ `postgres` và `trino`. API và Streamlit chạy cùng container, và `SCENARIO_API_BASE_URL` mặc định trỏ tới API trong container. Khi đổi cấu hình runtime, kiểm tra những biến được truyền trong `environment` của Compose.

## 2. Khởi động

Sau khi cấp credential lần đầu, dùng `make up` để mở toàn bộ stack. Với môi trường mới, mở Console để cấp quyền trước.

```bash
make up SERVICES=platform-app
```

`platform-app` chạy FastAPI cổng 8000 và Streamlit cổng 8501. API quản trị Polaris dùng chung FastAPI tại `/api/v1/access`. Compose khởi động các dependency quản trị cùng ứng dụng.

### Tạo quyền và credential trên Streamlit

1. Mở `http://127.0.0.1:8501` → **Quản trị quyền**. Đăng nhập bằng admin cấu hình qua `API_BOOTSTRAP_ADMIN_EMAIL`/`API_BOOTSTRAP_ADMIN_PASSWORD`; admin khác và `system_admin` không được quản trị hạ tầng.
2. MinIO không cần cấu hình tay: `minio-init` tạo bucket `MINIO_LAKEHOUSE_BUCKET` và account `MINIO_POLARIS_ACCESS_KEY` mà Polaris dùng để cấp credential lưu trữ tạm. Workload không có account MinIO riêng.
3. Trong Polaris, tạo catalog với bucket tương ứng, principal, principal role và catalog role. Gán principal → principal role → catalog role và các privilege cần thiết. Tên catalog phải khớp cấu hình consumer. Hai file Trino `infrastructure/trino/catalog/lakehouse*.properties` hiện đặt warehouse là `lakehouse`; đổi `POLARIS_CATALOG_NAME` trong `.env` không tự sửa các file này.
4. Mở **Cấp cho workload**, chọn principal có credential đã lưu và cấp cho consumer cần dùng:

| Workload | Volume | File consumer đọc |
|---|---|---|
| `batch-airflow` | `polaris-batch-airflow-credentials` | `/run/polaris/clients.env` trong Airflow |
| `trino-reader` | `polaris-trino-reader-credentials` | `/run/polaris/clients.env` trong Trino |
| `trino-writer` | `polaris-trino-writer-credentials` | `/run/polaris-writer/clients.env` trong Trino |

Sau khi thay credential đang dùng, khởi động lại consumer. Luồng cấp và các ràng buộc ở [platform-app](../services/platform-app/README.md#cấp-credential-polaris-cho-workload).

### Khởi tạo metadata Superset lần đầu

Điền các biến Superset trong `.env` rồi chạy `make up`. Compose đợi PostgreSQL sẵn sàng, tạo database và role metadata, chạy migration và tạo admin nếu thiếu rồi mới mở Superset.

Với volume PostgreSQL đã có, `POSTGRES_PASSWORD` và `SUPERSET_DB_PASSWORD` phải khớp mật khẩu đang lưu. Các bước khởi tạo giữ mật khẩu của tài khoản đã tạo.

## 3. Khởi tạo schema và nạp dữ liệu

PostgreSQL init tạo database/user `ecommerce`, `airflow`, `polaris`. Sau đó `platform-admin-init` áp dụng `database/schema.sql` nếu schema nguồn trống, tạo admin bootstrap và chuẩn bị thư mục secret/audit. Database mới chỉ có tài khoản admin, chưa có product/catalog mẫu. Khi schema đã đủ, startup giữ dữ liệu và password. Schema dở dang cần migration riêng.

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

Chọn thư mục đích mới hoặc rỗng. Lệnh ghi Parquet theo ngày vào thư mục đích. Chi tiết dữ liệu và số đo lịch sử ở [generator](../generator/README.md).

Muốn nạp lại: `uv run --locked --env-file .env --package data-generator -- datagen reset --yes` (xóa trắng 28 bảng nguồn, từ chối nếu có đơn do API tạo), rồi `make seed` và `make generate`. Preset, kiểm tra và số đo: [generator](../generator/README.md).

### API và Streamlit

```bash
make up SERVICES=platform-app
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

Compose truyền `API_BOOTSTRAP_ADMIN_EMAIL` vào API để xác định người được quản trị hạ tầng. `platform-admin-init` dùng email và password để tạo tài khoản lần đầu. Chạy trên host cần schema và admin đã khởi tạo, cùng các đường dẫn credential và exporter phù hợp. `--env-file .env` đưa biến Trino và UI vào môi trường tiến trình.

## 4. Chạy pipeline OLTP

Sau khi có schema nguồn, dữ liệu và quyền workload (`batch-airflow`, `trino-reader`, `trino-writer`):

```bash
make source-reader   # role chỉ SELECT 27 bảng, cần POSTGRES_ECOMMERCE_READER_PASSWORD
make up              # bật toàn bộ stack
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
# Khi lần init xong:
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
```

`airflow-init` tạo connection `oltp_postgres` từ `POSTGRES_ECOMMERCE_DB` và `POSTGRES_ECOMMERCE_READER_*` ở lần khởi động đầu; sau đó sửa trong Airflow UI (**Admin → Connections**). DAG, tầng dữ liệu và cách chạy lại: [airflow/README.md](../airflow/README.md).

## 5. Access log

FastAPI ghi access log dạng JSON ra stdout. Xem log bằng `docker compose logs platform-app`.

`datagen logs` xuất log tổng hợp ra Parquet theo ngày và không nạp vào lakehouse:

```bash
uv run --locked --env-file .env --package data-generator -- \
  datagen logs --preset 1m --out data/generator/logs/1m
```

## 6. Dashboard Superset

Dashboard dành cho admin, đọc ba mart doanh thu và hiển thị bốn KPI, bốn biểu đồ. Sau khi `make up` báo stack sẵn sàng và `lakehouse_oltp_init` chạy xong, tạo dashboard bằng lệnh sau.

```bash
make superset-sync
```

Mở `http://127.0.0.1:8088`, đăng nhập bằng `SUPERSET_ADMIN_USER`/`SUPERSET_ADMIN_PASSWORD`. Sync in đường dẫn dashboard trong `SUPERSET_RESULT`; mở URL tương ứng hoặc danh sách Dashboards.

Dataset đọc bảng Gold hiện tại, nên batch mới hiện ngay trên dashboard. Chạy sync lại khi định nghĩa dataset hoặc biểu đồ thay đổi.

Superset dùng Python/runtime và driver lock riêng trong [infrastructure/superset](../infrastructure/superset/), một web worker/bốn thread, không Redis/Celery. Chưa có RBAC viewer/store hoặc hardening public. Cookie/proxy flags không tự bật HTTPS. Lưu `SUPERSET_SECRET_KEY` ổn định cùng backup metadata; lifecycle snapshot ở [OPERATIONS.md](OPERATIONS.md#6-bảo-trì-và-lưu-trữ).

## 7. Endpoint và dừng dịch vụ

Port dưới đây là mặc định, cần xem override/bind address trong `.env` và Compose:

| Service | Host endpoint |
|---|---|
| Streamlit / FastAPI | `127.0.0.1:8501` / `127.0.0.1:8000` |
| Airflow | `localhost:8080` |
| Trino HTTPS | `https://127.0.0.1:8084`, cần CA được tạo bởi `trino-auth-init` |
| Superset | `127.0.0.1:8088` |
| MinIO API / console | `127.0.0.1:9000` / `127.0.0.1:9001` |
| Polaris catalog / health | `127.0.0.1:8181/api/catalog` / `127.0.0.1:8182/q/health` |
| PostgreSQL | `localhost:5432` |
| Lakehouse access admin | `127.0.0.1:8000/api/v1/access`, chung FastAPI; yêu cầu admin bootstrap |
| Credential exporter | Nội bộ `credential-exporter:8011`, không publish port |

Cấu hình Compose dành cho môi trường local. Kiểm tra địa chỉ bind của PostgreSQL trước khi chạy trên máy có public interface.

```bash
make ps
# Dừng, giữ container và volume:
docker compose stop
# Gỡ container/network, vẫn giữ named volumes:
make down
```
