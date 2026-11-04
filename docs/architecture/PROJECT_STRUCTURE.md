# Cấu trúc mã nguồn

Đồ án là monorepo gồm hệ thống nguồn, nền tảng xử lý dữ liệu và lớp phục vụ phân tích. [Kiến trúc hệ thống](SYSTEM_ARCHITECTURE_VISUAL.md) mô tả ba đường dữ liệu.

## 1. Các thư mục chính

| Đường dẫn | Nội dung |
|---|---|
| `apps/storefront/` | Next.js: Storefront/POS/Admin/BI Hub |
| `services/ecommerce-api/app/` | FastAPI, model OLTP, nghiệp vụ, auth, logging, analytics |
| `database/schema.sql` | DDL MySQL cuối cùng: 28 bảng, index, khóa và CHECK |
| `database/seeds/` | Catalog, tồn mở đầu và bootstrap admin |
| `generator/src/generator/` | Sinh dữ liệu SQL và access log theo cấu hình |
| `generator/configs/` | Quy mô và phân phối dataset |
| `pipelines/config/default.yml` | Prefix staging OLTP |
| `pipelines/src/lakehouse/config.py` | Danh sách 27 bảng nguồn, PK/change timestamp và mapping PII |
| `pipelines/src/jobs/oltp/` | Extract, Bronze, Silver, Gold và quality gate |
| `pipelines/src/jobs/logs/` | Flink streaming log và Spark rollup Gold |
| `pipelines/src/jobs/cdc/` | Flink lưu CDC vào Bronze |
| `pipelines/src/jobs/maintenance/` | Bảo trì Iceberg |
| `pipelines/src/lakehouse/` | Thư viện schema, transform, DQ và session; không manifest/cursor riêng |
| `airflow/dags/` | Batch/init OLTP, hard delete thủ công, log mart và hai DAG maintenance |
| `infrastructure/` | Docker images, MinIO policy, Polaris RBAC, Kafka/Flink/Trino |
| `scripts/` | Start/stop, nạp dữ liệu và tạo lưu lượng API |
| `docs/` | Kiến trúc, chỉ số, contract, vận hành và tài liệu tiểu luận |
| `docs/diagram/` | Sơ đồ Chương 3: draw.io, SVG và trang xem HTML |

## 2. Điểm vào dữ liệu

| Thành phần | File |
|---|---|
| Launcher | [start_all.sh](../../scripts/start_all.sh) |
| Batch DAG | [lakehouse_oltp_pipeline.py](../../airflow/dags/lakehouse_oltp_pipeline.py) |
| OLTP Gold | [gold.py](../../pipelines/src/lakehouse/oltp/gold/), [gold/schema.py](../../pipelines/src/lakehouse/oltp/gold/schema.py) |
| OLTP Silver | [writer.py](../../pipelines/src/lakehouse/oltp/silver/writer.py), [schema.py](../../pipelines/src/lakehouse/oltp/silver/schema.py) |
| OLTP quality | [gate.py](../../pipelines/src/lakehouse/oltp/quality/gate.py), kiểm tra theo domain và [reconcile.py](../../pipelines/src/lakehouse/oltp/quality/reconcile.py) |
| Access log stream | [kafka_to_lakehouse.py](../../pipelines/src/jobs/logs/kafka_to_lakehouse.py) |
| CDC stream | [mysql_cdc_to_bronze.py](../../pipelines/src/jobs/cdc/mysql_cdc_to_bronze.py) |
| Query serving | [analytics service](../../services/ecommerce-api/app/modules/analytics/service.py) |
| Storage authorization | [MinIO](../../infrastructure/minio), [Polaris](../../infrastructure/polaris/rbac.yml) |

## 3. Workspace và profile

Python workspace có `ecommerce-api`, `data-generator`, `batch-pipeline` dùng chung `uv.lock`. Spark/Flink runtime nằm trong các Docker image xử lý dữ liệu.

Profile `core` chỉ chứa Web/API và MySQL. `batch` chứa Airflow cùng nền tảng storage/catalog/query; `streaming` chứa Kafka/Debezium/Flink, storage/catalog/query và job CDC audit vào Bronze. `tools` chứa Polaris Console, LibreDB và Trino Admin. `lakehouse-tools` giữ Spark SQL client chạy một lần. Spark chạy `local[2]` trong container thực thi; không có master/worker. `.env.example` và launcher mặc định chọn `core`; launcher `--profiles` ghi đè danh sách cũ trong `.env`. Khi dùng Compose trực tiếp, `--profile` cộng thêm vào `COMPOSE_PROFILES`.

Các dịch vụ storage/catalog dùng chung và được kết nối qua Docker network. Xem service/port chính xác trong [Compose](../../docker-compose.yml).

## 4. Quy tắc phụ thuộc

- Frontend → FastAPI; API → MySQL/Trino.
- Spark batch đọc MySQL, ghi staging và bảng Iceberg.
- Debezium đọc binlog, Flink đọc Kafka.
- Polaris quản lý catalog và quyền; MinIO lưu file.
- Trino phục vụ SQL; không thay thế OLTP.
- Generator tạo dữ liệu nguồn, không chèn số liệu dựng sẵn trực tiếp vào Gold.

CI và các lệnh phát triển hiện tại được mô tả tại [README tổng](../../README.md#development-commands).
