# Cấu trúc mã nguồn

Đồ án là monorepo gồm hệ thống nguồn, nền tảng xử lý dữ liệu và lớp phục vụ phân tích. [Kiến trúc hệ thống](SYSTEM_ARCHITECTURE_VISUAL.md) mô tả ba đường dữ liệu.

## 1. Các thư mục chính

| Đường dẫn | Nội dung |
|---|---|
| `services/storefront/` | Next.js: Storefront/POS/Admin/BI Hub |
| `services/platform-app/` | Một package/image Python chung cho FastAPI và Streamlit; Dockerfile, start.sh, dependency |
| `services/platform-app/src/platform_app/ui/` | Giao diện Streamlit điều khiển generator |
| `services/platform-app/src/platform_app/api/` | FastAPI, model OLTP, nghiệp vụ, auth, logging, analytics |
| `database/schema.sql` | DDL MySQL cuối cùng: 28 bảng, index, khóa và CHECK |
| `database/seeds/` | Catalog, tồn mở đầu và bootstrap admin |
| `generator/src/generator/` | Sinh dữ liệu SQL và access log theo cấu hình |
| `generator/configs/` | Quy mô và phân phối dataset |
| `pipelines/config/default.yml` | Prefix staging OLTP |
| `pipelines/src/lakehouse/utils/config.py` | Danh sách 27 bảng nguồn, PK/change timestamp và mapping PII |
| `pipelines/src/jobs/oltp/` | Extract, Bronze, Silver, Gold và quality gate |
| `pipelines/src/jobs/logs/` | Flink streaming log và Spark rollup Gold |
| `pipelines/src/jobs/cdc/` | Flink lưu CDC vào Bronze |
| `pipelines/src/jobs/maintenance/` | Bảo trì Iceberg |
| `pipelines/src/lakehouse/` | Thư viện schema, transform, DQ và session; không manifest/cursor riêng |
| `airflow/dags/` | Batch/init OLTP, hard delete thủ công, log mart và hai DAG maintenance |
| `infrastructure/` | Docker images, MinIO policy, Polaris RBAC, Kafka/Flink/Trino |
| `Makefile` | Build/start/stop và kiểm tra hạ tầng qua Compose |
| `docs/` | Kiến trúc, chỉ số, contract, vận hành và tài liệu tiểu luận |
| `docs/diagram/` | Sơ đồ Chương 3: draw.io, SVG và trang xem HTML |

## 2. Điểm vào dữ liệu

| Thành phần | File |
|---|---|
| Lệnh vận hành | [Makefile](../../Makefile) |
| Batch DAG | [lakehouse_oltp_pipeline.py](../../airflow/dags/lakehouse_oltp_pipeline.py) |
| OLTP Bronze | [writer.py](../../pipelines/src/lakehouse/oltp/bronze/writer.py) |
| Shared utilities | [utils/](../../pipelines/src/lakehouse/utils/) — config, Spark, SQL loader và maintenance |
| OLTP Gold | [gold/](../../pipelines/src/lakehouse/oltp/gold/), [gold/schema.py](../../pipelines/src/lakehouse/oltp/gold/schema.py) |
| OLTP Silver | [writer.py](../../pipelines/src/lakehouse/oltp/silver/writer.py), [schema.py](../../pipelines/src/lakehouse/oltp/silver/schema.py) |
| OLTP quality | [gate.py](../../pipelines/src/lakehouse/oltp/quality/gate.py), kiểm tra theo domain và [reconcile.py](../../pipelines/src/lakehouse/oltp/quality/reconcile.py) |
| Access log stream | [kafka_to_lakehouse.py](../../pipelines/src/jobs/logs/kafka_to_lakehouse.py) |
| CDC stream | [mysql_cdc_to_bronze.py](../../pipelines/src/jobs/cdc/mysql_cdc_to_bronze.py) |
| Query serving | [analytics service](../../services/platform-app/src/platform_app/api/modules/analytics/service.py) |
| Storage authorization | [MinIO](../../infrastructure/minio), [Polaris](../../infrastructure/polaris/rbac.yml) |

## 3. Workspace và profile

Python workspace có `platform-app`, `data-generator`, `batch-pipeline` dùng chung `uv.lock`. Package `platform_app` chứa `api/` và `ui/`; generator vẫn là CLI/thư viện riêng. Spark/Flink runtime nằm trong các Docker image xử lý dữ liệu.

Profile `core` chứa PostgreSQL, MinIO, Polaris, Airflow, Trino và platform-app; `batch` chứa Spark SQL client local; `streaming` chứa Kafka, Debezium Connect và Flink. Các init thuộc nhóm service tương ứng. Các job đăng ký CDC/submit Flink nằm trong streaming với scale 0, chỉ chạy thủ công. Khi chọn batch hoặc streaming, bật kèm core. Makefile mặc định chọn core.

Bootstrap MinIO/Polaris mới và chuyển schema/SQL nghiệp vụ từ MySQL sang PostgreSQL còn chờ triển khai. Build/healthcheck của service không xác nhận pipeline nghiệp vụ chạy đầu cuối.

Các dịch vụ storage/catalog dùng chung và được kết nối qua Docker network. Xem service/port chính xác trong [Compose](../../docker-compose.yml).

## 4. Quy tắc phụ thuộc

- Frontend → FastAPI trong `platform-app`; kết nối API được cấu hình PostgreSQL/Trino. Logic/schema nguồn MySQL cũ còn chờ chuyển đổi.
- Spark batch đọc MySQL, ghi staging và bảng Iceberg.
- Debezium được cấu hình đọc PostgreSQL WAL; consumer CDC Flink cũ vẫn cần chuyển đổi.
- Polaris quản lý catalog và quyền; MinIO lưu file.
- Trino phục vụ SQL; không thay thế OLTP.
- Generator tạo dữ liệu nguồn, không chèn số liệu dựng sẵn trực tiếp vào Gold.

CI và các lệnh phát triển hiện tại được mô tả tại [README tổng](../../README.md#development-commands).
