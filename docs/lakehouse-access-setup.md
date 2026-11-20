# Tạo bucket và cấp quyền để chạy toàn bộ Lakehouse

Làm theo thứ tự từ **Bước 1 đến Bước 23** để chạy full batch OLTP. Khi Airflow báo `PUBLISHED`, tiếp tục **Bước 24 đến Bước 26** để thu và xử lý access log, hoặc **Bước 27 đến Bước 28** để xem dashboard Superset. Sau khi đã cấp quyền và khởi tạo Superset, dùng **Bước 29** để mở lại toàn bộ stack.

Các lệnh chạy từ thư mục gốc repo. Các bước dùng tên mặc định `lakehouse` cho cả bucket MinIO và catalog Polaris. Trino đang được cấu hình chọn warehouse Polaris `lakehouse`. Nếu đã tạo một tài nguyên, đối chiếu tên, cấu hình và quyền rồi tiếp tục bước kế tiếp. [Cấu hình catalog Trino](../infrastructure/trino/catalog)

## Chuẩn bị Console

### Bước 1 Chuẩn bị tệp môi trường

Cần Docker Engine và Docker Compose v2. Nếu nạp dữ liệu mẫu, chuẩn bị thêm Python 3.11 và `uv`. Tạo `.env` nếu chưa có.

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

Trong `.env`, kiểm tra `MINIO_LAKEHOUSE_BUCKET=lakehouse` và `POLARIS_CATALOG_NAME=lakehouse`. Kiểm tra các biến tài khoản quản trị MinIO và Polaris, các cặp `MINIO_*_ACCESS_KEY` và `MINIO_*_SECRET_KEY`, cùng `POSTGRES_ECOMMERCE_READER_USER`, `POSTGRES_ECOMMERCE_READER_PASSWORD`, `TRINO_READER_PASSWORD` và `DBT_TRINO_PASSWORD`. Nếu dùng Superset, cấu hình ba biến ở Bước 27 trước khi khởi động PostgreSQL lần đầu. [Mẫu cấu hình](../.env.example) · [Nhóm biến môi trường](setup.md#1-chuẩn-bị-môi-trường)

### Bước 2 Bật Console và đăng nhập

```bash
docker compose --env-file .env --profile '*' config --quiet
make up
```

Mở [Streamlit](http://127.0.0.1:8501), chọn **Quản trị quyền**. Nhập email và mật khẩu từ `API_BOOTSTRAP_ADMIN_EMAIL` và `API_BOOTSTRAP_ADMIN_PASSWORD`. Sau khi đăng nhập, màn hình sẽ có bộ chọn **Hệ thống** với hai mục **MinIO** và **Polaris**.

## Tạo tài nguyên MinIO

### Bước 3 Tạo bucket lakehouse

1. Chọn **Hệ thống → MinIO → Tạo mới**.
2. Mở **Tạo bucket, account hoặc policy MinIO**.
3. Ở **Đối tượng mới**, chọn **Bucket**.
4. Trong **Tên đối tượng / access key**, nhập `lakehouse`.
5. Bấm **Tạo trên MinIO**. Mở **Danh sách** và kiểm tra bucket `lakehouse` xuất hiện. Nếu danh sách còn cũ, bấm **Tải lại quyền**. [Form tạo bucket](../services/platform-app/src/platform_app/ui/views/access_create.py)

### Bước 4 Tạo account lưu trữ cho Polaris

Trong **MinIO → Tạo mới**, chọn **Đối tượng mới → Account**. Ở **Tên đối tượng / access key**, nhập giá trị `MINIO_POLARIS_ACCESS_KEY`, mặc định là `lakehouse-polaris`. Ở **Secret key**, nhập giá trị `MINIO_POLARIS_SECRET_KEY`. Bấm **Tạo trên MinIO**.

Nhập giá trị thực trong `.env` vào các ô. Form chấp nhận secret từ 8 đến 128 ký tự ASCII in được, không có khoảng trắng.

### Bước 5 Tạo account lưu trữ cho Airflow và Trino writer

Giữ **Đối tượng mới → Account**. Nhập giá trị `MINIO_BATCH_ACCESS_KEY`, mặc định là `lakehouse-batch`, vào ô tên. Nhập giá trị `MINIO_BATCH_SECRET_KEY` vào **Secret key** rồi bấm **Tạo trên MinIO**. Airflow batch và catalog ghi của Trino cùng dùng account này.

### Bước 6 Tạo account lưu trữ cho Flink

Giữ **Đối tượng mới → Account**. Nhập giá trị `MINIO_FLINK_ACCESS_KEY`, mặc định là `lakehouse-flink`, vào ô tên. Nhập giá trị `MINIO_FLINK_SECRET_KEY` vào **Secret key** rồi bấm **Tạo trên MinIO**.

### Bước 7 Tạo account lưu trữ cho Trino reader

Giữ **Đối tượng mới → Account**. Nhập giá trị `MINIO_TRINO_ACCESS_KEY`, mặc định là `lakehouse-trino`, vào ô tên. Nhập giá trị `MINIO_TRINO_SECRET_KEY` vào **Secret key** rồi bấm **Tạo trên MinIO**. Qua **Danh sách** và kiểm tra bốn account đều ở trạng thái **enabled**. [Cấu hình account lưu trữ](setup.md#tạo-quyền-và-credential-trên-streamlit)

### Bước 8 Tạo policy đọc ghi cho Polaris và Flink

1. Ở **MinIO → Tạo mới**, đổi **Đối tượng mới** sang **Policy dữ liệu**.
2. Nhập tên `lakehouse-storage-rw`. Chọn **Bucket** `lakehouse`.
3. Nhập `warehouse/` vào **Prefix được truy cập**.
4. Trong **Quyền object**, chọn **Đọc**, **Ghi (kèm multipart upload)** và **Xóa**.
5. Bấm **Tạo trên MinIO**.

### Bước 9 Tạo policy đọc ghi cho batch

Tạo thêm một **Policy dữ liệu** tên `lakehouse-batch-rw` trên bucket `lakehouse`. Dán ba dòng sau vào **Prefix được truy cập**. Chọn **Đọc**, **Ghi (kèm multipart upload)** và **Xóa**, rồi bấm **Tạo trên MinIO**.

```text
warehouse/
landing/
state/
```

Prefix kết thúc bằng `/`. Form tự thêm wildcard và quyền liệt kê các prefix. [Mẫu policy MinIO](../infrastructure/minio/policies)

### Bước 10 Tạo policy chỉ đọc cho Trino reader

Tạo **Policy dữ liệu** tên `lakehouse-storage-ro` trên bucket `lakehouse`. Nhập prefix `warehouse/`, chọn **Đọc**, rồi bấm **Tạo trên MinIO**.

### Bước 11 Gán policy cho bốn account MinIO

1. Mở tab **Gán / Gỡ quyền**.
2. Chọn **Account MinIO** `lakehouse-polaris` và **Policy** `lakehouse-storage-rw`.
3. Chọn **Gán** rồi bấm **Áp dụng MinIO**.
4. Lặp lại cho ba cặp còn lại trong bảng. Kiểm tra cột **Policy trực tiếp** ở **Danh sách** khi hoàn tất.

| Account mặc định | Policy nhận |
|---|---|
| `lakehouse-polaris` | `lakehouse-storage-rw` |
| `lakehouse-flink` | `lakehouse-storage-rw` |
| `lakehouse-batch` | `lakehouse-batch-rw` |
| `lakehouse-trino` | `lakehouse-storage-ro` |

Nếu đã đổi access key trong `.env`, chọn tên account thực tế đã tạo từ Bước 4 đến Bước 7. [Form gán policy MinIO](../services/platform-app/src/platform_app/ui/views/access.py)

## Tạo catalog và cấp quyền Polaris

### Bước 12 Tạo catalog lakehouse

1. Chuyển **Hệ thống → Polaris → Tạo mới**.
2. Mở **Tạo catalog, principal hoặc role Polaris**.
3. Ở **Đối tượng Polaris mới**, chọn **Catalog**.
4. Điền **Tên đối tượng Polaris** là `lakehouse` và **Bucket MinIO** là `lakehouse`.
5. Bấm **Tạo trên Polaris**. Trong **Danh sách**, kiểm tra catalog `lakehouse` đã có. Catalog dùng đường dẫn `s3://lakehouse/warehouse`. [Form tạo catalog](../services/platform-app/src/platform_app/ui/views/access_create.py)

### Bước 13 Tạo bốn principal Polaris

Trong **Polaris → Tạo mới**, chọn **Principal (client account)**. Nhập từng tên `batch_airflow`, `trino_writer`, `flink_landing`, `trino_reader`. Bấm **Tạo trên Polaris** sau mỗi tên. Polaris sinh client ID và secret, Console lưu chúng cho bước cấp credential.

### Bước 14 Tạo principal role và catalog role

1. Chọn **Principal role**, tạo `pipeline_writer` và `pipeline_reader`.
2. Chọn **Catalog role**, tạo `lakehouse_writer` và `lakehouse_reader`.
3. Với mỗi đối tượng, nhập tên rồi bấm **Tạo trên Polaris**.

### Bước 15 Gán principal vào principal role

Mở **Gán / Gỡ quyền**. Ở **Loại liên kết**, chọn **Principal → Principal role**. Chọn principal trong **Đối tượng** và principal role trong **Role / quyền**, rồi chọn **Gán** → **Áp dụng Polaris**. Lặp cho đủ các dòng.

| Đối tượng | Role / quyền |
|---|---|
| `batch_airflow` | `pipeline_writer` |
| `trino_writer` | `pipeline_writer` |
| `flink_landing` | `pipeline_writer` |
| `trino_reader` | `pipeline_reader` |

### Bước 16 Gán principal role vào catalog role

Đổi **Loại liên kết** sang **Principal role → Catalog role**. Gán `pipeline_writer` → `lakehouse_writer`, sau đó gán `pipeline_reader` → `lakehouse_reader`. Bấm **Áp dụng Polaris** cho từng liên kết.

### Bước 17 Gán quyền vào từng catalog role

1. Chọn **Loại liên kết → Catalog role → Quyền catalog** và đặt **Phạm vi quyền** là `catalog`.
2. Chọn **Đối tượng** `lakehouse_writer` và **Role / quyền** `CATALOG_MANAGE_CONTENT`. Chọn **Gán** → **Áp dụng Polaris**. Quyền này cho phép các job tạo namespace, tạo bảng và quản lý nội dung trong catalog local.
3. Đổi **Đối tượng** sang `lakehouse_reader`. Chọn lần lượt từng quyền bên dưới trong **Role / quyền**, bấm **Áp dụng Polaris** sau mỗi lượt.

```text
CATALOG_READ_PROPERTIES
NAMESPACE_LIST
NAMESPACE_READ_PROPERTIES
TABLE_LIST
TABLE_READ_PROPERTIES
TABLE_READ_DATA
VIEW_LIST
VIEW_READ_PROPERTIES
```

Kiểm tra bảng liên kết trong tab **Gán / Gỡ quyền** để thấy đủ chuỗi **principal → principal role → catalog role → quyền**. [Form gán quyền Polaris](../services/platform-app/src/platform_app/ui/views/access.py) · [RBAC Polaris 1.8](https://polaris.apache.org/releases/1.8.0/managing-security/access-control/)

### Bước 18 Cấp credential Polaris cho Docker

1. Mở **Polaris → Cấp cho workload**.
2. Chọn `trino-reader` ở **Workload nhận credential** và `trino_reader` ở **Principal cấp cho workload**.
3. Bấm **Cấp credential cho workload**. Lặp lại với ba cặp còn lại.

| Workload nhận credential | Principal cấp cho workload |
|---|---|
| `trino-reader` | `trino_reader` |
| `trino-writer` | `trino_writer` |
| `batch-airflow` | `batch_airflow` |
| `flink` | `flink_landing` |

Kiểm tra cả bốn dòng có **Credential** là **Đã có** và **Quyền file** là **Đúng**. Exporter ghi `clients.env` vào volume từng workload. Trino đọc hai file riêng cho reader và writer. Khi thay credential đang dùng, khởi động lại consumer. Với Flink, dừng job cũ và submit lại khi cần áp dụng credential mới. [Màn hình cấp credential](../services/platform-app/src/platform_app/ui/views/access_workloads.py) · [Volume credential](setup.md#tạo-quyền-và-credential-trên-streamlit)

## Chạy full batch OLTP

### Bước 19 Cấp quyền đọc PostgreSQL cho Airflow

Khi PostgreSQL đã có schema nghiệp vụ, chạy lệnh dưới để tạo role reader và cấp `SELECT` trên 27 bảng nguồn. Nếu vừa đổi biến reader trong `.env`, áp dụng cấu hình bằng `docker compose --profile core up -d --wait postgres` trước. [Script cấp quyền reader](../infrastructure/postgres/configure-source-reader.sh)

```bash
make source-reader
```

### Bước 20 Bật Trino và Airflow

```bash
make core
docker compose --profile core ps trino airflow-scheduler
```

Kiểm tra cả `trino` và `airflow-scheduler` đã lên `healthy`. Mở [Airflow](http://localhost:8080). Tài khoản đăng nhập lấy từ `AIRFLOW_ADMIN_USER` và `AIRFLOW_ADMIN_PASSWORD`.

### Bước 21 Nạp dữ liệu mẫu nếu cần

Nếu PostgreSQL đang trống và cần lịch sử cho dashboard, chạy hai lệnh dưới. Preset `1m` sinh khoảng một triệu đơn online, cộng thêm đơn POS. Nếu đã có dữ liệu cần phân tích, tiếp tục Bước 22. [Generator và điều kiện nạp dữ liệu](../generator/README.md)

```bash
make seed
make generate PRESET=1m
```

### Bước 22 Trigger full batch

Chạy hai lệnh dưới để bật DAG thủ công và tạo một lần chạy. Chờ batch incremental đang chạy kết thúc trước khi trigger vì cả hai DAG ghi vào cùng namespace phục vụ. [DAG full batch](../airflow/dags/init_lakehouse_oltp.py)

```bash
docker compose exec -T airflow-scheduler airflow dags unpause init_lakehouse_oltp
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp
```

### Bước 23 Xác nhận batch đã công bố dữ liệu

Trong **Airflow → DAGs**, mở **`init_lakehouse_oltp` → lần chạy mới nhất → `run_batch_pipeline` → Logs**. Dòng `POLARS_BATCH` hiện số dòng đọc theo bảng. Dòng `BATCH_PIPELINE_RESULT` có `"status": "PUBLISHED"` xác nhận đã công bố dữ liệu. Có thể truy vấn manifest qua Trino để xem phiên công bố mới nhất. [Kiểm tra bằng SQL Trino](operations.md#3-kiểm-tra-bằng-sql-trino)

```sql
SELECT run_id, published_at, schema_version, source_mode
FROM lakehouse.system.analytics_publications
ORDER BY published_at DESC
LIMIT 1;
```

Nếu cần lịch incremental hằng ngày, bật DAG bằng lệnh sau. DAG chạy lúc **03:00 giờ Việt Nam**. [DAG incremental](../airflow/dags/lakehouse_oltp_pipeline.py)

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
```

## Bật luồng access log

### Bước 24 Bật Kafka cho FastAPI và mở profile streaming

Đặt `KAFKA_ENABLED=true` trong `.env`, lưu lại rồi chạy lệnh dưới. `make stream` bật `core,streaming` và áp dụng cấu hình Kafka vào `platform-app`.

```bash
make stream
```

### Bước 25 Submit job Flink

```bash
docker compose --profile core --profile streaming run --rm flink-submit
```

Mở [Flink UI](http://localhost:8085) và đợi job lên **RUNNING**. [Script submit Flink](../infrastructure/flink/bin/submit-job.sh)

### Bước 26 Tạo access log và bật DAG xử lý log

Gọi API hoặc dùng **Mô phỏng API** trên Streamlit để tạo request sau khi Flink đã chạy. Dùng client SQL kết nối Trino để kiểm tra Landing có sự kiện.

```sql
SELECT event_id, event_ts
FROM lakehouse.landing.access_logs
ORDER BY event_ts DESC
LIMIT 5;
```

Khi truy vấn trả về sự kiện và batch OLTP đã công bố, bật `lakehouse_logs_marts` để xử lý log theo giờ.

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_logs_marts
```

Nếu cần xem ngay, chạy lệnh sau khi không có run log hoặc batch nào đang chạy. Trong Airflow, mở task `rollup_logs_gold_marts` của run mới và xem kết quả `NATIVE_LOG_RESULT` trong **Logs**. [Thiết lập access log](setup.md#5-access-log) · [DAG mart log](../airflow/dags/lakehouse_logs_marts.py)

```bash
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_logs_marts
```

## Bật Superset

### Bước 27 Chuẩn bị database và admin Superset

Kiểm tra `SUPERSET_DB_PASSWORD`, `SUPERSET_SECRET_KEY` và `SUPERSET_ADMIN_PASSWORD` trong `.env`. Nếu vừa đổi biến PostgreSQL cho Superset, chạy `docker compose --profile core up -d --wait postgres` khi các job đang rảnh để container nhận cấu hình mới. Sau đó chạy:

```bash
docker compose --profile core --profile bi build superset
make superset-db
make superset-init
```

### Bước 28 Bật Superset và sync dashboard

Sau khi batch đã `PUBLISHED`, chạy:

```bash
docker compose --profile core --profile bi up -d --wait superset
make superset-sync
```

Đăng nhập [Superset](http://localhost:8088) bằng `SUPERSET_ADMIN_USER` và `SUPERSET_ADMIN_PASSWORD`. Mở đường dẫn dashboard trong dòng `SUPERSET_RESULT`. Khi cần xem publication batch mới, chạy lại `make superset-sync` và dùng URL được in ra. [Thiết lập và đồng bộ Superset](setup.md#6-superset-tùy-chọn)

## Khởi động lại và xử lý lỗi

### Bước 29 Mở lại đầy đủ ba profile

Dùng lệnh dưới khi tài nguyên MinIO, credential Polaris và metadata Superset đã sẵn sàng. `flink-submit` bỏ qua thao tác submit nếu đã có job phù hợp ở trạng thái **RUNNING**.

```bash
make up-all PROFILES=core,streaming,bi
docker compose --profile core --profile streaming run --rm flink-submit
make status
```

### Tìm bước cần kiểm tra lại khi có lỗi

| Triệu chứng | Về bước nào |
|---|---|
| Trino báo `Polaris credential file is not ready: /run/polaris/clients.env` | Về Bước 18, kiểm tra cả `trino-reader` và `trino-writer`. Sau khi cấp đủ, chạy `docker compose --profile core up -d --wait trino`. |
| Workload báo S3 `403` | Về Bước 4 đến Bước 11. Kiểm tra access key và secret khớp `.env`, policy gán đúng bucket, prefix và quyền object. |
| Iceberg REST báo thiếu quyền đọc hoặc ghi bảng | Về Bước 15 đến Bước 18. Kiểm tra principal được cấp và lần theo liên kết đến catalog role. |
| Batch không đăng nhập được PostgreSQL | Về Bước 19 và kiểm tra biến reader cùng kết quả `make source-reader`. |
| Batch báo `Legacy analytical schemas require a full run with --migrate-schema` | Catalog đã có schema cũ. Rà migration v3 sang v4 trước khi chạy full trên các bảng đang phục vụ. [Kiểm tra schema trước migration](../pipelines/src/lakehouse/oltp/contract_migration.py) |
| Flink chạy nhưng Landing trống | Về Bước 24 đến Bước 26, kiểm tra `KAFKA_ENABLED=true` đã áp dụng cho `platform-app` và tạo request sau khi Flink `RUNNING`. |
| Superset sync báo thiếu publication | Về Bước 23, kiểm tra batch đã `PUBLISHED` rồi chạy lại Bước 28. |

Xem log startup và trạng thái container bằng các lệnh dưới. [Hướng dẫn xử lý sự cố](operations.md#4-troubleshooting)

```bash
docker compose --profile core logs --tail=100 trino credential-exporter
docker compose --profile core ps -a
```
