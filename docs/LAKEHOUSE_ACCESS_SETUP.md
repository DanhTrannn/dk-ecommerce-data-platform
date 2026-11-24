# Cấp quyền để chạy toàn bộ Lakehouse

Làm theo thứ tự từ **Bước 1 đến Bước 15** để mở stack, chạy pipeline OLTP và tạo dashboard. Sau lần thiết lập đầu tiên, dùng `make up` mỗi khi cần mở lại toàn bộ stack.

Các lệnh chạy từ thư mục gốc repo. Các bước dùng tên mặc định `lakehouse` cho cả bucket MinIO và catalog Polaris. Trino đang được cấu hình chọn warehouse Polaris `lakehouse`. Nếu đã tạo một tài nguyên, đối chiếu tên, cấu hình và quyền rồi tiếp tục bước kế tiếp. [Cấu hình catalog Trino](../infrastructure/trino/catalog)

## Chuẩn bị Console

### Bước 1 Chuẩn bị tệp môi trường

Cần Docker Engine và Docker Compose v2. Nếu nạp dữ liệu mẫu, chuẩn bị thêm Python 3.11 và `uv`. Tạo `.env` nếu chưa có.

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

Trong `.env`, kiểm tra `MINIO_LAKEHOUSE_BUCKET=lakehouse` và `POLARIS_CATALOG_NAME=lakehouse`. Kiểm tra các biến tài khoản quản trị MinIO và Polaris, cặp `MINIO_POLARIS_ACCESS_KEY`/`MINIO_POLARIS_SECRET_KEY`, cùng `POSTGRES_ECOMMERCE_READER_USER`, `POSTGRES_ECOMMERCE_READER_PASSWORD`, `TRINO_READER_PASSWORD` và `DBT_TRINO_PASSWORD`. Điền các secret Superset trước khi khởi động PostgreSQL lần đầu. [Mẫu cấu hình](../.env.example) · [Nhóm biến môi trường](SETUP.md#1-chuẩn-bị-môi-trường)

### Bước 2 Bật Console và đăng nhập

```bash
docker compose config --quiet
make up SERVICES=platform-app
```

Mở [Streamlit](http://127.0.0.1:8501), chọn **Quản trị quyền**. Nhập email và mật khẩu từ `API_BOOTSTRAP_ADMIN_EMAIL` và `API_BOOTSTRAP_ADMIN_PASSWORD`. Sau khi đăng nhập, màn hình quản trị Polaris hiện ra.

MinIO không cần cấu hình tay: service `minio-init` chạy khi khởi động, tạo bucket `MINIO_LAKEHOUSE_BUCKET`, policy `lakehouse-polaris` và account `MINIO_POLARIS_ACCESS_KEY` mà Polaris dùng để cấp credential lưu trữ tạm cho từng bảng. Các workload không có account MinIO riêng. [Script minio-init](../infrastructure/minio/init.sh)

## Tạo catalog và cấp quyền Polaris

### Bước 3 Tạo catalog lakehouse

1. Mở tab **Tạo mới**.
2. Mở **Tạo catalog, principal hoặc role Polaris**.
3. Ở **Đối tượng Polaris mới**, chọn **Catalog**.
4. Điền **Tên đối tượng Polaris** là `lakehouse` và **Bucket MinIO** là `lakehouse`.
5. Bấm **Tạo trên Polaris**. Trong **Danh sách**, kiểm tra catalog `lakehouse` đã có. Catalog dùng đường dẫn `s3://lakehouse/warehouse`. [Form tạo catalog](../services/platform-app/src/platform_app/ui/views/access_create.py)

### Bước 4 Tạo ba principal Polaris

Trong **Tạo mới**, chọn **Principal (client account)**. Nhập từng tên `batch_airflow`, `trino_writer`, `trino_reader`. Bấm **Tạo trên Polaris** sau mỗi tên. Polaris sinh client ID và secret, Console lưu chúng cho bước cấp credential.

### Bước 5 Tạo principal role và catalog role

1. Chọn **Principal role**, tạo `pipeline_writer` và `pipeline_reader`.
2. Chọn **Catalog role**, tạo `lakehouse_writer` và `lakehouse_reader`.
3. Với mỗi đối tượng, nhập tên rồi bấm **Tạo trên Polaris**.

### Bước 6 Gán principal vào principal role

Mở **Gán / Gỡ quyền**. Ở **Loại liên kết**, chọn **Principal → Principal role**. Chọn principal trong **Đối tượng** và principal role trong **Role / quyền**, rồi chọn **Gán** → **Áp dụng Polaris**. Lặp cho đủ các dòng.

| Đối tượng | Role / quyền |
|---|---|
| `batch_airflow` | `pipeline_writer` |
| `trino_writer` | `pipeline_writer` |
| `trino_reader` | `pipeline_reader` |

### Bước 7 Gán principal role vào catalog role

Đổi **Loại liên kết** sang **Principal role → Catalog role**. Gán `pipeline_writer` → `lakehouse_writer`, sau đó gán `pipeline_reader` → `lakehouse_reader`. Bấm **Áp dụng Polaris** cho từng liên kết.

### Bước 8 Gán quyền vào từng catalog role

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

### Bước 9 Cấp credential Polaris cho Docker

1. Mở tab **Cấp cho workload**.
2. Chọn `trino-reader` ở **Workload nhận credential** và `trino_reader` ở **Principal cấp cho workload**.
3. Bấm **Cấp credential cho workload**. Lặp lại với hai cặp còn lại.

| Workload nhận credential | Principal cấp cho workload |
|---|---|
| `trino-reader` | `trino_reader` |
| `trino-writer` | `trino_writer` |
| `batch-airflow` | `batch_airflow` |

Kiểm tra cả ba dòng có **Credential** là **Đã có** và **Quyền file** là **Đúng**. Exporter ghi `clients.env` vào volume từng workload. Trino đọc hai file riêng cho reader và writer. Khi thay credential đang dùng, khởi động lại consumer. [Màn hình cấp credential](../services/platform-app/src/platform_app/ui/views/access_workloads.py) · [Volume credential](SETUP.md#tạo-quyền-và-credential-trên-streamlit)

## Chạy pipeline OLTP

### Bước 10 Cấp quyền đọc PostgreSQL cho Airflow

Khi PostgreSQL đã có schema nghiệp vụ, chạy lệnh dưới để tạo role reader và cấp `SELECT` trên 27 bảng nguồn (batch nạp 19 bảng, hai bảng tồn kho chỉ dùng để đối soát). Nếu vừa đổi biến reader trong `.env`, áp dụng cấu hình bằng `make up SERVICES=postgres` trước. [Script cấp quyền reader](../infrastructure/postgres/configure-source-reader.sh)

```bash
make source-reader
```

### Bước 11 Mở toàn bộ stack

Khi chạy `make up`, Compose tự khởi tạo metadata và admin Superset nếu thiếu. [Cấu hình metadata Superset](SETUP.md#khởi-tạo-metadata-superset-lần-đầu)

```bash
make up
docker compose ps trino airflow-scheduler
```

Kiểm tra cả `trino` và `airflow-scheduler` đã lên `healthy`. Mở [Airflow](http://localhost:8080). Tài khoản đăng nhập lấy từ `AIRFLOW_ADMIN_USER` và `AIRFLOW_ADMIN_PASSWORD`.

### Bước 12 Nạp dữ liệu mẫu nếu cần

Nếu nguồn demo chưa có lịch sử đơn, dùng preset `100k` để sinh khoảng 100.000 đơn online và đơn POS đi kèm. Nếu đã có dữ liệu cần phân tích, tiếp tục Bước 13. [Generator và điều kiện nạp dữ liệu](../generator/README.md)

```bash
make seed
make generate PRESET=100k
make generate-verify PRESET=100k
```

### Bước 13 Chạy lần nạp đầu

Bật và chạy DAG init. DAG này tạo bảng, nạp toàn bộ nguồn rồi dựng Silver và Gold. [Pipeline OLTP](../airflow/README.md)

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_init
docker compose exec -T airflow-scheduler airflow dags trigger lakehouse_oltp_init
```

### Bước 14 Xác nhận và bật lịch hằng ngày

Trong **Airflow → DAGs → `lakehouse_oltp_init` → lần chạy mới nhất**, mọi task trong các nhóm `staging`, `reconcile`, `bronze`, `silver`, `gold`, `maintenance` phải xanh. Kiểm tra Gold có dữ liệu:

```sql
SELECT count(*) FROM lakehouse.gold.fact_order;
```

Sau đó bật DAG chạy incremental lúc **03:00 giờ Việt Nam** mỗi ngày:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_sync
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_incremental
```

`lakehouse_oltp_sync` chạy sau mỗi lần incremental để đồng bộ thay đổi cột và dòng bị xoá ở nguồn.

## Luồng access log

FastAPI ghi access log ra stdout. [Access log](SETUP.md#5-access-log)

## Dashboard Superset

### Bước 15 Đồng bộ dashboard

Sau khi batch full đã chạy xong, chạy:

```bash
make superset-sync
```

Đăng nhập bằng `SUPERSET_ADMIN_USER` và `SUPERSET_ADMIN_PASSWORD`. Mở bốn dashboard từ danh sách **Dashboards** hoặc theo URL trong `SUPERSET_RESULT.dashboards`. Dashboard luôn đọc bảng Gold hiện tại. [Superset](http://localhost:8088) · [Danh sách dashboard và cách lọc ngày](SETUP.md#6-dashboard-superset)

## Khởi động lại và xử lý lỗi

### Tìm bước cần kiểm tra lại khi có lỗi

| Triệu chứng | Về bước nào |
|---|---|
| Trino báo `Polaris credential file is not ready: /run/polaris/clients.env` | Về Bước 9, kiểm tra cả `trino-reader` và `trino-writer`. Sau khi cấp đủ, chạy `make up SERVICES=trino`. |
| Workload báo S3 `403` | Xem log `docker compose logs minio-init` và kiểm tra secret trong `.env` có khớp account Polaris dùng. Chạy lại `docker compose up minio-init`, rồi khởi động lại Polaris. |
| Iceberg REST báo thiếu quyền đọc hoặc ghi bảng | Về Bước 6 đến Bước 9. Kiểm tra principal được cấp và lần theo liên kết đến catalog role. |
| Batch không đăng nhập được PostgreSQL | Về Bước 10 và kiểm tra biến reader cùng kết quả `make source-reader`. |
| Superset sync báo thiếu bảng Gold | Về Bước 14, kiểm tra `lakehouse_oltp_init` đã chạy xong rồi chạy lại Bước 15. |

Xem log startup và trạng thái container bằng các lệnh dưới. [Hướng dẫn xử lý sự cố](OPERATIONS.md#4-xử-lý-sự-cố)

```bash
docker compose logs --tail=100 minio-init trino credential-exporter
docker compose ps -a
```
