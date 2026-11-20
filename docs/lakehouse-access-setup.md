# Tạo bucket và cấp quyền để chạy toàn bộ Lakehouse

Để chạy lần đầu, khởi động Console, tạo bucket và cấp quyền MinIO, rồi tạo catalog và cấp credential Polaris. Chạy **batch OLTP trước** để có dữ liệu đã công bố. Sau đó bật **Kafka và Flink** để thu access log. **Superset** cần Trino và publication OLTP nên có thể khởi tạo ngay khi batch hoàn tất.

Các lệnh chạy từ thư mục gốc repo. Bucket MinIO và catalog Polaris là hai đối tượng riêng, cùng có tên mặc định `lakehouse`. Hai catalog Trino trong repo đang dùng warehouse Polaris `lakehouse`. [Cấu hình catalog Trino](../infrastructure/trino/catalog)

## Chuẩn bị cấu hình và bật Console

Cần Docker Engine và Docker Compose v2. Dùng Python 3.11 và `uv` nếu nạp dữ liệu mẫu từ máy host.

1. Tạo `.env` từ `.env.example` nếu chưa có. Kiểm tra các nhóm biến bên dưới trước khi khởi động.
2. Chạy `make up` để bật Streamlit, FastAPI, PostgreSQL, MinIO, Polaris và credential exporter.
3. Mở [Streamlit](http://127.0.0.1:8501) → **Quản trị quyền**. Dùng email và mật khẩu của tài khoản admin bootstrap để đăng nhập.

| Việc cần làm | Biến cần kiểm tra trong `.env` |
|---|---|
| Đăng nhập Console | `API_BOOTSTRAP_ADMIN_EMAIL`, `API_BOOTSTRAP_ADMIN_PASSWORD` |
| Cấu hình tài khoản quản trị MinIO và Polaris | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `POLARIS_REALM`, `POLARIS_ROOT_CLIENT_ID`, `POLARIS_ROOT_CLIENT_SECRET` |
| Đặt tên kho lưu trữ | `MINIO_LAKEHOUSE_BUCKET` và `POLARIS_CATALOG_NAME`, mặc định đều là `lakehouse` |
| Tạo account lưu trữ | Bốn cặp `MINIO_*_ACCESS_KEY` và `MINIO_*_SECRET_KEY` ở phần MinIO |
| Kết nối nguồn và Trino | `POSTGRES_ECOMMERCE_READER_USER`, `POSTGRES_ECOMMERCE_READER_PASSWORD`, `TRINO_READER_PASSWORD`, `DBT_TRINO_PASSWORD` |
| Khởi tạo Superset | `SUPERSET_DB_PASSWORD`, `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_PASSWORD` |

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
```

Sau khi điền cấu hình, kiểm tra Compose rồi khởi động Console.

```bash
docker compose --env-file .env --profile '*' config --quiet
make up
```

`make up` chạy bootstrap tài khoản quản trị. Các bước bên dưới tạo tài nguyên riêng cho từng workload. Với stack đã có bucket, account hoặc role, kiểm tra cấu hình đã lưu và tiếp tục từ phần còn thiếu. [Cấu hình môi trường và bootstrap](setup.md)

## Tạo bucket, account và policy trên MinIO

### Tạo bucket

1. Trong **Quản trị quyền**, chọn **Hệ thống → MinIO**.
2. Vào **Tạo mới** rồi mở **Tạo bucket, account hoặc policy MinIO**.
3. Ở **Đối tượng mới**, chọn **Bucket**.
4. Nhập `lakehouse` vào **Tên đối tượng / access key** và bấm **Tạo trên MinIO**.
5. Vào **Danh sách** để kiểm tra bucket đã xuất hiện. Nếu Console đang hiển thị trạng thái cũ, bấm **Tải lại quyền**. [Form tạo tài nguyên MinIO](../services/platform-app/src/platform_app/ui/views/access_create.py)

### Tạo account cho từng workload

Trong cùng form **Tạo mới**, đổi **Đối tượng mới** sang **Account**. Với mỗi dòng bên dưới, nhập **giá trị** biến `ACCESS_KEY` vào **Tên đối tượng / access key**, nhập **giá trị** biến `SECRET_KEY` vào **Secret key** rồi bấm **Tạo trên MinIO**. Secret có từ 8 đến 128 ký tự ASCII in được và không có khoảng trắng.

| Dùng cho | Access key mặc định | Biến access key | Biến secret key |
|---|---|---|---|
| Polaris truy cập kho đối tượng | `lakehouse-polaris` | `MINIO_POLARIS_ACCESS_KEY` | `MINIO_POLARIS_SECRET_KEY` |
| Airflow batch và catalog ghi của Trino | `lakehouse-batch` | `MINIO_BATCH_ACCESS_KEY` | `MINIO_BATCH_SECRET_KEY` |
| Flink ghi Landing | `lakehouse-flink` | `MINIO_FLINK_ACCESS_KEY` | `MINIO_FLINK_SECRET_KEY` |
| Catalog đọc của Trino | `lakehouse-trino` | `MINIO_TRINO_ACCESS_KEY` | `MINIO_TRINO_SECRET_KEY` |

Ví dụ, với `MINIO_BATCH_ACCESS_KEY=lakehouse-batch`, nhập `lakehouse-batch` vào ô tên. Secret lấy từ `MINIO_BATCH_SECRET_KEY`. Với account đã có, kiểm tra tên khớp `.env`, trạng thái đang bật và policy được gán. [Cấu hình account lưu trữ](setup.md#tạo-quyền-và-credential-trên-streamlit)

### Tạo policy và gán cho account

1. Trong **Tạo mới**, đổi **Đối tượng mới** sang **Policy dữ liệu**.
2. Điền tên policy ở **Tên đối tượng / access key**, chọn **Bucket** `lakehouse`.
3. Trong **Prefix được truy cập**, nhập mỗi prefix trên một dòng với dấu `/` ở cuối. Form tự thêm wildcard và quyền liệt kê các prefix.
4. Chọn **Quyền object** theo bảng rồi bấm **Tạo trên MinIO**.

| Tên policy gợi ý | Prefix nhập vào form | Quyền object | Account nhận policy |
|---|---|---|---|
| `lakehouse-storage-rw` | `warehouse/` | **Đọc**, **Ghi (kèm multipart upload)**, **Xóa** | `lakehouse-polaris`, `lakehouse-flink` |
| `lakehouse-batch-rw` | `warehouse/`, `landing/`, `state/` | **Đọc**, **Ghi (kèm multipart upload)**, **Xóa** | `lakehouse-batch` |
| `lakehouse-storage-ro` | `warehouse/` | **Đọc** | `lakehouse-trino` |

Với `lakehouse-batch-rw`, dán ba dòng dưới đây vào ô prefix. [Mẫu policy MinIO](../infrastructure/minio/policies)

```text
warehouse/
landing/
state/
```

Vào **Gán / Gỡ quyền**. Chọn **Account MinIO** → **Policy** → **Gán** → **Áp dụng MinIO**. Ở lượt đầu, chọn `lakehouse-polaris` và `lakehouse-storage-rw`. Lặp lại theo bảng cho ba account còn lại. Kiểm tra cột **Policy trực tiếp** trong **Danh sách** để xác nhận từng liên kết. [Form gán quyền MinIO](../services/platform-app/src/platform_app/ui/views/access.py)

## Tạo catalog và gán quyền Polaris

### Tạo catalog, principal và role

1. Chuyển **Hệ thống → Polaris → Tạo mới** và mở **Tạo catalog, principal hoặc role Polaris**.
2. Chọn **Đối tượng Polaris mới → Catalog**. Nhập `lakehouse` vào **Tên đối tượng Polaris** và `lakehouse` vào **Bucket MinIO**. Bấm **Tạo trên Polaris**. Catalog dùng đường dẫn `s3://lakehouse/warehouse`.
3. Chọn **Principal (client account)** và tạo bốn principal theo bảng. Polaris sinh client ID và secret khi tạo. Console lưu chúng để cấp cho workload.
4. Chọn **Principal role** và tạo `pipeline_writer`, `pipeline_reader`. Chọn **Catalog role** và tạo `lakehouse_writer`, `lakehouse_reader`. Mỗi role trong bảng chỉ cần tạo một lần. [Form tạo tài nguyên Polaris](../services/platform-app/src/platform_app/ui/views/access_create.py)

| Workload | Principal | Principal role | Catalog role |
|---|---|---|---|
| `batch-airflow` | `batch_airflow` | `pipeline_writer` | `lakehouse_writer` |
| `trino-writer` | `trino_writer` | `pipeline_writer` | `lakehouse_writer` |
| `flink` | `flink_landing` | `pipeline_writer` | `lakehouse_writer` |
| `trino-reader` | `trino_reader` | `pipeline_reader` | `lakehouse_reader` |

### Gán role và quyền catalog

Vào **Gán / Gỡ quyền**. Trong **Loại liên kết**, chọn lần lượt các mục sau. Sau mỗi liên kết, chọn **Gán** rồi bấm **Áp dụng Polaris**.

1. **Principal → Principal role**. Chọn `batch_airflow` ở **Đối tượng**, rồi chọn `pipeline_writer` ở **Role / quyền**. Lặp lại cho ba principal còn lại theo bảng.
2. **Principal role → Catalog role**. Gán `pipeline_writer` → `lakehouse_writer` và `pipeline_reader` → `lakehouse_reader`.
3. **Catalog role → Quyền catalog**. Chọn **Phạm vi quyền** là `catalog`. Với `lakehouse_writer`, gán `CATALOG_MANAGE_CONTENT` để các workload ghi có thể tạo namespace, tạo bảng và cập nhật nội dung trong catalog local. Với `lakehouse_reader`, gán từng quyền chỉ đọc bên dưới.

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

Kiểm tra bảng liên kết hiển thị đủ chuỗi **principal → principal role → catalog role → quyền**. Polaris kiểm soát thao tác trên catalog Iceberg. Policy MinIO kiểm soát đọc và ghi tệp trong bucket. [Form gán quyền Polaris](../services/platform-app/src/platform_app/ui/views/access.py) · [RBAC Polaris 1.8](https://polaris.apache.org/releases/1.8.0/managing-security/access-control/)

## Cấp credential cho các workload Docker

1. Mở **Polaris → Cấp cho workload**.
2. Ở **Workload nhận credential**, chọn `trino-reader`. Ở **Principal cấp cho workload**, chọn `trino_reader`.
3. Bấm **Cấp credential cho workload**. Lặp lại với `trino-writer` → `trino_writer`, `batch-airflow` → `batch_airflow` và `flink` → `flink_landing`.
4. Kiểm tra cột **Credential** hiện **Đã có**, cột **Quyền file** hiện **Đúng** và tên principal đúng với bảng.

Exporter ghi `clients.env` vào volume của từng workload. Trino cần cả `/run/polaris/clients.env` cho reader và `/run/polaris-writer/clients.env` cho writer. Sau khi thay credential của consumer đang chạy, khởi động lại consumer để đọc file mới. Job Flink đang chạy cần được submit lại để nhận credential mới. [Màn hình cấp credential](../services/platform-app/src/platform_app/ui/views/access_workloads.py) · [Đường dẫn và volume workload](setup.md#tạo-quyền-và-credential-trên-streamlit)

## Chạy full batch OLTP qua Airflow

Sau khi cấp credential, chạy `make source-reader` để tạo tài khoản PostgreSQL có quyền `SELECT` trên 27 bảng nghiệp vụ. Chạy `make core` rồi kiểm tra Trino và Airflow scheduler đã lên `healthy`. Nếu vừa đổi các biến reader trong `.env`, áp dụng lại cấu hình PostgreSQL bằng `docker compose --profile core up -d --wait postgres` trước khi chạy `make source-reader`. [Script cấp quyền PostgreSQL reader](../infrastructure/postgres/configure-source-reader.sh)

```bash
make source-reader
make core
docker compose --profile core ps trino airflow-scheduler
```

Nếu database đang trống và cần lịch sử mẫu cho dashboard, chạy seed và generator trước khi trigger batch. Preset `1m` sinh khoảng một triệu đơn online, cộng thêm đơn POS. Với database đã có dữ liệu cần phân tích, chuyển sang trigger. [Generator và điều kiện nạp dữ liệu](../generator/README.md)

```bash
make seed
make generate PRESET=1m
```

Bật DAG thủ công và trigger một lần.

```bash
docker compose exec -T airflow-scheduler airflow dags unpause init_lakehouse_oltp
docker compose exec -T airflow-scheduler airflow dags trigger init_lakehouse_oltp
```

Trong [Airflow](http://localhost:8080), mở **`init_lakehouse_oltp` → lần chạy mới nhất → `run_batch_pipeline` → Logs**. Dòng `POLARS_BATCH` hiện số dòng đọc theo từng bảng. Dòng `BATCH_PIPELINE_RESULT` có `"status": "PUBLISHED"` xác nhận batch đã công bố dữ liệu. Có thể kiểm tra publication từ một client SQL kết nối Trino qua catalog đọc `lakehouse`. [Kiểm tra SQL Trino](operations.md#3-kiểm-tra-bằng-sql-trino)

```sql
SELECT run_id, published_at, schema_version, source_mode
FROM lakehouse.system.analytics_publications
ORDER BY published_at DESC
LIMIT 1;
```

Sau khi full batch thành công, có thể bật `lakehouse_oltp_pipeline` để chạy incremental lúc **03:00 giờ Việt Nam**. Chờ batch đang chạy kết thúc trước khi trigger lại full init vì hai DAG dùng cùng namespace phục vụ. [Lịch DAG incremental](../airflow/dags/lakehouse_oltp_pipeline.py)

```bash
docker compose exec -T airflow-scheduler airflow dags unpause lakehouse_oltp_pipeline
```

## Bật Kafka và Flink để xử lý access log

1. Đặt `KAFKA_ENABLED=true` trong `.env`.
2. Chạy `make stream` để bật profile `core,streaming` và áp dụng cấu hình Kafka vào FastAPI.
3. Submit job Flink. Mở [Flink UI](http://localhost:8085) và đợi job lên **RUNNING**.

```bash
make stream
docker compose --profile core --profile streaming run --rm flink-submit
```

4. Gọi API hoặc chạy **Mô phỏng API** trên Streamlit để tạo access log. Flink ghi sự kiện vào `lakehouse.landing.access_logs`.
5. Sau khi OLTP đã có publication và Landing đã nhận log, bật DAG **`lakehouse_logs_marts`** trong Airflow. DAG chạy theo giờ để xử lý và công bố mart log. Nếu cần kiểm tra ngay, dùng **Trigger** khi không có run log nào đang chạy. [Luồng access log](setup.md#5-access-log) · [DAG công bố mart log](../airflow/dags/lakehouse_logs_marts.py)

## Khởi tạo và đồng bộ Superset

1. Kiểm tra `SUPERSET_DB_PASSWORD`, `SUPERSET_SECRET_KEY` và `SUPERSET_ADMIN_PASSWORD` trong `.env`. Nếu vừa cập nhật biến PostgreSQL cho Superset, áp dụng lại service `postgres` khi các job đang rảnh.
2. Build image, tạo database metadata và khởi tạo admin Superset.
3. Bật Superset rồi sync dashboard từ publication OLTP đã thành công.

```bash
docker compose --profile core --profile bi build superset
make superset-db
make superset-init
docker compose --profile core --profile bi up -d --wait superset
make superset-sync
```

Mở đường dẫn dashboard được in trong `SUPERSET_RESULT` trên [Superset](http://localhost:8088). Sau khi batch công bố phiên mới, chạy lại `make superset-sync` để lấy dashboard ghim vào các snapshot mới. [Thiết lập Superset](setup.md#6-superset-tùy-chọn)

## Khởi động lại toàn bộ stack sau khi đã cấp quyền

Khi bucket, policy, bốn credential Polaris và metadata Superset đã sẵn sàng, dùng lệnh dưới đây để bật toàn bộ profile. Sau khi Flink khởi động, chạy `flink-submit` để kiểm tra hoặc submit job. Script bỏ qua bước submit nếu job phù hợp đã ở trạng thái **RUNNING**. [Lệnh Make](../Makefile) · [Script submit Flink](../infrastructure/flink/bin/submit-job.sh)

```bash
make up-all PROFILES=core,streaming,bi
docker compose --profile core --profile streaming run --rm flink-submit
make status
```

## Kiểm tra khi dịch vụ hoặc pipeline lỗi

| Triệu chứng | Kiểm tra tiếp |
|---|---|
| Trino báo `Polaris credential file is not ready: /run/polaris/clients.env` | Mở **Cấp cho workload** và kiểm tra `trino-reader`. Kiểm tra cả `trino-writer` vì Trino đọc hai file khi khởi động. Sau khi cấp đủ, chạy `docker compose --profile core up -d --wait trino`. |
| Workload báo lỗi S3 `403` | Kiểm tra tên account khớp biến `ACCESS_KEY`, secret thực khớp `.env`, và policy của account bao đúng bucket, prefix và quyền object. |
| Iceberg REST báo thiếu quyền đọc hoặc ghi bảng | Xem principal đang được cấp, lần theo liên kết đến catalog role và kiểm tra phạm vi quyền. |
| Batch không đăng nhập được PostgreSQL | Kiểm tra biến reader và kết quả `make source-reader`. |
| Batch báo `Legacy analytical schemas require a full run with --migrate-schema` | Catalog đã có schema cũ. Rà quy trình migration v3 sang v4 trước khi chạy full trên các bảng đang phục vụ. [Kiểm tra schema trước migration](../pipelines/src/lakehouse/oltp/contract_migration.py) |
| Flink chạy nhưng Landing trống | Kiểm tra `KAFKA_ENABLED=true` đã được áp dụng cho `platform-app` và gọi API sau khi Flink lên **RUNNING**. |
| Superset sync báo thiếu publication | Mở Airflow để kiểm tra full batch đã tới `PUBLISHED`, rồi chạy lại sync. |

Xem log startup và trạng thái container bằng các lệnh sau. [Hướng dẫn xử lý sự cố](operations.md#4-troubleshooting)

```bash
docker compose --profile core logs --tail=100 trino credential-exporter
docker compose --profile core ps -a
```
