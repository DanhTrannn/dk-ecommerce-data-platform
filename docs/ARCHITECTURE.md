# Kiến trúc hệ thống

Cấu trúc dữ liệu và KPI ở [DATA.md](DATA.md), cài đặt ở [SETUP.md](SETUP.md), vận hành ở [OPERATIONS.md](OPERATIONS.md). Chi tiết pipeline OLTP ở [airflow/README.md](../airflow/README.md).

## 1. Phạm vi

Đồ án mô phỏng một nhà bán lẻ thời trang tại Việt Nam, bán online và tại cửa hàng; tiền tệ VND, ngày nghiệp vụ theo Asia/Ho_Chi_Minh. Trọng tâm gồm: dữ liệu sinh có căn cứ, nguồn giao dịch nhất quán, một Data Lakehouse mở có quản trị quyền tự vận hành, và tầng Gold phục vụ phân tích.

| Phần | Hiện trạng |
|---|---|
| Nguồn nghiệp vụ | FastAPI + PostgreSQL 28 bảng |
| Generator | `datagen generate/verify/logs/reset`; preset OLTP `100k`/`1m`/`10m`/`50m` |
| Console | Streamlit: 8 kịch bản qua API (mua, giao, giao thất bại, huỷ, đánh giá, đổi trả, coupon, POS), quản trị quyền Polaris |
| Pipeline OLTP | Airflow: staging → Bronze → Silver → Gold, 19 bảng nguồn, 27 model Gold; đồng bộ dòng bị xoá bằng xoá mềm và thay đổi cột đã duyệt |
| Access log | API ghi JSON ra stdout. Generator sinh log lịch sử dạng Parquet. Nạp vào lakehouse là hướng phát triển |
| BI | 4 dashboard Superset, 27 biểu đồ, đọc 14 mart OLTP Gold qua Trino |

Không dùng Spark, Debezium hay CDC.

## 2. Component

| Component | Vai trò | Mã nguồn |
|---|---|---|
| `platform-app` | Một container chạy FastAPI (cổng 8000) và Streamlit (8501); router quản trị quyền `/api/v1/access` | [services/platform-app](../services/platform-app/) |
| `platform-admin-init` | Chạy một lần: áp schema nguồn trống, tạo admin bootstrap | [provisioning](../services/platform-app/src/platform_app/provisioning/) |
| `credential-exporter` | Nhận yêu cầu đã ký, ghi credential Polaris vào volume của từng workload | [credential_export](../services/platform-app/src/platform_app/credential_export/) |
| PostgreSQL 16.8 | Nguồn `ecommerce`; metadata `airflow`, `polaris`, `superset` | [database](../database/), [scripts](../infrastructure/postgres/) |
| MinIO | Object storage cho dữ liệu và metadata Iceberg; `minio-init` tạo bucket và account lưu trữ duy nhất, của Polaris | [minio](../infrastructure/minio/) |
| Polaris 1.8.0 | Iceberg REST catalog, quyền theo principal/role, cấp credential lưu trữ theo bảng | |
| Airflow 3.3.2 | Bốn DAG OLTP (init, incremental, sync, purge); LocalExecutor; thư viện pipeline cài vào Python của image | [airflow](../airflow/), [image](../infrastructure/airflow/Dockerfile) |
| dbt 1.11 (dbt-trino) | Dựng Silver và Gold trên Trino | [dbt](../dbt/) |
| Trino 483 | Catalog `lakehouse` chỉ đọc cho phân tích, `lakehouse_etl` cho dbt ghi `silver`/`gold`; HTTPS, mật khẩu và SQL access rules | [trino](../infrastructure/trino/) |
| Superset 6.1.0 | Dashboard đọc bảng Gold | [superset](../infrastructure/superset/) |

Ba package `platform-app`, `data-generator` và `batch-pipeline` (`airflow/`) dùng chung [uv.lock](../uv.lock). Airflow và Superset có image riêng.

## 3. Luồng dữ liệu OLTP

```text
API / datagen ──► PostgreSQL ──► staging ──► [reconcile] ──► bronze ──► silver ──► gold ──► Trino ──► Superset
                                 PyIceberg     chỉ init      PyIceberg   dbt merge   dbt
```

| Tầng | Nội dung |
|---|---|
| `staging` | Bản trích xuất mới nhất. Mỗi lần nạp xoá trắng staging của bảng rồi ghi lại từ nguồn |
| `bronze` | Phiên bản hiện tại theo khóa chính, giữ projection nguồn và cờ xóa mềm. UUID được biểu diễn dưới dạng chuỗi trong quá trình trích xuất. |
| `silver` | Kiểu dữ liệu chuẩn hoá (UUID, timestamp), merge theo khoá chính; dbt test `matches_bronze` bảo đảm cùng tập khoá với Bronze |
| `gold` | 7 dimension, 6 fact, 14 mart và 2 seed; dimension và fact merge dòng đổi, mart tính lại các ngày bị ảnh hưởng |

`lakehouse_oltp_init` dùng chung mốc `created_at < run_after` cho các bảng. Mỗi task trích xuất đọc trong transaction riêng. Đối soát số dòng, tổng tiền và sổ kho chạy trước Bronze. `lakehouse_oltp_incremental` chạy 03:00 hằng ngày theo cursor `updated_at` hoặc `created_at`. `lakehouse_oltp_sync` chạy theo Asset từ incremental hoặc chạy tay. Sync xử lý cột đã duyệt hoặc kiểu tương thích, đánh dấu khóa đã xóa và nạp lại khóa xuất hiện trở lại. `lakehouse_oltp_purge` chạy tay để xóa cứng. [Chi tiết pipeline](../airflow/README.md)

### Dashboard BI

Superset đọc bảng Gold hiện hành qua `lakehouse` và tài khoản `analytics_reader`. `make superset-sync` quản lý cấu hình của bốn dashboard từ mã Python. Sau khi tạo dashboard, các lần nạp OLTP cập nhật dữ liệu mà biểu đồ đọc trực tiếp. [Thiết lập và danh sách dashboard](SETUP.md#6-dashboard-superset)

### Access log

FastAPI ghi sự kiện `ecommerce.access` 1.0.0 ra stdout. `datagen logs` viết dữ liệu lịch sử theo hợp đồng 1.1.0 ra Parquet. Compose hiện triển khai đường nạp OLTP. Nạp log vào lakehouse bằng streaming và nạp file, cùng phễu theo phiên, là hướng phát triển của khóa luận; thiết kế được giữ lại. [Hợp đồng dữ liệu](DATA.md#4-access-log-contract-và-xử-lý) · [Thiết kế luồng log](ACCESS_LOG_DESIGN.md)

## 4. Định danh và quyền

- Airflow (`batch-airflow`), Trino reader và Trino writer nhận credential principal riêng qua `credential-exporter`. Polaris cấp credential MinIO tạm thời theo phạm vi bảng mỗi workload được đọc hoặc ghi.
- Pipeline đọc PostgreSQL bằng role chỉ SELECT (`make source-reader`) qua connection Airflow `oltp_postgres`, phiên read-only.
- Trino xác thực `analytics_reader` và `dbt_writer` bằng mật khẩu qua HTTPS; SQL access rules giới hạn `dbt_writer` vào schema `silver` và `gold`.
- MinIO chỉ có một account lưu trữ cho Polaris, do `minio-init` tạo. Quản trị Polaris trên Streamlit chỉ dành cho admin bootstrap; thao tác ghi cần CSRF và được ghi audit trước và sau.
- API giao dịch dùng JWT cookie, CSRF và kiểm tra role/cửa hàng theo route. Superset có login riêng, chưa kế thừa quyền API.

## 5. Triển khai

Thiết lập lần đầu dùng `make up SERVICES=platform-app` để bật Console và cấp credential Polaris. Sau đó `make up` bật toàn bộ stack. `superset-db-init` và `superset-init` chuẩn bị metadata trước khi web Superset khởi động. [Thứ tự thiết lập](SETUP.md#2-khởi-động)

## 6. Hướng phát triển

- Luồng access log: nạp streaming và nạp file vào lakehouse, mart phễu theo phiên. [Thiết kế](ACCESS_LOG_DESIGN.md)
- Phân quyền Superset theo cửa hàng.
- Kịch bản nhập kho trên Console; HA, backup/restore và hardening khi triển khai public.
