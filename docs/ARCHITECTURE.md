# Kiến trúc hệ thống

Cấu trúc dữ liệu và KPI ở [DATA.md](DATA.md), cài đặt ở [SETUP.md](SETUP.md), vận hành ở [OPERATIONS.md](OPERATIONS.md). Chi tiết pipeline OLTP ở [airflow/README.md](../airflow/README.md).

## 1. Phạm vi

Đồ án mô phỏng một nhà bán lẻ thời trang tại Việt Nam, bán online và tại cửa hàng; tiền tệ VND, ngày nghiệp vụ theo Asia/Ho_Chi_Minh. Trọng tâm gồm: dữ liệu sinh có căn cứ, nguồn giao dịch nhất quán, một Data Lakehouse mở có quản trị quyền tự vận hành, và tầng Gold phục vụ phân tích.

| Phần | Hiện trạng |
|---|---|
| Nguồn nghiệp vụ | FastAPI + PostgreSQL 28 bảng |
| Generator | `datagen generate/verify/logs/reset`; preset OLTP `1m`/`10m`/`50m` |
| Console | Streamlit: kịch bản mua online qua API, quản trị quyền MinIO/Polaris |
| Pipeline OLTP | Airflow: staging → Bronze → Silver → Gold, 19 bảng nguồn, 27 model Gold |
| Access log | Đang thiết kế lại: API vẫn có thể gửi Kafka, đã gỡ job Flink và DAG log; thiết kế giữ ở `streaming/sql` và `dbt/models/logs` |
| BI | Superset tùy chọn, ba mart doanh thu; sẽ làm lại sau khi các pipeline ổn định |

Không dùng Spark, Debezium hay CDC.

## 2. Component

| Component | Vai trò | Mã nguồn |
|---|---|---|
| `platform-app` | Một container chạy FastAPI (cổng 8000) và Streamlit (8501); router quản trị quyền `/api/v1/access` | [services/platform-app](../services/platform-app/) |
| `platform-admin-init` | Chạy một lần: áp schema nguồn trống, tạo admin bootstrap | [provisioning](../services/platform-app/src/platform_app/provisioning/) |
| `credential-exporter` | Nhận yêu cầu đã ký, ghi credential Polaris vào volume của từng workload | [credential_export](../services/platform-app/src/platform_app/credential_export/) |
| PostgreSQL 16.8 | Nguồn `ecommerce`; metadata `airflow`, `polaris`, `superset` | [database](../database/), [scripts](../infrastructure/postgres/) |
| MinIO | Object storage cho dữ liệu và metadata Iceberg | [policy mẫu](../infrastructure/minio/policies/) |
| Polaris 1.8.0 | Iceberg REST catalog, quyền theo principal/role, cấp credential lưu trữ theo bảng | |
| Airflow 3.3.2 | Hai DAG OLTP; LocalExecutor; thư viện pipeline cài vào Python của image | [airflow](../airflow/), [image](../infrastructure/airflow/Dockerfile) |
| dbt 1.11 (dbt-trino) | Dựng Silver và Gold trên Trino | [dbt](../dbt/) |
| Trino 483 | Catalog `lakehouse` chỉ đọc cho phân tích, `lakehouse_etl` cho dbt ghi `silver`/`gold`; HTTPS, mật khẩu và SQL access rules | [trino](../infrastructure/trino/) |
| Kafka 3.8, Flink 1.19 | Hạ tầng cho luồng access log (profile `streaming`) | [flink](../infrastructure/flink/) |
| Superset 6.1.0 | Dashboard đọc bảng Gold | [superset](../infrastructure/superset/) |

Python workspace dùng chung [uv.lock](../uv.lock): `platform-app`, `data-generator`, `batch-pipeline` (`airflow/`). Airflow, Flink và Superset có image riêng.

## 3. Luồng dữ liệu OLTP

```text
API / datagen ──► PostgreSQL ──► staging ──► [reconcile] ──► bronze ──► silver ──► gold ──► Trino ──► Superset
                                 PyIceberg     chỉ init      PyIceberg   dbt merge   dbt
```

| Tầng | Nội dung |
|---|---|
| `staging` | Bản trích xuất mới nhất; mỗi lần nạp xoá trắng rồi ghi lại, nên chạy lại cho cùng kết quả |
| `bronze` | Dòng nguồn giữ nguyên kiểu dữ liệu, phiên bản hiện tại theo khoá chính |
| `silver` | Kiểu dữ liệu chuẩn hoá (UUID, timestamp), merge theo khoá chính; dbt test `matches_bronze` bảo đảm cùng tập khoá với Bronze |
| `gold` | 7 dimension, 6 fact, 14 mart và 2 seed, dựng lại mỗi lần chạy |

`lakehouse_oltp_init` nạp toàn bộ nguồn tại một mốc chung và đối soát với nguồn trước khi Bronze đổi. `lakehouse_oltp_incremental` chạy 03:00 hằng ngày theo cursor `updated_at`/`created_at`. Ràng buộc khoá, trường bắt buộc và khoá ngoại đã được PostgreSQL bảo đảm nên pipeline không kiểm tra lại. Chạy incremental không đồng bộ dòng bị xoá ở nguồn; chạy init thì có.

## 4. Định danh và quyền

- Mỗi workload có danh tính riêng: Airflow (`batch-airflow`), Flink, Trino reader và Trino writer đều nhận credential principal Polaris qua `credential-exporter`. Không workload nào giữ key MinIO: Polaris cấp credential lưu trữ tạm thời, chỉ trong phạm vi bảng được đọc/ghi.
- Pipeline đọc PostgreSQL bằng role chỉ SELECT (`make source-reader`) qua connection Airflow `oltp_postgres`, phiên read-only.
- Trino xác thực `analytics_reader` và `dbt_writer` bằng mật khẩu qua HTTPS; SQL access rules giới hạn `dbt_writer` vào schema `silver` và `gold`.
- Quản trị MinIO/Polaris trên Streamlit chỉ dành cho admin bootstrap; thao tác ghi có CSRF, revision, journal và readback.
- API giao dịch dùng JWT cookie, CSRF và kiểm tra role/cửa hàng theo route. Superset có login riêng, chưa kế thừa quyền API.

## 5. Triển khai

| Profile | Service |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris, Airflow, Trino, platform-app, admin init, credential exporter |
| `streaming` | Kafka, Flink |
| `bi` | Superset |

`make up` chỉ bật console/API và các dependency quản trị; `make core` bật cả Airflow và Trino sau khi quyền workload đã sẵn sàng. Nạp dữ liệu, cấp quyền và chạy pipeline là thao tác chủ động.

## 6. Chưa hoàn tất

- Luồng access log (Flink, Bronze/Silver log, mart log) và loader cho log sinh tổng hợp.
- Đồng bộ dòng bị xoá ở nguồn khi chạy incremental.
- Dashboard Superset cho các mart còn lại và quyền theo cửa hàng.
- Runner cho nghiệp vụ ngoài mua online; HA, backup/restore và hardening khi triển khai public.
