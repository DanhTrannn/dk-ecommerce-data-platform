# Kiến trúc hệ thống

Cập nhật theo codebase ngày **15/11/2026**. Các kết quả nghiệm thu được tách khỏi trạng thái dịch vụ đang chạy trong [operations.md](operations.md). Cấu trúc dữ liệu và chỉ số nằm trong [data.md](data.md); lệnh cài đặt nằm trong [setup.md](setup.md).

## 1. Phạm vi và hiện trạng

Đồ án mô phỏng **retail thời trang tại Việt Nam**, dùng VND và ngày nghiệp vụ Asia/Ho_Chi_Minh. Trọng tâm là sinh dữ liệu có căn cứ, giao dịch nguồn nhất quán, hai luồng xử lý Lakehouse: batch OLTP và log API qua Kafka/Flink và phân tích trên Data Lakehouse.

| Phần | Đã triển khai | Giới hạn hiện tại |
|---|---|---|
| Nguồn nghiệp vụ | FastAPI + PostgreSQL; catalog, tài khoản, giỏ, checkout, POS, tồn kho, giao vận, đổi trả, review | Có API không đồng nghĩa Streamlit đã tự động hóa mọi nghiệp vụ |
| Generator | CLI `datagen generate/verify/logs/reset`, notebook EDA và preset OLTP `1m`/`10m`/`50m`, log L1–L4 | Log tổng hợp mới ra Parquet; chưa có loader vào Lakehouse hoặc mapping typed cho session/order của contract 1.1.0 |
| Streamlit | Platform Console: runner mua online qua API, CRUD MinIO/Polaris, binding và cấp credential workload | Runner hiện mỗi lượt một khách mới/một SKU; chưa có fulfillment/POS/đổi trả tự động |
| Batch | PostgreSQL (psycopg2, theo trang) → Bronze → Silver → Gold/mart → quality gate → publication | Cần chạy init trước; batch interval không tự backfill mọi lịch sử |
| Access log | API → Kafka → Flink Landing; batch native xử lý Bronze/Silver/fact, dbt dựng ba mart, publication riêng | Producer opt-in; DLQ chưa tự replay; Landing và mart có độ mới khác nhau |
| BI | Superset 6.1.0 qua Trino, PoC doanh thu với hai mart/bốn KPI/bốn chart | Admin-only, chưa bao phủ 18 mart/RBAC cửa hàng; sync tạo URL mới theo publication |

Không có Spark, Debezium hoặc PostgreSQL CDC trong runtime hiện tại; Flink chỉ dùng cho log API. Không có marketplace đa người bán, thu thập dữ liệu đối thủ hoặc ML đang được nghiệm thu trong phạm vi này. Lịch sử thiết kế trong `FINAL_REPORT.md` không thay thế mô tả hiện trạng ở bốn tài liệu kỹ thuật.

## 2. Component và trách nhiệm

| Component | Vai trò | Mã nguồn/cấu hình chính |
|---|---|---|
| `platform-app` | Một container, hai process FastAPI/Streamlit; khi một process dừng thì container đóng process còn lại | [Dockerfile](../services/platform-app/Dockerfile), [package](../services/platform-app/src/platform_app/) |
| Generator | Quy luật, preset và lệnh `datagen` sinh lịch sử OLTP và log truy cập tổng hợp | [generator](../generator/), [OLTP presets](../generator/oltp/configs/), [log rules](../generator/logs/configs/) |
| PostgreSQL 16.8 | OLTP `ecommerce`; metadata `airflow`, `polaris`; metadata `superset` được tạo riêng khi bật BI | [schema](../database/schema.sql), [scripts](../infrastructure/postgres/) |
| MinIO | Object storage cho staging và dữ liệu/metadata Iceberg | [policy mẫu](../infrastructure/minio/policies/) |
| Polaris 1.8.0 | Iceberg REST catalog, namespace và quyền principal/catalog | [quản trị Polaris](../services/platform-app/src/platform_app/access_admin/polaris.py) |
| Polars 2.1 / PyIceberg 0.12 | Đọc nguồn, Bronze/Silver, quality audit, publication và maintenance trong process job | [lakehouse](../pipelines/src/lakehouse/), [jobs](../pipelines/src/jobs/), [image](../infrastructure/airflow/Dockerfile) |
| dbt 1.11 (dbt-trino) | Dựng Gold/mart trên Trino; dbt tests là một phần của quality gate | [dbt project](../pipelines/dbt/) |
| Airflow 3.3.2 | API/UI, scheduler và DAG processor riêng; LocalExecutor | [DAGs](../airflow/dags/), [image](../infrastructure/airflow/) |
| Kafka 3.8 | Vận chuyển log API từ producer tới Flink | [Compose](../docker-compose.yml) |
| Flink 1.19 | Một job raw access-log capture vào Landing | [runtime](../infrastructure/flink/), [entrypoints](../pipelines/src/jobs/) |
| Trino 483 | Iceberg qua Polaris: `lakehouse` đọc, `lakehouse_etl` ghi Gold cho dbt; HTTPS/password và SQL ACL | [catalog](../infrastructure/trino/catalog/lakehouse.properties) |
| Superset 6.1.0 | Dashboard từ publication, metadata riêng trên PostgreSQL | [implementation](../infrastructure/superset/) |
| API quản trị trong `platform-app` | Router `/api/v1/access` chung cổng 8000; CRUD và phân quyền MinIO/Polaris | [access_admin](../services/platform-app/src/platform_app/access_admin/) |
| `credential-exporter` | API nội bộ nhận yêu cầu đã ký, ghi credential vào bốn volume consumer; chạy root riêng | [credential_export](../services/platform-app/src/platform_app/credential_export/) |
| `platform-admin-init` | One-off khởi tạo schema nguồn trống, admin bootstrap và storage secret/audit | [admin bootstrap](../services/platform-app/src/platform_app/provisioning/admin.py) |

Python workspace gồm `data-generator`, `platform-app`, `batch-pipeline`, dùng chung [uv.lock](../uv.lock). Airflow (kèm venv batch Polars/PyIceberg/dbt), Flink và Superset có image/runtime riêng; không cài Superset vào virtualenv ứng dụng. Superset bổ sung driver Trino/PostgreSQL bằng lock riêng và không cần Redis/Celery trong PoC.

## 3. Hai đường dữ liệu

### Batch OLTP

1. API (hoặc seed catalog) tạo/thay đổi dữ liệu PostgreSQL; `datagen generate` nạp một lần dữ liệu lịch sử tổng hợp.
2. Reader PostgreSQL SELECT-only (psycopg2) đọc 27 bảng, phân trang theo khóa, mỗi trang tối đa 10.000 dòng; Polars xử lý, PyIceberg commit.
3. Init đọc toàn bộ; DAG định kỳ dùng cửa sổ UTC `[start, end)` do Airflow truyền.
4. Bronze giữ các phiên bản nguồn chống trùng khi replay; Silver upsert theo PK/cursor, typing và pseudonymization.
5. dbt trên Trino xây dimension/fact/mart từ Silver đã ghim snapshot; quality gate kiểm tra rồi công bố một manifest snapshot.
6. Trino/API đọc snapshot đã công bố. Superset sync tạo bộ assets cố định theo manifest đó.

Batch là writer nghiệp vụ duy nhất cho Silver/Gold. Hard delete ở nguồn (giỏ/wishlist) không được đồng bộ: Silver giữ các dòng đã bị xóa ở PostgreSQL. `init_lakehouse_oltp` và `lakehouse_oltp_pipeline` là hai DAG riêng. Job ingest/Gold/log/maintenance dùng chung advisory lock theo namespace, từ chối writer đồng thời. Lock chỉ bao một job, không bao cả chuỗi ingest → publish của DAG; vẫn cần điều phối tránh hai DAG xen kẽ.

### Access log

Middleware ghi request thực → producer nền có broker acknowledgement → Kafka → Flink append `landing.access_logs` → Bronze → Silver → `fact_web_events` (Polars) → ba mart (dbt) → `logs_analytics_publications`.

HTTP realtime đọc một snapshot Landing, còn HTTP metrics đọc publication log. Log lỗi contract không bị bỏ khỏi raw layers. Log request `observed` không chứng minh người gọi là khách thật: runner cũng tạo request được quan sát. Seed dữ liệu không tự tạo HTTP event. Generator log lịch sử ghi Parquet bằng contract 1.1.0; chưa có loader vào Landing và parser typed chưa giữ `session.id`/`ecommerce.order_number`. Không coi đầu ra generator là dữ liệu đã được công bố trên Lakehouse.

## 4. Publication và serving

| Nguồn | Manifest | Cách đọc |
|---|---|---|
| OLTP | `lakehouse.system.analytics_publications`, schema 3, mode `batch` | Chỉ publish sau Gold gate; pin Silver/Gold theo `snapshots_json` |
| Log | `lakehouse.system.logs_analytics_publications`, schema 1 | Độc lập OLTP; pin ba log mart, kèm cutoff `observed_at` và snapshot đầu vào |
| HTTP realtime | Snapshot Landing được chọn khi đọc | Tạm thời, chưa phải publication mart; có dữ liệu chất lượng/loại trừ |

[PublishedReader](../services/platform-app/src/platform_app/api/modules/analytics/reader.py) thêm `FOR VERSION AS OF` cho bảng được công bố. API HTTP có last-good cache giới hạn tuổi, đánh dấu degraded; không tổng hợp OLTP thay cho OLAP. Các API business BI cũ đã bị gỡ, không mô tả chúng như endpoint đang phục vụ.

Superset dùng `mart_order_daily` và `mart_product_sales_daily` của cùng manifest, không JOIN hai grain để cộng đơn. Sync kiểm tra snapshot trước khi ghi metadata; cùng manifest thì reuse, manifest mới tạo dashboard/dataset/chart mới. URL cũ giữ nguyên dữ liệu; refresh trang không tự đổi phiên. Các snapshot phải còn trong retention của Iceberg để tiếp tục đọc được.

## 5. Định danh và quyền

- API giao dịch dùng JWT cookie, CSRF cho ghi và role/store checks theo từng route. Endpoint internal có secret riêng. Không khẳng định đã có HTTPS/rate limit hoặc triển khai public an toàn chỉ từ cấu hình local.
- Batch dùng PostgreSQL reader riêng; ứng dụng/seed dùng owner nghiệp vụ. Không dùng Superset metadata owner làm account nguồn.
- Không tự tạo account/role/policy mẫu. Admin bootstrap tạo workload user/policy MinIO và principal/role/catalog Polaris trên Streamlit; MinIO key/secret phải khớp cấu hình consumer. Workload dùng tài khoản riêng thay vì root MinIO.
- Streamlit gọi `/api/v1/access` trên FastAPI của `platform-app`. Backend kiểm tra tài khoản còn active, role `admin` và danh tính đúng admin bootstrap ở mỗi request quản trị; thao tác ghi cần CSRF, revision, journal và readback. Admin khác hoặc `system_admin` không đủ quyền. UI hỗ trợ CRUD và policy theo form giới hạn; không xoay secret hoặc chỉnh group MinIO.
- `platform-app` chạy non-root nhưng giữ credential admin MinIO/Polaris và mount `access-admin-secrets` chứa credential principal cùng khóa ký exporter. FastAPI và Streamlit dùng chung container/môi trường; đây là sự đánh đổi khi gộp backend. Exporter chạy root ở container riêng, đọc nguồn secret ở chế độ read-only và là service duy nhất ghi cả bốn volume credential consumer.
- Tạo principal chỉ lưu credential nguồn. Admin dùng tab **Cấp cho workload** để cấp/thay/gỡ file qua exporter; trạng thái phân công nằm trong `credential-export-state`. Consumer giữ credential trong bộ nhớ cần restart hoặc submit lại job sau thay đổi. Bootstrap giữ nguyên password/dữ liệu đã có, không tự migrate hoặc reset tài nguyên.
- Superset có login/role riêng, chưa kế thừa RBAC API hoặc phạm vi cửa hàng. Trino xác thực `analytics_reader`/`dbt_writer` bằng password qua HTTPS và giới hạn catalog/table bằng SQL ACL; quyền Polaris/MinIO của hai catalog bổ sung các lớp kiểm soát bên dưới.

## 6. Triển khai và điểm vào

| Profile | Service |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris/bootstrap, Airflow/init, Trino/auth init, platform-app, admin init và credential-exporter |
| `streaming` | Kafka/topic init, Flink JM/TM; submit job scale 0 |
| `bi` | Superset web; init/sync scale 0, dùng kèm core |

Service và port cụ thể lấy từ [Compose](../docker-compose.yml). Không có Polaris Console hoặc profile `tools`. `make up` khởi động console/API cùng dependency quản trị; `make up-all` khởi động toàn nhóm sau khi quyền và credential workload đã sẵn sàng. Schema/admin bootstrap tự chạy theo dependency; nạp dataset, tạo quyền workload và chạy pipeline vẫn là thao tác chủ động.

| Thư mục | Trách nhiệm |
|---|---|
| `services/platform-app/src/platform_app/api/` | Route/service giao dịch và HTTP observability |
| `services/platform-app/src/platform_app/ui/` | Platform Console: scenario API và quản trị quyền |
| `services/platform-app/src/platform_app/access_admin/` | Router quản trị quyền dùng chung FastAPI |
| `generator/` | CLI sinh/verify/reset OLTP, xuất log tổng hợp Parquet, notebook EDA và preset YAML |
| `pipelines/src/lakehouse/` | OLTP/log/streaming transforms, SQL templates, quality và publication |
| `pipelines/src/jobs/` | Entry point thực thi trên engine |
| `airflow/dags/` | Điều phối init/batch/log/maintenance |
| `infrastructure/` | Docker/runtime/config, provisioning và Superset |
| `database/` | PostgreSQL schema renderer và seed |

### Engine batch

Image Airflow (`infrastructure/airflow/Dockerfile`) chứa venv riêng `/opt/pipeline-venv` với Polars 2.1.0, PyIceberg 0.12.0, PyArrow và dbt-trino theo `uv.lock`; DAG gọi venv này qua `PIPELINE_PYTHON`. Venv tách khỏi Python của Airflow vì hai bên cần phiên bản khác nhau cho pyarrow, boto3, pydantic, sqlparse. Chi tiết job, kiểm tra và giới hạn ở [pipelines README](../pipelines/README.md).

## 7. Phần chưa hoàn tất

Loader log tổng hợp và mapping session/order chưa có. Cần nghiệm thu lại generator → pipeline → publication → dashboard với bộ dữ liệu hiện tại. Hard delete ở nguồn chưa được đồng bộ sang Silver. Superset còn cần mở rộng tới các nhóm mart khác, quyền viewer/store, chọn publication mới và vòng đời asset/snapshot. Chưa chuyển hết BI hoặc tự sync từ Airflow. Generator chưa hiệu chỉnh toàn bộ hành vi theo dữ liệu Việt Nam; runner chưa bao phủ mọi API nghiệp vụ. Benchmark production, HA/disaster recovery, tự replay DLQ và hardening triển khai public chưa được nghiệm thu.
