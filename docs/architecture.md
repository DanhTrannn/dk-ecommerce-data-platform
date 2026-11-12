# Kiến trúc hệ thống

Cập nhật theo codebase ngày **11/11/2026**. Các kết quả nghiệm thu được tách khỏi trạng thái dịch vụ đang chạy trong [operations.md](operations.md). Cấu trúc dữ liệu và chỉ số nằm trong [data.md](data.md); lệnh cài đặt nằm trong [setup.md](setup.md).

## 1. Phạm vi và hiện trạng

Đồ án mô phỏng **retail thời trang tại Việt Nam**, dùng VND và ngày nghiệp vụ Asia/Ho_Chi_Minh. Trọng tâm là sinh dữ liệu có căn cứ, giao dịch nguồn nhất quán, ba đường thu thập dữ liệu và phân tích trên Data Lakehouse.

| Phần | Đã triển khai | Giới hạn hiện tại |
|---|---|---|
| Nguồn nghiệp vụ | FastAPI + PostgreSQL; catalog, tài khoản, giỏ, checkout, POS, tồn kho, giao vận, đổi trả, review | Có API không đồng nghĩa Streamlit đã tự động hóa mọi nghiệp vụ |
| Generator | CLI 0.10, SQL PostgreSQL, cấu hình địa điểm, sampler trọng số sản phẩm H&M | Chỉ R03 đã áp dụng từ EDA; các tỷ lệ hành vi còn lại là giả định/kịch bản |
| Streamlit | Sinh job, xem/tải kết quả, nạp SQL chủ động, runner mua online, quản trị binding quyền | Runner hiện mỗi lượt một khách mới/một SKU; chưa có fulfillment/POS/đổi trả tự động |
| Batch | PostgreSQL JDBC → staging → Bronze → Silver → Gold/mart → quality gate → publication | Cần chạy init trước; batch interval không tự backfill mọi lịch sử |
| CDC | PostgreSQL WAL → Debezium → Kafka → Flink → Bronze audit | Không tự áp CDC vào OLTP Silver/Gold hoặc cập nhật dashboard nghiệp vụ |
| Access log | API → Kafka → Flink Landing; Spark giờ xử lý Bronze/Silver/fact/ba mart và publication riêng | Producer opt-in; DLQ chưa tự replay; Landing và mart có độ mới khác nhau |
| BI | Superset 6.1.0 qua Trino, PoC doanh thu với hai mart/bốn KPI/bốn chart | Admin-only, chưa bao phủ 18 mart/RBAC cửa hàng; sync tạo URL mới theo publication |
| Frontend cũ | Next.js/React/ECharts còn trong `services/storefront`, vẫn có CI | Không có service Compose; các trang gọi API BI đã bỏ không còn hoạt động |

Không có marketplace đa người bán, thu thập dữ liệu đối thủ hoặc ML đang được nghiệm thu trong phạm vi này. Lịch sử thiết kế trong `FINAL_REPORT.md` không thay thế mô tả hiện trạng ở bốn tài liệu kỹ thuật.

## 2. Component và trách nhiệm

| Component | Vai trò | Mã nguồn/cấu hình chính |
|---|---|---|
| `platform-app` | Một container, hai process FastAPI/Streamlit; khi một process dừng thì container đóng process còn lại | [Dockerfile](../services/platform-app/Dockerfile), [package](../services/platform-app/src/platform_app/) |
| Generator | Thư viện/CLI độc lập, không cần kết nối database để xuất SQL | [generator](../generator/src/generator/), [presets](../generator/configs/) |
| PostgreSQL 16.8 | OLTP `ecommerce`; metadata `airflow`, `polaris`; metadata `superset` được tạo riêng khi bật BI | [schema](../database/schema.sql), [scripts](../infrastructure/postgres/) |
| MinIO | Object storage cho staging và dữ liệu/metadata Iceberg | [provisioning/policy](../infrastructure/minio/) |
| Polaris 1.8.0 | Iceberg REST catalog, namespace và quyền principal/catalog | [RBAC](../infrastructure/polaris/rbac.yml) |
| Spark 3.5.9 | Extract/transform/quality/maintenance, chạy `local[2]` trong process job | [lakehouse](../pipelines/src/lakehouse/), [jobs](../pipelines/src/jobs/) |
| Airflow 3.3.2 | API/UI, scheduler và DAG processor riêng; LocalExecutor | [DAGs](../airflow/dags/), [image](../infrastructure/airflow/) |
| Debezium 3.0.8 / Kafka 3.8 | Snapshot/pgoutput từ WAL và vận chuyển event; Kafka cũng nhận log API | [connector](../infrastructure/debezium/register-connector.sh) |
| Flink 1.19 | Hai job độc lập: CDC audit và raw access-log capture | [runtime](../infrastructure/flink/), [entrypoints](../pipelines/src/jobs/) |
| Trino 483 | Truy vấn Iceberg qua Polaris; catalog `READ_ONLY` | [catalog](../infrastructure/trino/catalog/lakehouse.properties) |
| Superset 6.1.0 | Dashboard từ publication, metadata riêng trên PostgreSQL | [implementation](../infrastructure/superset/) |
| `lakehouse-access` | API nội bộ để đọc/gán/gỡ binding MinIO/Polaris | [access_admin](../services/platform-app/src/platform_app/access_admin/) |

Python workspace gồm `data-generator`, `platform-app`, `batch-pipeline`, dùng chung [uv.lock](../uv.lock). Spark, Flink, Airflow và Superset có image/runtime riêng; không cài Superset vào virtualenv ứng dụng. Superset bổ sung driver Trino/PostgreSQL bằng lock riêng và không cần Redis/Celery trong PoC.

## 3. Ba đường dữ liệu

### Batch OLTP

1. Generator SQL được nạp chủ động hoặc API tạo/thay đổi dữ liệu PostgreSQL.
2. Spark reader SELECT-only đọc 27 bảng, phân trang theo khóa, mỗi trang tối đa 10.000 dòng.
3. Init đọc toàn bộ; DAG định kỳ dùng cửa sổ UTC `[start, end)` do Airflow truyền. Parquet staging được thay mới cho từng bảng; đây không phải bản lưu trữ lịch sử.
4. Bronze giữ các phiên bản nguồn chống trùng khi replay; Silver MERGE theo PK/cursor, typing và pseudonymization.
5. Gold xây dimension/fact/mart, quality gate kiểm tra rồi công bố một manifest snapshot.
6. Trino/API đọc snapshot đã công bố. Superset sync tạo bộ assets cố định theo manifest đó.

Batch là writer nghiệp vụ cho Silver/Gold. Không phối hợp CDC để hai engine đồng thời materialize cùng state. `init_lakehouse_oltp` và `lakehouse_oltp_pipeline` là hai DAG riêng; phải tự điều phối để chúng không chạy chồng nhau.

### CDC audit

PostgreSQL WAL → Debezium PostgreSQL connector (`pgoutput`) → topic `<prefix>.public.<table>`/transaction → Flink → `lakehouse.bronze.oltp_cdc_events`.

`make source-cdc` quản lý publication 27 bảng, SELECT/REPLICATION riêng và replica identity FULL; connector không tự tạo publication. Event giữ before/after, source LSN/transaction, raw key/value và tọa độ Kafka. `customer_credentials` bị loại. Tombstone và transaction control vẫn được lưu; chi tiết contract ở [data.md](data.md#4-cdc-audit).

CDC hỗ trợ audit và truy vết; dashboard OLTP vẫn thay đổi sau batch/publication mới. DAG hard delete riêng chỉ đồng bộ hai bảng được cho phép và không purge dữ liệu PostgreSQL.

### Access log

Middleware ghi request thực → producer nền có broker acknowledgement → Kafka → Flink append `landing.access_logs` → Spark Bronze → Silver → `fact_web_events` → ba mart → `logs_analytics_publications`.

HTTP realtime đọc một snapshot Landing, còn HTTP metrics đọc publication log. Log lỗi contract không bị bỏ khỏi raw layers. Log request `observed` không chứng minh người gọi là khách thật: runner cũng tạo request được quan sát. Xuất/import SQL không tự tạo HTTP event.

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
- Batch dùng PostgreSQL reader riêng; ứng dụng/importer dùng owner nghiệp vụ; CDC có role replication riêng. Không dùng Superset metadata owner làm account nguồn.
- Provisioning MinIO tạo bốn account Polaris/Spark/Flink/Trino với policy riêng. Workload không dùng root MinIO. Polaris tạo `spark_writer`, `flink_writer`, `trino_reader`, bốn volume credential consumer và state root riêng.
- Provisioning khởi tạo tài nguyên còn thiếu; chạy lại giữ secret/quyền đã có, không phải bộ đồng bộ liên tục hoặc tự rotate.
- Streamlit quản trị các binding có sẵn qua `lakehouse-access`. Backend kiểm tra nguồn auth, CSRF, revision, journal và readback. Không tạo account, sửa policy JSON hoặc xoay secret trong UI.
- Superset có login/role riêng, chưa kế thừa RBAC API hoặc phạm vi cửa hàng. Username Trino là nhãn client; quyền warehouse thực đến từ catalog reader/Polaris/MinIO hiện có.

## 6. Triển khai và điểm vào

| Profile | Service |
|---|---|
| `core` | PostgreSQL, MinIO, Polaris/bootstrap, Airflow/init, Trino, platform-app, lakehouse-access; provisioning chạy thủ công scale 0 |
| `batch` | Spark client local, dùng kèm core |
| `streaming` | Kafka/topic init, Debezium Connect, Flink JM/TM; đăng ký connector và submit job scale 0 |
| `bi` | Superset web; init/sync scale 0, dùng kèm core |

Service và port cụ thể lấy từ [Compose](../docker-compose.yml). Không có Spark cluster riêng, Polaris Console, Next.js service hoặc profile `tools`. Build, healthcheck, schema bootstrap, nạp dataset, provisioning và chạy pipeline là các bước khác nhau.

| Thư mục | Trách nhiệm |
|---|---|
| `services/platform-app/src/platform_app/api/` | Route/service giao dịch và HTTP observability |
| `services/platform-app/src/platform_app/ui/` | Console, worker/journal, import và scenario API |
| `services/platform-app/src/platform_app/access_admin/` | Quản trị binding riêng |
| `generator/src/generator/` | Config/validation, catalog, giao dịch, SQL export và manifest |
| `pipelines/src/lakehouse/` | OLTP/log/streaming transforms, SQL templates, quality và publication |
| `pipelines/src/jobs/` | Entry point thực thi trên engine |
| `airflow/dags/` | Điều phối init/batch/log/hard-delete/maintenance |
| `infrastructure/` | Docker/runtime/config, provisioning và Superset |
| `database/` | PostgreSQL schema renderer và seed |
| `services/storefront/` | Frontend cũ, chưa dọn khỏi repo |

### Pilot thay engine batch

Có runtime Python tùy chọn tại `infrastructure/batch/Dockerfile`: Polars 2.1.0 xử lý DataFrame, PyIceberg 0.12.0 commit Iceberg qua Polaris, PyArrow 18.1.0 chuyển đổi dữ liệu và psycopg2 đọc nguồn. CLI pilot xử lý `categories` trong namespace `polars_pilot_*`. CLI batch mới xử lý đủ 27 nguồn trong các namespace `polars_batch_*_bronze`, `*_silver`, `*_quarantine`, có validation/PII/quarantine; cả hai không ghi namespace serving. CLI Gold native bổ sung 27 bảng Gold, quality gate và publication trong namespace migration riêng. Airflow/Spark và hai job Flink hiện tại giữ nguyên. Chưa tạo một service luôn chạy mới trong Compose. Cách chạy và giới hạn ở [pipelines README](../pipelines/README.md#polars--pyiceberg-migration-pilot).

## 7. Phần chưa hoàn tất

Native Bronze/Silver đã được nghiệm thu trên 27 nguồn trong fixture riêng; Gold/quality/publication có runtime migration riêng mô tả bên dưới; log ingest tới fact, hard-delete/maintenance và Airflow vẫn cần chuyển trước khi bỏ Spark. Superset còn cần mở rộng tới các nhóm mart khác, quyền viewer/store, chọn publication mới và vòng đời asset/snapshot. Chưa chuyển hết BI hoặc tự sync từ Airflow. Dọn Next.js sau khi có đủ chức năng thay thế. Generator chưa hiệu chỉnh toàn bộ hành vi theo dữ liệu Việt Nam; runner chưa bao phủ mọi API nghiệp vụ. Benchmark production, HA/disaster recovery, tự replay DLQ và hardening triển khai public chưa được nghiệm thu.


### Gold native và ranh giới publication

`jobs.oltp.run_polars_gold` dùng cùng advisory lock theo prefix với batch nguồn. Job ghim 27 snapshot Silver, xây 27 bảng Gold bằng Polars, commit qua PyIceberg, rồi đọc lại đúng snapshot vừa ghi để chạy 55 kiểm tra. Kết quả audit được lưu cả khi gate thất bại; chỉ gate đạt mới cập nhật publication. Các commit bảng độc lập; manifest là ranh giới đọc một tập snapshot đã kiểm chứng, không phải transaction Iceberg đa bảng.

Manifest schema v3 ghi đúng 54 tên bảng vật lý `lakehouse.<prefix>_silver.*` và `lakehouse.<prefix>_gold.*`. Nó không giả tên serving `lakehouse.gold.*`. Superset và reader production hiện tại chưa chuyển sang hợp đồng namespace này. `--verify-only` chỉ audit, không publish. Airflow, access-log ingest tới fact, hard delete và maintenance vẫn dùng Spark; chưa cutover hoặc gỡ Spark.


### Mart access log native

`jobs.logs.run_polars_marts` đọc một snapshot cố định của `lakehouse.gold.fact_web_events`, lọc theo `--as-of`, rồi xây ba mart route, nhu cầu sản phẩm và funnel bằng Polars. Catalog sản phẩm/biến thể được đọc từ đúng snapshot của một publication OLTP batch schema v3; có thể chọn catalog native hoặc serving và chỉ định `run_id`. Thay đổi Silver chưa được công bố không tự đi vào lần tính này.

Job dùng advisory lock chung theo prefix, ghi output trong `<prefix>_gold`, đọc lại snapshot của chính các commit đó, audit 15 kiểm tra và cập nhật `<prefix>_system.logs_analytics_publications` khi đạt. Publication log độc lập với publication OLTP, ghim ba snapshot output và bốn input: fact, bảng publication catalog, products và variants. Không ghim lại snapshot mới sau audit. `--verify-only` chỉ audit.

Bước này chỉ chuyển rollup/publication. Spark vẫn xử lý Landing → Bronze → Silver → fact, gồm parser, dedup và gate xung đột payload. Native không tự thu Kafka hoặc thay DAG, HTTP Metrics, Superset hay reader serving. Chưa chuyển hard delete/maintenance.
