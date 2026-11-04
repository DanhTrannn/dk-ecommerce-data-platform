# Kiến trúc Web và Data Lakehouse

Hệ thống xử lý giao dịch thương mại điện tử và xây dựng dữ liệu phân tích. Trọng tâm đồ án là đường dữ liệu từ nguồn đến Lakehouse và lớp SQL phục vụ báo cáo.

Bản vẽ tổng quan: [08-system-architecture.drawio](../diagram/08-system-architecture.drawio). Chi tiết bảng: [Lakehouse Design](../project/LAKEHOUSE_DESIGN_PLAN.md). Công thức: [Metrics Catalog](METRICS_CATALOG.md).

## 1. Thành phần và ranh giới

| Khối | Trách nhiệm |
|---|---|
| Next.js 15 / React 19 | Storefront, POS, Admin và BI Hub dùng ECharts; gọi FastAPI |
| FastAPI / SQLAlchemy | Auth, RBAC, giao dịch ngắn với MySQL, access log và API analytics |
| MySQL 8.4 | 28 bảng OLTP; nguồn batch và binlog CDC |
| Debezium / Kafka Connect | Snapshot ban đầu và thu row binlog của 27 bảng phân tích |
| Kafka 3.8 | Topic access log và các topic CDC theo bảng |
| Flink 1.19 | Job access log và job CDC độc lập; checkpoint định kỳ |
| Airflow 2.10 / Spark 3.5 | Điều phối/thực thi batch, biến đổi, đối soát và bảo trì |
| MinIO | Lưu file Landing, data/metadata Iceberg và state |
| Iceberg 1.10 | Định dạng bảng, snapshot, schema và commit |
| Polaris 1.6 / PostgreSQL | REST catalog, quyền truy cập và metadata quản trị |
| Trino 483 | Đọc Iceberg qua Polaris/MinIO để phục vụ SQL |
| LibreDB Studio | Giao diện SQL cho người vận hành |

Frontend không truy cập MySQL/Trino trực tiếp. FastAPI đọc Trino cho tất cả KPI; MySQL phục vụ giao dịch, auth/RBAC và đối soát nguồn. API không gọi Spark để xử lý từng request.

## 2. Ba đường dữ liệu

| Luồng | Đường đi | Điểm ra và giới hạn |
|---|---|---|
| Batch OLTP | MySQL → Spark → Landing Parquet/manifest → Bronze → Silver → Gold | DAG có cursor tăng dần và quality gate |
| CDC OLTP | MySQL binlog → Debezium → Kafka → Flink | Append-only `bronze.oltp_cdc_events` → Spark CDC → typed Silver / dependent Gold → publication manifest |
| Access log | FastAPI middleware → Kafka → Flink | Chỉ append Landing; Spark giờ xử lý Bronze → Silver → Gold fact → ba mart và publication |

`customer_credentials` bị loại khỏi batch và CDC. PII của các bảng phân tích cần được xử lý trước khi phục vụ ở Silver/Gold.

Batch xây dữ liệu phân tích có đối soát; CDC lưu lịch sử để truy vết/replay. Bản ghi xóa trong binlog không đồng nghĩa Silver batch đã tự áp dụng xóa đó.

JSONL lịch sử do `generator export-logs` tạo và upload lên `landing/logs/` là file thô. Job Flink hiện tại đọc Kafka; upload file không tự kích hoạt xử lý sang Gold.

## 3. Lưu trữ và chất lượng

Catalog `lakehouse` có sáu namespace: `landing`, `bronze`, `silver`, `gold`, `quarantine`, `system`.

- Batch Landing: `s3://lakehouse/landing/oltp/<table>/...`.
- Iceberg warehouse: `s3://lakehouse/warehouse/`; vị trí bảng do catalog/Iceberg quản lý.
- Cursor/checkpoint/state có prefix riêng trong bucket.
- Bronze giữ dữ liệu nguồn và lineage. Silver chuẩn hóa, khử trùng, MERGE và pseudonymize. Gold tạo dimension/fact/mart.
- Quarantine lưu bản ghi không hợp lệ; system lưu kết quả đối soát/chất lượng.

Gate trước Gold so sánh MySQL/Silver. Gate sau Gold kiểm tra parity, khóa, tham chiếu và lineage. Kết quả ghi vào `lakehouse.system.data_quality_results`; lỗi gate làm DAG thất bại. Đây là logic vận hành của pipeline.

Batch mode giữ cơ chế cursor commit sau Bronze và bảng Gold commit độc lập. Mặc định CDC mode, Spark áp dụng bản ghi theo transaction vào Silver, làm mới các bảng Gold phụ thuộc, rồi công bố offsets cùng snapshot IDs trong `system.analytics_publications`. API chỉ đọc phiên công bố hoàn chỉnh. Writer batch và CDC dùng chung `flock` trên cùng Docker host; không chạy hai chế độ cùng lúc. `payments` vẫn dùng cursor created_at trong batch mode, trong khi CDC theo dõi cập nhật cũ. Chi tiết tại [schema](OLTP_SCHEMA.md#3-bảng-và-cursor-trích-xuất) và [CDC](CDC_DASHBOARD_DATA_SPEC.md#6-cdc-materialization-and-dashboard-publication).

## 4. Lịch chạy và độ mới

| Thành phần | Cấu hình mã nguồn |
|---|---|
| `lakehouse_oltp_pipeline` | Hằng ngày 02:00, Asia/Ho_Chi_Minh |
| `lakehouse_oltp_maintenance` | Hằng ngày 03:00 |
| `lakehouse_streaming_maintenance` | Mỗi 2 giờ, chỉ maintenance |
| `lakehouse_logs_marts` | Mỗi giờ, độc lập maintenance |
| Flink access logs/CDC | Liên tục, checkpoint 10 giây; Spark CDC polling mặc định 30 giây |
| BI Hub | Các dashboard tự tải lại sau 15 giây khi trang hoạt động |

Checkpoint và chu kỳ tải lại UI không phải cam kết latency end-to-end. Gold OLTP thay đổi theo batch thực tế. Trang system tách tuổi phiên công bố, heartbeat CDC, pending events và sai lệch so với MySQL. Không có dữ liệu mới thì phiên công bố không đổi; poll 15 giây của trình duyệt không phải SLA lan truyền.

DAG được tạo ở trạng thái paused theo Compose. Streaming maintenance kiểm tra có job RUNNING và chọn một job phù hợp; chưa xác nhận riêng cả CDC và access-log job đều khỏe. Lịch maintenance và batch ở hai DAG khác nhau không tạo dependency chờ nhau.

## 5. Serving và phân quyền

BI Hub `/admin/analytics` gọi FastAPI; API xác thực vai trò và cửa hàng, đọc Trino/MySQL rồi trả JSON cho ECharts. Trino đọc Iceberg qua Polaris/MinIO; MySQL cung cấp trạng thái vận hành hiện tại. Frontend không gọi các engine xử lý dữ liệu trực tiếp.

### Vai trò và nguồn dữ liệu

| Vai trò hiển thị | Vai trò tài khoản | Nguồn chính / nội dung |
|---|---|---|
| Executive | `admin` | Published `mart_order_daily` (tài chính, số đơn, yêu cầu trả và COD đang giao) |
| Sales | `sales_manager` | Published `mart_order_daily`, `mart_product_sales_daily`, `mart_sales_daily` |
| Marketing | `marketing_manager` | Hai mart log phễu/nhu cầu sản phẩm qua publication log riêng; ba mart summary/coupon/review qua publication OLTP |
| Store | `store_manager` | Published `mart_store_overview`, `mart_order_daily`, `mart_inventory_current`; lọc cửa hàng được phân công |
| Inventory | `inventory_manager` | Published `mart_inventory_summary`, `mart_inventory_current`; lịch sử từ `mart_inventory_health` |
| Operations | `operations_manager` | Published `mart_operations_summary`, `mart_cod_region_daily`, `mart_logistics_performance`, `mart_product_returns` |
| System | `system_admin` | Published mart_order_daily đối soát MySQL; mart_hourly_route_metrics từ publication log độc lập |

Admin có quyền truy cập các dashboard; các vai trò khác bị giới hạn bởi `validate_role_access`. Store manager bị cố định theo cửa hàng được gán bằng `resolve_effective_store_id`. Đây là lọc ở ứng dụng/API, không phải row-level security tự động trong Trino.

Quyền xem dashboard không cấp quyền ghi nghiệp vụ. UI cho `operations_manager` mở orders/logistics/returns nhưng API tương ứng hiện yêu cầu `admin`. Coupon/review dành cho marketing/admin; sản phẩm/nhập kho dành cho inventory/admin. Các route confirm/cancel cửa hàng chưa gắn dependency CSRF. Hai route điều chỉnh/cấp tồn chi nhánh hiện chỉ flush, chưa commit trong router hoặc dependency; không xem response 204 là bằng chứng đã lưu thay đổi.

### API và trạng thái nguồn

| Endpoint | Mục đích |
|---|---|
| `GET /api/v1/admin/analytics/role-metrics` | Chỉ số theo vai trò và store hợp lệ |
| `GET /api/v1/admin/analytics/stores` | Danh sách cửa hàng theo quyền |
| `GET /api/v1/admin/analytics/sales-trend` | Biểu đồ theo ngày, quyền admin/sales_manager |
| `GET /api/v1/admin/analytics/http-metrics` | Báo cáo HTTP từ mart log pin snapshot, quyền admin/system_admin; publication/cache độc lập đối soát OLTP |
| `GET /api/v1/admin/analytics/overview` | Tổng quan OLAP đã công bố, quyền admin |

Response chỉ số có `data_source`, `source_run_id`, `data_updated_at`, `is_degraded`, và metadata cache. Trino lỗi dùng phản hồi OLAP thành công gần nhất trong giới hạn tuổi cache, sau đó trả 503; API không chuyển KPI sang MySQL. Mỗi nguồn đọc bộ snapshot đã công bố của nguồn đó; Marketing có hai bộ snapshot độc lập cho log và nghiệp vụ. [Metrics Catalog](METRICS_CATALOG.md) định nghĩa công thức, grain và giới hạn diễn giải; dữ liệu generator vẫn là dữ liệu tổng hợp sau khi xử lý qua pipeline.

Store overview và danh sách chọn cửa hàng cũng đọc `mart_store_overview`; API không tổng hợp Fact/Silver cho các luồng Store/Inventory. Tồn hiện tại không cộng qua ngày; thời điểm quan sát tồn được trả riêng với thời điểm publication. Operations chỉ đọc mart, thời gian chờ giữ theo `operations_observed_at`, không tính lại bằng đồng hồ API. Không có deadline giao thì tỷ lệ đúng hạn null; hoàn tiền đề nghị ở SKU không đồng nghĩa refund thành công.

Executive, sales, marketing, store, inventory, operations, system, admin overview, store overview, và sales trend đều đọc Trino. Các trang phân tích tự tải lại 15 giây khi đang hiển thị. Poll giao diện không đồng nghĩa dữ liệu nguồn mới đã được xử lý.

BI Hub tổ chức từng góc nhìn theo tổng quan → xu hướng (khi có API phù hợp) → phân nhóm/bảng chi tiết → bước kiểm tra. Bộ chọn 7/30/90 ngày chỉ tác động biểu đồ và bảng xu hướng, không đổi KPI toàn nguồn. Bộ chọn cửa hàng áp dụng góc nhìn store theo quyền của API. Tìm kiếm, sắp xếp và xuất CSV giới hạn trong từng bảng.

Giao diện hiển thị thời điểm công bố snapshot (UTC+7), nguồn OLAP và cảnh báo cache, và cung cấp từ điển chỉ số theo nguồn phản hồi. Không dựng target/delta khi API chưa cung cấp. Các bước marketing được trình bày như lượng request thành công, các nhóm giao vận có thể chồng lặp không dùng làm cơ cấu đơn. Khi System dùng cache OLAP, dữ liệu không biểu thị một lần đối soát mới. Các hành động là hướng dẫn hoặc liên kết tới màn hình nghiệp vụ có quyền truy cập.

Đối soát system vẽ từng metric thành biểu đồ riêng, so sánh MySQL/Gold: số đơn có đơn vị đơn, các trường tiền có đơn vị VND và trục độc lập. Tooltip tiền giữ giá trị đầy đủ, badge dùng variance_percent từ API. Chỉ hiển thị cặp so sánh khi nguồn là lakehouse và có dữ liệu; cache OLAP được đánh dấu degraded, không dùng để kết luận đối soát mới.

Nguồn serving: [router](../../services/ecommerce-api/app/modules/analytics/router.py), [service](../../services/ecommerce-api/app/modules/analytics/service.py), [schemas](../../services/ecommerce-api/app/modules/analytics/schemas.py), [BI Hub](../../apps/storefront/src/app/admin/analytics/page.tsx).

### Quyền truy cập storage và catalog

MinIO có bảy identity theo workload, được đồng bộ tại chỗ theo cấu hình repo mỗi lần init; tài khoản đúng cấu hình được giữ nguyên, quyền dư trên tài khoản quản lý bị thu hồi và tài khoản ngoài danh sách được bảo toàn. Polaris có `spark_writer`, `flink_writer`, `trino_reader`, `trino_admin`; credential tách theo consumer. Code batch chỉ đọc OLTP, nhưng Compose mặc định dùng `MYSQL_USER` của ứng dụng; chưa tách MySQL user chỉ có quyền SELECT. Polaris chỉ bootstrap từ YAML ở lần đầu; sau đó quyền được quản lý trên Console/API, init không áp lại YAML hoặc tự reset credential. Cài đặt cũ được tiếp nhận mà không sửa quyền. Chi tiết state và khôi phục nằm trong [SETUP](../runbook/SETUP.md).

Writer Polaris hiện có quyền quản lý nội dung ở mức catalog, chưa giới hạn riêng từng bảng. MySQL CDC user riêng có các quyền replication/SELECT được grant trên `*.*`; danh sách capture 27 bảng không phải giới hạn quyền SQL tương đương. Bronze CDC giữ raw row images nên vẫn cần kiểm soát PII.

## 6. Triển khai cục bộ

Profile chính: `core` (Web/API/MySQL), `batch` (Airflow và nền tảng phân tích), `streaming` (Kafka/Debezium/Flink và Spark CDC hiện tại), `tools` (Polaris Console/LibreDB/Trino Admin). `lakehouse-tools` cung cấp Spark SQL client dùng khi cần. Spark chạy local trong container thực thi, không có master/worker. `.env.example` và launcher mặc định chỉ bật `core`.

Stack local dùng HTTP. TLS, Secure cookie và secret manager production chưa được đóng gói đầy đủ trong Compose hiện tại.

Nguồn: [docker-compose.yml](../../docker-compose.yml), [DAGs](../../airflow/dags), [jobs](../../pipelines/src/jobs), [Polaris RBAC](../../infrastructure/polaris/rbac.yml).

Marketing giữ trạng thái, run ID, thời điểm công bố/quan sát và tuổi cache riêng cho log và nghiệp vụ; không gán một run ID chung cho hai publication. Một nguồn lỗi không xóa phần còn đọc được. Pipeline log xử lý tuần tự Landing → Bronze → Silver → fact, sau đó rollup đọc fact pin snapshot và danh mục Silver pin theo publication OLTP để ánh xạ slug/UUID/variant; API chỉ đọc mart. Job thay toàn bộ ba mart, kiểm tra đầu ra rồi công bố snapshot cùng nhau, kể cả khi nguồn rỗng. Flink chỉ ghi Landing; Spark là writer của Bronze/Silver/fact log. Raw không hợp lệ giữ ở Landing/Bronze; realtime API/dashboard còn chờ.

Admin Overview đọc mart_admin_overview và mart_order_daily cùng bộ snapshot, không tổng hợp Fact/Silver/Dimension ở API. System dùng order mart cho phía Lakehouse; MySQL chỉ cung cấp count và tổng giá trị đơn tại source_observed_at trong cùng câu SQL. Kết quả matched/mismatch/unavailable/cached không được diễn giải như sức khỏe mọi pipeline. Tuổi publication là tuổi metadata, không phải SLA. Khi cache, thời điểm đo nguồn giữ nguyên và giao diện không trình bày như đối soát mới.
