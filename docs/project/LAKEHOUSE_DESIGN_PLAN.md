# Mô hình Data Lakehouse hiện tại

Tài liệu mô tả bảng và luồng có trong mã nguồn. Kiến trúc tổng thể ở [System Architecture](../architecture/SYSTEM_ARCHITECTURE_VISUAL.md), công thức ở [Metrics Catalog](../architecture/METRICS_CATALOG.md).

## 1. Catalog và storage

Polaris quản lý catalog `lakehouse` với sáu namespace: `landing`, `bronze`, `silver`, `gold`, `quarantine`, `system`. MinIO lưu file; Iceberg quản lý bảng/snapshot. PostgreSQL lưu metadata dịch vụ Polaris/Airflow.

| Vùng | Nội dung |
|---|---|
| `landing/oltp/<table>/...` | Parquet và manifest của từng lần trích xuất MySQL |
| `landing/logs/...` | JSONL.gz lịch sử đã upload; chưa có job tự đọc file thô vào Gold |
| `warehouse/` | Iceberg data/metadata theo vị trí do catalog quản lý |
| State/checkpoint prefixes | Cursor batch, checkpoint Flink và trạng thái tác vụ |
| `quarantine` | Dữ liệu lỗi schema/nghiệp vụ đã xử lý PII theo quy tắc |
| `system.data_quality_results` | Bằng chứng kiểm tra chất lượng và đối soát |

Prefix raw Landing là vị trí file. Namespace `lakehouse.landing` còn chứa bảng Iceberg streaming access log; đây là hai dạng lưu trữ khác nhau.

## 2. Batch OLTP

Nguồn là 27 bảng khai báo trong [config.py](../../pipelines/src/lakehouse/config.py), loại `customer_credentials` khỏi 28 bảng OLTP.

- **Extract:** Spark reads the fixed Airflow interval `[start,end)` using each table's change timestamp. No lookback, per-table cursor or manifest. A separate manual `init_lakehouse_oltp` DAG reads the full source with no timestamp predicate.
- **Staging:** Reusable Parquet under `landing/oltp/staging/<table>`, overwritten each run including empty intervals.
- **Bronze:** Raw source versions merged by primary key/change timestamp. Replaying staging does not append duplicate versions; prior versions remain. No run/chunk checkpoint.
- **Silver:** Read Bronze versions matching each staging file's keys/change timestamps, validate/dedup, pseudonymize PII and MERGE by PK/change timestamp. Historical reruns cannot overwrite newer rows.
- **Gold:** Build dimensions/facts/marts from Silver, validate and publish snapshot IDs for serving. Reader pinning is retained; publication does not carry forward CDC progress.

Airflow allows one active batch. There is no source-wide transaction or cross-table rollback. Late commits outside the interval are intentionally not covered by lookback; recovery is an explicit interval rerun/full extract. Source updates must update the configured change timestamp (`payments` now uses updated_at). Hard deletes are audit events only until the separately approved manual delete-sync workflow is implemented. History and existing publications are not dropped.

## 3. Gold OLTP: 26 bảng (6 dimension, 6 fact, 14 mart)

Nguồn DDL: [gold_ddl.py](../../pipelines/src/lakehouse/oltp/gold_ddl.py). Nguồn biến đổi: [gold.py](../../pipelines/src/lakehouse/oltp/gold/).

### Sáu dimension

| Bảng | Grain / nội dung |
|---|---|
| `dim_date` | Một ngày; thuộc tính lịch |
| `dim_customer` | Một customer; thuộc tính đã pseudonymize |
| `dim_product` | Một product, kèm danh mục |
| `dim_variant` | Một SKU/variant |
| `dim_store` | Một cửa hàng, kèm city |
| `dim_delivery_staff` | Một nhân viên giao vận |

### Sáu fact

| Bảng | Grain / nội dung |
|---|---|
| `fact_order` | Một đơn; order totals, giá vốn, trạng thái và revenue flags |
| `fact_order_item` | Một chi tiết đơn; số lượng, giá bán và cost snapshot |
| `fact_shipment` | Một shipment; trạng thái, mốc giao hàng và COD |
| `fact_return_exchange` | Một return item; request cha, variant, action và refund |
| `fact_inventory_current` | Một variant × địa điểm; tồn hiện tại |
| `fact_inventory_daily_snapshot` | Ngày snapshot × variant × địa điểm kho/cửa hàng |

Tên bảng snapshot không chứng minh đang có đầy đủ lịch sử mọi ngày. Cần kiểm tra dữ liệu thực tế và cách ghi của lần chạy trước khi diễn giải chuỗi thời gian.

### Mười bốn mart

| Bảng | Grain chính |
|---|---|
| `mart_sales_daily` | Ngày × kênh × cửa hàng × danh mục; số đơn không cộng qua danh mục |
| `mart_order_daily` | Ngày đặt hàng Việt Nam × kênh × cửa hàng; số đơn cộng được, tài chính, yêu cầu trả và COD đang giao |
| `mart_product_sales_daily` | Ngày đặt hàng Việt Nam × sản phẩm; doanh thu thuần và lượng bán sau trả hàng |
| `mart_logistics_performance` | Ngày tạo shipment (UTC) × nhân viên giao vận × địa bàn được phân công; có số mẫu/tổng thời gian để tính trung bình trọng số, mẫu deadline riêng |
| `mart_product_returns` | Ngày trả × variant × action × reason; số yêu cầu distinct trong nhóm SKU không cộng qua SKU, tiền là đề nghị hoàn |
| `mart_inventory_health` | Ngày snapshot × loại địa điểm × địa điểm × danh mục |
| `mart_inventory_current` | Một dòng SKU × địa điểm, chỉ số dư hiện tại và thời điểm quan sát gốc |
| `mart_inventory_summary` | Một dòng tổng quan hiện tại; SKU hết hàng distinct trên mọi địa điểm, lượng/giá trị và số phiếu nhập |
| `mart_store_overview` | Một dòng/cửa hàng, gồm cửa hàng chưa có đơn/tồn; số đơn/doanh thu, nhân sự và trạng thái tồn |
| `mart_operations_summary` | Một dòng tổng quan tại mốc quan sát; số đơn chờ, shipment, yêu cầu và refund tổng hợp độc lập |
| `mart_cod_region_daily` | Ngày tạo shipment (UTC) × nhóm vùng địa chỉ đơn COD; bộ đếm cộng được |
| `mart_marketing_summary` | Một dòng coverage coupon/review và mốc quan sát nghiệp vụ |
| `mart_coupon_usage` | Một dòng/coupon, lượt dùng/hạn mức, khả dụng và redeemed trong 15 phút tại mốc đo |
| `mart_product_review_summary` | Một dòng/sản phẩm có review, số review và approved 1–2 sao trong 45 phút tại mốc đo |

Các mart bổ sung: `mart_order_daily`, `mart_product_sales_daily`, `mart_inventory_current`, `mart_inventory_summary`, `mart_store_overview`, `mart_operations_summary`, `mart_cod_region_daily`, `mart_marketing_summary`, `mart_coupon_usage`, `mart_product_review_summary`. Nguồn DDL đầy đủ là `gold_ddl.py`.

Order count ở mart sales không được cộng qua các danh mục để suy ra số đơn distinct. Công thức revenue order-level và item-level có khác biệt; xem Metrics Catalog.

## 4. Access logs và CDC

| Nhóm | Bảng / cách xử lý |
|---|---|
| Log Landing | `landing.access_logs`, nguồn streaming Kafka |
| Log Bronze | `bronze.web_events` |
| Log Silver | `silver.silver_logs` |
| Log Gold fact | `gold.fact_web_events` |
| Log Gold marts | `mart_hourly_route_metrics`, `mart_daily_product_demand`, `mart_marketing_funnel_daily` |
| CDC Bronze | `bronze.oltp_cdc_events`; envelope, operation, before/after, event_id và replay coordinates |

Job Flink access log sử dụng StatementSet để ghi các tầng của log. Spark `build_logs_gold` thực hiện rollup mart qua DAG `lakehouse_logs_marts` mỗi giờ, độc lập maintenance. DAG maintenance vẫn chạy mỗi hai giờ; chưa chuyển sang thủ công.

Ba mart log lần lượt có grain ngày × giờ × route × method, ngày × product, và **một dòng/ngày** chứa các cột bước phễu. Mỗi rollup đọc một snapshot fact và danh mục Silver pin theo publication OLTP, dựng lại toàn bộ ba mart để bao phủ các ngày có sự kiện tới mốc đo. Publication log độc lập công bố đủ ba snapshot sau kiểm tra; full overwrite cũng xóa kết quả current bị cũ khi nguồn không có dòng, giữ lịch sử snapshot. Flink và Spark chuẩn hóa UTC; dữ liệu log cũ theo giờ Việt Nam chưa được tự động chuyển đổi; xem [Access Log Design](../architecture/ACCESS_LOG_DESIGN.md) khi đối chiếu mốc ngày/giờ.

CDC log có thể chứa nhiều thay đổi của cùng khóa nguồn. Đếm sự kiện không tương đương đếm bản ghi hiện tại. CDC chưa nối trực tiếp vào các Silver/Gold OLTP ở mục 3.

## 5. Bảo trì và quyền

DAG OLTP maintenance chạy 03:00 hằng ngày; streaming maintenance chạy hai giờ/lần. Profile maintenance OLTP hiện liệt kê 15 bảng Gold và 8 bảng Silver; streaming liệt kê bốn bảng log. Danh sách này chưa bao phủ toàn bộ Bronze/Silver hoặc Bronze CDC. Kiểm tra [TABLE_PROFILES](../../pipelines/src/jobs/maintenance/iceberg_table_maintenance.py) trước khi kết luận một bảng đã được bảo trì.

Các tác vụ gồm compaction (OLTP đặt mục tiêu file 128 MiB), rewrite manifests, expire snapshots (retain_last 10 cho OLTP, 20 cho log), orphan cleanup với cutoff mặc định ba ngày. Tác vụ quarantine xóa dòng quá 30 ngày ở `quarantine.silver_oltp_violations`; không phải retention toàn bucket hoặc mọi loại quarantine. DAG maintenance không có dependency chờ batch, và các DAG được tạo paused theo Compose.

Polaris dùng bốn principal tách biệt; MinIO có bảy identity theo workload. `trino_reader` phục vụ truy vấn đọc, `trino_admin` dùng khi cần quản trị. Credential mỗi consumer nằm trong volume riêng.

Số dòng từng bảng và hiệu năng không được cố định trong tài liệu thiết kế. Khi báo cáo, truy vấn snapshot/run cụ thể và ghi lại môi trường thực thi.

