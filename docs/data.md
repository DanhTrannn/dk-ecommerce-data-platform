# Dữ liệu, pipeline và chỉ số

Cập nhật theo codebase ngày **10/11/2026**. Nguồn định nghĩa chính là [schema PostgreSQL](../database/schema.sql), [catalog trích xuất](../pipelines/src/lakehouse/utils/config.py), [Gold schema](../pipelines/src/lakehouse/oltp/gold/schema.py), [log transforms](../pipelines/src/lakehouse/logs/) và [presets generator](../generator/configs/). Các con số mô phỏng/nghiệm thu không phải số liệu kinh doanh thực tế.

## 1. Nguồn và lineage

| Nguồn | Đầu vào | Định danh và giới hạn |
|---|---|---|
| Generator SQL | Config/preset, seed, anchor, quy mô, địa điểm và phân phối | Version 0.10, `postgresql-v1`, logical identity/UUID5 xác định, `data_origin=synthetic` |
| API nghiệp vụ | Cart/checkout/POS, kho, giao vận, review, hoàn tiền… | Giữ lineage API hiện có `manual`/`generation_run_id=NULL`; journal scenario không tự biến thành lineage nguồn |
| CDC | Snapshot và WAL của 27 bảng PostgreSQL | Transport event ID; không phải số đơn hoặc current-state table |
| HTTP log | Request thực nhận bởi middleware | ID server 32 ký tự hex, `data_origin=observed`; không đồng nhất với khách thật/session |
| Public dataset nghiên cứu | Notebook EDA, không được đọc ở generator runtime | H&M 2019 dùng cho R03; Olist/UCI và khảo sát chỉ là tham chiếu trong giới hạn nghiên cứu |

Không dùng chung `generation_run_id`, Airflow `run_id`, HTTP `request.id` và CDC `event_id` như cùng một khóa. Chúng mô tả các vòng đời khác nhau. Source/processing/publication time cũng không thay thế nhau.

## 2. Schema OLTP

PostgreSQL có **28 bảng**. [ORM](../services/platform-app/src/platform_app/api/models/) và [DDL](../database/schema.sql) là nơi tra cột, CHECK, FK, index và default đầy đủ; không giữ một bản sao bảng cột dễ lệch code trong tài liệu.

- UUID nguồn là native UUID; tiền/ID chủ yếu BIGINT, trạng thái được ràng buộc bằng CHECK/FK/UNIQUE. Số ID tự sinh dùng identity.
- `TIMESTAMP WITHOUT TIME ZONE` lưu UTC; API/session và trigger `updated_at` hỗ trợ hợp đồng này. Trigger không tự tạo ledger/history nghiệp vụ.
- Import ID tường minh phải đồng bộ sequence; importer thực hiện trong transaction cùng dữ liệu. Schema bootstrap chỉ dành cho database trống, không phải migration/reset cho volume đang có.
- Số dư tồn = `opening_on_hand` + tổng ledger theo variant/location; tồn đầu không phải trần tồn. Phiếu nhập và hoàn hàng có thể làm tăng tồn.

| Nhóm | Bảng và quan hệ chính |
|---|---|
| Định danh | `customers`; `customer_credentials` gắn customer, chỉ dùng auth và bị loại khỏi analytics |
| Catalog | `categories` → `products` → `product_variants` |
| Khám phá/mua | `carts` → `cart_items`; `wishlist_items` gắn customer/product |
| Đơn/thanh toán | `orders` → `order_items`, `order_status_history`, `payments`; `refunds` gắn payment/đơn |
| Coupon/review | `coupons`, `coupon_redemptions`; `product_reviews` gắn mua hàng và product |
| Địa điểm/kho | `cities`, `stores`, `inventory`, `store_inventory`; tồn theo variant và location |
| Nhập/ledger | `inbound_receipts` → `inbound_receipt_items`; `inventory_transactions` ghi biến động |
| Giao hàng | `delivery_staff`, `shipments` → `shipment_attempts` |
| Đổi trả | `return_requests` → `return_items`, liên kết đơn/item; refund có vòng đời riêng |

Checkout ghi đơn/item/payment/history, trừ tồn và xử lý coupon trong transaction. COD bắt đầu confirmed/pending, hoàn tất mới ghi nhận thu tiền; hủy chưa thu không tự sinh refund. Giao thất bại/hủy/nhận trả phải có biến động kho phù hợp. API khóa hàng và retry lỗi transaction theo policy hiện có; không coi dữ liệu SQL nhập tay là đã đi qua API.

### Catalog trích xuất

27 bảng được cấu hình tường minh, không tự đưa bảng mới vào batch/CDC. `customer_credentials` không được cấp SELECT cho batch reader hoặc đưa vào publication CDC. Các PK/cursor sau là khóa trích xuất; `store_inventory_id` là khóa kỹ thuật dù PK vật lý của tồn cửa hàng là `(store_id, variant_id)`.

| Bảng | Khóa trích xuất | Cursor | Kiểu |
|---|---|---|---|
| `customers` | `customer_id` | `updated_at` | `mutable` |
| `categories` | `category_id` | `updated_at` | `mutable` |
| `products` | `product_id` | `updated_at` | `mutable` |
| `product_variants` | `variant_id` | `updated_at` | `mutable` |
| `carts` | `cart_id` | `updated_at` | `mutable` |
| `cart_items` | `cart_item_id` | `updated_at` | `mutable` |
| `wishlist_items` | `wishlist_item_id` | `updated_at` | `mutable` |
| `orders` | `order_id` | `updated_at` | `mutable` |
| `order_items` | `order_item_id` | `created_at` | `append_only` |
| `payments` | `payment_id` | `updated_at` | `mutable` |
| `order_status_history` | `order_status_history_id` | `created_at` | `append_only` |
| `inventory` | `variant_id` | `updated_at` | `mutable` |
| `coupons` | `coupon_id` | `updated_at` | `mutable` |
| `coupon_redemptions` | `coupon_redemption_id` | `updated_at` | `mutable` |
| `refunds` | `refund_id` | `updated_at` | `mutable` |
| `product_reviews` | `review_id` | `updated_at` | `mutable` |
| `cities` | `city_id` | `updated_at` | `mutable` |
| `stores` | `store_id` | `updated_at` | `mutable` |
| `store_inventory` | `store_inventory_id` | `updated_at` | `mutable` |
| `delivery_staff` | `staff_id` | `updated_at` | `mutable` |
| `shipments` | `shipment_id` | `updated_at` | `mutable` |
| `return_requests` | `return_id` | `updated_at` | `mutable` |
| `return_items` | `return_item_id` | `updated_at` | `mutable` |
| `inventory_transactions` | `transaction_id` | `created_at` | `append_only` |
| `inbound_receipts` | `receipt_id` | `updated_at` | `mutable` |
| `inbound_receipt_items` | `item_id` | `created_at` | `append_only` |
| `shipment_attempts` | `attempt_id` | `created_at` | `append_only` |

## 3. Landing, Bronze, Silver và Gold

Catalog mặc định `lakehouse`, bucket theo `MINIO_LAKEHOUSE_BUCKET` (mặc định `lakehouse`), các namespace `landing`, `bronze`, `silver`, `gold`, `system`, `quarantine`. Dữ liệu/metadata Iceberg lưu trên MinIO, Polaris giữ catalog. Object storage không tự xác nhận một bộ bảng đã sẵn sàng cho BI.

| Tầng | Batch OLTP | Access log |
|---|---|---|
| Landing/staging | Parquet `landing/oltp/staging/<table>` dùng lại cho lần đọc kế; không phải lịch sử bất biến | `landing.access_logs` append raw envelope và projected fields, giữ cả payload không hợp lệ/lặp |
| Bronze | `<table>` giữ source versions và `_source_system=postgresql_ecommerce`, metadata nguồn/ingestion | `bronze.web_events` giữ raw hash, typed envelope, `_validation_error`; distinct raw payload |
| Silver | `silver_<table>` MERGE theo PK/cursor; giữ state mới hơn, PII tokenization | `silver.silver_logs` chỉ event hợp lệ, dedup theo immutable request ID |
| Gold | 6 dimension + 6 fact + 15 mart nghiệp vụ | `fact_web_events` + 3 mart log |
| System/quarantine | Kết quả DQ, publication; dòng vi phạm được cách ly | Publication log độc lập; raw lỗi vẫn ở Bronze |

Batch interval dùng UTC có timezone `[start,end)`; append-only đọc `created_at`, mutable đọc `updated_at`. Extract phân trang theo PK 10.000 dòng và materialize từng trang trước khi ghi. Trang đầu thay staging, kể cả interval rỗng. Bronze chống lặp theo source PK/cursor; Silver đọc delta tương ứng, không ghi đè state mới bằng backfill cũ. Gold được xây lại, quality gate chạy rồi mới publish.

Silver chuẩn hóa UUID JDBC dạng chuỗi thành **16 bytes** theo schema phân tích BINARY hiện có. Resolver log hỗ trợ cả encoding đúng và bytes của chuỗi UUID từ các lần batch cũ. Interval chỉ chuẩn hóa các dòng được đọc; cần init có kiểm soát nếu muốn chuẩn hóa toàn bộ current state. Không tự sửa snapshot lịch sử.

PII của customer, đơn và nhân sự được chuẩn hóa lower/trim rồi token hóa SHA-256 với salt bắt buộc; giữ NULL, bỏ cột raw theo mapping. Quarantine cũng xử lý tên cột PII. Đây là pseudonymization, không phải bảo đảm ẩn danh tuyệt đối; raw OLTP/Bronze/CDC vẫn cần quyền phù hợp. Salt thay đổi làm đổi token.

### Luồng thử Polars/PyIceberg

Pilot `categories` tái sử dụng catalog nguồn: phân trang theo PK, interval `[start,end)`, staging riêng theo namespace/run, Bronze theo `(category_id, updated_at)` và Silver theo PK với điều kiện cursor không cũ hơn state hiện có. UUID Silver là 16 bytes; `slug` không có trong nguồn được điền NULL theo schema phân tích. Interval rỗng tạo staging typed rỗng, không xóa lịch sử. Mỗi invocation được khóa theo namespace bằng PostgreSQL session advisory lock.

Pilot không tổng quát hóa validation/PII/quarantine cho 27 bảng. Category thiếu key/code/name làm job thất bại; chưa ghi production quarantine. Nó không thay đổi schema hoặc snapshot của các bảng đang phục vụ BI.

## 4. CDC audit

`lakehouse.bronze.oltp_cdc_events` là bảng append audit, không phải bản sao current state.

| Nhóm trường | Ý nghĩa |
|---|---|
| `event_id`, `topic`, `kafka_partition`, `kafka_offset` | ID `topic:partition:offset`; có thể lặp nếu replay transport, không có UNIQUE vật lý |
| `database_name`, `source_schema`, `table_name`, `operation` | Nguồn và loại event (`r/c/u/d`, control BEGIN/END, tombstone…) |
| `source_connector`, `postgres_lsn`, `postgres_tx_id`, `transaction_id` | Tọa độ PostgreSQL/transaction; nullable ở snapshot/control khi nguồn không cung cấp |
| `record_key`, `before_payload`, `after_payload`, `raw_payload` | Raw key/envelope và row images; FULL replica identity phục vụ before image |
| `source_ts`, `event_ts`, `ingested_at`, `event_date` | Source, connector/transport và sink time theo UTC; partition không phải mốc publication |
| Cột binlog MySQL cũ | Được giữ cho history; NULL với event PostgreSQL mới |

Tombstone được nhận diện bằng `raw_payload IS NULL`, không chỉ operation `t` (cũng có thể là truncate có payload). LSN không phải khóa duy nhất toàn cục. Connector dùng slot `ecommerce_cdc`, publication `ecommerce_publication`, group source/checkpoint được giữ; không tự reset offset khi chạy lại.

`manual_lakehouse_hard_delete` chỉ đồng bộ `cart_items` và `wishlist_items`: cần actual delete event, bằng chứng soft-delete trong before image và source deletion time cách mốc DAG ít nhất 30 ngày. Không áp cho products/coupons, không purge PostgreSQL và không xóa Bronze/audit history. Batch/backfill cũ có thể đưa lại state đã xóa; phối hợp thủ công và đọc [operations.md](operations.md#5-phục-hồi-và-replay).

## 5. Access-log contract và xử lý

Contract mang `schema.name=ecommerce.access`, version `1.0.0`. Parser thực thi tại [parser.py](../pipelines/src/lakehouse/logs/parser.py); middleware/builder tại [API core](../services/platform-app/src/platform_app/api/core/access_logging.py). Không mô tả parser như một full JSON Schema validator.

| Trường | Ý nghĩa |
|---|---|
| `timestamp`, `observed_timestamp` | ISO timestamp có timezone; chuẩn hóa UTC |
| `request.id` | 32 hex do server tạo; không tin ID caller để dedup |
| `service.*`, `event.*` | Service context, action HTTP và duration monotonic ns |
| `http.request_method/route/status_code` | Method, route template hoặc `__unmatched__`, mã 100–599 |
| `actor.*`, `ecommerce.*` | Actor/action, product slug/public UUID hoặc variant UUID nếu route cung cấp |
| `severity_*`, `error.*`, `trace_id/span_id` | Mức lỗi và trace fields; không đồng nghĩa đã triển khai distributed tracing |
| `data_origin` | `observed` cho request thực nhận, kể cả request mô phỏng |

Không ghi request body, password, Authorization/cookie hoặc thanh toán nhạy cảm. Search có PII được redaction; route chuẩn giúp tránh dùng raw path làm dimension vô hạn. GET `/health/live` và `/health/ready` không phát event.

Flink giữ decoded raw payload, kể cả sai contract. Spark dùng raw hash để chống nhân bản Bronze, rồi ID cho Silver/fact; ID trùng nhưng hai valid payload khác nhau làm job lỗi trước publication. Mỗi layer đọc predecessor đã persist nên retry có thể tiếp tục sau lỗi giữa các commit. Cutoff cố định, không dùng event-time watermark để bỏ log đến muộn.

Product resolver dùng Silver products/variants theo snapshot OLTP đã công bố khi build log mart. Khóa chưa resolve giữ riêng; không gán sản phẩm tùy ý. Log publication lưu các snapshot đầu vào và mốc `observed_at`. Không tự nạp JSONL lịch sử vào Gold hoặc replay DLQ.

## 6. Dimension, fact và 18 mart

Dimension nghiệp vụ: `dim_date`, `dim_customer`, `dim_product`, `dim_variant`, `dim_store`, `dim_delivery_staff`. Fact nghiệp vụ: `fact_order` (một dòng/đơn), `fact_order_item` (một dòng/item), `fact_shipment`, `fact_return_exchange`, `fact_inventory_daily_snapshot`, `fact_inventory_current`. Log có `fact_web_events` theo request ID.

| Mart | Grain/phạm vi |
|---|---|
| `mart_order_daily` | Ngày đặt hàng × kênh × điểm bán; đếm đơn cộng được |
| `mart_sales_daily` | Ngày × kênh × điểm bán × danh mục; order count không cộng qua danh mục |
| `mart_product_sales_daily` | Ngày × sản phẩm |
| `mart_logistics_performance` | Ngày shipment UTC × nhân sự × địa bàn phân công |
| `mart_product_returns` | Ngày × variant × action/reason; không cộng distinct request qua SKU |
| `mart_inventory_health` | Ngày quan sát × nhóm địa điểm/danh mục |
| `mart_inventory_current` | Location type/id × variant; số dư hiện tại |
| `mart_inventory_summary` | Tổng quan tại mốc quan sát; một summary key |
| `mart_store_overview` | Một cửa hàng, số dư và tổng quan theo mốc build |
| `mart_operations_summary` | Một summary, thời gian chờ tại `operations_observed_at` |
| `mart_cod_region_daily` | Ngày shipment UTC × vùng địa chỉ đơn COD |
| `mart_marketing_summary` | Một summary tại `marketing_observed_at` |
| `mart_coupon_usage` | Một coupon, usage và cửa sổ redemption |
| `mart_product_review_summary` | Một sản phẩm, số review và negative window |
| `mart_admin_overview` | Một summary theo mốc build |
| `mart_hourly_route_metrics` | Ngày UTC × giờ × route × method |
| `mart_daily_product_demand` | Ngày UTC × product/resolved-or-raw key |
| `mart_marketing_funnel_daily` | Ngày UTC; request-event counts |

Bốn summary hiện tại không phải chuỗi lịch sử theo mọi mốc build. Không cộng nhiều ngày snapshot tồn thành tồn hiện tại. DDL đầy đủ và partition nằm trong [SQL templates](../pipelines/src/lakehouse/sql_templates/spark/).

## 7. Định nghĩa KPI và chất lượng

| Chỉ số | Công thức/diễn giải |
|---|---|
| Tổng đơn | `SUM(mart_order_daily.total_orders)`; mọi trạng thái, không cộng count theo category |
| Giá trị đặt hàng | `SUM(order_total_vnd)` từ total đơn, gồm phí giao hàng; không phải doanh thu ghi nhận |
| Doanh thu hàng hóa | Dòng hàng trừ giảm giá phân bổ, chỉ recognized delivered/completed/returned; không gồm phí vận chuyển |
| Hoàn tiền hàng hóa | Refund succeeded, liên kết đúng payment/đơn, phân bổ/giới hạn theo tiền hàng; không đồng nhất mọi cash refund |
| Doanh thu thuần | Gross merchandise revenue − successful merchandise refunds |
| Giá vốn | Snapshot `order_items.cost_price_vnd` theo lượng đã ghi nhận, hoàn nhập lượng trả completed/inspection passed; thiếu cost làm gate thất bại |
| Lợi nhuận gộp | Net revenue − recognized COGS; chưa trừ đầy đủ thuế/quảng cáo/chi phí vận hành |
| Tỷ lệ/biên | Tính từ tổng tử/mẫu hợp lệ, không trung bình tỷ lệ các dòng; không có mẫu số thì giữ NULL |
| Tồn | Opening + ledger; current value = on_hand × cost hiện có; low-stock thường ≤5 gồm 0, stockout =0 |
| Giao vận | Duration trung bình = tổng duration/tổng mẫu, không AVG của AVG; thiếu estimated_delivery_at không chứng nhận đúng hạn/SLA |
| Vận hành chờ | paid/confirmed tại mốc build; ngưỡng 2 giờ là tiêu chí rà soát, không phải SLA |
| Coupon gần đây | Redeemed trong 15 phút tại mốc marketing, bỏ released/future; usage_limit NULL là không giới hạn |
| Negative review | Approved 1–2 sao trong 45 phút tại mốc marketing, không suy nguyên nhân từ số lượng |
| HTTP lỗi | `(4xx + 5xx) / total_requests ×100`; duration ns/1e6 thành ms; percentile theo route/giờ |
| HTTP success | Cột legacy `success_2xx_count` thực tế đếm status <400; không gọi là chỉ 2xx |
| Funnel | Thành công product_detail/catalog_search → cart_item_set/cart_add → checkout_quote/checkout_submit; purchase chỉ checkout_submit |

Funnel là **request events**, không phải unique people/session/order; step có thể chồng lặp và tỷ lệ vượt 100%. PUT giỏ không chứng minh lượng tăng. Không cộng distinct actor qua ngày/sản phẩm thành số khách duy nhất của kỳ. COD checkout chưa thu chưa tạo doanh thu; refund và thanh toán là dòng tiền riêng.

Phân biệt NULL/chưa quan sát, 0 được đo từ nguồn hợp lệ và unavailable. Không tự tạo target, dự báo, biến động kỳ trước hoặc nhận định nhân quả. Ngày nghiệp vụ OLTP là ngày đặt theo Việt Nam; logs/shipments có UTC grain. Tồn kho dùng ngày quan sát thực, không backdate số dư hiện tại theo interval lịch sử.

Gold gate kiểm tra count/tiền/key/reference/lineage/PII và parity item → order → mart; chỉ publish sau khi đạt. Kết quả lưu `lakehouse.system.data_quality_results`. Đối soát source–Silver đọc nguồn hiện tại, nên cần nguồn đứng yên để so cùng thời điểm; tolerance theo cấu hình/lệnh, không coi so lệch thời gian là mất dữ liệu.

## 8. Dữ liệu sinh và căn cứ

Generator không đọc public CSV/notebook lúc runtime. Notebook [reference_datasets_eda.ipynb](../generator/notebooks/reference_datasets_eda.ipynb) giữ quy trình nghiên cứu, làm sạch, phép đo và đề xuất R01–R07. Hiện chỉ **R03: H&M 2019 product-popularity rank weights** được tích hợp sampler. Olist 2017 và UCI UK là tham chiếu khác thị trường/grain; chưa dùng để hiệu chỉnh toàn bộ mô hình.

R03 chuẩn hóa counts của 10 nhóm thứ hạng thành xác suất, phân bổ cho catalog giả lập, shuffle thứ hạng theo seed, chọn product theo trọng số rồi chọn variant còn lại. Một variant không lặp trong đơn; giỏ nhiều item không hoàn lại có thể làm tỷ trọng đầu ra khác trọng số đầu vào. Ánh xạ catalog, phân bố đều trong nhóm và chọn variant là giả định, không phải phân phối SKU tại Việt Nam.

Khảo sát/thống kê Việt Nam được ghi nhận trong notebook để đặt bối cảnh chi tiêu, đa kênh, thanh toán, Tết và hậu mãi. Không chuyển tỷ lệ người trả lời, sở thích hoặc market share thành xác suất theo đơn tại một shop. Chưa EDA dữ liệu giao dịch retail Việt Nam để khẳng định tính đại diện. Bảng hỏi/Gen Z hoặc tập review gán nhãn không tự cho phân phối giao dịch, rating hay tỷ lệ đổi trả.

Preset `small.yml`: 500 customers, 60 products, 240 variants, 3.000 orders, 12 tháng, seed `20260724`, anchor `2026-07-01T00:00:00Z`; không dùng ngày máy làm anchor ngầm. Đây là **cấu hình**, không chứng nhận số dòng nguồn đã nạp. Có bản ghi bổ sung như moderator; seed API chạy riêng có thể làm tổng catalog/source lớn hơn quy mô generator.

Cấu hình địa điểm mặc định có 5 thành phố/6 cửa hàng; chọn qua `locations.store_codes` hoặc `[]` để không sinh. Địa điểm không tự tạo POS orders, tồn cửa hàng, phiếu nhập hoặc staff assignment. Nhóm khách/giờ mua/coupon/COD/review/return hiện vẫn là kịch bản. Thay quy mô có thể làm ràng buộc đơn/khách/variant khác phân phối mong muốn; cần đo đầu ra thay vì suy từ tham số.

### Danh mục tham số generator

Các giá trị dưới đây mô tả preset và logic hiện có, không phải tỷ lệ thực nghiệm của shop Việt Nam. Cấu hình YAML và mã [transactions.py](../generator/src/generator/transactions.py) là nơi đối chiếu khi thay sampler. Định danh dataset giữ config/version; đổi đường dẫn tài liệu tham khảo không đổi dữ liệu sinh.

#### Quy mô và thời gian

| Chỉ số | Giá trị preset | Ý nghĩa | Phân loại |
|---|---|---|---|
| `scenario_id` | `small-repurchase-v2` | Tên kịch bản, dùng để nhận diện đầu ra | Kỹ thuật |
| `dataset_size` | `small` | Nhãn quy mô; không tự suy ra số dòng | Kỹ thuật |
| `seed` | 20260724 | Hạt giống để tái lập các lựa chọn ngẫu nhiên | Kỹ thuật |
| `anchor_time` | `2026-07-01T00:00:00+00:00` | Mốc cuối lịch sử được sinh | Kỹ thuật |
| `history_months` | 12 | Cửa sổ lịch sử; code cũ quy ước một tháng = 30 ngày | Giả định |
| `modes` | `seed_master`, `historical_transactions`, `repurchase_history` | Nhãn kịch bản trong cấu hình/manifest; chưa tự chứng minh có ba đường chạy độc lập | Kỹ thuật |
| `scale.customers` | 500 | Số khách mua hàng; tài khoản phục vụ quản trị có thể là dòng bổ sung | Quy mô |
| `scale.products` | 60 | Số sản phẩm trong catalog | Quy mô |
| `scale.variants` | 240 | Số tổ hợp size/màu của sản phẩm | Quy mô |
| `scale.orders` | 3000 | Tổng đơn mục tiêu; có thể xung đột khoảng đơn theo nhóm khách | Quy mô |
| `business_timezone` | `Asia/Ho_Chi_Minh` | Múi giờ dùng cho giờ mua và ngày chiến dịch; timestamp lưu theo UTC | Ngữ cảnh VN |

#### Thời điểm mua và chiến dịch — giả định

`day_of_week`, từ thứ Hai đến Chủ nhật: **[0,9; 0,95; 1; 1,05; 1,2; 1,5; 1,4]**. Giá trị lớn làm ngày đó dễ được chọn hơn; chưa đo được đường theo thứ tại shop VN.

Hai bộ trọng số theo giờ, thứ tự từ 00h đến 23h:

| Giờ | Ngày thường | Ngày chiến dịch |
|---|---:|---:|
| 00 | 0,05 | 2,80 |
| 01 | 0,02 | 1,20 |
| 02 | 0,02 | 0,22 |
| 03 | 0,02 | 0,08 |
| 04 | 0,03 | 0,05 |
| 05 | 0,05 | 0,06 |
| 06 | 0,09 | 0,10 |
| 07 | 0,14 | 0,16 |
| 08 | 0,18 | 0,22 |
| 09 | 0,20 | 0,28 |
| 10 | 0,22 | 0,34 |
| 11 | 0,26 | 0,55 |
| 12 | 0,28 | 1,35 |
| 13 | 0,26 | 0,72 |
| 14 | 0,24 | 0,36 |
| 15 | 0,24 | 0,30 |
| 16 | 0,27 | 0,36 |
| 17 | 0,34 | 0,48 |
| 18 | 0,50 | 0,72 |
| 19 | 0,72 | 1,10 |
| 20 | 0,95 | 1,65 |
| 21 | 1,10 | 2,05 |
| 22 | 1,00 | 1,80 |
| 23 | 0,60 | 1,30 |

| Chỉ số | Giá trị kịch bản | Ý nghĩa |
|---|---|---|
| `tet.month_start/day_start` | 1/25 | Bắt đầu cửa sổ Tết giả định: 25/01 dương lịch |
| `tet.month_end/day_end` | 2/18 | Kết thúc ngày 18/02 dương lịch; chưa phải lịch Tết từng năm |
| `tet.peak` | 2,8 | Hệ số trọng số ở giữa cửa sổ, giảm về hai đầu |
| Ngày đôi 1/1–8/8 | `boost=4`, `after_days=0` | Tăng trọng số đúng ngày chiến dịch |
| Ngày đôi 9/9–12/12 | `boost=4,8`, `after_days=0` | Tăng trọng số đúng ngày chiến dịch |
| Black Friday | Thứ Sáu thứ tư tháng 11; `boost=3,2`, `after_days=2` | Chiến dịch kéo dài ngày chính và hai ngày sau |
| Cửa sổ kéo lần mua về chiến dịch | ±7 ngày | Tìm ngày chiến dịch gần lần mua dự kiến; phụ thuộc `campaign_affinity` |
| Phút mua | 0–59, đồng đều | Độ phân tán timestamp trong giờ đã chọn |

Nghiên cứu Việt Nam hỗ trợ xem xét giai đoạn trước Tết, chưa xác định các hệ số trên. Trong kịch bản, trọng số ngày nhân theo thứ × Tết × chiến dịch; các chiến dịch trùng ngày có thể nhân chồng. Đây là cơ chế kịch bản, chưa phải các tác động độc lập đã đo.

#### Catalog, giá và giỏ hàng — giả định

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| Tỷ trọng tạo catalog `categories` | áo 22; đầm 18; váy 15; quần 14; khoác 12; phụ kiện 9; giày 5; túi 5 | Xác suất gán nhóm khi tạo sản phẩm; khác tỷ trọng đơn hoặc doanh thu |
| `price_bands[0]` | 79.000–199.000 VND; trọng số 30 | Dải giá nền sản phẩm |
| `price_bands[1]` | 199.000–399.000 VND; trọng số 40 | Dải giá nền sản phẩm |
| `price_bands[2]` | 399.000–699.000 VND; trọng số 20 | Dải giá nền sản phẩm |
| `price_bands[3]` | 699.000–2.500.000 VND; trọng số 10 | Dải giá nền sản phẩm |
| Bước giá | 1.000 VND | Độ chia khi lấy giá trong dải |
| Chênh giá variant | +5.000 VND theo thứ tự variant, chặn ở giá tối đa | Cách tạo giá khác nhau giữa size/màu |
| Giá vốn variant | 35%–45% giá bán theo thứ tự variant | Giá vốn mô phỏng, chưa phải số liệu biên lợi nhuận thực tế |
| `order_size` | 1/2/3/4 variant: 55%/30%/12%/3% | Số variant khác nhau mục tiêu mỗi đơn; giới hạn bởi các variant có trọng số sản phẩm dương |
| `quantity_per_item` | 1/2/3 đơn vị: 70%/25%/5% | Số lượng của mỗi variant trong đơn |
| Phí giao hàng | 30.000 VND; miễn nếu subtotal ≥500.000 VND | Phí tính theo tiền hàng trước giảm giá |
| Size/màu | 5 size, 12 màu; tối đa 60 variant/sản phẩm | Tổ hợp hợp lệ của catalog; không phải tỷ lệ nhu cầu theo size/màu |
| Phân bổ variant | Chia đều cho sản phẩm, phần dư theo thứ tự | Đáp ứng đúng tổng variant cấu hình |
| Lưu trữ sản phẩm | `floor(products/20)` | Khoảng 5% catalog được archive cuối cửa sổ; lịch sử giao dịch vẫn giữ |

Phân phối giỏ Olist và Quantity UCI chưa thay thế các trọng số trên vì khác grain và phạm vi bán lẻ/bán sỉ. Giá trị đơn được tính từ dòng hàng, không lấy riêng một AOV cố định.

#### Khách hàng và mua lại — giả định

| Nhóm | Tỷ trọng | Khoảng đơn cấu hình/năm | Khoảng mua | `campaign_affinity` |
|---|---:|---|---|---:|
| `loyal` | 15% | 4–10 | 20–60 ngày | 0,72 |
| `regular` | 35% | 2–4 | 45–120 ngày | 0,48 |
| `one_off` | 50% | 1 | Không cấu hình khoảng mua lại | 0,38 |

`campaign_affinity` là xác suất thử kéo lần mua về chiến dịch gần đó, không phải tỷ lệ đơn dùng coupon. Các nhãn nhóm là lựa chọn kịch bản, chưa được phân loại từ khách Việt.

**Giới hạn được giữ thành giả định:** số đơn được lấy trong khoảng rồi co giãn theo tỷ lệ để khớp `scale.orders`; vì vậy các khoảng trên là đầu vào phân bổ, không bảo đảm số đơn cuối cùng hoặc nhóm `one_off` chỉ có một đơn. Lịch mua còn bị giới hạn bởi cửa sổ lịch sử, thời điểm tài khoản ngừng hoạt động và số đơn còn phải sinh; khoảng mua thực tế có thể ngắn hơn khoảng cấu hình. Tài khoản đã inactive trước cửa sổ không được phân bổ đơn; tổng đơn phân bổ cho các tài khoản còn thời gian hợp lệ.

| Chỉ số bổ sung | Giá trị kịch bản | Ý nghĩa |
|---|---|---|
| Tỷ lệ tên nữ | 80% | Cách tạo tên cho bối cảnh catalog; không phải cơ cấu khách Việt đã đo |
| Khách inactive | Khoảng 4%, khách đầu tiên luôn active | Trạng thái tài khoản mô phỏng |
| Tuổi tài khoản trước cửa sổ | 1–180 ngày | Tạo tài khoản trước giao dịch |
| Thời điểm chuyển inactive | 1–60 ngày trước anchor | Giới hạn cuối lịch mua; thời điểm tạo tài khoản được giữ trước mốc này |
| Địa chỉ | Số nhà 1–999; danh sách đường/phường/quận chọn độc lập tại TP.HCM | Địa chỉ tổng hợp; chưa bảo đảm ánh xạ địa giới hợp lệ |
| Tài khoản bổ sung | Một người kiểm duyệt; 10 nhân viên giao hàng | Phục vụ quan hệ FK và kịch bản giao nhận/review |

#### Coupon — giả định

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| `base_usage_rate` | 0,14 | Xác suất thử dùng coupon thường khi đủ điều kiện |
| `campaign_usage_rate` | 0,64 | Xác suất thử dùng coupon chiến dịch |
| `midnight_usage_rate` | 0,82 | Xác suất thử dùng coupon 0h–1h khi có coupon hợp lệ |
| `first_order_usage_rate` | 0,42 | Xác suất thử dùng coupon chào mừng ở lần mua đầu |
| `customer_multipliers` | loyal 1,25; regular 1; one_off 0,8 | Nhân xác suất theo nhóm, chặn tối đa 1 |
| `campaign_percentage_values` | 10%, 12%, 15% | Mức giảm chiến dịch |
| `midnight_fixed_values_vnd` | 30.000; 50.000; 70.000 | Mức giảm cố định cho khung nửa đêm |
| `everyday_percentage` | 8% | Mức giảm coupon thường |
| `welcome_fixed_vnd` | 50.000 | Mức giảm coupon chào mừng |
| `everyday_minimum_subtotal_vnd` | 299.000 | Ngưỡng tiền hàng coupon thường |
| `campaign_minimum_subtotal_vnd` | 199.000 | Ngưỡng tiền hàng coupon chiến dịch, midnight và welcome |
| Phân bổ mức giảm chiến dịch | Luân phiên theo thứ tự ngày chiến dịch | Chọn giá trị trong các danh sách giảm giá; chưa phải phân phối nhu cầu khuyến mại |
| Cửa sổ coupon 0h | Hai giờ | Chỉ dùng trong đầu ngày chiến dịch |
| Giới hạn mỗi khách | 1 cho welcome/campaign/midnight; 10 cho everyday | Quy tắc sử dụng coupon |
| Tổng giới hạn coupon | `max(100, orders+100)` | Hạn mức kịch bản; không đo nhu cầu thị trường |

Ưu tiên coupon theo điều kiện: midnight → campaign → welcome ở lần đầu → everyday. Tỷ lệ sử dụng cuối cùng còn phụ thuộc đủ giá tối thiểu, cửa sổ hiệu lực, hạn mức và nhóm khách. Coupon cố định không vượt subtotal; giảm phần trăm tính từ subtotal. Coupon cho mức giảm bằng 0 không được gắn vào đơn.

#### Trạng thái, thanh toán và giao nhận — giả định

| Chỉ số/điều kiện | Giá trị kịch bản | Ý nghĩa |
|---|---|---|
| COD/VietQR | Công thức modulo hướng tới 60%/40% | Lựa chọn theo vị trí khách/lần mua, sau đó xét điều kiện khóa COD; không bảo đảm tỷ lệ toàn bộ đơn |
| Hủy đơn thường/chiến dịch | 3,5% / 6,5% | Xác suất nền khi đơn đủ tuổi ≥2 giờ |
| Hủy theo nhóm | loyal −1,2 điểm %; regular 0; one_off +1,8 điểm % | Cộng vào xác suất nền |
| Hủy có coupon | +1,2 điểm % | Cộng vào xác suất hủy; tổng chặn trong [0,1] |
| Lý do hủy | nhầm size/màu 28; địa chỉ 18; lựa chọn khác 18; nhầm mã 14; hết nhu cầu 22 | Trọng số lý do trong nhóm đơn hủy |
| Hoàn tất | 97% khi chưa hủy và đơn ≥5 ngày | Xác suất có điều kiện, chưa trừ tình huống boom COD |
| Xác nhận | Đơn chưa hủy/chưa hoàn tất và ≥8 giờ | Điều kiện chọn trạng thái snapshot |
| Thời gian xác nhận VietQR | 2–18 giờ sau đặt | Mốc nghiệp vụ, giới hạn bởi anchor |
| Thời gian hoàn tất | 24–96 giờ sau xác nhận | Thời gian mô phỏng hiện tại, chưa dùng trung vị Olist |
| Thời gian hủy | 15–480 phút sau đặt | Mốc hủy của đơn đã được chọn hủy |
| Khách rủi ro COD | `customer_index % 8 == 3` | Khoảng một phần tám khách theo thứ tự |
| Boom COD | Đơn COD chưa hủy, ≥3 ngày; nhóm rủi ro mỗi vị trí chia hết cho 3, nhóm khác chia hết cho 31 | Kịch bản theo vị trí, không phải xác suất thất bại thị trường |
| Thời điểm giao thất bại | Hai ngày sau xác nhận/đặt | Mốc thất bại, chặn bởi anchor |
| Số lần giao | Thành công 1, thất bại 3 | Tạo shipment attempt hợp lệ |
| Khoảng lần giao thất bại | 6 giờ | Các lần thử liên tiếp; chặn ở thời điểm thất bại |
| Khóa COD | Ba lần giao thất bại đã xảy ra trước hoặc tại lúc đặt | Sau mốc này đơn mới dùng VietQR; chưa tính các thất bại vẫn còn ở tương lai tại lúc đặt |
| Kênh bán | Chỉ sinh online trong kịch bản | Chưa có phân phối POS; không suy từ khảo sát đa kênh |

COD chỉ được ghi nhận đã trả tiền khi hoàn tất; đơn COD chưa hoàn tất giữ payment pending. VietQR ghi nhận thanh toán khi đặt, hủy sau thanh toán có refund. Snapshot `boom_count`/`is_cod_blocked` tổng hợp đến anchor; cập nhật bộ đếm trong file SQL đặt `updated_at` theo anchor. Vì thế `customers.updated_at` cuối cùng có thể muộn hơn mốc inactive được lấy mẫu nội bộ. Đây là tính nhất quán nghiệp vụ, không phải bằng chứng tỷ lệ thanh toán thực tế.

#### Review và đổi trả — giả định

| Chỉ số | Giá trị kịch bản | Ý nghĩa |
|---|---|---|
| Xác suất thử tạo review | loyal 58%; regular 38%; one_off 24% | Áp dụng trên từng item của đơn hoàn tất; tên cấu hình `completed_order_rates` không có nghĩa mẫu số là đơn |
| `rating_weights` | Điểm 1–5: [2, 5, 13, 38, 42] | Phân phối điểm độc lập với giao nhận trong kịch bản |
| `status_weights` | approved 94; rejected 6 | Trọng số trạng thái kiểm duyệt |
| `delay_days` | 1–14 ngày, cộng 0–23 giờ | Thời gian viết review sau hoàn tất; vượt anchor hoặc từ lúc hoàn trả toàn bộ được xử lý thì không tạo |
| Thời gian kiểm duyệt từ chối | 1–48 giờ sau review | Tạo metadata kiểm duyệt, chặn bởi anchor |
| Chọn đơn đổi trả | `order_index % 25 == 7`, đã hoàn tất và còn ≥3 ngày trước anchor | Kịch bản theo thứ tự; không phải tỷ lệ hoàn hàng quan sát |
| Chọn exchange | `order_index % 10 < 7`, đồng thời có variant thay thế | Điều kiện modulo tương tác với chọn đơn; không khẳng định 70% các ca đổi trả |
| Item/số lượng trả | Item đầu tiên, một đơn vị | Phạm vi đổi trả đơn giản |
| Ngày yêu cầu/xử lý | Hoàn tất +2 ngày; xử lý thêm +1 ngày | Trình tự đổi trả |
| Tiền hoàn một đơn vị | `round(unit_price × (1 − discount/subtotal))` | Phân bổ giảm giá theo tỷ lệ; không hoàn tiền khi đổi hàng |

Nguồn Olist mô tả review theo đơn và có ngày hẹn giao; repo review theo item và chưa có ngày hẹn. Chưa thay trọng số review bằng phân phối Olist khi chưa giải quyết ánh xạ này. Thiếu bằng chứng VN cho xác suất viết review, kiểm duyệt và đổi trả.

#### Wishlist, giỏ chưa checkout và tồn kho — giả định

| Chỉ số | Giá trị kịch bản | Ý nghĩa |
|---|---|---|
| Khách có wishlist | 55% | Xác suất tạo wishlist |
| Sản phẩm/wishlist | 1–5, không vượt catalog | Số sản phẩm của mỗi khách được chọn |
| Phần wishlist gắn với giao dịch | Mục tiêu 40%, ít nhất một khi có lịch sử đơn chưa hủy; chặn bởi số sản phẩm có giao dịch | Các lựa chọn còn lại vẫn có thể trùng lịch sử, nên tỷ lệ cuối không bảo đảm 40%; chưa đo chuyển đổi |
| Thêm trước khi mua | 1–30 ngày và 0–23 giờ | Timestamp wishlist, chặn đầu cửa sổ |
| Thêm lại trước khi mua | 0–7 ngày sau lần đầu | Giữ thời điểm không vượt lần mua |
| Thêm lại khi chưa mua | 0–29 ngày sau lần đầu | Giữ thời điểm không vượt anchor |
| Xóa wishlist chưa mua | 18% | Chọn `is_present=false` |
| Thời gian xóa | 0–14 ngày sau lần thêm cuối | Giữ thứ tự thời gian và không vượt anchor |
| Giỏ gắn với đơn | Tạo trước đặt 5–10.080 phút | Giỏ đã checkout, khác giỏ active |
| Giỏ active | Một giỏ cho mỗi khách active | Tạo dữ liệu giỏ chưa checkout |
| Giỏ active cũ | 65% chọn cập nhật 2–20 ngày trước anchor | Proxy giỏ ít hoạt động; không phải funnel bỏ giỏ đã đo |
| Giỏ active gần đây | Cập nhật 1–12 giờ trước anchor | Nhóm còn lại |
| Tuổi giỏ so với cập nhật | 1–72 giờ | Thời gian tạo giỏ active |
| Item giỏ active | 1–3 variant, mỗi variant 1–3 đơn vị | Số lượng giỏ chưa checkout |
| Tồn đệm mỗi variant | 80–200 đơn vị | Giả định đủ hàng cho lịch sử; không phải chính sách bổ sung hàng |
| Tồn đầu | Tổng xuất kho, gồm hàng đổi, + tồn đệm | Bảo đảm lịch sử không âm |
| Tồn cuối | Tồn đầu + tổng biến động | Ràng buộc đối soát tồn kho |

## 9. Hồ sơ nguồn nghiên cứu generator

Phần này giữ hồ sơ nguồn S01–S11 của nghiên cứu ngày **08/11/2026**, để tra cứu cùng [notebook EDA](../generator/notebooks/reference_datasets_eda.ipynb). Đây là bối cảnh và giới hạn của nghiên cứu đã có, không phải kết quả kiểm định mới trong lần gom tài liệu. **Chỉ R03 từ H&M đã được chuyển thành trọng số runtime**; các tỷ lệ khảo sát Việt Nam dưới đây chưa được dùng để hiệu chỉnh generator. S09–S11 là hướng tham khảo, chưa có EDA dữ liệu gốc.


### S01. Cục Thống kê — Khảo sát mức sống dân cư 2024

[Thông cáo kết quả KSMS 2024](https://www.nso.gov.vn/tin-tuc-thong-ke/2025/05/thong-cao-bao-chi-ket-qua-khao-sat-muc-song-dan-cu-nam-2024/), công bố năm 2025.

- **Mẫu:** 46.995 hộ tại 63 tỉnh/thành theo địa giới năm khảo sát; thông tin chi tiêu đại diện đến cấp toàn quốc, thành thị/nông thôn và sáu vùng.
- **Bằng chứng:** chi tiêu bình quân đầu người/tháng gần 3 triệu đồng; thành thị gần 3,8 triệu, nông thôn khoảng 2,5 triệu. Chi đời sống của nhóm thu nhập cao nhất gấp 3,1 lần nhóm thấp nhất.
- **Dùng được:** căn cứ cho khác biệt về khả năng chi tiêu giữa các nhóm khách, thay vì dùng một mức chi tiêu giống nhau.
- **Giới hạn:** đây là tổng chi tiêu đầu người, gồm nhiều nhu cầu. Không chuyển trực tiếp thành giá sản phẩm, giá trị đơn hay ngân sách tại một shop; số bình quân chưa đủ để xác định toàn bộ phân phối.

### S02. TGM — Vietnam E-Commerce Insights 2025

[Thông cáo và phương pháp khảo sát](https://tgmresearch.com/press-tgm-vietnam-ecommerce-insights-2025.html), công bố 20/06/2025, trang cập nhật 30/06/2026.

- **Mẫu:** 1.120 người, 18–54 tuổi; CAWI từ 18/12/2024 đến 22/01/2025. Nhà cung cấp công bố hiệu chỉnh theo tuổi và giới.
- **Bằng chứng:** 50% mua online hằng tuần; 92% người mua online sử dụng smartphone. Các ngành được mua gồm quần áo/phụ kiện 63%, chăm sóc cá nhân 55%, làm đẹp 48%.
- **Dùng được:** căn cứ cho sự khác nhau về tần suất mua và bối cảnh mua nhiều ngành hàng.
- **Giới hạn:** tần suất trên nhiều nơi bán không phải số đơn tại một shop. Tỷ lệ người mua ngành hàng không phải tỷ trọng doanh thu hoặc xác suất chọn SKU. Không coi cập nhật trang năm 2026 là khảo sát mới.

### S03. PwC — Voice of the Consumer 2024, Việt Nam

[Kết quả công bố tại Việt Nam](https://www.pwc.com/vn/vn/publications/vietnam-publications/voice-of-consumer-2024.html).

- **Mẫu:** 515 người tại Việt Nam, thuộc khảo sát hơn 7.000 người ở 11 thị trường châu Á–Thái Bình Dương.
- **Bằng chứng:** các kênh mua sắm phổ biến gồm điện thoại 67%, cửa hàng 63%, máy tính 44%; 71% cho biết đã từng mua trực tiếp qua mạng xã hội.
- **Dùng được:** căn cứ cho một khách có thể mua cả online và tại cửa hàng, đồng thời tiếp cận sản phẩm qua nhiều kênh.
- **Giới hạn:** các nhóm có thể trùng nhau. Không dùng 67%/63% làm tỷ trọng đơn online/POS hoặc coi 71% là tỷ trọng đơn social commerce. Trang tóm tắt chưa cung cấp đủ thiết kế mẫu để khẳng định đại diện mọi nhóm dân cư.

### S04. TGM — Ưu tiên phương thức thanh toán năm 2025

[Bài phân tích COD và BNPL](https://tgmresearch.vn/thanh-toan-thuong-mai-dien-tu-viet-nam-cod-bnpl.html) và [câu hỏi về phương thức được ưu tiên nhất](https://tgmstatbox.com/stats/preferred-payment-methods-for-online-shopping-vietnam/).

- **Bằng chứng:** bài phân tích công bố 35% người trả lời ưu tiên COD, với mẫu ghi tại kết quả là 1.142 người.
- **Dùng được:** củng cố việc giữ COD bên cạnh thanh toán điện tử.
- **Giới hạn:** cùng bài lại ghi phương pháp với mẫu 1.120; chưa giải thích được khác biệt. Câu hỏi đo sở thích, không đo số giao dịch thực tế. Chưa dùng 35% để thay tỷ trọng thanh toán theo đơn; phần còn lại cũng không đồng nghĩa toàn bộ là VietQR. Chỉ đọc phần công khai, chưa truy cập bộ bảng đầy đủ.

### S05. Ipsos — Mua sắm Tết 2026

[TẾT 2026 in Vietnam: Festive hearts, sharper carts](https://www.ipsos.com/en-vn/tet-2026-vietnam-festive-hearts-sharper-carts), công bố 06/04/2026.

- **Mẫu:** 500 người, 18–55 tuổi, tại TP.HCM, Hà Nội, Đà Nẵng và Cần Thơ.
- **Bằng chứng:** 53% cho biết phần lớn việc mua sắm Tết diễn ra trong 1–2 tuần trước Tết.
- **Dùng được:** căn cứ cho giai đoạn mua sắm trước Tết. Khi thiết kế lịch, dùng ngày Tết của từng năm và khoảng cách đến Tết, thay vì cố định một khoảng ngày dương lịch cho mọi năm.
- **Giới hạn:** khảo sát bốn đô thị, không đại diện đầy đủ cả nước. 53% người có cửa sổ mua chính trước Tết không phải 53% tổng đơn; chưa suy ra được hệ số tăng đơn từng ngày.

### S06. NIQ — Mùa Tết và hàng tiêu dùng nhanh

[Winning Vietnam’s Tet Holiday — FMCG 2025](https://nielseniq.com/global/en/insights/analysis/2025/winning-vietnams-tet-holiday-optimizing-fmcg-growth-for-the-next-season/), công bố 30/06/2025.

- **Bằng chứng:** NIQ cho biết mùa Tết đóng góp khoảng 20% giá trị bán hàng FMCG cả năm.
- **Dùng được:** bằng chứng bổ sung rằng mức độ mùa vụ phụ thuộc ngành hàng; nhu cầu tích trữ, biếu tặng có thể quan trọng với FMCG.
- **Giới hạn:** không áp tỷ lệ này cho thời trang hoặc toàn bộ bán lẻ. Giá trị bán hàng không phải số đơn; trang tóm tắt không công bố đủ mẫu và cách xác định cửa sổ mùa Tết để tái lập phép tính. Chỉ dùng định hướng mùa vụ, chưa dùng làm trọng số.

### S07. Metric — Thị trường sàn thương mại điện tử 2025

[Công bố báo cáo TMĐT Việt Nam 2025](https://metric.vn/insights/cong-bo-bao-cao-thuong-mai-dien-tu-viet-nam-2025/), ngày 28/01/2026; kỳ thống kê 01/01–31/12/2025 trên Shopee, TikTok Shop, Lazada và Tiki.

- **Bằng chứng:** riêng ngành Sức khỏe, nhóm giá 200–500 nghìn đồng đóng góp 25% doanh số. Riêng Shopee và TikTok Shop, 2,12% shop thuộc Mall đóng góp 32,6% doanh số.
- **Dùng được:** tham khảo dải giá theo đúng ngành; ghi nhận sự tập trung doanh số giữa người bán.
- **Giới hạn:** tỷ trọng doanh số theo giá không phải tỷ trọng số lượng sản phẩm. Tập trung theo shop không chứng minh phân phối bán chạy theo SKU. Đây là thống kê của nền tảng nghiên cứu, chưa có đơn hàng gốc để tái lập; chưa đủ căn cứ đặt giá, AOV hoặc quy luật Pareto cho repo.

### S08. Milieu — Khuyến mại, giao nhận và đổi trả năm 2025

[Vietnam’s online shoppers demand speed and reliability in 2025](https://mili.eu/vietnams-online-shoppers-demand-speed-and-reliability-in-2025/), ngày 08/09/2025.

- **Bằng chứng:** 64% nêu giảm giá, 55% nêu phí giao hàng thấp/miễn phí là động lực mua. Giao trễ được 37% nhắc đến trong các phàn nàn. Trong trải nghiệm đổi trả gần nhất, 60% liên quan hàng lỗi/sai, 23% thiếu/không nhận được, 12% đổi ý.
- **Dùng được:** chọn các tình huống khuyến mại và nguyên nhân hậu mãi có ý nghĩa nghiệp vụ.
- **Giới hạn:** bài không công bố cỡ mẫu, thời gian thu thập và trọng số. Các tỷ lệ trên không phải tỷ lệ dùng voucher, giao trễ hoặc hoàn hàng trên tổng đơn. Cơ cấu lý do mới liệt kê 95%; chưa chuyển thành phân phối đầy đủ. Chỉ dùng định hướng, chưa chốt tham số.


### S09. Hành vi mua ngẫu hứng trên TikTok Shop — Data in Brief 2024

[Bài báo, DOI 10.1016/j.dib.2024.111047](https://pmc.ncbi.nlm.nih.gov/articles/PMC11546432/) · [Mendeley Data, phiên bản 1](https://data.mendeley.com/datasets/46bxcjgws4/1).

- **Dữ liệu:** 361 phản hồi hợp lệ của sinh viên tại 10 tỉnh/thành Đồng bằng sông Cửu Long, thu thập 01–03/2024; chọn mẫu thuận tiện. Kho dữ liệu công bố giấy phép CC BY 4.0.
- **Dùng được:** tham khảo bảng hỏi và phân tích yếu tố như khan hiếm, cách trình bày sản phẩm, niềm tin, động cơ mua ngẫu hứng.
- **Giới hạn:** bảng hỏi tự khai, không phải lịch sử giao dịch. Không suy ra tỷ lệ đặt hàng tức thời, xác suất dùng voucher hay quan hệ nhân quả cho mọi người Việt. Mới kiểm tra bài báo và metadata kho dữ liệu, chưa EDA dữ liệu gốc.

### S10. Social commerce của sinh viên Gen Z — Data in Brief 2026

[Bài báo, DOI 10.1016/j.dib.2026.112529](https://pmc.ncbi.nlm.nih.gov/articles/PMC12907679/) · [Mendeley Data, phiên bản 2](https://data.mendeley.com/datasets/g59k85hrd5/2).

- **Dữ liệu:** 757 phản hồi hợp lệ, thu thập 10/2024–03/2025, chọn mẫu thuận tiện; có biến thu nhập, giới tính, tần suất mua, nơi cư trú và các thang đo niềm tin/hành vi. Kho dữ liệu công bố CC BY 4.0.
- **Dùng được:** ứng viên để khảo sát thêm khác biệt giữa các nhóm khách và cách đặt câu hỏi tần suất mua; kiểm tra bảng hỏi trước khi tính chỉ số.
- **Giới hạn:** chỉ bao phủ sinh viên Gen Z; thang điểm Likert về hành vi không tương đương số đơn hoặc số tiền đã mua. Mới kiểm tra bài báo và metadata, chưa EDA dữ liệu gốc.

### S11. UIT-ViOCD — Phàn nàn trong đánh giá sản phẩm tiếng Việt

[Vietnamese Complaint Detection on E-Commerce Websites](https://arxiv.org/abs/2104.11969), bản sửa năm 2021.

- **Dữ liệu được bài báo mô tả:** 5.485 đánh giá được gán nhãn thủ công, thuộc bốn nhóm sản phẩm.
- **Dùng được:** tài liệu tham khảo cho nội dung và cách diễn đạt phàn nàn bằng tiếng Việt, nếu sau này cần sinh nội dung review.
- **Giới hạn:** chưa xác minh quyền truy cập/giấy phép dữ liệu gốc. Tập gán nhãn không mặc nhiên đại diện tỷ lệ phàn nàn, phân phối sao hoặc tỷ lệ hoàn hàng ngoài thị trường. Không cần thêm mô hình NLP vào repo chỉ để sử dụng ý tưởng này.
