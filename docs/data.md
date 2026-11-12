# Dữ liệu, pipeline và chỉ số

Cập nhật theo codebase ngày **11/11/2026**. Nguồn định nghĩa chính là [schema PostgreSQL](../database/schema.sql), [catalog trích xuất](../pipelines/src/lakehouse/utils/config.py), [Gold schema](../pipelines/src/lakehouse/oltp/gold/schema.py), [log transforms](../pipelines/src/lakehouse/logs/) và [presets generator](../generator/README.md). Các con số mô phỏng/nghiệm thu không phải số liệu kinh doanh thực tế.

## 1. Nguồn và lineage

| Nguồn | Đầu vào | Định danh và giới hạn |
|---|---|---|
| Generator (chưa triển khai) | Preset `generator/oltp/configs/*.yml`, `generator/logs/configs/access_logs.yml` | Generator SQL 0.10 cũ đã gỡ; schema vẫn giữ `data_origin`/`generation_run_id` cho dữ liệu tổng hợp |
| API nghiệp vụ | Cart/checkout/POS, kho, giao vận, review, hoàn tiền… | Giữ lineage API hiện có `manual`/`generation_run_id=NULL`; journal scenario không tự biến thành lineage nguồn |
| HTTP log | Request thực nhận bởi middleware | ID server 32 ký tự hex, `data_origin=observed`; không đồng nhất với khách thật/session |
| Public dataset nghiên cứu | Notebook EDA (H&M, Olist, eCommerce Behavior) → preset YAML | Chưa có runtime đọc preset; khảo sát Việt Nam chỉ để đối chiếu |

Không dùng chung `generation_run_id`, Airflow `run_id` và HTTP `request.id` như cùng một khóa. Chúng mô tả các vòng đời khác nhau. Source/processing/publication time cũng không thay thế nhau.

## 2. Schema OLTP

PostgreSQL có **28 bảng**. [ORM](../services/platform-app/src/platform_app/api/models/) và [DDL](../database/schema.sql) là nơi tra cột, CHECK, FK, index và default đầy đủ; không giữ một bản sao bảng cột dễ lệch code trong tài liệu.

- UUID nguồn là native UUID; tiền/ID chủ yếu BIGINT, trạng thái được ràng buộc bằng CHECK/FK/UNIQUE. Số ID tự sinh dùng identity.
- Thời gian là `TIMESTAMPTZ`; API ghi UTC có tzinfo, trigger `updated_at` dùng `statement_timestamp()`. Trigger không tự tạo ledger/history nghiệp vụ.
- Công cụ nạp ID tường minh phải đồng bộ sequence trong cùng transaction với dữ liệu. Schema bootstrap chỉ dành cho database trống, không phải migration/reset cho volume đang có.
- Số dư tồn = `opening_on_hand` + tổng ledger theo variant/location; tồn đầu không phải trần tồn. Phiếu nhập và hoàn hàng có thể làm tăng tồn.

| Nhóm | Bảng và quan hệ chính |
|---|---|
| Định danh | `customers`; `customer_credentials` gắn customer, chỉ dùng auth và bị loại khỏi analytics |
| Catalog | `categories` → `products` → `product_variants` |
| Khám phá/mua | `carts` → `cart_items`; `wishlist_items` gắn customer/product |
| Đơn/thanh toán | `orders` → `order_items`, `order_status_history`, `payments`; `refunds` gắn payment, refund trả hàng gắn thêm `return_id` |
| Coupon/review | `coupons`, `coupon_redemptions`; `product_reviews` gắn mua hàng và product |
| Địa điểm/kho | `cities`, `stores`, `inventory`, `store_inventory`; tồn theo variant và location |
| Nhập/ledger | `inbound_receipts` → `inbound_receipt_items`; `inventory_transactions` ghi biến động, trỏ tới đơn/phiếu nhập/yêu cầu trả hàng nguồn |
| Giao hàng | `delivery_staff`, `shipments` → `shipment_attempts` |
| Đổi trả | `return_requests` → `return_items`, liên kết đơn/item; mỗi yêu cầu có tối đa một refund riêng |

Checkout ghi đơn/item/payment/history, trừ tồn và xử lý coupon trong transaction. COD bắt đầu confirmed/pending, hoàn tất mới ghi nhận thu tiền; hủy chưa thu không tự sinh refund. Giao thất bại/hủy/nhận trả phải có biến động kho phù hợp. API khóa hàng và retry lỗi transaction theo policy hiện có; không coi dữ liệu SQL nhập tay là đã đi qua API.

### Catalog trích xuất

27 bảng được cấu hình tường minh, không tự đưa bảng mới vào batch. `customer_credentials` không được cấp SELECT cho batch reader. Các PK/cursor sau là khóa trích xuất; `store_inventory_id` là khóa kỹ thuật dù PK vật lý của tồn cửa hàng là `(store_id, variant_id)`.

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
| Landing/staging | Parquet `landing/polars-batch/<prefix>/<run_id>/<table>/page-*.parquet` theo từng run; không phải lịch sử bất biến | `landing.access_logs` append raw envelope và projected fields, giữ cả payload không hợp lệ/lặp |
| Bronze | `<table>` giữ source versions và `_source_system=postgresql_ecommerce`, metadata nguồn/ingestion | `bronze.web_events` giữ raw hash, typed envelope, `_validation_error`; distinct raw payload |
| Silver | `silver_<table>` upsert theo PK/cursor; giữ state mới hơn, PII tokenization | `silver.silver_logs` chỉ event hợp lệ, dedup theo immutable request ID |
| Gold | 6 dimension + 6 fact + 15 mart nghiệp vụ | `fact_web_events` + 3 mart log |
| System/quarantine | Kết quả DQ, publication; dòng vi phạm được cách ly | Publication log độc lập; raw lỗi vẫn ở Bronze |

Batch interval dùng UTC có timezone `[start,end)`; append-only đọc `created_at`, mutable đọc `updated_at`. Extract phân trang theo PK 10.000 dòng; mọi trang của mọi bảng được ghi staging trước, rồi mới ghi Bronze/Silver. Interval rỗng không xóa lịch sử. Bronze chống lặp theo source PK/cursor; Silver đọc delta tương ứng, không ghi đè state mới bằng backfill cũ. Gold được xây lại, quality gate chạy rồi mới publish.

Silver chuẩn hóa UUID đọc từ PostgreSQL dạng chuỗi thành **16 bytes** theo schema phân tích BINARY hiện có. Interval chỉ chuẩn hóa các dòng được đọc; cần init có kiểm soát nếu muốn chuẩn hóa toàn bộ current state. Không tự sửa snapshot lịch sử.

PII của customer, đơn và nhân sự được chuẩn hóa lower/trim rồi token hóa SHA-256 với salt bắt buộc; giữ NULL, bỏ cột raw theo mapping. Quarantine cũng xử lý tên cột PII. Đây là pseudonymization, không phải bảo đảm ẩn danh tuyệt đối; raw OLTP/Bronze vẫn cần quyền phù hợp. Salt thay đổi làm đổi token.

### Batch Polars/PyIceberg

Mỗi lần chạy khóa theo namespace bằng PostgreSQL session advisory lock. Schema Bronze đọc từ metadata cursor PostgreSQL, UUID giữ dạng chuỗi ở Bronze; schema Silver đọc hợp đồng cột trong `sql_templates/contracts` qua adapter chỉ hỗ trợ các kiểu đã khai báo và từ chối kiểu khác. JSON nguồn được giữ dạng văn bản, không parse rồi serialize lại. Schema mới phải được review trước khi mở rộng adapter.

Batch dedup theo PK/cursor/thời gian ingest, giữ guard chống backfill cũ và ghi nhiều mã lỗi validation theo thứ tự cố định. Token SHA-256 giữ trim ký tự space U+0020, lowercase, salt và NULL. Gold lấy `shipping_region` là tên thành phố từ `orders.shipping_city_id`, không dò địa chỉ gốc. Quarantine chỉ giữ JSON đã token hóa các cột PII theo pattern hiện có, bỏ trường NULL, dedup bằng source table/record/mã lỗi và partition theo ngày. Không có thêm cam kết ẩn danh cho nội dung free-text/JSON.

DAG chạy với prefix `serving`; CLI nhận prefix khác để chạy thử trong namespace riêng. Việc stage toàn bộ nguồn hoàn tất trước khi merge dữ liệu, nhưng các commit bảng vẫn độc lập, không phải transaction đa bảng. Đọc nguồn theo trang không đồng nghĩa snapshot giao dịch nhất quán toàn bộ 27 bảng khi nguồn đang thay đổi.

## 4. Access-log contract và xử lý

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

Flink giữ decoded raw payload, kể cả sai contract. Pipeline log dùng raw hash để chống nhân bản Bronze, rồi ID cho Silver/fact; ID trùng nhưng hai valid payload khác nhau làm job lỗi trước publication. Mỗi layer đọc predecessor đã persist nên retry có thể tiếp tục sau lỗi giữa các commit. Cutoff cố định, không dùng event-time watermark để bỏ log đến muộn.

Product resolver dùng Silver products/variants theo snapshot OLTP đã công bố khi build log mart. Khóa chưa resolve giữ riêng; không gán sản phẩm tùy ý. Log publication lưu các snapshot đầu vào và mốc `observed_at`. Không tự nạp JSONL lịch sử vào Gold hoặc replay DLQ.

## 5. Dimension, fact và 18 mart

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
| `mart_cod_region_daily` | Ngày shipment UTC × thành phố giao của đơn COD (thiếu thì `Khu vực khác`) |
| `mart_marketing_summary` | Một summary tại `marketing_observed_at` |
| `mart_coupon_usage` | Một coupon, usage và cửa sổ redemption |
| `mart_product_review_summary` | Một sản phẩm, số review và negative window |
| `mart_admin_overview` | Một summary theo mốc build |
| `mart_hourly_route_metrics` | Ngày UTC × giờ × route × method |
| `mart_daily_product_demand` | Ngày UTC × product/resolved-or-raw key |
| `mart_marketing_funnel_daily` | Ngày UTC; request-event counts |

Bốn summary hiện tại không phải chuỗi lịch sử theo mọi mốc build. Không cộng nhiều ngày snapshot tồn thành tồn hiện tại. DDL đầy đủ và partition nằm trong [SQL contracts](../pipelines/src/lakehouse/sql_templates/contracts/gold/).

## 6. Định nghĩa KPI và chất lượng

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

## 7. Dữ liệu sinh và căn cứ

Hiện **chưa có runtime sinh dữ liệu**: package `data-generator` (CLI, SQL export, importer, console Streamlit) đã bị gỡ. Thư mục [generator](../generator/README.md) chỉ giữ quy luật và preset để generator mới đọc sau này. Mỗi khối YAML ghi `source`: `measured` (đo trong notebook), `transfer` (đo ở nguồn nước ngoài, áp cho shop thời trang VN như giả định chuyển nguồn) hoặc `assumption` (tham số kịch bản). YAML là danh mục tham số chính thức; tài liệu này không chép lại giá trị.

### Preset OLTP

[`generator/oltp/configs/`](../generator/oltp/configs/) có `1m.yml`, `10m.yml`, `50m.yml`, mục tiêu khoảng 1, 10 và 50 triệu đơn trong 12 tháng (260.000 / 2.600.000 / 13.000.000 khách có mua; 500 / 1.000 / 2.000 sản phẩm). Tổng đơn là **kết quả** của quy luật tần suất mua, không phải đầu vào: đơn kỳ vọng = khách có mua × 3,846 với 12 tháng lịch sử. Ba preset chỉ khác `scenario_id` và `scale`; seed `20260724`, anchor `2026-07-01T00:00:00+00:00`, múi giờ `Asia/Ho_Chi_Minh`.

Quy luật đo trong [purchase_behavior_eda.ipynb](../generator/oltp/notebooks/purchase_behavior_eda.ipynb) (H&M online 2019, Olist 2017):

| Mã | Hành vi | Quy luật | Nhãn |
|---|---|---|---|
| R01 | Số đơn mỗi khách | Log-series, θ = T/(α+T), α = 0,1141 năm | measured |
| R02 | Khoảng cách giữa hai đơn | Mũ, trung bình T(1 − c/k)/(k+1), c = 0,518 | measured |
| R03 | Độ bán chạy sản phẩm | LogNormal(0; 1,35) × hệ số nhóm hàng | measured |
| R04 | Giỏ hàng | Hình học p = 0,296; bảng biến thể/số lượng | transfer |
| R05 | Lịch | Ngày sale ×6,39, ba ngày sau ×2,6; thứ trong tuần bằng nhau; giờ theo Olist | transfer |
| R06 | Đánh giá theo giao trễ | Bảng số sao theo mức trễ; tỷ lệ trễ 5,5% | measured/transfer |

Khoảng giá, số ngày giao, tỷ lệ hủy/COD/trả hàng, coupon, Tết, wishlist và giỏ bỏ dở là `assumption`. Notebook mục 7.1 đối chiếu một số giá trị với số liệu Việt Nam công khai; các số đó không dùng để hiệu chỉnh.

### Quy luật access log

[`generator/logs/configs/access_logs.yml`](../generator/logs/configs/access_logs.yml), đo trong [browsing_behavior_eda.ipynb](../generator/logs/notebooks/browsing_behavior_eda.ipynb) trên eCommerce Behavior (2–31/10/2019, phiên `apparel`):

| Mã | Hành vi | Quy luật | Nhãn |
|---|---|---|---|
| L1 | Lưu lượng | Phiên không mua = 41,39 × phiên có mua (khoảng hợp lý 31–52) | transfer |
| L2 | Lượt xem mỗi phiên | Có mua: hình học p = 0,1044; không mua: log-series θ = 0,9239 | transfer |
| L3 | Nhịp request | Log-normal, trung vị 36,2 giây, σ = 0,938 | transfer |

Giỏ bỏ dở, trọng số lượt xem sản phẩm, các bước request phụ, tỷ lệ khách ẩn danh và lưu lượng ngày sale là `assumption`. Hợp đồng log không có session id nên hành trình nhiều phiên không được mô hình hóa.

Các con số trên mô tả dữ liệu nguồn công khai và giả định, **không** chứng nhận tính đại diện cho shop Việt Nam.

## 8. Hồ sơ nguồn nghiên cứu generator

Phần này giữ hồ sơ nguồn S01–S11 của nghiên cứu ngày **08/11/2026**, để tra cứu cùng [notebook EDA OLTP](../generator/oltp/notebooks/purchase_behavior_eda.ipynb). Đây là bối cảnh và giới hạn của nghiên cứu đã có, không phải kết quả kiểm định mới trong lần gom tài liệu. Các tỷ lệ khảo sát Việt Nam dưới đây không được dùng để hiệu chỉnh preset generator. S09–S11 là hướng tham khảo, chưa có EDA dữ liệu gốc.


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


### Hợp đồng Gold native

Gold native giữ 27 schema hiện có: dimension, fact và 15 mart OLTP. Adapter Arrow đọc schema SQL hiện có, gồm DATE/DOUBLE và COMMENT. Các phép phân bổ giảm giá/hoàn tiền dùng tích Int128 và hiệu phần nguyên tích lũy, trả về VND Int64; không dùng số thực để phân bổ tiền. Giá vốn lấy snapshot dòng đơn. COD chưa ghi nhận doanh thu được giữ tách khỏi doanh thu đã ghi nhận.

`--as-of` là mốc nghiệp vụ có timezone cho cửa sổ coupon 15 phút, review 45 phút và các bảng quan sát. `_gold_ingested_at` là thời điểm xử lý thực. `--batch-date` chỉ định ngày logic của audit, mặc định ngày UTC của mốc nghiệp vụ. Snapshot tồn kho dùng ngày Việt Nam: chỉ ghi đè partition ngày hiện tại, giữ ngày cũ; bảng current/summary chỉ đọc ngày đang quan sát. Dimension ngày thay thế phạm vi năm 2025–2027.

Gate native giữ 55 kiểm tra về đối soát, tài chính, tồn kho, vận hành, marketing, admin, khóa, freshness và PII. Audit lưu ở `<prefix>_system.data_quality_results`; publication ở `<prefix>_system.analytics_publications`, ghim cả Silver/Gold đã audit. Trạng thái không có snapshot được giữ NULL trong manifest. Đọc lại publication cũ vẫn dùng snapshot cũ dù các bảng được rebuild; retention snapshot phải tôn trọng các manifest còn dùng.


### Hợp đồng ba mart log native

Giữ nguyên tên/cột/grain của `mart_hourly_route_metrics`, `mart_daily_product_demand` và `mart_marketing_funnel_daily`. Ngày/giờ tính theo UTC. Các chỉ số funnel là số request thành công, không phải số người/session/đơn hàng duy nhất; visitor đếm actor khác NULL. Tên cũ `success_2xx_count` vẫn tính HTTP dưới 400 (gồm 3xx). Mẫu số bằng 0 trả NULL.

Ánh xạ sản phẩm hỗ trợ slug, ID số và UUID 16 bytes; UUID có dấu gạch được chuẩn hóa, slug giữ dấu gạch. Product token có ưu tiên hơn variant token. Khóa chưa khớp giữ `raw-product:`/`raw-variant:` để vẫn thấy nhu cầu chưa ánh xạ; mapping làm nhân bản event bị chặn.

Percentile native dùng quantile `equiprobable`, tương ứng nearest-rank chính xác; bỏ duration NULL, làm tròn ba chữ số. Tham khảo [Polars quantile](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.quantile.html).

15 kiểm tra gồm timestamp/ID/ngày UTC, trạng thái HTTP/flags, miền duration, ba đối chiếu output sau ghi, ba kiểm tra grain, bảo toàn tổng request route/funnel, checkout và product actions. Audit append vào `<prefix>_system.logs_quality_results`; publication log dùng schema v1 với tên bảng vật lý migration. Gate này không thay thế kiểm tra raw-payload conflict ở Bronze.
