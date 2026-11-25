# Dữ liệu, pipeline và chỉ số

Nguồn định nghĩa: [schema PostgreSQL](../database/schema.sql), [danh sách bảng trích xuất](../airflow/lakehouse/oltp/sources.py), [dbt](../dbt/) và [preset generator](../generator/README.md). Chi tiết chạy pipeline ở [airflow/README.md](../airflow/README.md). Số liệu mô phỏng không phải số liệu kinh doanh thật.

## 1. Nguồn và lineage

| Nguồn | Đầu vào | Định danh và giới hạn |
|---|---|---|
| Generator OLTP (`datagen`) | Preset `generator/oltp/configs/*.yml` | Nạp một lần 12 tháng lịch sử; mọi dòng khách/đơn mang `data_origin=synthetic`, `generation_run_id=<scenario_id>:<seed>`. `datagen logs` đọc preset log `generator/logs/configs/*.yml` và ghi log truy cập mùa sale ra Parquet (`data_origin="synthetic"`) |
| API nghiệp vụ | Cart/checkout/POS, kho, giao vận, review, hoàn tiền… | Giữ lineage API hiện có `manual`/`generation_run_id=NULL`; journal scenario không tự biến thành lineage nguồn |
| HTTP log | Request thực nhận bởi middleware | ID server 32 ký tự hex, `data_origin=observed`; không đồng nhất với khách thật/session |
| Public dataset nghiên cứu | Notebook EDA (H&M, Olist, eCommerce Behavior) → preset YAML | Runtime `datagen` đọc/validate YAML OLTP và log; khảo sát Việt Nam chỉ để đối chiếu |

Không dùng chung `generation_run_id` và HTTP `request.id` như cùng một khóa. Chúng mô tả các vòng đời khác nhau. Source/processing/publication time cũng không thay thế nhau.

## 2. Schema OLTP

PostgreSQL có **28 bảng**. [ORM](../services/platform-app/src/platform_app/api/models/) và [DDL](../database/schema.sql) là nơi tra cột, CHECK, FK, index và default đầy đủ; không giữ một bản sao bảng cột dễ lệch code trong tài liệu.

- UUID nguồn là native UUID; tiền/ID chủ yếu BIGINT, trạng thái được ràng buộc bằng CHECK/FK/UNIQUE. Số ID tự sinh dùng identity.
- Thời gian là `TIMESTAMPTZ`; API ghi UTC có tzinfo, trigger `updated_at` dùng `statement_timestamp()` khi dữ liệu đổi mà caller không đổi timestamp (giữ timestamp lịch sử tường minh). Trigger không tự tạo ledger/history nghiệp vụ.
- Công cụ nạp ID tường minh phải đồng bộ sequence trong cùng transaction với dữ liệu. Schema bootstrap chỉ dành cho database trống, không phải migration/reset cho volume đang có.
- Số dư tồn = `opening_on_hand` + tổng ledger theo variant/location; tồn đầu không phải trần tồn. Phiếu nhập và hoàn hàng có thể làm tăng tồn. Generator nhập hàng ban đầu bằng phiếu nhập, còn seed catalog ghi tồn đầu thành giao dịch `adjustment`; cả hai để `opening_on_hand = 0`, nên riêng sổ kho đã cộng ra `on_hand`. Gold tính tồn chỉ từ sổ kho.

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

Batch nạp 19 bảng được cấu hình tường minh trong `SOURCE_TABLES`, không tự đưa bảng mới vào. Mỗi bảng có danh sách cột được nạp (`columns`) và danh sách cột cố ý bỏ (`excluded`, gồm dữ liệu cá nhân như `receiver_phone`). Cột mới ở nguồn chưa thuộc danh sách nào không được nạp và làm DAG sync báo lỗi cho tới khi được duyệt ([airflow/README.md](../airflow/README.md#column-changes)). `customer_credentials` không được cấp SELECT cho batch reader. Reader còn được SELECT `inventory` và `store_inventory`, nhưng chỉ để đối soát sổ kho khi chạy full; carts, cart_items, wishlist_items, coupon_redemptions, shipment_attempts và inbound_receipts không được nạp vì không bảng Gold nào dùng.

| Bảng | Khóa | Cursor |
|---|---|---|
| `customers` | `customer_id` | `updated_at` |
| `categories` | `category_id` | `updated_at` |
| `products` | `product_id` | `updated_at` |
| `product_variants` | `variant_id` | `updated_at` |
| `cities` | `city_id` | `updated_at` |
| `stores` | `store_id` | `updated_at` |
| `delivery_staff` | `staff_id` | `updated_at` |
| `orders` | `order_id` | `updated_at` |
| `order_items` | `order_item_id` | `created_at` |
| `payments` | `payment_id` | `updated_at` |
| `refunds` | `refund_id` | `updated_at` |
| `order_status_history` | `order_status_history_id` | `created_at` |
| `coupons` | `coupon_id` | `updated_at` |
| `shipments` | `shipment_id` | `updated_at` |
| `return_requests` | `return_id` | `updated_at` |
| `return_items` | `return_item_id` | `updated_at` |
| `product_reviews` | `review_id` | `updated_at` |
| `inventory_transactions` | `transaction_id` | `created_at` |
| `inbound_receipt_items` | `item_id` | `created_at` |

## 3. Các tầng lakehouse

Catalog Polaris `lakehouse`, bucket `MINIO_LAKEHOUSE_BUCKET`, namespace `staging`, `bronze`, `silver`, `gold`.

| Tầng | Nội dung | Ghi bởi |
|---|---|---|
| `staging.<bảng>` | Bản trích xuất của lần chạy gần nhất, kèm `_ingested_at`; xoá trắng mỗi lần nạp | PyIceberg |
| `bronze.<bảng>` | Projection nguồn theo khóa chính với `_is_deleted`, `_deleted_at`. UUID được chuyển thành chuỗi khi trích xuất. | PyIceberg |
| `silver.<bảng>` | Cùng dòng với Bronze; UUID thành 16 byte, timestamp `timestamp(6) with time zone` | dbt (merge) |
| `gold` | 7 dimension, 6 fact, 14 mart, 2 seed; `gold._mart_state` giữ mốc của mart | dbt (merge; mart tính lại theo ngày) |

- **Init** đọc mọi dòng tạo trước mốc bắt đầu lần chạy, đối soát với nguồn tại cùng mốc (số dòng; tổng tiền `orders`/`payments`/`refunds`; số dư sổ kho so với `on_hand − opening_on_hand` của `inventory`/`store_inventory` trừ biến động sau mốc), rồi ghi đè Bronze trong một commit, giữ lại dòng đã đánh dấu xoá và đánh dấu xoá dòng nguồn không còn.
- **Incremental** đọc các dòng có cursor trong data interval `[start, end)`, không lookback. Bronze upsert và không để dòng có cursor cũ hơn ghi đè bản hiện có. Bronze là trạng thái hiện tại, không phải lịch sử mọi thay đổi.
- **Silver** chỉ merge các dòng Bronze mới hơn `_ingested_at` lớn nhất của Silver; test `matches_bronze` chặn khi Silver và Bronze lệch khoá.
- **Xoá ở nguồn** được `lakehouse_oltp_sync` phát hiện bằng cách so khoá, rồi giữ làm lịch sử (xoá mềm) ở Bronze, Silver và dimension, fact của Gold; mart chỉ tính dòng còn sống. Cách hoạt động ở [airflow/README.md](../airflow/README.md#deleted-rows).
- Khoá chính, trường bắt buộc, khoá ngoại và CHECK đã được PostgreSQL bảo đảm nên không kiểm tra lại. Dữ liệu cá nhân không bị băm; chỉ các cột khai báo trong `sources.py` được nạp.

## 4. Access-log contract và xử lý

FastAPI ghi access log ra stdout và bộ sinh log ghi file Parquet. Việc nạp hai nguồn này vào lakehouse chưa được triển khai.

Middleware API phát `schema.name=ecommerce.access` phiên bản `1.0.0`. Generator phát `1.1.0` với `session.id` và `ecommerce.order_number`. [Mã ghi access log của API](../services/platform-app/src/platform_app/api/core/access_logging.py)

| Trường | Ý nghĩa |
|---|---|
| `timestamp`, `observed_timestamp` | ISO timestamp có timezone; chuẩn hóa UTC |
| `request.id` | 32 hex do server tạo; không tin ID caller để dedup |
| `service.*`, `event.*` | Service context, action HTTP và duration monotonic ns |
| `http.request_method/route/status_code` | Method, route template hoặc `__unmatched__`, mã 100–599 |
| `actor.*`, `ecommerce.*` | Actor/action, product slug/public UUID hoặc variant UUID nếu route cung cấp |
| `severity_*`, `error.*`, `trace_id/span_id` | Mức lỗi và trace fields; không đồng nghĩa đã triển khai distributed tracing |
| `data_origin` | `observed` cho request thực nhận, kể cả request mô phỏng; `synthetic` cho Parquet do generator tạo |
| `session.id`, `ecommerce.order_number` | Mã phiên và số đơn trong raw payload của generator 1.1.0 |

Không ghi request body, password, Authorization/cookie hoặc thanh toán nhạy cảm. Search có PII được redaction; route chuẩn giúp tránh dùng raw path làm dimension vô hạn. GET `/health/live` và `/health/ready` không phát event.

## 5. Dimension, fact và 14 mart OLTP

Dimension gồm `dim_date` (lịch, ngày lễ và ngày sale từ seed `calendar_events`), `dim_city` (vùng giao và SLA từ seed `delivery_sla`), `dim_store`, `dim_product`, `dim_variant`, `dim_customer` và `dim_staff`. Fact gồm `fact_order` (một dòng mỗi đơn), `fact_order_item` (mỗi dòng đơn), `fact_shipment` (mỗi vận đơn), `fact_return` (mỗi dòng trả hàng), `fact_inventory_daily` (ngày × vị trí × biến thể) và `fact_review` (mỗi đánh giá).

| Mart | Grain |
|---|---|
| `mart_orders_daily` | Ngày đặt × kênh × cửa hàng × thành phố × phương thức thanh toán; đơn theo nhóm trạng thái |
| `mart_sales_daily` | Ngày ghi nhận × kênh × cửa hàng × thành phố; doanh thu, hoàn tiền, giá vốn, lãi gộp |
| `mart_product_sales_daily` | Ngày ghi nhận × kênh × cửa hàng × sản phẩm |
| `mart_pos_hourly` | Ngày × cửa hàng × nhân viên bán × giờ; đơn POS đã ghi nhận |
| `mart_cancellations_daily` | Ngày đặt × kênh × lý do hủy × phương thức thanh toán |
| `mart_delivery_daily` | Ngày kết thúc giao × thành phố × nhân viên giao × phương thức thanh toán; đúng hạn, thời gian giao, COD |
| `mart_fulfilment_backlog_daily` | Ngày × giai đoạn (chờ xuất kho / đang giao) × nhóm tuổi đơn 0–4 / 5–8 / 9+ ngày |
| `mart_returns_daily` | Ngày yêu cầu × biến thể × loại (đổi/hoàn) |
| `mart_inventory_current` | Vị trí × biến thể tại ngày `DBT_AS_OF`; bán trung bình 28 ngày |
| `mart_inventory_health_daily` | Ngày × vị trí × danh mục; số biến thể còn/hết hàng, giá trị nhập |
| `mart_coupon_daily` | Ngày đặt × coupon × đợt sale |
| `mart_reviews_daily` | Ngày đánh giá × sản phẩm × giao đúng hạn/trễ |
| `mart_customer_value` | Một khách đã mua; giá trị theo đơn đã ghi nhận |
| `mart_customer_cohort_monthly` | Tháng mua đầu × tháng hoạt động |

Không cộng nhiều ngày tồn thành tồn hiện tại. Model và data test nằm trong [dbt/models/oltp](../dbt/models/oltp/).

## 6. Định nghĩa KPI và chất lượng

| Chỉ số | Công thức/diễn giải |
|---|---|
| Tổng đơn | `SUM(mart_orders_daily.orders)`; mọi trạng thái |
| Giá trị đặt hàng | `SUM(mart_orders_daily.order_value_vnd)`, gồm phí giao hàng; không phải doanh thu ghi nhận |
| Doanh thu hàng hóa | Dòng hàng trừ giảm giá phân bổ, ghi nhận vào ngày muộn hơn giữa `completed_at` và `paid_at`; không gồm phí vận chuyển |
| Hoàn tiền | Refund succeeded của đơn đã ghi nhận, vào ngày hoàn thành công; refund của đơn chưa từng ghi nhận (trả tiền rồi hủy) không trừ doanh thu |
| Doanh thu thuần | Doanh thu hàng hóa − hoàn tiền |
| Giá vốn | Snapshot `order_items.cost_price_vnd` × số lượng đã ghi nhận; hàng trả đạt kiểm định về kho được trừ lại giá vốn vào ngày hoàn tiền (`returned_cogs_vnd`) |
| Lợi nhuận gộp | Doanh thu thuần − (giá vốn − giá vốn hàng trả về); đơn bị trả toàn bộ cho lãi 0. Chưa trừ thuế/quảng cáo/chi phí vận hành |
| Tỷ lệ/biên | Tính từ tổng tử/mẫu hợp lệ, không trung bình tỷ lệ các dòng; không có mẫu số thì giữ NULL |
| Tồn | Cộng dồn sổ kho theo vị trí tới ngày; giá trị = tồn × giá vốn hiện tại của biến thể |
| Giao vận | Đúng hạn khi ngày giao (giờ VN) ≤ ngày đặt + `sla_days` của thành phố nhận. SLA = mức trên của vùng GHTK + 1 ngày xử lý + 1 ngày chờ lấy hàng: TP.HCM 4, Bình Dương/Cần Thơ/Hà Nội 6, Đà Nẵng/Hải Phòng 8. Thời gian trung bình = tổng/tổng mẫu, không AVG của AVG |
| Độ phủ tồn kho | `closing_qty / avg_daily_sales_28d`. Giữ NULL khi 28 ngày không có lượng bán. |
| Tỷ lệ hết hàng | Số vị trí × SKU hết hàng chia tổng số vị trí × SKU đã quan sát. |
| Mua lặp lại | Số khách đã mua nhiều lần chia số khách có đơn ghi nhận trong nhóm lọc theo ngày mua đầu. |
| Tỷ lệ hoạt động cohort | Số khách hoạt động ở tháng tương đối chia cho số khách ở tháng 0 của cùng cohort. |

KPI nhận giá trị NULL khi thiếu quan sát hoặc mẫu số. Số 0 biểu thị giá trị đo được. Mọi cột ngày trong Gold OLTP theo giờ Việt Nam. Timestamp giữ UTC. Chuỗi ngày tồn kho và backlog kết thúc ở ngày `DBT_AS_OF`. Các dashboard dùng ngày phù hợp với nghiệp vụ của từng dataset. [Danh sách dashboard](SETUP.md#6-dashboard-superset)

Kiểm tra chặn gồm đối soát với nguồn và `matches_bronze` (mục 3). Sau Gold, data test dbt (khóa, phân bổ giảm giá bảo toàn, đơn khớp Silver, các mart doanh thu khớp nhau, tồn khớp sổ kho) chỉ ghi cảnh báo vào log, không chặn.

## 7. Dữ liệu sinh và căn cứ

Lệnh `datagen` nạp 12 tháng lịch sử OLTP tổng hợp vào PostgreSQL. Dùng `make generate PRESET=100k` và `make generate-verify PRESET=100k` để nạp rồi kiểm tra sổ kho, R01–R06 và các tỷ lệ kịch bản. R07 định nghĩa cách sinh thời gian giao theo vùng. Preset `1m` đã nạp có thể dùng với `datagen logs --preset 1m --out <thư mục>` để sinh log mùa sale từ 01/11 đến 15/12/2025. Mỗi đơn online trong cửa sổ có một phiên kết thúc bằng `checkout_submit` mang `order_number`. Mỗi ngày phiên có một file Parquet (`raw_payload`, `event_at`) theo hợp đồng `ecommerce.access` 1.1.0. YAML là nguồn tham số runtime. Nhãn `measured` là số đo từ notebook, `transfer` là giả định khi áp số đo nước ngoài cho cửa hàng Việt Nam, `assumption` là tham số kịch bản. [Hướng dẫn và kết quả kiểm tra generator](../generator/README.md)

### Preset OLTP

[`generator/oltp/configs/`](../generator/oltp/configs/) có `100k.yml`, `1m.yml`, `10m.yml`, `50m.yml`, mục tiêu khoảng 0,1, 1, 10 và 50 triệu đơn online trong 12 tháng (26.000 / 260.000 / 2.600.000 / 13.000.000 khách có mua; 200 / 500 / 1.000 / 2.000 sản phẩm). `100k` dùng để chạy thử pipeline nhanh. Tổng đơn là **kết quả** của quy luật tần suất mua, không phải đầu vào: đơn online kỳ vọng = khách có mua × 3,846 với 12 tháng lịch sử; POS cộng thêm đơn theo tỷ lệ riêng. Các preset chỉ khác `scenario_id` và `scale`; seed `20260724`, anchor `2026-07-01T00:00:00+00:00`, múi giờ `Asia/Ho_Chi_Minh`.

Quy luật đo trong [purchase_behavior_eda.ipynb](../generator/oltp/notebooks/purchase_behavior_eda.ipynb) (H&M online 2019, Olist 2017):

| Mã | Hành vi | Quy luật | Nhãn |
|---|---|---|---|
| R01 | Số đơn mỗi khách | Log-series, θ = T/(α+T), α = 0,1141 năm | measured |
| R02 | Khoảng cách giữa hai đơn | Mũ, trung bình T(1 − c/k)/(k+1), c = 0,518 | measured |
| R03 | Độ bán chạy sản phẩm | LogNormal(0; 1,35) × hệ số nhóm hàng | measured |
| R04 | Giỏ hàng | Hình học p = 0,296; bảng biến thể/số lượng | transfer |
| R05 | Lịch | Ngày sale ×6,39, ba ngày sau ×2,6; thứ trong tuần bằng nhau; giờ theo Olist | transfer |
| R06 | Đánh giá theo giao trễ | Bảng số sao theo mức trễ; tỷ lệ trễ 5,5% | measured/transfer |
| R07 | Thời gian giao | Hãng lấy hàng cùng ngày nếu shop giao trước 10h30, sau đó lấy ngày kế; từ ngày lấy, số ngày đều trong khoảng của vùng: nội thành TP.HCM 1–2, nội miền (Bình Dương, Cần Thơ) 2–4, Hà Nội 3–4, liên miền (Đà Nẵng, Hải Phòng) 4–6 | cam kết công bố của GHTK Express (bảng giá từ 10/11/2026) |

Khoảng giá, tỷ lệ hủy/COD/trả hàng, coupon, Tết, wishlist và giỏ bỏ dở là `assumption`. Notebook mục 7.1 đối chiếu một số giá trị với số liệu Việt Nam công khai; các số đó không dùng để hiệu chỉnh. Mục 7.2 lấy thời gian giao theo vùng (R07) từ bảng công bố của GHTK, đối chiếu GHN.

### Quy luật access log

[`generator/logs/configs/`](../generator/logs/configs/) có `1m.yml`, `10m.yml`, `50m.yml` (cùng quy luật, mỗi file đi kèm preset OLTP cùng tên), đo trong [browsing_behavior_eda.ipynb](../generator/logs/notebooks/browsing_behavior_eda.ipynb) trên eCommerce Behavior (phiên `apparel`; L1–L3 tháng 10/2019, L4 tháng 11/2019):

| Mã | Hành vi | Quy luật | Nhãn |
|---|---|---|---|
| L1 | Lưu lượng | Phiên không mua = 41,39 × phiên có mua (khoảng hợp lý 31–52) | transfer |
| L2 | Lượt xem mỗi phiên | Có mua: hình học p = 0,1044; không mua: log-series θ = 0,9239 | transfer |
| L3 | Nhịp request | Log-normal, trung vị 36,2 giây, σ = 0,938 | transfer |
| L4 | Chuyển đổi ngày sale | Ngày sự kiện sale, r × 0,35 (ngày đông nhất 17/11/2019: r = 16,2 so với trung vị 45,9; một đợt sale duy nhất) | transfer |

Tỷ lệ phiên không mua thêm giỏ, trọng số lượt xem sản phẩm, các bước request phụ và việc lưu lượng ngày sale bám theo đơn là `assumption`. Hành trình nhiều phiên (xem hôm nay, mua vài ngày sau) không được mô hình hóa.

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


## 9. Hợp đồng Gold dbt/Trino

Gold OLTP có 27 model (7 dimension, 6 fact, 14 mart), hai seed (`delivery_sla`, `calendar_events`) và bảng trạng thái `gold._mart_state`. Airflow tạo một task cho mỗi model hoặc seed theo đồ thị `ref()` và `live()`. Dimension và fact merge theo khóa. Những fact cần theo dõi ngày cũ giữ `_previous_dates` để mart tính cả ngày dòng vừa rời đi. Phần lớn mart tính lại ngày hoặc khách bị ảnh hưởng kể từ watermark. `mart_customer_cohort_monthly` và `mart_inventory_current` dựng lại toàn bộ. Dimension/fact có `_is_deleted`, còn mart tính các dòng hiệu lực. Giảm giá được phân bổ theo tỷ lệ tiền dòng, làm tròn xuống tích lũy để tổng phân bổ bằng tiền giảm của đơn. Giá vốn lấy snapshot dòng đơn.

`DBT_AS_OF` dùng `run_after` của DAG để chốt ngày lập báo cáo. Nó xác định ngày tồn hiện tại và mốc thời gian của chuỗi tồn hoặc backlog. `dim_date` phủ các năm có dữ liệu. Năm data test trong `dbt/tests/oltp` chạy ở task `gold.tests` với mức cảnh báo. Các schema test của model chạy cùng `dbt build` theo cấu hình của model.
