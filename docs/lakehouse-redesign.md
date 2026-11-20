# Thiết kế lại schema lakehouse

Ngày: 2026-11-13. Trạng thái: đã duyệt; cập nhật phần log cùng ngày; thêm mục 13 đối chiếu với `dev` ngày 2026-11-20.

## 1. Mục đích

Lakehouse hiện có 93 bảng (landing 1, bronze 28, silver 28, gold 31, quarantine 1, system 4), nhưng chỉ vài bảng
có người đọc: API đọc `landing.access_logs` và `gold.mart_hourly_route_metrics`, Superset đọc `mart_order_daily` và
`mart_product_sales_daily`. Phần còn lại là bản sao (staging Parquet, mart log tính hai lần bằng Polars và dbt,
`raw_payload` lưu hai nơi), bảng không ai đọc (Bronze OLTP, `dim_date`, `dim_customer`), mart tổng một dòng, hoặc chỉ
số sai (`fact_shipment.is_on_time` luôn NULL).

Thiết kế mới dựng lại schema theo **nhóm người xem dashboard**: mỗi mart trả lời một câu hỏi có nghĩa của một nhóm.
Lakehouse gom hai loại dữ liệu đến bằng hai kiểu nạp: bảng quan hệ OLTP nạp theo lô (ELT từ PostgreSQL) và sự kiện
log JSON bán cấu trúc (streaming Kafka → Flink cho traffic thật, nạp file cho log lịch sử do generator sinh). Hai loại
gặp nhau trong Gold: phễu web nối phiên trong log với đơn thật trong OLTP. Không có báo cáo realtime.

Ưu tiên: **có ý nghĩa + dễ vận hành**. Không bảng thừa, không cơ chế thừa.

## 2. Mục tiêu, không làm, phản mục tiêu

**Mục tiêu**

- 3 tầng Bronze → Silver → Gold, mỗi tầng một vai trò rõ.
- 28 bảng Gold (7 dim, 6 fact, 15 mart) phục vụ 5 nhóm người xem.
- Log từ hai đường nạp (streaming và nạp file) vào cùng một bảng Bronze, Silver và mart không phân biệt nguồn.
- Mart theo ngày chỉ tính lại những ngày bị dữ liệu mới chạm tới.

**Không làm trong thiết kế này**

- Dashboard Superset cụ thể (làm sau khi schema xong).
- Generator log lịch sử (tài liệu thiết kế riêng, dùng schema này).
- Báo cáo realtime (bỏ). Kịch bản traffic Streamlit chỉ mô tả ở mục 11, chưa thiết kế chi tiết.
- Dashboard cho `system_admin` và `city_planner`.

**Phản mục tiêu** (chạy được nhưng vẫn là thất bại)

- Một con số có hai nơi tính.
- Mart không có người xem hoặc không trả lời câu hỏi nào.
- Ngưỡng/hằng số gán tay không có căn cứ.
- Đọc cả bảng lớn vào RAM (job log và bước kiểm tra Gold hiện đang làm vậy).

## 3. Ràng buộc

- Nguồn: PostgreSQL của project (`database/schema.sql`), dữ liệu tổng hợp `data_origin='synthetic'` và dữ liệu thật
  từ storefront `manual`.
- Công cụ giữ nguyên: Iceberg + Polaris, Trino, dbt (Gold), Polars/pyiceberg (Bronze, Silver), Flink (log), Airflow.
- Không thêm dependency mới.
- Quy mô kiểm: preset 1m. Preset 10m (DB nguồn 67 GB) chỉ để thử tải; không tối ưu riêng cho nó.

## 4. Các cách tiếp cận đã cân nhắc

| Cách | Mô tả | Đánh đổi | Kết quả |
|---|---|---|---|
| A. Giữ khung hiện tại, chỉ dọn mart | landing + staging Parquet → Bronze/Silver, publication ghim snapshot | Ít sửa nhất, nhưng giữ nguyên các bản sao và cơ chế publication 3 phần | Bỏ |
| **B. Bronze là bản sao nguồn, Silver là trạng thái hiện tại, Gold dựng bằng dbt** | Bỏ landing; OLTP extract ghi thẳng Bronze; Flink ghi Bronze log; Silver đọc Bronze; kiểm tra chặn trước Gold; đọc thẳng bảng hiện tại | Phải sửa extract, Flink sink, phần đọc của Superset/API | **Chọn** |
| C. Bỏ Bronze, Silver đọc thẳng nguồn | Ít tầng nhất | Mất bản sao gốc để đối chiếu/nạp lại; log vẫn cần một tầng thô | Bỏ |

Các lựa chọn con đã chốt khi thiết kế:

- Bỏ landing; Bronze bắt đầu luồng nạp (đã chốt).
- Giữ `dim_date` ("dim date luôn cần cho lakehouse") và `dim_customer`, thêm mart khách hàng (đã chốt).
- Bỏ toàn bộ nhóm system: không bảng kết quả kiểm tra, không quarantine, không publication, không mart log hệ
  thống (đã chốt).
- Không băm dữ liệu cá nhân (đã chốt).
- Mã phiên trong log, phiên hết sau 30 phút không hoạt động (đã chốt). Cả API và generator log ghi hợp đồng 1.1.0
  để hai đường nạp đồng bộ và traffic thật cũng vào phễu (đã chốt).
- Nối phiên với đơn bằng mã đơn ghi trong log `checkout_submit`, không thêm cột phiên vào bảng đơn OLTP (đã chốt).
- Bỏ báo cáo realtime; log chỉ phục vụ phân tích theo ngày trong lakehouse (đã chốt).
- Log lịch sử nạp bằng file, không phát lại qua Kafka/Flink (đã chốt).

## 5. Quy ước chung

- **Ngày nghiệp vụ** theo giờ `Asia/Ho_Chi_Minh`. Mọi cột `*_date` và `hour` trong Gold là giờ Việt Nam; timestamp
  giữ UTC.
- **Ghi nhận doanh thu**: vào ngày đơn **hoàn tất và đã thanh toán đủ** (`completed_at` có giá trị và `paid_at` có
  giá trị; ngày ghi nhận = ngày muộn hơn trong hai mốc). (đã chốt)
- **Hoàn tiền** giảm doanh thu vào ngày hoàn tiền thành công (`refunds.completed_at`, `status='succeeded'`).
  (đề xuất)
- **Giá vốn** = `order_items.cost_price_vnd` (giá vốn chụp lúc bán). Giá trị tồn kho tính theo
  `product_variants.cost_price_vnd` (giá vốn hiện tại).
- **Đúng hạn giao**: thời gian từ đặt đơn tới giao thành công ≤ SLA của thành phố nhận hàng.
  SLA: thành phố có kho trung tâm (HCM) = nội thành 2 ngày; mọi thành phố khác = liên tỉnh 5 ngày.
- **Cập nhật mart theo ngày**: mỗi lần chạy chỉ tính lại những ngày bị các dòng Silver vừa thay đổi chạm tới (ví dụ
  một đơn vừa hoàn tất chạm ngày đặt của nó trong mart đơn và ngày ghi nhận của nó trong mart doanh thu). Mart không
  theo ngày (`mart_inventory_current`, `mart_customer_value`, `mart_customer_cohort_monthly`) dựng lại toàn bộ.
  (đề xuất)
- **Kiểm tra chất lượng là chốt chặn, không phải báo cáo**: kiểm tra chạy trên dữ liệu sắp ghi vào Silver; trượt thì
  task Airflow lỗi, ghi rõ kiểm tra nào và bao nhiêu dòng, Silver và Gold giữ bản cũ. Các phép đối soát (số dòng
  nguồn so với Silver, tổng tiền `orders`/`payments`/`refunds`, sổ kho cộng dồn so với `inventory`/`store_inventory`)
  là kiểm tra như mọi kiểm tra khác, truy vấn thẳng PostgreSQL phía nguồn. Kiểm tra sau khi dựng Gold chỉ ghi log,
  không chặn. Lịch sử kiểm tra nằm trong log Airflow, không ghi vào lakehouse.
- **Dữ liệu cá nhân không băm**. `customer_credentials` không bao giờ được nạp.
- Kênh, phương thức thanh toán, lý do huỷ, loại đổi/trả, giai đoạn giao là **cột chữ** trong fact/mart, không tách
  dimension.

## 6. Bronze: bản sao nguồn, chỉ ghi thêm

### 6.1 OLTP: 19 bảng

| Nhóm | Bảng |
|---|---|
| Dimension | `customers`, `categories`, `products`, `product_variants`, `cities`, `stores`, `delivery_staff` |
| Đơn, tiền | `orders`, `order_items`, `payments`, `refunds`, `order_status_history`, `coupons` |
| Giao, đổi trả, đánh giá | `shipments`, `return_requests`, `return_items`, `product_reviews` |
| Kho | `inventory_transactions`, `inbound_receipt_items` |

Không nạp (8): `carts`, `cart_items`, `wishlist_items` (không mart nào dùng), `coupon_redemptions` (`orders.coupon_id`
đủ), `shipment_attempts` (`shipments.attempt_count` đủ), `inbound_receipts` (ngày nhập lấy từ sổ kho), `inventory`,
`store_inventory` (tồn tính từ sổ kho; đối soát truy vấn thẳng nguồn). Cần thêm bảng thì thêm vào cấu hình rồi nạp
lại từ PostgreSQL.

Quy tắc:

- Tên `bronze.<tên bảng nguồn>`, cùng cột và kiểu với nguồn, thêm `_ingested_at` (timestamptz) và `_run_id`.
- Phân vùng theo ngày của `_ingested_at`.
- Nạp tăng dần theo cursor (`updated_at`, hoặc `created_at` với bảng chỉ ghi thêm như cấu hình hiện tại). Một dòng sửa
  nhiều lần có nhiều phiên bản trong Bronze.
- Extract ghi thẳng Bronze theo từng trang, không qua staging Parquet. Không có `raw_payload` cho OLTP.
- Đọc nguồn theo trang, ghi theo lô, không giữ cả bảng trong RAM.

### 6.2 Log: `bronze.web_events`

- Chỉ giữ dữ liệu thô: `raw_payload` (chuỗi JSON gốc), `event_at` (trường `timestamp` của sự kiện, UTC),
  `_ingested_at`, `_source` (`stream` hoặc `backfill`). Mọi việc tách trường, kiểm hợp đồng, bỏ trùng nằm ở Silver,
  nên hai đường nạp không lặp logic.
- Phân vùng theo ngày của `event_at` (thời điểm request), không theo lúc nhận: log lịch sử đến hôm nay nhưng thuộc
  ngày của nó.
- Hai đường nạp vào cùng bảng:
  - **Streaming** (`_source='stream'`): API → Kafka → Flink. Flink chỉ lấy `timestamp` từ JSON để điền `event_at`.
  - **Nạp file** (`_source='backfill'`): generator log ghi mỗi ngày một file Parquet (`raw_payload`, `event_at`) vào
    thư mục tạm; job `jobs.logs.load_backfill --path <thư mục>` append vào bảng. File tạm không thuộc lakehouse, xoá
    sau khi nạp.
- Sự kiện sai hợp đồng vẫn nằm ở đây; Silver loại chúng.
- Giữ vô thời hạn, không có hạn xoá.

## 7. Silver: trạng thái hiện tại, sạch

- Mỗi bảng Bronze OLTP có một bảng `silver.<tên bảng nguồn>`, merge theo khoá chính: lấy phiên bản mới nhất theo cursor
  rồi `_ingested_at`. Giữ đủ cột nguồn, chuẩn hoá kiểu.
- Chỉ merge các dòng Bronze của lần chạy này (`_run_id`), không quét lại toàn bộ Bronze.
- Kiểm tra chất lượng (khoá chính duy nhất, khoá ngoại tới được, ràng buộc nghiệp vụ, đối soát) chạy trước khi merge.
- `silver.web_events`: tách `raw_payload` của `bronze.web_events`; chỉ giữ sự kiện đúng hợp đồng (chấp nhận cả 1.0.0
  và 1.1.0; sự kiện 1.0.0 cũ có `session_id`, `order_number` null và không vào phễu); bỏ trùng theo `request.id`; có
  `session_id`, `actor_key` (nếu đã đăng nhập), hành động thương mại (`ecommerce_action`), `order_number` (chỉ có ở
  `checkout_submit` thành công), sản phẩm (slug hoặc public id, giữ nguyên như log), route, status, độ trễ, ngày và giờ theo giờ Việt
  Nam. Xử lý tăng dần theo các dòng Bronze mới (`_ingested_at`), tính lại những ngày `event_at` mà chúng chạm tới;
  không đọc cả bảng.

## 8. Gold: 28 bảng (dbt trên Trino)

### 8.1 Dimension (7)

| Bảng | Nguồn | Ghi chú |
|---|---|---|
| `dim_date` | sinh | ngày, thứ, tuần, tháng, quý, năm, ngày lễ/sale |
| `dim_city` | `cities` + seed SLA | **mới**: vùng giao (nội thành/liên tỉnh), `sla_days` |
| `dim_store` | `stores` | `store_key`, thành phố |
| `dim_product` | `products` + `categories` | danh mục |
| `dim_variant` | `product_variants` | size, màu, giá, giá vốn hiện tại |
| `dim_customer` | `customers` (role `customer`) | thành phố, ngày tạo, trạng thái, nguồn dữ liệu |
| `dim_staff` | `delivery_staff` + `customers` có role nhân viên xuất hiện trong `orders.staff_id` | thay `dim_delivery_staff`; `staff_role` = `delivery` / `sales`; khoá riêng cho từng nguồn |

### 8.2 Fact (6)

| Bảng | Grain | Nội dung chính |
|---|---|---|
| `fact_order` | đơn | kênh, cửa hàng, thành phố nhận, khách, nhân viên bán, phương thức thanh toán, coupon (mã, %, đợt sale), trạng thái, ngày đặt, ngày ghi nhận, ngày huỷ, lý do huỷ (từ `order_status_history`), subtotal, giảm giá, phí ship, tổng |
| `fact_order_item` | dòng đơn | biến thể, số lượng, đơn giá, giảm giá phân bổ, doanh thu, giá vốn, ngày ghi nhận |
| `fact_shipment` | lần giao của đơn | nhân viên giao, trạng thái, số lần giao, mốc dispatch/giao/thất bại, COD phải thu/đã thu, `is_on_time` tính theo SLA |
| `fact_return` | dòng yêu cầu đổi/trả | loại (đổi/hoàn), trạng thái, số lượng, số lượng chấp nhận, tiền hoàn yêu cầu/đã trả, ngày yêu cầu, ngày xử lý |
| `fact_inventory_daily` | ngày × vị trí (kho trung tâm/cửa hàng) × biến thể | đầu ngày, nhập, bán online, bán POS, chuyển đi, chuyển đến, trả về, điều chỉnh, cuối ngày, giá trị theo giá vốn. Dựng từ `inventory_transactions` |
| `fact_review` | đánh giá | sản phẩm, khách, số sao, trạng thái duyệt, ngày, giao đúng hạn hay trễ (nối qua dòng đơn → đơn → shipment) |

Log không có fact: mart log đọc thẳng `silver.web_events`.

### 8.3 Mart theo nhóm người xem (15)

**Vận hành**

| Mart | Grain | Chỉ số |
|---|---|---|
| `mart_delivery_daily` | ngày kết thúc giao × thành phố × nhân viên giao × phương thức thanh toán | số shipment, thành công, thất bại (boom), có giao lại, đúng hạn; thời gian đặt→giao và dispatch→giao (tổng + số đếm); COD phải thu/đã thu |
| `mart_fulfilment_backlog_daily` | cuối ngày × giai đoạn (chờ dispatch / đang giao) × nhóm tuổi đơn | số đơn, tuổi trung bình, COD đang chờ thu |
| `mart_returns_daily` | ngày yêu cầu × biến thể × đổi/hoàn | số yêu cầu, số lượng, số lượng chấp nhận, tiền hoàn yêu cầu/đã trả, thời gian xử lý |

Lý do trả hàng **không** vào grain: nguồn là chữ tự do khách tự gõ.

**Kho**

| Mart | Grain | Chỉ số |
|---|---|---|
| `mart_inventory_current` | vị trí × biến thể (ngày mới nhất) | tồn, giá trị, bán trung bình/ngày (28 ngày), số ngày đủ bán, ngày bán gần nhất. Không có cờ đánh giá |
| `mart_inventory_health_daily` | ngày × vị trí × danh mục | số biến thể còn hàng/hết hàng, tổng tồn, giá trị, số lượng và giá trị nhập (`inbound_receipt_items.total_cost_vnd`), số lượng bán |

**Kinh doanh**

| Mart | Grain | Chỉ số |
|---|---|---|
| `mart_orders_daily` | ngày đặt × kênh × cửa hàng × thành phố × thanh toán | đơn đặt (số, giá trị), trong đó hoàn tất / huỷ / thất bại / đang xử lý |
| `mart_cancellations_daily` | ngày đặt × kênh × lý do × thanh toán | số đơn huỷ, giá trị |
| `mart_sales_daily` | ngày ghi nhận × kênh × cửa hàng × thành phố | đơn ghi nhận, số món, doanh thu gộp, giảm giá, giá vốn, lãi gộp, hoàn tiền, doanh thu thuần |
| `mart_product_sales_daily` | ngày ghi nhận × kênh × cửa hàng × sản phẩm | số lượng, doanh thu, giá vốn, lãi gộp, số lượng trả |

**Marketing**

`mart_product_demand_daily` bị bỏ: preset log đặt lượt xem tỷ lệ thuận với lượt mua, nên tỷ lệ xem/mua của mọi
sản phẩm xấp xỉ nhau và mart không trả lời được câu hỏi của nó.

| Mart | Grain | Chỉ số |
|---|---|---|
| `mart_web_funnel_daily` | ngày | số phiên ở mỗi bước (định nghĩa bên dưới); kèm số request mỗi bước làm cột tham khảo; tổng số đơn online trong ngày và số đơn khớp được với phiên |
| `mart_coupon_daily` | ngày đặt × coupon | số đơn dùng, tiền giảm, giá trị đơn, số đơn dùng coupon bị huỷ; mang sẵn mã, %, đợt sale |
| `mart_reviews_daily` | ngày đánh giá × sản phẩm × đúng hạn/trễ | số đánh giá, sao trung bình, phân bố 1–5 sao, số bị từ chối |
| `mart_customer_cohort_monthly` | tháng mua đầu × số tháng sau đó | số khách còn mua, số đơn, doanh thu thuần |
| `mart_customer_value` | khách | thành phố, ngày mua đầu/cuối, số đơn, doanh thu thuần, giá trị đơn trung bình, tỷ trọng online/POS, đã mua lại chưa |

Các bước của `mart_web_funnel_daily`, ngày theo giờ Việt Nam của request:

| Bước | Đếm |
|---|---|
| Xem sản phẩm | số phiên có `product_detail` |
| Thêm giỏ | số phiên có `cart_item_set` |
| Checkout | số phiên có `checkout_quote` hoặc `checkout_submit` |
| Đặt đơn | số phiên có `checkout_submit` thành công mà `order_number` khớp một đơn online trong `fact_order` |

Hai cột đối chiếu (tổng đơn online, đơn khớp được với phiên) cho biết log có bị hụt không: khớp thấp hơn hẳn tổng tức
là log mất. Đơn POS không thuộc phễu.

Mart khách hàng chỉ dùng `customer_key`, không mang tên/SĐT.

**Cửa hàng**: `mart_pos_hourly` (ngày × cửa hàng × giờ × nhân viên bán: số đơn, doanh thu, số món). Ngoài ra dùng lại
`mart_sales_daily`, `mart_product_sales_daily`, `mart_inventory_current` lọc theo `store_key`. Đơn online không thuộc
cửa hàng nào. Quản lý cửa hàng chỉ thấy `store_key` của mình.

**Điều hành**: không có mart riêng; KPI cộng từ các mart trên.

## 9. Ai đọc gì

| Người đọc | Đọc |
|---|---|
| Superset | mart Gold, đọc thẳng bảng hiện tại (không ghim snapshot) |
| Giờ cập nhật dữ liệu | thời điểm commit mới nhất của bảng (metadata snapshot Iceberg) |

## 10. Bỏ đi

- Tầng landing và `landing.access_logs`; staging Parquet `landing/polars-batch/...`.
- Báo cáo realtime: endpoint realtime trong API (`realtime_service.py`), panel realtime trên Streamlit.
- Namespace `system` (bảng kết quả kiểm tra OLTP/log, `analytics_publications`, `logs_analytics_publications`), namespace
  `quarantine`, cơ chế ghim snapshot (`publication.py`, `pinned()` trong dbt, đọc theo version ở Superset/API).
- Bản tính mart log bằng Polars (`logs/polars/marts.py`), `fact_web_events`, `int_resolved_events`.
- Gold cũ: `fact_inventory_current`, `mart_logistics_performance`, `mart_cod_region_daily`,
  `mart_operations_summary`, `mart_product_returns`, `mart_inventory_summary`, `mart_store_overview`,
  `mart_marketing_summary`, `mart_admin_overview`, `mart_coupon_usage`, `mart_product_review_summary`,
  `mart_inventory_health`, `mart_hourly_route_metrics`, `mart_daily_product_demand`, `mart_marketing_funnel_daily`.
- Đổi tên: `fact_return_exchange` → `fact_return`, `fact_inventory_daily_snapshot` → `fact_inventory_daily`,
  `mart_order_daily` → `mart_orders_daily`, `dim_delivery_staff` → `dim_staff`.
- Băm dữ liệu cá nhân và biến môi trường salt.

## 11. Thay đổi kéo theo ngoài lakehouse

- **Hợp đồng log** `ecommerce.access` 1.0.0 → 1.1.0: thêm hai trường tuỳ chọn `session.id` và
  `ecommerce.order_number` (chỉ ở `checkout_submit` thành công); sự kiện 1.0.0 cũ vẫn hợp lệ. API và generator log
  cùng ghi 1.1.0, nên log streaming từ API thật cũng vào phễu và nối với đơn thật.
- **API**: middleware cấp cookie phiên ngẫu nhiên HttpOnly; phiên giữ nguyên qua đăng nhập, hết sau 30 phút không có
  request (mỗi request gia hạn). Handler checkout gán `request.state.order_number` sau khi tạo đơn; middleware log đọc
  nó như đang đọc `actor_key`. `ACCESS_LOG_SCHEMA_VERSION` = `1.1.0`. Bỏ vai trò `system_admin` khỏi analytics cùng endpoint đối soát, http-metrics và realtime.
  Phần đọc publication (`analytics/reader.py`) bỏ.
- **Streamlit, kịch bản "traffic liên tục"** (trang Scenarios, dựa trên `ui/scenarios/purchase.py` và worker có sẵn):
  bơm event liên tục qua API thật để demo đường streaming. Mỗi phiên ảo một client HTTP riêng (cookie phiên riêng);
  trộn phiên theo L1 của preset log (khoảng 41 phiên chỉ xem : 1 phiên mua), lượt xem theo L2, giãn cách theo L3; 7,5%
  phiên không mua đăng nhập và thêm giỏ; phiên mua dùng lại luồng `purchase` (tạo đơn thật, trừ tồn kho). Người demo
  chỉnh số phiên song song và bấm Bắt đầu/Dừng. Không có panel hiển thị: người demo chứng minh dữ liệu đang chảy bằng
  câu SQL trên Trino hoặc SQL Lab của Superset (`bronze.web_events` với `_source='stream'`), và sau lần chạy pipeline
  lô thì xem phiên/đơn của ngày demo trong `mart_web_funnel_daily` (đã chốt).
- **Superset**: dataset đọc thẳng bảng Gold, bỏ ghim publication (`infrastructure/superset/sales.py`).
- **Generator OLTP**: thời gian giao theo vùng (nội thành/liên tỉnh) thay cho đều 2–6 ngày.
- **Generator log** (thiết kế riêng): sinh `session.id` theo quy tắc 30 phút; đọc đơn online từ OLTP và dựng phiên
  quanh mỗi đơn, phiên kết thúc bằng `checkout_submit` mang `order_number` của đơn đó; cửa sổ là mùa sale
  (11–12/2025) để phễu có biến động; ghi file Parquet theo ngày cho job nạp file.
- **Airflow**: DAG OLTP = nạp Bronze → kiểm tra → merge Silver → dbt Gold; DAG log = Silver log tăng dần → dbt mart
  log. Nạp file lịch sử là lệnh chạy tay một lần, không có DAG. Bỏ bước publish.

## 12. Để người làm quyết

- Kiểu partition cụ thể của từng bảng Gold, cách dbt xoá-và-ghi lại những ngày bị chạm.
- Tên cột cụ thể trong từng bảng, miễn theo `snake_case` và đơn vị `_vnd`.
- Cách lưu SLA cho `dim_city` (seed dbt hoặc cấu hình) — đề xuất seed dbt.
- Nhóm tuổi đơn trong backlog — đề xuất bám SLA: ≤ 2 ngày, 3–5 ngày, > 5 ngày.
- Cohort và giá trị khách tính trên đơn đã ghi nhận doanh thu (đề xuất, cho khớp với `mart_sales_daily`);
  khách vãng lai POS (không có `customer_id`) không vào hai mart này.
- Ngày kết thúc giao trong `mart_delivery_daily` = `delivered_at` hoặc `failed_at`; "có giao lại" =
  `attempt_count > 1`.

## 13. Đối chiếu với `dev` (commit `47cd42c`, 2026-11-20)

Mục 1 mô tả lakehouse lúc viết thiết kế. Sau đó PR #21 và #22 đã refactor pipeline lô OLTP. Bảng dưới đây so từng
điểm của thiết kế với code hiện tại.

**Đã khớp với thiết kế, giữ nguyên**

| Điểm | Code hiện tại |
|---|---|
| Bỏ staging Parquet | `oltp/polars/pipeline.py` ghi từng trang đọc từ PostgreSQL thẳng vào Bronze/Silver |
| Bỏ băm PII | `privacy.py` đã xoá, không còn salt |
| Đọc nguồn theo trang, trong một giao dịch đọc | `source_pages` + `source_read_transaction` |
| Một job lô cho cả OLTP | `jobs.oltp.run_batch_pipeline`, một task Airflow |

**Khác thiết kế, phải đổi**

| Điểm | Code hiện tại | Theo thiết kế |
|---|---|---|
| Kiểm tra chất lượng | `audit` chạy **sau** khi đã merge Silver và dựng Gold; Superset không thấy dữ liệu lỗi chỉ nhờ ghim publication | Kiểm tra chạy **trước** khi merge Silver; trượt thì task lỗi, Silver và Gold giữ bản cũ. Vì vậy không cần publication |
| Publication | `system.analytics_publications` (v4); Superset (`infrastructure/superset/sales.py`) và API (`analytics/reader.py`) đọc theo snapshot ghim | Bỏ; Superset đọc thẳng bảng Gold hiện tại |
| Trạng thái chạy | `system.batch_runs`: STARTED → INGESTED → BUILT → VALIDATED → PUBLISHED, chạy tiếp từ bước lỗi | Bỏ. Airflow retry chạy lại cả lần nạp; chạy lại an toàn vì Silver merge theo khoá chính lấy phiên bản mới nhất, dbt xoá-và-ghi lại những ngày bị chạm |
| Kết quả kiểm tra | `system.data_quality_results`, `system.logs_quality_results` | Chỉ trong log Airflow |
| Quarantine | `quarantine.silver_oltp_violations` | Bỏ; dòng vi phạm làm task lỗi |
| Bảng nguồn | 27 bảng, mỗi bảng có danh sách cột Bronze/Silver (`utils/config.py`) | 19 bảng (mục 6.1). Danh sách cột chỉ cần để không nạp cột bí mật; Bronze giữ đủ cột nguồn còn lại |
| Cột lineage Bronze | 7 cột `_source_*`, `_ingested_at_utc`, `_source_bronze_run_id` | `_ingested_at`, `_run_id` |
| Gold | 27 model dbt OLTP + 4 model log kiểu cũ | 28 bảng mới (mục 8), danh sách bỏ/đổi tên ở mục 10 |
| Log | Flink ghi `landing.access_logs`; mart log tính bằng Polars và dbt; hợp đồng API 1.0.0 | `bronze.web_events` thô cho cả streaming và nạp file, `silver.web_events`, hợp đồng 1.1.0 (mục 6.2, 7, 11) |
| Realtime | `analytics/realtime_service.py` | Bỏ |

**Phần đã có sẵn (không cần làm lại)**

- Generator log (`generator/src/datagen/logs/`, lệnh `datagen logs`) đã ghi hợp đồng 1.1.0 và file Parquet
  `(raw_payload, event_at)` theo ngày, đúng đầu vào của `jobs.logs.load_backfill` ở mục 6.2.

**Thứ tự refactor đề xuất** (mỗi bước một PR, sau bước nào pipeline cũng chạy được)

1. OLTP Bronze/Silver: 19 bảng, kiểm tra trước merge, bỏ quarantine, `batch_runs` và bảng kết quả kiểm tra.
2. Gold OLTP 28 bảng; Superset và API đọc thẳng bảng Gold; bỏ publication.
3. Log: hợp đồng 1.1.0 phía API, Flink ghi `bronze.web_events`, `silver.web_events`, `jobs.logs.load_backfill`,
   `mart_web_funnel_daily`; bỏ landing, realtime và mart log cũ.
4. Kịch bản traffic liên tục trên Streamlit.
