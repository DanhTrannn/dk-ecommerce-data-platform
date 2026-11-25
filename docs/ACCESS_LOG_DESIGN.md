# Thiết kế luồng access log

Thiết kế đã duyệt (2026-11-13), **chưa xây**. Luồng cũ (Flink ghi `landing.access_logs`, mart log tính hai lần) và các file SQL của nó đã gỡ. Khi xây, làm theo cách của pipeline OLTP: mỗi tầng chạy lại an toàn, task theo bảng hoặc model.

## Mục đích

Gom hai loại dữ liệu đến bằng hai kiểu nạp: bảng quan hệ OLTP nạp theo lô, và sự kiện log JSON bán cấu trúc nạp bằng streaming (traffic thật qua API) và bằng file (log lịch sử do generator sinh). Hai loại gặp nhau ở `mart_web_funnel_daily`, nối phiên web với đơn online thật.

## Hợp đồng log `ecommerce.access` 1.1.0

- Thêm hai trường tuỳ chọn: `session.id` và `ecommerce.order_number` (chỉ ở `checkout_submit` thành công). Sự kiện 1.0.0 vẫn hợp lệ nhưng không vào phễu.
- Generator log (`datagen logs`) đã ghi 1.1.0. API còn ghi 1.0.0 (`ACCESS_LOG_SCHEMA_VERSION` trong `access_logging.py`) và cần:
  - middleware cấp cookie phiên ngẫu nhiên HttpOnly, giữ qua đăng nhập, hết sau 30 phút không có request;
  - handler checkout gán `request.state.order_number` để middleware log đọc, như đang đọc `actor_key`.

## Tầng dữ liệu

| Bảng | Nội dung |
|---|---|
| `bronze.web_events` | Chỉ dữ liệu thô: `raw_payload` (JSON gốc), `event_at` (trường `timestamp`, UTC), `_ingested_at`, `_source` (`stream` hoặc `backfill`). Phân vùng theo ngày của `event_at`. Giữ cả sự kiện sai hợp đồng; giữ vô thời hạn |
| `silver.web_events` | Tách `raw_payload`; chỉ giữ sự kiện đúng hợp đồng 1.0.0 hoặc 1.1.0; bỏ trùng theo `request.id`; có `session_id`, `actor_key`, `ecommerce_action`, `order_number`, sản phẩm, route, status, độ trễ, ngày và giờ Việt Nam. Tăng dần theo `_ingested_at` của Bronze, tính lại các ngày `event_at` bị chạm |
| `gold.mart_web_funnel_daily` | Grain ngày (giờ Việt Nam): số phiên mỗi bước, số request mỗi bước để tham khảo, tổng đơn online trong ngày và số đơn khớp được với phiên |

Mọi việc tách trường, kiểm hợp đồng, bỏ trùng nằm ở Silver, nên hai đường nạp không lặp logic.

**Các bước phễu**

| Bước | Đếm |
|---|---|
| Xem sản phẩm | Số phiên có `product_detail` |
| Thêm giỏ | Số phiên có `cart_item_set` |
| Checkout | Số phiên có `checkout_quote` hoặc `checkout_submit` |
| Đặt đơn | Số phiên có `checkout_submit` thành công mà `order_number` khớp một đơn online trong `fact_order` |

Số đơn khớp thấp hơn hẳn tổng đơn online nghĩa là log bị hụt. Đơn POS không thuộc phễu.

## Đường nạp

- **Streaming** (`_source='stream'`): API → Kafka → Flink → `bronze.web_events`. Flink chỉ lấy `timestamp` để điền `event_at`.
- **Nạp file** (`_source='backfill'`): generator ghi mỗi ngày một file Parquet `(raw_payload, event_at)`; một lệnh chạy tay append vào Bronze, không cần DAG. File tạm không thuộc lakehouse.
- **Airflow:** một DAG log dựng Silver tăng dần rồi dbt mart log.

## Demo streaming

Kịch bản "traffic liên tục" trên trang Scenarios của Streamlit, dựa trên `ui/scenarios/purchase.py` và worker có sẵn:

- mỗi phiên ảo một client HTTP và cookie riêng;
- trộn khoảng 41 phiên chỉ xem cho 1 phiên mua, theo preset log; 7,5% phiên không mua vẫn đăng nhập và thêm giỏ;
- phiên mua dùng luồng `purchase` (tạo đơn thật, trừ tồn kho);
- người demo chỉnh số phiên song song và bấm Bắt đầu/Dừng, rồi kiểm tra bằng SQL trên `bronze.web_events` (`_source='stream'`) và `mart_web_funnel_daily`.

## Không làm

`mart_product_demand_daily`: preset log đặt lượt xem tỷ lệ thuận lượt mua, nên tỷ lệ xem/mua của mọi sản phẩm gần như nhau và mart không trả lời được câu hỏi của nó.
