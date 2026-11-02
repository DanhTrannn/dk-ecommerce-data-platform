# Danh mục chỉ số đang triển khai

Tài liệu mô tả công thức trong pipeline và API hiện tại. Đây là định nghĩa tính toán, không phải kết quả benchmark hay cam kết SLA. Dữ liệu tổng hợp cần được ghi rõ khi dùng để minh họa kết quả đồ án.

Nguồn chính: [Gold OLTP](../../pipelines/src/lakehouse/oltp/gold/), [Gold logs](../../pipelines/src/lakehouse/logs/gold.py), [analytics service](../../services/ecommerce-api/app/modules/analytics/service.py).

Vai trò dashboard, API và phạm vi truy cập được mô tả trong [kiến trúc serving](SYSTEM_ARCHITECTURE_VISUAL.md#5-serving-và-phân-quyền).

## 1. Quy ước đọc số liệu

- Tiền là số nguyên VND. Lợi nhuận trong Gold là doanh thu trừ giá vốn, chưa trừ đầy đủ chi phí vận hành, thuế và quảng cáo.
- Phải kiểm tra grain trước khi SUM hoặc JOIN. `fact_order` có một dòng/đơn; `fact_order_item` có một dòng/chi tiết đơn.
- Đếm đơn toàn hệ thống bằng `COUNT(DISTINCT order_id)` trên fact. Một đơn có nhiều nhóm sản phẩm sẽ xuất hiện ở nhiều dòng `mart_sales_daily`.
- `data_source=lakehouse`. Khi Trino lỗi, API có thể giữ kết quả OLAP đã đọc thành công trong thời hạn cache và đánh dấu degraded; không tổng hợp MySQL để thay thế.
- Ngày hôm nay ở chỉ số cửa hàng dùng Asia/Ho_Chi_Minh. Log Flink mới và Spark dùng UTC. Các partition log cũ theo giờ Việt Nam cần được replay/migrate rõ ràng trước khi so sánh chuỗi thời gian.

## 2. Doanh thu, giá vốn và lợi nhuận

Publication schema 3 dùng chung transformation cho batch và CDC. Tiền là số nguyên VND. Ngày báo cáo là ngày đặt hàng theo Asia/Ho_Chi_Minh; giao hàng hoặc hoàn tiền về sau có thể làm thay đổi kết quả của ngày đặt hàng cũ.

| Chỉ số | Công thức hiện tại | Nguồn / grain |
|---|---|---|
| GMV executive / tổng giá trị đơn | SUM `order_total_vnd` = SUM `orders.total_vnd`, gồm mọi trạng thái và phí vận chuyển | `fact_order`, một dòng/đơn; chỉ số giá trị đơn đặt, không phải doanh thu đã ghi nhận |
| Doanh thu hàng hóa trước hoàn tiền | Tổng giá trị dòng hàng trừ giảm giá được phân bổ; chỉ ghi nhận đơn `delivered`, `completed`, `returned` | `gross_revenue_vnd` ở item, order và sales mart; không gồm phí vận chuyển |
| Giảm giá từng dòng | Phân bổ theo giá trị dòng hàng trước giảm giá; hiệu hai phần nguyên của tổng lũy kế theo `order_item_id` | Tổng phân bổ đúng giảm giá cấp đơn, không phát sinh sai số cộng VND |
| Hoàn tiền hàng hóa | Refund `succeeded`, nối payment để xác định đơn; ưu tiên các dòng trả hoàn tất và kiểm định đạt, phần còn lại phân bổ theo giá trị hàng hóa chưa hoàn; tổng giới hạn bởi tiền hàng của đơn | `refunded_amount_vnd`; phần vượt giá trị hàng hóa không được trừ tiếp vào doanh thu hàng hóa |
| Doanh thu thuần | Doanh thu hàng hóa trước hoàn tiền trừ hoàn tiền hàng hóa thành công | `net_revenue_vnd`, cùng định nghĩa ở fact, mart, executive, admin và store |
| Giá vốn ghi nhận | `(quantity - completed_passed_return_quantity) × order_items.cost_price_vnd` của đơn đã ghi nhận doanh thu | Dùng giá vốn snapshot lúc đặt hàng; thiếu giá vốn thì chặn publication, không lấy giá vốn hiện tại thay thế |
| Lợi nhuận gộp | Doanh thu thuần trừ giá vốn ghi nhận | `net_profit_vnd` / `gross_profit_vnd`; chưa trừ đầy đủ chi phí vận hành, thuế và quảng cáo |
| Biên lợi nhuận | Lợi nhuận gộp / doanh thu thuần × 100; API trả 0 khi mẫu số không dương | Executive, admin và sales mart |
| Total orders | COUNT DISTINCT order_id, gồm mọi trạng thái | `fact_order`; không cộng tổng đơn qua danh mục |
| AOV executive | Doanh thu thuần / số đơn đã giao, gồm đơn có trả hàng; 0 khi không có mẫu số | `is_revenue_recognized` ở fact_order |
| Sales trend | Tổng net revenue, COGS và profit của mart theo ngày; số đơn lấy COUNT DISTINCT từ fact | Cùng công thức với executive nhưng có bộ lọc kỳ |
| Tỷ lệ giao thất bại | Số đơn `failed_delivery` / tổng đơn × 100 | `fact_order` |
| Tỷ lệ yêu cầu trả hàng | COUNT DISTINCT return_id không cancelled / tổng đơn × 100 | Silver return requests; chưa phải tỷ lệ số đơn trả distinct |

Đơn chờ thanh toán, đã xác nhận hoặc đang giao chưa tạo doanh thu hàng hóa. Đơn đã trả vẫn giữ doanh thu ban đầu; hoàn tiền thành công được trừ riêng để xử lý trả một phần. Giá vốn được hoàn nhập theo số lượng trả hoàn tất và kiểm định đạt.

Tiền thanh toán thành công và tiền hoàn cho khách là dòng tiền riêng, có thể bao gồm vận chuyển hoặc hoàn tiền cho đơn hủy. Không dùng tổng payment thay cho doanh thu hàng hóa. Tổng tiền thu hộ COD đang giao lấy `order_total_vnd`, không lấy doanh thu đã ghi nhận.

Kiểm tra trước publication đối chiếu tổng doanh thu trước hoàn, hoàn tiền, doanh thu thuần, giá vốn và lợi nhuận giữa item → order → sales mart. Đơn vị là VND; không dùng tổng số đơn theo danh mục để đối soát số đơn toàn hệ thống.

## 3. Chỉ số vận hành từ Lakehouse đã công bố

| Chỉ số | Công thức / phạm vi |
|---|---|
| Doanh thu/số đơn cửa hàng hôm nay | Tổng `fact_order.net_revenue_vnd` / đếm đơn đặt của store trong ngày Việt Nam; chỉ doanh thu được áp dụng điều kiện ghi nhận |
| Tồn kho thấp cửa hàng | Số dòng `fact_inventory_current` có `is_low_stock`, tức `on_hand_quantity <= 5`, tại store được phép |
| Giá trị tồn kho | Tổng `on_hand × variant.cost_price_vnd` ở kho trung tâm và cửa hàng |
| Số lượng tồn | Tổng on_hand theo từng nhóm kho/cửa hàng |
| Số SKU hết hàng | Số variant distinct xuất hiện với on_hand=0 ở kho hoặc cửa hàng; không phải số cặp SKU–địa điểm |
| Tốc độ xuất hàng | Số lượng bán trong 60 phút / 60; thời gian dự kiến hết hàng lấy tồn hiện tại chia tốc độ |
| Đơn chờ xử lý | Query paid/confirmed/processing; processing không nằm trong CHECK trạng thái orders hiện tại, nên dữ liệu đúng DDL chỉ đóng góp paid/confirmed |
| Đơn chờ quá 2 giờ | Đếm paid/confirmed với created_at trước ngưỡng 2 giờ |
| Thời gian chờ trung bình | Trung bình số giờ từ created_at tới hiện tại của đơn paid/confirmed/processing |
| Giao vận có vấn đề | Trường API `shipping_sla_violations_count` hiện đếm shipment attempt_count > 1 hoặc status failed; chưa phải so sánh deadline SLA |
| Tỷ lệ COD lỗi theo vùng | Shipment COD failed / tổng shipment COD trong nhóm địa chỉ; nhóm vùng dựa trên từ khóa địa chỉ |
| Tiền COD đang giao | Tổng order total của COD có shipment dispatched/in_transit; chưa phải số dư tiền thực tế shipper đang giữ |
| Coupon burn rate | Số redemption trong 15 phút / 15; phần trăm sử dụng = used_count / total_usage_limit, không tự đặt limit khi coupon không có cap |
| Đánh giá xấu gần đây | Đếm rating <= 2 trong 45 phút, nhóm theo product |

Mục tiêu 15 triệu/ngày, các mốc doanh thu chia sẵn theo giờ, biên lợi nhuận nội suy, tỷ lệ chia tiền cho nhà vận chuyển và nhãn “viral” áp cứng đã được bỏ. `target_achievement_percent`, run-rate, margin-erosion và channel-growth không có dữ liệu khi chưa có cách tính từ nguồn phù hợp; UI ẩn các biểu đồ tương ứng.

Một số trường API vẫn optional để tương thích giao diện. Khi thiếu dữ liệu, API trả `null` hoặc danh sách rỗng; target là `null` và UI ẩn phần chỉ tiêu. Các trường SLA/COD cần được diễn giải theo công thức trong tài liệu này, không chỉ theo tên trường.

## 4. Access log và hành vi web

| Chỉ số | Công thức / grain |
|---|---|
| Total requests | Số dòng sự kiện; mart theo event_date × giờ của event_ts trong Spark × route × method; xem quy tắc múi giờ ở mục 1 |
| Duration | `event_duration_ns / 1_000_000`, đơn vị ms |
| Error rate | `(4xx + 5xx) / total_requests × 100` |
| Avg / p50 / p95 / p99 | AVG duration và `percentile_approx` theo nhóm route/giờ |
| Slow request | duration_ms >= 1000 |
| Product demand | Đếm action product_detail/cart_add/cart_remove/wishlist_add/checkout_quote theo ngày × product |
| Funnel views | Action product_detail/catalog_search hoặc route sản phẩm |
| Funnel cart / checkout | Action hoặc route cart / checkout tương ứng |
| Funnel purchase | Action order_complete/order_confirm_internal hoặc checkout_submit thành công |
| Conversion marketing | Purchase events / view events × 100, cửa sổ 30 ngày |

Funnel hiện đếm **sự kiện**, không phải unique visitor hoặc chuỗi session đã gắn kết. Nhiều request của cùng người có thể được tính nhiều lần; tỷ lệ không được hiểu là xác suất một người mua hàng. `total_visitors` là tên API cho lượt sự kiện đầu phễu. Cột `success_2xx_count` ở mart hiện cộng `is_success`, mà logic bao gồm cả phản hồi <400.

Marketing không fallback MySQL khi thiếu log. Số sự kiện không mặc định là số khách duy nhất hoặc số giao dịch; trường total_visitors hiện là số sự kiện đầu phễu.

## 5. Chỉ số kỹ thuật DE

| Chỉ số | Cách lấy / giới hạn |
|---|---|
| Row count từng tầng | COUNT theo table/run_id hoặc snapshot cụ thể; phân biệt Bronze history và Silver current state |
| Source–Silver variance | `abs(source-target)/abs(source) × 100`; source=0/target!=0 được tính 100%; ngưỡng gate mặc định 0,05% |
| Gold parity | Count/tiền, PK, FK, lineage và PII token được kiểm tra bởi quality gate |
| Gold freshness | Thời gian hiện tại trừ `published_at` của publication; không phải tuổi của từng bản ghi nguồn |
| CDC event count | Đếm theo table_name, operation, event_date; số event khác số bản ghi hiện hữu ở nguồn |
| CDC traceability | event_id, before/after, binlog và Kafka offset; kiểm tra replay/truy vết từ Bronze |
| Processing duration / throughput | Lấy từ log thời gian và khối lượng lần chạy; repo không ấn định con số benchmark |

Ở CDC mode, API đánh giá heartbeat, trạng thái xử lý, độ trễ sự kiện và variance; ngưỡng hiển thị tuổi publication là 5 phút. Ở batch mode, API không đòi heartbeat CDC và dùng ngưỡng publication 1.500 phút cho lịch hằng ngày. Đây là ngưỡng cảnh báo, chưa phải SLA đã đo.

Đối soát dashboard dùng hai metric: `total_orders` so COUNT đơn MySQL với COUNT DISTINCT order_id Gold; `order_total_vnd` so SUM orders.total_vnd với SUM fact_order.order_total_vnd, đều gồm mọi trạng thái. Chênh lệch = abs(OLTP−Gold)/OLTP × 100, làm tròn hai chữ số; OLTP=0 thì 0% nếu Gold=0, ngược lại 100%.

UI vẽ từng metric riêng để số đơn không dùng chung thang với doanh thu. Số đơn có đơn vị đơn; tiền có trục VND, nhãn rút gọn và tooltip giá trị đầy đủ; badge dùng variance_percent từ API. Khi không có nguồn đối soát mới, không tự tạo variance để thay thế.

## 6. Cách trình bày kết quả

Ghi rõ dataset, dữ liệu tổng hợp hay phát sinh thật, thời gian chạy, tầng/bảng, bộ lọc, grain, công thức, đơn vị và nguồn response. Các tài liệu A–D mô tả nghiệp vụ và giới hạn, không cung cấp bộ số liệu đo để thay thế truy vấn thực tế.

## 7. Ngày quan sát tồn kho

`fact_inventory_current` và snapshot tồn kho mới dùng ngày quan sát thực tế theo Asia/Ho_Chi_Minh, đối chiếu với `_gold_ingested_at`. Ngày logical của Airflow dùng để truy vết kỳ xử lý, không được dùng để ghi tồn hiện tại đè lên ngày quá khứ. Lịch sử snapshot của các ngày khác được giữ nguyên.
