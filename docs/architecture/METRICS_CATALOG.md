# Danh mục chỉ số đang triển khai

Tài liệu mô tả công thức trong pipeline và API hiện tại. Đây là định nghĩa tính toán, không phải kết quả benchmark hay cam kết SLA. Dữ liệu tổng hợp cần được ghi rõ khi dùng để minh họa kết quả đồ án.

Nguồn chính: [Gold OLTP](../../pipelines/src/lakehouse/oltp/gold/), [Gold logs](../../pipelines/src/lakehouse/logs/gold.py), [analytics service](../../services/ecommerce-api/app/modules/analytics/service.py).

Vai trò dashboard, API và phạm vi truy cập được mô tả trong [kiến trúc serving](SYSTEM_ARCHITECTURE_VISUAL.md#5-serving-và-phân-quyền).

## 1. Quy ước đọc số liệu

- Tiền là số nguyên VND. Lợi nhuận trong Gold là doanh thu trừ giá vốn, chưa trừ đầy đủ chi phí vận hành, thuế và quảng cáo.
- Phải kiểm tra grain trước khi SUM hoặc JOIN. `fact_order` có một dòng/đơn; `fact_order_item` có một dòng/chi tiết đơn.
- Executive và xu hướng cộng `total_orders` từ `mart_order_daily`; pipeline đếm từ một dòng/đơn trước khi tổng hợp. Một đơn có nhiều nhóm sản phẩm sẽ xuất hiện ở nhiều dòng `mart_sales_daily`.
- `data_source=lakehouse`. Khi Trino lỗi, API có thể giữ kết quả OLAP đã đọc thành công trong thời hạn cache và đánh dấu degraded; không tổng hợp MySQL để thay thế.
- Ngày hôm nay ở chỉ số cửa hàng dùng Asia/Ho_Chi_Minh. Log Flink mới và Spark dùng UTC. Các partition log cũ theo giờ Việt Nam cần được replay/migrate rõ ràng trước khi so sánh chuỗi thời gian.

## 2. Doanh thu, giá vốn và lợi nhuận

Publication schema 3 công bố kết quả batch; CDC hiện chỉ ghi audit Bronze. Tiền là số nguyên VND. Ngày báo cáo là ngày đặt hàng theo Asia/Ho_Chi_Minh; giao hàng hoặc hoàn tiền về sau có thể làm thay đổi kết quả của ngày đặt hàng cũ.

| Chỉ số | Công thức hiện tại | Nguồn / grain |
|---|---|---|
| GMV executive / tổng giá trị đơn | SUM `order_total_vnd` = SUM `orders.total_vnd`, gồm mọi trạng thái và phí vận chuyển | Executive đọc `mart_order_daily.order_total_vnd`; pipeline tổng hợp từ fact một dòng/đơn. Chỉ số giá trị đơn đặt, không phải doanh thu đã ghi nhận |
| Doanh thu hàng hóa trước hoàn tiền | Tổng giá trị dòng hàng trừ giảm giá được phân bổ; chỉ ghi nhận đơn `delivered`, `completed`, `returned` | `gross_revenue_vnd` ở item, order và sales mart; không gồm phí vận chuyển |
| Giảm giá từng dòng | Phân bổ theo giá trị dòng hàng trước giảm giá; hiệu hai phần nguyên của tổng lũy kế theo `order_item_id` | Tổng phân bổ đúng giảm giá cấp đơn, không phát sinh sai số cộng VND |
| Hoàn tiền hàng hóa | Refund `succeeded`, nối payment để xác định đơn; ưu tiên các dòng trả hoàn tất và kiểm định đạt, phần còn lại phân bổ theo giá trị hàng hóa chưa hoàn; tổng giới hạn bởi tiền hàng của đơn | `refunded_amount_vnd`; phần vượt giá trị hàng hóa không được trừ tiếp vào doanh thu hàng hóa |
| Doanh thu thuần | Doanh thu hàng hóa trước hoàn tiền trừ hoàn tiền hàng hóa thành công | `net_revenue_vnd`, cùng định nghĩa ở fact, mart, executive, admin và store |
| Giá vốn ghi nhận | `(quantity - completed_passed_return_quantity) × order_items.cost_price_vnd` của đơn đã ghi nhận doanh thu | Dùng giá vốn snapshot lúc đặt hàng; thiếu giá vốn thì chặn publication, không lấy giá vốn hiện tại thay thế |
| Lợi nhuận gộp | Doanh thu thuần trừ giá vốn ghi nhận | `net_profit_vnd` / `gross_profit_vnd`; chưa trừ đầy đủ chi phí vận hành, thuế và quảng cáo |
| Biên lợi nhuận | Lợi nhuận gộp / doanh thu thuần × 100 | Executive trả `null` khi mẫu số không dương; admin và category mart giữ cách xử lý hiện tại |
| Total orders | SUM `mart_order_daily.total_orders`, gồm mọi trạng thái | Ngày × kênh × cửa hàng; không cộng tổng đơn qua danh mục |
| AOV executive | SUM net revenue / SUM recognized_orders; `null` khi không có mẫu số | `mart_order_daily`; mẫu số gồm delivered/completed/returned, không phải tổng đơn mọi trạng thái |
| Sales trend | SUM net revenue, COGS, profit và total_orders từ `mart_order_daily` theo ngày | Cùng công thức với executive nhưng có bộ lọc kỳ; ngày không có dòng không tự điền 0 |
| Tỷ lệ giao thất bại | Số đơn `failed_delivery` / tổng đơn × 100 | `mart_order_daily.boom_orders / total_orders` |
| Tỷ lệ yêu cầu trả hàng | COUNT DISTINCT return_id không cancelled / tổng đơn × 100 | Pipeline tổng hợp Silver request theo đơn trước khi JOIN và rollup `mart_order_daily`; chưa phải tỷ lệ đơn trả distinct |

Executive, Sales và xu hướng chỉ truy vấn mart trong cùng publication schema 3. `mart_order_daily` có grain ngày × kênh × cửa hàng; `mart_product_sales_daily` có grain ngày × sản phẩm; `mart_sales_daily` tiếp tục phục vụ danh mục. Số đơn đóng góp điểm bán đếm đơn có doanh thu thuần dương; tổng đơn Executive/xu hướng gồm mọi trạng thái. Top sản phẩm xếp theo doanh thu thuần, số lượng đã trừ hàng trả hoàn tất/kiểm định đạt. Tỷ trọng danh mục được tính lại từ tổng tiền, không cộng hoặc lấy trung bình tỷ lệ từng dòng.

Mart rỗng làm KPI Executive trả `null`, danh sách Sales/xu hướng trả rỗng; giao diện hiển thị — hoặc trạng thái chưa có dữ liệu. Có dòng nguồn hợp lệ nhưng không phát sinh doanh thu thì số tiền 0 vẫn được giữ. AOV/biên lợi nhuận/tỷ trọng không có mẫu số hợp lệ trả `null`. Mart chưa nằm trong publication gây phản hồi unavailable hoặc cache có đánh dấu, không đọc fact/MySQL để thay thế. Store/Inventory được cập nhật ở đợt 3b như phần dưới; Operations cập nhật ở đợt 3c như phần dưới; Marketing cập nhật ở đợt 3d như phần dưới; System và admin overview cập nhật ở đợt 3e như phần dưới.

Đơn chờ thanh toán, đã xác nhận hoặc đang giao chưa tạo doanh thu hàng hóa. Đơn đã trả vẫn giữ doanh thu ban đầu; hoàn tiền thành công được trừ riêng để xử lý trả một phần. Giá vốn được hoàn nhập theo số lượng trả hoàn tất và kiểm định đạt.

Tiền thanh toán thành công và tiền hoàn cho khách là dòng tiền riêng, có thể bao gồm vận chuyển hoặc hoàn tiền cho đơn hủy. Không dùng tổng payment thay cho doanh thu hàng hóa. Tổng tiền thu hộ COD đang giao lấy `order_total_vnd`, không lấy doanh thu đã ghi nhận.

Kiểm tra trước publication đối chiếu tổng doanh thu trước hoàn, hoàn tiền, doanh thu thuần, giá vốn và lợi nhuận giữa item → order → category/order/product marts. Đơn vị là VND; không dùng tổng số đơn theo danh mục để đối soát số đơn toàn hệ thống.

## 3. Chỉ số vận hành từ Lakehouse đã công bố

| Chỉ số | Công thức / phạm vi |
|---|---|
| Doanh thu/số đơn cửa hàng hôm nay | SUM net_revenue_vnd / total_orders từ `mart_order_daily`, lọc store_key và ngày Việt Nam. Publication cùng ngày và không có dòng thì 0; publication ngày cũ không chứng nhận được 0 của hôm nay nên trả null |
| Tồn thấp / hết hàng cửa hàng | `mart_store_overview.low_stock_count` / `stockout_count`; ngưỡng ≤5 gồm cả 0, hết hàng chỉ =0. Danh sách từ `mart_inventory_current` tối đa 10 dòng; tổng số hết hàng không lấy từ độ dài danh sách |
| Giá trị tồn kho | `mart_inventory_summary.total_inventory_value_vnd`, tổng on_hand × giá vốn hiện có; chỉ lấy số dư hiện tại |
| Số lượng tồn | `warehouse_stock_units` / `store_stock_units` ở mart tổng quan; thiếu dòng quan sát của nhóm thì null, dòng có số dư 0 vẫn giữ 0 |
| Số SKU hết hàng | `mart_inventory_summary.stockout_count`: variant distinct có on_hand=0 trên kho/cửa hàng, không cộng các count distinct theo địa điểm |
| Cảnh báo tồn tại kho | Hai danh sách tối đa 10 dòng: `on_hand=0` và `0<on_hand<=5`, không chồng lặp. API bỏ depletion_velocity và overselling_prevented; không suy ra đã ngăn bán vượt tồn từ dữ liệu tổng hợp |
| Phiếu nhập | COUNT receipt_id được build vào `mart_inventory_summary`; là số phiếu ghi nhận, không phải số phiếu nhận đủ hàng |
| Lịch sử tồn | `mart_inventory_health`, tổng hợp riêng từng snapshot_date × nhóm địa điểm trong 30 ngày gần nhất; không cộng các ngày thành tồn hiện tại |
| Đơn chờ xử lý | `mart_operations_summary.pending_orders`, chỉ paid/confirmed; không có trạng thái processing trong DDL nguồn |
| Đơn chờ từ 2 giờ / thời gian trung bình | Tính tại operations_observed_at khi build mart, không lấy NOW ở API; không có đơn chờ hợp lệ thì trung bình null. Ngưỡng 2 giờ là tiêu chí rà soát, không phải SLA |
| Shipment cần kiểm tra | attempt_count > 1 OR status failed; chỉ đếm một lần/shipment. API dùng problematic_shipments_count thay shipping_sla_violations_count |
| Shipment thất bại | status failed, API failed_shipments_count; không phải số đơn hay riêng COD |
| COD theo vùng | mart_cod_region_daily theo ngày shipment UTC × vùng địa chỉ đơn COD. Tỷ lệ tính lại từ tổng failed/total; không dùng địa bàn phân công nhân sự thay vùng địa chỉ đơn |
| Giao vận theo nhân sự | mart_logistics_performance theo ngày UTC × nhân sự × địa bàn được phân công. Thời gian trung bình = SUM duration_minutes_sum / SUM duration_sample_count; không lấy trung bình các trung bình |
| Đúng hạn | Chỉ delivered có is_on_time khác null được đưa vào mẫu số. Nguồn hiện chưa có estimated_delivery_at nên is_on_time/rate null, eligible count 0; không mặc định tất cả đúng hạn |
| Yêu cầu đổi trả toàn hệ thống | Summary đếm header return_id không cancelled, độc lập số dòng/SKU |
| Đổi trả theo SKU | mart_product_returns bỏ cancelled; total_return_requests đếm distinct return_id trong nhóm ngày/SKU/action/reason; return_item_count là số dòng. Không cộng request count qua SKU để suy ra tổng yêu cầu |
| Lượng đề nghị / lượng chấp nhận | total_returned_quantity giữ tên cũ cho lượng đề nghị; accepted_return_quantity chỉ lấy yêu cầu completed và dòng inspection passed |
| Tiền đề nghị hoàn | requested_refund_amount_vnd lấy từ dòng return_items, không lặp tiền header. total_refund_amount_vnd ở mart giữ làm alias tương thích cho tiền đề nghị, không phải tiền đã trả |
| Tiền hoàn thành công | Summary chỉ lấy refunds.status=succeeded; có thể gồm vận chuyển/đơn hủy. Không phân bổ xuống SKU khi không có liên kết phù hợp; không đồng nhất với hoàn tiền hàng hóa ở Executive |
| Tiền COD đang giao | Tổng order total gắn shipment assigned/picked_up/in_transit; chưa phải số dư tiền thực tế shipper giữ |
| Lượt voucher gần đây | mart_coupon_usage: redemption còn trạng thái redeemed trong 15 phút tại marketing_observed_at / 15; loại released và bản ghi tương lai. Hạn mức null và phần trăm null nếu không giới hạn |
| Đánh giá xấu gần đây | mart_product_review_summary: approved, rating <=2, trong 45 phút tại marketing_observed_at, nhóm product; loại bản ghi tương lai |

Store overview và danh sách chọn cửa hàng đọc `mart_store_overview`, vẫn lọc theo role/store ở API. Số đơn hoàn thành dùng status completed; nhân sự dùng store_manager active. Cửa hàng đã được công bố nhưng chưa có đơn/nhân sự có count 0; chưa có dòng tồn thì trạng thái tồn null. `inventory_observed_at` phản ánh thời điểm quan sát từ current fact, tách với data_updated_at của publication.

Tốc độ bán 60 phút và thời gian dự kiến hết hàng đã bỏ vì thời điểm tạo đơn không chứng minh thời điểm xuất hàng. Dữ liệu nguồn và lịch sử snapshot vẫn giữ nguyên.

Operations chỉ đọc bốn mart đã công bố. `operations_observed_at` là mốc tính thời gian chờ; `data_updated_at` là thời điểm publication, có thể muộn hơn lúc build. Refresh không tự làm tuổi đơn tăng trên snapshot cũ; cần publication mới để cập nhật các phép đo này. Mỗi nguồn rỗng giữ giá trị null cho tổng chưa có quan sát; nguồn có dòng nhưng không thỏa điều kiện có count 0, tỷ lệ không có mẫu số giữ null. Các phần còn dữ liệu vẫn hiển thị.

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
| Product demand | Request thành công theo action product_detail/cart_item_set/cart_add/cart_item_remove/cart_remove/wishlist_add/checkout_quote có khóa sản phẩm/biến thể; grain ngày UTC × sản phẩm đã ánh xạ hoặc khóa chưa ánh xạ |
| Funnel views | Request thành công action product_detail/catalog_search; không cộng API đọc reviews hoặc mọi route sản phẩm |
| Funnel cart / checkout | Request thành công action cart_item_set/cart_add; checkout_quote/checkout_submit. PUT giỏ là đặt lượng, chưa chứng minh tăng lượng |
| Funnel purchase | Chỉ request checkout_submit thành công; cột vật lý orders_completed giữ tên cũ, không phải số đơn duy nhất và không cộng order_complete/order_confirm_internal |
| Checkout / đầu phễu | SUM orders_completed / SUM product_views × 100 trong 30 ngày UTC kết thúc tại ngày observed_at của publication log; mẫu số 0 trả null |

Phễu đếm **request events**, không phải người duy nhất hoặc chuỗi session. Request lặp vẫn được tính; các bước có thể chồng lặp, tỷ lệ có thể vượt 100%. Không cộng distinct actor qua ngày/sản phẩm thành người trong kỳ. Các trường API total_visitors/total_purchases và nhánh proxy OLTP đã bỏ; dùng view_events_count/checkout_success_events_count. Khi có quan sát log nhưng không có action tương ứng, bộ đếm 0 hợp lệ; chưa có dòng log trong kỳ trả KPI null và danh sách bước rỗng.

Marketing đọc hai mart log và ba mart nghiệp vụ. `logs_analytics_publications` pin snapshot ba mart log, độc lập `analytics_publications` của OLTP; nguồn log ghi observed_at, nguồn nghiệp vụ ghi marketing_observed_at. API không JOIN Fact/Silver/Dimension để thay mart. Tỷ lệ trong kỳ tính từ tổng bộ đếm, không lấy trung bình tỷ lệ ngày. API không trả một run ID/thời điểm công bố chung cho hai nguồn; log_status/commerce_status chứa metadata riêng. Một nguồn unavailable/cache không làm nguồn còn đọc được biến mất; cả hai chưa đọc được và không có cache hợp lệ thì trả 503. Cache vẫn có giới hạn tuổi.

Mart nhu cầu sản phẩm ánh xạ product slug/public UUID và variant public UUID qua catalog Silver **đã pin** khi build. Khóa chưa ánh xạ giữ riêng, không ghép sản phẩm tùy ý hoặc bỏ sự kiện có variant. Tỷ lệ giỏ/xem là tỷ lệ request, không phải xác suất mua. Không hiển thị actor distinct như số khách trong kỳ. Cột success_2xx_count ở mart HTTP vẫn đếm is_success (<400); đây là tên cũ, không phải chỉ 2xx.

Coupon mart giữ mỗi coupon; API chọn tối đa 10 mã active, chưa archived, đang trong thời hạn tại mốc đo, sắp theo used_count. Không giới hạn giữ null, không coi hạn mức lượt là ngân sách quảng cáo. Review mart lưu mỗi sản phẩm có review; API lấy tối đa 5 sản phẩm có approved 1–2 sao trong cửa sổ. Không có bản ghi nguồn được hiển thị là thiếu quan sát; có nguồn nhưng không có mã/đánh giá thỏa điều kiện là danh sách rỗng.

Rollup log chạy bằng DAG riêng mỗi giờ, đọc fact snapshot và xây lại đủ ba mart từ tập sự kiện tới mốc quan sát, kiểm tra trước công bố. Khoảng 15/45 phút là cửa sổ tại mốc mart nghiệp vụ, không phải thời gian thực khi refresh. Các dòng mart và snapshot cũ vẫn được giữ trong lịch sử Iceberg; không xóa nguồn log. Flink hiện chỉ append Landing; Spark giờ xử lý tuần tự Bronze → Silver → fact trước khi rollup và công bố ba mart.

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

System hiện đọc SUM(total_orders) và SUM(order_total_vnd) từ mart_order_daily đã pin. MySQL cung cấp COUNT(order_id) và SUM(total_vnd) trong cùng một câu SQL; source_observed_at ghi thời điểm hoàn tất lần đọc. Đây là tổng giá trị đơn mọi trạng thái, không phải doanh thu thuần. Hai phía có mốc thời gian khác nhau, nguồn có thể tiến trước batch. Không dùng CDC heartbeat/pending để suy ra sức khỏe Silver/Gold.

Chênh tuyệt đối = ABS(MySQL − mart), giữ số nguyên cho đơn/VND. Chênh tương đối = chênh tuyệt đối / MySQL × 100 khi mẫu số >0; nguồn 0 trả null cho phần trăm, vẫn xác định mismatch theo chênh tuyệt đối. Thiếu quan sát phía nào giữ null phía đó và trạng thái unavailable. Khi đọc được cả hai phía, matched nếu cả hai chênh tuyệt đối bằng 0, mismatch nếu có chênh. Kết quả cache mang trạng thái cached, giữ thời điểm đo nguồn cũ và không được hiển thị như lần đối soát mới. Publication age chỉ đo tuổi metadata; không có SLA/khẳng định pipeline healthy trong hợp đồng này.

## 6. Cách trình bày kết quả

Ghi rõ dataset, dữ liệu tổng hợp hay phát sinh thật, thời gian chạy, tầng/bảng, bộ lọc, grain, công thức, đơn vị và nguồn response. Các tài liệu A–D mô tả nghiệp vụ và giới hạn, không cung cấp bộ số liệu đo để thay thế truy vấn thực tế.

## 7. Ngày quan sát tồn kho

`fact_inventory_current` và snapshot tồn kho mới dùng ngày quan sát thực tế theo Asia/Ho_Chi_Minh, đối chiếu với `_gold_ingested_at`. Ngày logical của Airflow dùng để truy vết kỳ xử lý, không được dùng để ghi tồn hiện tại đè lên ngày quá khứ. Lịch sử snapshot của các ngày khác được giữ nguyên.


## 8. Admin Overview từ mart

mart_admin_overview có một dòng current tại admin_observed_at của DAG. Các nguồn được aggregate độc lập trước khi cross join để tránh nhân số sản phẩm, review, khách và đơn. Active variant yêu cầu cả product/variant active; tồn thấp chỉ lấy kho trung tâm và catalog active, ngưỡng ≤5 gồm cả 0. Khách hàng đếm role=customer, không suy ra đã kích hoạt. Review là tổng bản ghi nguồn; active coupon yêu cầu active, chưa archived, starts_at<=mốc đo<ends_at. Có coverage nhưng không thỏa điều kiện giữ 0; chưa có quan sát domain trả null. Không có coverage kho thì không kết luận tồn thấp bằng 0.

Các bộ đếm trạng thái là số đơn, gồm pending_payment/paid/confirmed/shipping/delivered/completed/cancelled/failed_delivery/returned. Failed delivery không giới hạn COD; returned không phải số yêu cầu trả. API kiểm tra tổng dòng đơn của summary khớp SUM(total_orders) từ order mart. UI dùng total_orders được công bố cho mẫu số, hiển thị đầy đủ trạng thái và không cộng các đơn vị khác nhau thành số việc cần làm. Thanh tỷ lệ bằng 0 giữ chiều rộng 0.

Tài chính Admin lấy SUM gross_revenue_vnd/refunded_amount_vnd/net_revenue_vnd/cogs_vnd/gross_profit_vnd từ mart_order_daily cùng publication, theo công thức nhận diện và phân bổ đã dùng ở Executive. Biên lợi nhuận = profit/net khi net>0; không có mẫu số hoặc không có mart row trả null. Thời điểm quan sát admin khác thời điểm publication và thời điểm UI nhận response. API chỉ đọc mart; không dùng MySQL để thay thế dữ liệu tổng quan thiếu.

## 9. HTTP Metrics — báo cáo theo giờ

API `GET /api/v1/admin/analytics/http-metrics?days=7` dành cho admin/system_admin; chỉ đọc `mart_hourly_route_metrics` pin snapshot từ publication log schema 1. Bộ chọn 1/7/30 ngày ở System chỉ đổi phần HTTP; API nhận 1–30 ngày. Cửa sổ gồm các ngày UTC, bắt đầu 00:00 ngày cách ngày `observed_at` của publication `days - 1` ngày, kết thúc ở mốc quan sát đó. Ngày cuối có thể chưa đủ 24 giờ. Publication log độc lập publication OLTP; tải lại giao diện không kích hoạt rollup.

| Chỉ số | Cách tính / grain | Giới hạn |
|---|---|---|
| Tổng request | SUM total_requests trên toàn kỳ | Sự kiện đã ghi nhận, không phải người/session/giao dịch duy nhất |
| Thành công | SUM success_2xx_count | Tên cột legacy; ingestion hiện đếm is_success (<400), UI ghi 2xx/3xx |
| Lỗi 4xx / 5xx | SUM từng bộ đếm | Là phản hồi HTTP, không tự suy ra nguyên nhân hoặc thất bại nghiệp vụ |
| Tỷ lệ lỗi | 100 × (SUM 4xx + SUM 5xx) / SUM request | Mẫu số 0 → null; không trung bình các tỷ lệ nhóm |
| Độ trễ trung bình | SUM(avg_duration_ms × total_requests) / SUM trọng số nhóm có mean | Mean nhóm đã làm tròn; ingestion hiện cung cấp duration cho mỗi sự kiện hợp lệ; nhóm thiếu mean bị loại khỏi trọng số, không thay bằng 0 |
| Chuỗi theo giờ | Tổng bộ đếm theo ngày UTC × giờ UTC | Giờ không có quan sát là khoảng trống, không nội suy 0 |
| Route cần kiểm tra | Aggregate kỳ theo route × method, top 20 theo số lỗi rồi mean latency | Chỉ là tập con; không cộng bảng top để thay tổng KPI |
| p50/p95/p99 | Giá trị percentile_approx tại ngày × giờ × route × method | Top 20 theo p95; không phải percentile toàn kỳ, không trung bình/cộng các percentile |

Phản hồi có `window_start`, `observed_at`, `source_run_id`, `data_updated_at` và metadata cache riêng. Mart rỗng giữ null tổng và danh sách rỗng; số 0 đã quan sát vẫn là 0. Lỗi đọc hoặc thiếu publication/snapshot dùng cache còn hạn theo đúng `days`, có đánh dấu degraded, hoặc 503 nếu không có cache hợp lệ. Cache giữ cửa sổ và thời điểm đo cũ. Khi một trong hai phần HTTP/đối soát đơn lỗi, phần còn đọc được vẫn hiển thị độc lập. Chưa triển khai luồng Landing realtime 10 giây trong phần này; không xác nhận SLA hoặc năng lực chịu tải từ các chỉ số trên.

## 10. Trình bày dashboard tối giản

Giao diện chỉ giữ KPI, biểu đồ, phạm vi và metadata cần thiết; định nghĩa đầy đủ tiếp tục nằm trong tài liệu này và [thiết kế dashboard](../design-analysis/DASHBOARD_DESIGN.md). Bảng chi tiết mặc định thu gọn, có tìm kiếm/sắp xếp/CSV; Top 5/10/20 được ghi rõ. Sales, Operations, Marketing và System có góc nhìn nhỏ để không trộn phạm vi độc lập. Không thay công thức, query hoặc dữ liệu nguồn trong đợt thiết kế này.

Marketing summary hiển thị số bản ghi coupon/review đã nạp, không phải số mã active hoặc số sản phẩm duy nhất. Sales cộng tổng từ toàn bộ store contributions, không từ top sản phẩm. Admin giữ đủ chín trạng thái đơn. Số 0 không có thanh giả dương; giá trị null để —; tiền/đơn đối soát vẫn dùng trục riêng. Lỗi tải mới giữ kết quả trước đó kèm cảnh báo, cache giữ mốc cũ; các kỳ date filter vẫn giữ phạm vi API hiện tại. Tất cả 15 mart OLTP và 3 mart log có ánh xạ KPI/biểu đồ/tra cứu trong tài liệu thiết kế.
