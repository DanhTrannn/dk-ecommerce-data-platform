# Thiết kế dashboard: nguyên tắc và độ phủ mart

Phạm vi: bảy góc nhìn BI, HTTP Metrics, trang tổng quan Admin và tổng quan cửa hàng. Tài liệu ghi quyết định trình bày; công thức nguồn vẫn theo [Metrics Catalog](../architecture/METRICS_CATALOG.md). Không thay đổi API, query, pipeline hoặc dữ liệu nghiệp vụ trong đợt thiết kế này.

## Cơ sở tham khảo

| Nguồn chính thức | Nguyên tắc được áp dụng |
|---|---|
| [Microsoft Power BI — Dashboard design tips](https://learn.microsoft.com/en-us/power-bi/create-reports/service-dashboards-design-tips) | Thiết kế theo người xem, ưu tiên số quan trọng ở đầu trang, bỏ chi tiết dư thừa, chọn biểu đồ và thang đo phù hợp. |
| [Tableau — Effective dashboards](https://help.tableau.com/current/pro/desktop/en-gb/dashboards_best_practices.htm) | Tập trung vài góc nhìn chính, kiểm tra ở kích thước thiết bị thực tế; tách nội dung khi phạm vi phân tích lớn. |
| [W3C — Contrast minimum](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html) | Mục tiêu tương phản chữ thường tối thiểu 4,5:1, chữ lớn 3:1; kiểm tra cặp màu thực tế thay vì chỉ chọn màu theo cảm quan. |

Các hướng dẫn trên là cơ sở lựa chọn, không phải chứng nhận accessibility hoặc hiệu năng cho ứng dụng. Không áp dụng quy tắc một màn hình bằng cách thu nhỏ chữ hoặc ép bảng rộng vào điện thoại.

## Người xem và mức chi tiết

Giả định thiết kế, chưa phải kết quả khảo sát người dùng thực tế:

| Nhóm | Nhịp sử dụng dự kiến | Thông tin ưu tiên |
|---|---|---|
| Executive / Sales | Rà soát kết quả định kỳ | Tổng quan tài chính, xu hướng, đóng góp sản phẩm/điểm bán |
| Store / Inventory | Trong ca và khi bổ sung hàng | Số liệu hôm nay, số dư và SKU thiếu hàng |
| Operations | Khi điều phối đơn/giao vận/đổi trả | Đơn chờ, shipment lỗi, nhân sự/địa bàn, SKU yêu cầu trả |
| Marketing | Khi rà soát tương tác/chương trình | Phễu request, sản phẩm quan tâm, voucher và review |
| System | Khi kiểm tra vận hành dữ liệu | Đối soát, độ mới, request/lỗi/latency và bảng route |

Chu kỳ xem dự kiến không thay lịch xử lý dữ liệu: Báo cáo mart poll 15 giây và thay đổi theo publication; HTTP Realtime đọc Landing có chu kỳ riêng 10 giây khi hiển thị.

## Quyết định trình bày

- Footer mua sắm được ẩn ở khu vực Store để đồng nhất với Header back office và tránh liên kết quảng bá trên dashboard.
- Header ngắn với phạm vi đo; KPI trước, biểu đồ sau. Bỏ banner quảng bá BI, khối mục tiêu/câu hỏi lặp, danh sách hướng dẫn chung và bảng định nghĩa thường trực.
- Mỗi góc nhìn chính ưu tiên 2–4 KPI; không tạo card chỉ để đủ cột. Sales dùng hai KPI từ tập đóng góp đầy đủ, không cộng top sản phẩm để giả định tổng doanh thu.
- Bar chart so sánh nhóm; line chart cho ngày/giờ. Trục bar chứa mốc 0, có hỗ trợ số âm; số 0 không vẽ thanh giả dương. Đơn và VND của đối soát dùng biểu đồ riêng.
- Màu chính moss/ink, màu nhấn amber/terracotta; nhãn và kiểu nét giúp phân biệt chuỗi. Không dựng target, delta hoặc nguyên nhân khi API chưa có bằng chứng.
- Marketing tách Tương tác / Voucher & đánh giá; Vận hành tách Giao vận / Đổi trả; System tách Đối soát / HTTP Metrics theo mart / HTTP Realtime từ Landing. Các nguồn/kỳ độc lập có metadata riêng, không gán một bộ lọc ngày chung giả định.
- Bảng tra cứu được thu gọn mặc định; vẫn có tìm kiếm, sắp xếp và CSV theo tập đã trả về. Top 5/10/20 được ghi trên tiêu đề. Tìm bảng không thay KPI hay biểu đồ.
- Card/tooltip cho phép đọc số tiền đầy đủ; giao diện chính dùng định dạng gọn. Đơn vị, kỳ và mốc công bố/quan sát giữ ngắn gọn. Cache và dữ liệu chưa đầy đủ vẫn có nhãn; lỗi tải mới không biến kết quả cũ thành dữ liệu mới.
- Giữ null thành —, số 0 đã quan sát thành 0, khoảng thời gian thiếu thành gap. Không thay mean có trọng số hay gộp percentile giờ thành percentile toàn kỳ.
- Không tải HTTP khi chưa mở góc nhìn này. Request xu hướng/HTTP cùng kỳ đang chạy được dùng lại; dữ liệu hiện có không bị gỡ khỏi trang mỗi lần refresh. Đổi kỳ không hiển thị nhầm kết quả kỳ cũ.

## Ánh xạ 18 mart đã build

Tên mart là bảng Gold; không phải mọi cột của mart đều cần xuất hiện trên màn hình chính. Các bảng tổng quan phục vụ KPI/coverage và các bảng phân nhóm phục vụ biểu đồ/tra cứu.

| Mart | Dashboard / góc nhìn | KPI hoặc biểu đồ / câu hỏi |
|---|---|---|
| `mart_order_daily` | Executive, Sales, Store, Admin, System | Doanh thu/lợi nhuận/đơn/AOV, xu hướng, điểm bán, đối soát đơn/giá trị |
| `mart_product_sales_daily` | Sales → Sản phẩm & danh mục | Sản phẩm nào tạo doanh thu? Top 10 và lượng bán thuần |
| `mart_sales_daily` | Sales → Sản phẩm & danh mục | Doanh thu/tỷ trọng danh mục; không cộng số đơn qua danh mục |
| `mart_inventory_summary` | Inventory | Giá trị tồn, tồn kho/cửa hàng, SKU có điểm hết, phiếu nhập ghi nhận |
| `mart_inventory_current` | Inventory, Store | SKU nào tồn thấp hoặc hết ở địa điểm được chọn? |
| `mart_inventory_health` | Inventory | Lượng tồn thay đổi qua các ngày quan sát thế nào? |
| `mart_store_overview` | Store BI, Store home | Tổng quan cửa hàng, nhân sự active và coverage tồn |
| `mart_operations_summary` | Operations | Chờ xuất/chờ từ 2 giờ, shipment thất bại, yêu cầu trả và refund thành công |
| `mart_logistics_performance` | Operations → Giao vận | Shipment thất bại theo nhân sự/địa bàn; bảng giữ số mẫu và thời gian vận chuyển |
| `mart_cod_region_daily` | Operations → Giao vận | Tỷ lệ COD thất bại theo vùng, kèm số mẫu trong chi tiết |
| `mart_product_returns` | Operations → Đổi trả | Lượng đề nghị trả theo SKU; tách số yêu cầu, lượng nhận đạt và tiền đề nghị |
| `mart_marketing_summary` | Marketing → Voucher & đánh giá | Số bản ghi coupon/review đã nạp và khả dụng của nguồn nghiệp vụ |
| `mart_coupon_usage` | Marketing → Voucher & đánh giá | Voucher khả dụng nào được dùng? Top 10, hạn mức và cửa sổ 15 phút |
| `mart_product_review_summary` | Marketing → Voucher & đánh giá | Sản phẩm có review approved 1–2 sao trong 45 phút tại mốc đo; Top 5 |
| `mart_admin_overview` | Admin | Đơn theo chín trạng thái, tồn thấp, chỉ số catalog/khách hàng có thể mở thêm |
| `mart_marketing_funnel_daily` | Marketing → Tương tác | Request thành công theo bước và checkout/đầu phễu trong 30 ngày UTC |
| `mart_daily_product_demand` | Marketing → Tương tác | Top 10 lượt xem sản phẩm, giỏ/yêu thích và khóa chưa ánh xạ |
| `mart_hourly_route_metrics` | System → HTTP Metrics | Request/lỗi theo giờ, tỷ lệ lỗi, mean latency; route Top 20 và percentile từng nhóm giờ |

Độ phủ được đối chiếu với registry Gold, truy vấn thật qua Trino và các góc nhìn thực sự render khi đổi tab. `data-mart` trong DOM phục vụ kiểm tra liên kết này, không phải nội dung hiển thị cho người dùng. Reader vẫn pin snapshot theo publication; MySQL của System chỉ là phép quan sát nguồn cho đối soát như trước.

## Giới hạn kỳ đo và tương tác

KPI Executive/Sales tổng hợp toàn kỳ đã nạp; bộ chọn 7/30/90 ngày chỉ đổi xu hướng. Store dùng ngày đặt hàng Việt Nam. Inventory giữ số dư hiện tại riêng với lịch sử 30 ngày. Marketing log giữ 30 ngày UTC, còn coupon/review dùng cửa sổ cố định tại mốc nghiệp vụ. HTTP dùng 1/7/30 ngày UTC tại mốc rollup, độc lập publication OLTP. Vì vậy giao diện không thêm bộ lọc ngày toàn trang khi backend chưa cung cấp hợp đồng đó.

Báo cáo log vẫn cập nhật qua pipeline giờ; chu kỳ tải giao diện hiện tại là 15 giây. HTTP Realtime được bổ sung riêng: cửa sổ 5/15/60 phút, bốn KPI, request/lỗi theo phút, route và chất lượng trong bảng/chi tiết thu gọn. Nhãn Landing/sơ bộ, UTC và mốc snapshot/đo tách khỏi publication; chỉ poll 10 giây khi hiển thị, lỗi giữ mốc cũ. Không sửa công thức mart hoặc xác nhận hiệu năng production. Đối chiếu số liệu, kiểm tra responsive/keyboard, null/0/âm/cache và các biểu đồ có đơn vị riêng là các bước xác minh của đợt giao diện; test/ảnh/log chỉ giữ ngoài Git.
