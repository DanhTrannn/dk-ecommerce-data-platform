# B — Nghiệp vụ phân tích theo vai trò

D&K kết hợp bán online và POS, dùng MySQL làm nguồn giao dịch và Lakehouse phục vụ lịch sử phân tích. Tài liệu ánh xạ câu hỏi quản trị với các góc nhìn BI đang có; công thức chi tiết được quản lý tại [Metrics Catalog](METRICS_CATALOG.md).

## 1. Phạm vi dữ liệu phục vụ quyết định

- Đơn, thanh toán, hoàn tiền và snapshot giá bán/giá vốn cung cấp dữ liệu bán hàng.
- Tồn kho tổng/cửa hàng, phiếu nhập và ledger cung cấp dữ liệu hàng hóa.
- Shipment, lần giao và trả hàng cung cấp dữ liệu vận hành sau bán.
- Access log cung cấp sự kiện HTTP/hành vi; chưa có mô hình session hoặc định danh unique visitor đầy đủ.
- Gold OLTP xây theo batch; store/inventory/operations đọc MySQL hiện tại. CDC lưu sự kiện ở Bronze, chưa là nguồn cập nhật trực tiếp cho toàn bộ dashboard.

Dữ liệu generator là tổng hợp. Mẫu phân phối hoặc quy mô cấu hình không phải số liệu kinh doanh thực tế hay kết quả đánh giá hiệu năng.

## 2. Góc nhìn và quyền truy cập

| Góc nhìn BI | Role tài khoản | Câu hỏi chính | Nguồn chính |
|---|---|---|---|
| Executive | `admin` | Quy mô đơn, doanh thu, giá vốn, lợi nhuận và rủi ro COD? | Gold order + mart returns; MySQL bổ sung |
| Sales | `sales_manager` | Kênh/cửa hàng/sản phẩm/danh mục nào đóng góp doanh thu? | Gold facts và sales mart |
| Marketing | `marketing_manager` | Lượng sự kiện chuyển đổi, sử dụng coupon và review xấu? | Mart log phễu/nhu cầu sản phẩm và mart OLTP coupon/review, hai publication riêng |
| Store | `store_manager` | Hôm nay bán bao nhiêu, SKU nào thiếu tại cửa hàng? | MySQL, cửa hàng được gán |
| Inventory | `inventory_manager` | Vốn tồn, lượng hàng và nguy cơ hết SKU? | MySQL |
| Operations | `operations_manager` | Đơn chờ, lần giao lỗi, COD và yêu cầu trả? | MySQL |
| System | `system_admin` | Gold mới tới đâu, lệch nguồn ở đâu và route HTTP nào cần kiểm tra? | Mart đơn hàng so với MySQL; mart HTTP theo publication log riêng |

`admin` được chọn mọi góc nhìn; role khác chỉ được góc nhìn tương ứng. Phạm vi cửa hàng được xử lý ở API, không phải Trino RLS. Quyền xem KPI khác quyền ghi nghiệp vụ: API đơn/giao vận/trả hàng hiện vẫn yêu cầu `admin` dù UI mở menu cho `operations_manager`.

## 3. Executive và Sales

Executive trả GMV, net revenue, COGS, gross profit/margin, total orders, AOV, boom rate và return rate. COD đang giao được bổ sung từ MySQL. Lợi nhuận ở đây là doanh thu trừ giá vốn, chưa bao gồm toàn bộ chi phí vận hành.

Sales trả đóng góp cửa hàng/kênh, sản phẩm bán chạy và cơ cấu danh mục. Endpoint sales trend cung cấp chuỗi revenue/COGS/profit/orders theo ngày với lựa chọn 7/30/90 ngày trên UI. Bộ lọc này không đồng thời giới hạn mọi KPI tổng quan.

Các giới hạn cần giữ khi viết báo cáo:

- Order fact loại một số trạng thái khỏi net revenue; sales mart chỉ tính item revenue của đơn delivered/completed. Hai cách ghi nhận không thể thay thế cho nhau.
- Số đơn trong sales mart có thể lặp qua danh mục. Không cộng các nhóm đó thành distinct orders toàn hệ thống.
- Fallback MySQL có quy tắc tiền thanh toán/hoàn tiền khác Gold. So sánh hai nguồn phải xét công thức và thời điểm.
- Chưa có bảng target hoặc mô hình dự báo được phục vụ. Các trường margin-erosion/channel-growth/viral/run-rate chưa có nguồn tính phù hợp không được hiển thị như số đo đã có.

## 4. Marketing

Dashboard đọc mart phễu và nhu cầu sản phẩm trong 30 ngày UTC kết thúc tại ngày quan sát log. Phễu đếm request thành công theo action xem/tìm sản phẩm, đặt lượng giỏ, tương tác checkout và checkout_submit thành công. Không cộng thêm xác nhận/hoàn tất đơn vào bước checkout thành công. Đây là số sự kiện, không phải người duy nhất, cohort hoặc hành trình session; các bước không bắt buộc giảm dần. Mart sản phẩm ánh xạ slug/UUID/variant qua danh mục đã pin khi build, giữ khóa chưa ánh xạ riêng.

Mart coupon giữ used_count, hạn mức và số redemption còn redeemed trong 15 phút tại marketing_observed_at, loại released và bản ghi tương lai. Không giới hạn giữ null; hạn mức lượt không phải ngân sách tiền quảng cáo. Mart review đếm approved, rating ≤2 trong 45 phút tại mốc đó theo sản phẩm. Refresh không tự tính lại các cửa sổ trên snapshot cũ.

Hai nguồn có snapshot publication, thời điểm quan sát và cache độc lập. Nguồn lỗi giữ phần còn đọc được; thiếu dữ liệu trả null/danh sách rỗng, không dùng MySQL proxy hoặc tổng hợp Fact/Silver ở API. Chưa có tích hợp quảng cáo, attribution campaign, ROAS/CAC hoặc NLP cảm xúc. Xem Metrics Catalog cho công thức và giới hạn diễn giải.

## 5. Cửa hàng và tồn kho

Store view trả doanh thu/số đơn hôm nay, tồn thấp và SKU hết tại cửa hàng được phép. Ranh giới ngày dùng `Asia/Ho_Chi_Minh`; tập trạng thái tính doanh thu được nêu trong Metrics Catalog. Không có chỉ tiêu doanh thu được lưu nên target achievement và run-rate không được dựng từ số mẫu.

Inventory view trả giá trị tồn theo giá vốn hiện tại, tổng lượng kho/cửa hàng, số phiếu nhập, SKU hết hàng và tín hiệu tốc độ bán trong 60 phút. Dự kiến thời gian hết hàng là phép chia tồn hiện tại cho tốc độ gần đây; chưa phải mô hình dự báo nhu cầu đã huấn luyện. Số SKU hết là distinct variant có tồn bằng 0 ở ít nhất một địa điểm, khác số cặp variant–địa điểm hết hàng.

Gold fact_inventory_current phục vụ KPI tồn hiện tại; fact_inventory_daily_snapshot lưu quan sát theo ngày. API vận hành giao dịch vẫn dùng MySQL, còn KPI dashboard tồn kho đọc Gold. Tên bảng daily snapshot không chứng minh có đầy đủ snapshot của tất cả ngày trong lịch sử.

## 6. Vận hành đơn và giao hàng

Operations view tổng hợp đơn chờ xử lý, đơn chờ quá hai giờ, thời gian chờ trung bình, giao vận có vấn đề, đơn failed_delivery, yêu cầu trả và COD failed theo vùng.

`shipping_sla_violations_count` hiện dựa trên shipment có nhiều lần giao hoặc trạng thái failed; chưa đối chiếu một deadline SLA cụ thể. Nhóm vùng dựa trên từ khóa địa chỉ. Tiền COD đang giao dựa trên tổng giá trị đơn có shipment đang vận chuyển, chưa phải sổ quyết toán tiền mặt thực tế của shipper. Các nhóm đơn/shipment có thể chồng lặp nên không diễn giải như những phần loại trừ nhau của một biểu đồ cơ cấu.

Shipper được lưu ở `delivery_staff`; thao tác cập nhật do API quản trị thực hiện. Chưa có ứng dụng/đăng nhập shipper hoặc tích hợp hãng giao vận bên ngoài.

## 7. Chất lượng và đối soát

System view đọc tổng số đơn và tổng giá trị đơn mọi trạng thái từ mart_order_daily đã pin, đối chiếu COUNT và SUM từ một câu SQL MySQL. source_observed_at ghi thời điểm đọc nguồn; publication_age_minutes là tuổi bộ snapshot được đọc. Trạng thái matched/mismatch/unavailable/cached phản ánh phép đối soát, không suy ra sức khỏe pipeline từ heartbeat CDC hoặc ngưỡng SLA. Nguồn có thể tiến trước batch nên chênh lệch cần được đối chiếu với kỳ chạy.

HTTP Metrics trong góc nhìn System đọc mart_hourly_route_metrics theo publication log riêng: request, lỗi 4xx/5xx, tỷ lệ lỗi và độ trễ trung bình có trọng số. Bộ chọn ngày UTC và dữ liệu/cache độc lập đối soát đơn; p95/p99 chỉ hiển thị tại nhóm giờ × route × method gốc, không suy ra percentile toàn kỳ. Đây là báo cáo theo giờ, chưa phải luồng Landing realtime.

UI vẽ riêng từng chỉ số với trục đơn/VND và hiển thị cả chênh tuyệt đối lẫn tương đối. Mẫu số nguồn 0 để phần trăm null; chênh tuyệt đối vẫn xác định mismatch. Thiếu một phía giữ giá trị phía còn đọc được; cache giữ mốc đo cũ và không được trình bày như lần đối soát mới. Admin Overview dùng mart_admin_overview cho các bộ đếm và mart_order_daily cho tài chính; các bộ đếm có coverage riêng, coupon tính tại as_of, trạng thái returned là số đơn thay vì yêu cầu trả.

## 8. Cách đọc nguồn và kết quả

Response KPI có `data_source`, publication ID/time và metadata cache. Thời điểm trình duyệt nhận phản hồi khác thời điểm pipeline công bố. Các dashboard tự refresh 15 giây khi tab hiển thị; polling không khởi chạy pipeline.

Khi trình bày kết quả, ghi rõ dataset, kỳ dữ liệu, grain, công thức, bộ lọc, đơn vị và nguồn. Các câu hỏi mở rộng như dự báo, CLV, retention cohort, recommendation và đánh giá hiệu quả quảng cáo cần dữ liệu/mô hình bổ sung; chưa phải chức năng hiện có.

## Nguồn đối chiếu

- [Analytics service](../../services/platform-app/src/platform_app/api/modules/analytics/service.py), [response schemas](../../services/platform-app/src/platform_app/api/modules/analytics/schemas.py), [router](../../services/platform-app/src/platform_app/api/modules/analytics/router.py).
- [Dashboard components](../../services/storefront/src/components/analytics), [BI page](../../services/storefront/src/app/admin/analytics/page.tsx).
- [Gold OLTP](../../pipelines/src/lakehouse/oltp/gold/), [Gold logs](../../pipelines/src/lakehouse/logs/gold.py).
- [Kiến trúc](SYSTEM_ARCHITECTURE_VISUAL.md), [nghiệp vụ nguồn](../project/BUSINESS_REQUIREMENTS.md).
