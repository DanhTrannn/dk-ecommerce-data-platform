# Kế hoạch dashboard theo vai trò và cửa hàng

Trạng thái: đề xuất triển khai. Bản đối chiếu là nhánh `dev` tại commit `383e25a`, với bốn dashboard, 27 biểu đồ và 14 mart OLTP Gold. Mốc đầu tiên là **hai quản lý mở cùng dashboard Cửa hàng của tôi và mỗi người thấy dữ liệu của đúng chi nhánh**. Sau khi một quản lý chuyển chi nhánh, lần đồng bộ quyền phải thu hồi được phạm vi cũ. [Kiến trúc hiện tại](ARCHITECTURE.md) · [Định nghĩa dashboard](../infrastructure/superset/retail.py)

## 1. Dashboard theo vai trò

| Vai trò API | Dashboard Superset | Phạm vi và chỉ số chính |
|---|---|---|
| `admin` | **Tổng quan điều hành** và các dashboard nghiệp vụ | Toàn chuỗi. Doanh thu thuần, lãi gộp, đơn đặt, giao đúng hạn và giá trị tồn |
| `sales_manager` | **Doanh thu bán lẻ**, **Bán hàng tại cửa hàng (POS)** | Toàn chuỗi. So sánh kỳ trước, phân tích theo kênh, cửa hàng và danh mục |
| `store_manager` | **Cửa hàng của tôi** | Cửa hàng được gán. Doanh thu và số đơn POS, bán theo giờ và nhân viên, giá trị tồn và SKU cần bổ sung |
| `inventory_manager` | **Tồn kho & bổ sung hàng** | Kho trung tâm và cửa hàng. Giá trị tồn, hết hàng, số ngày đủ bán, SKU cần bổ sung |
| `operations_manager` | **Vận hành đơn hàng** | Toàn chuỗi. SLA giao, tồn đọng theo tuổi đơn, giao thất bại, hủy, đổi trả và hoàn tiền |
| `marketing_manager` | **Khách hàng & marketing** | Số liệu tổng hợp toàn chuỗi. Mua lại, cohort, chi phí coupon, đánh giá theo tình trạng giao |
| `city_planner` | **Kế hoạch theo thành phố** | Thành phố được gán. Nhu cầu bán lẻ, đơn online theo nơi giao, POS theo địa bàn cửa hàng và SLA giao |
| `system_admin` | **Vận hành dữ liệu** | Chỉ số kỹ thuật như trạng thái DAG, độ mới của Gold và lần đồng bộ BI gần nhất |
| `customer` | Lịch sử mua trong ứng dụng | Đơn của chính mình qua API. Chặn truy cập Superset nội bộ |

Các cột `role`, `status`, `store_id` và `city_id` đã có ở nguồn. Mỗi tài khoản Superset được quản lý dùng một ánh xạ tường minh tới `public_id` của tài khoản API đã được duyệt dùng BI. Danh tính và mật khẩu đăng nhập Superset được quản lý riêng. Tài khoản quản trị Superset dùng để cấu hình BI. Người xem **Tổng quan điều hành** dùng role nghiệp vụ. [Mô hình tài khoản API](../services/platform-app/src/platform_app/api/models/customer.py) · [Khởi tạo Superset](../infrastructure/superset/initialize.py)

## 2. Đợt 1 — Dashboard Cửa hàng của tôi

### 2.1. Dataset và biểu đồ

- [ ] Tạo ba dataset dành cho cửa hàng trong `retail.py` từ `mart_pos_hourly`, `mart_inventory_current` và `mart_inventory_health_daily`. Đưa `store_key` vào đầu ra của từng dataset. Lấy tồn tại chi nhánh và dùng `store_key` để liên kết. Các nguồn lookup phục vụ bộ lọc cũng phải giữ khóa phạm vi.
- [ ] Dựng một dashboard dùng chung. Hàng đầu hiển thị doanh thu POS, số đơn POS, giá trị tồn tại ngày snapshot và tỷ lệ hết hàng. Bên dưới có đơn theo giờ, doanh thu theo nhân viên và bảng SKU cần bổ sung kèm lượng tồn và số ngày đủ bán.
- [ ] Áp bộ lọc ngày cho doanh số và lịch sử tồn. Thẻ và bảng tồn hiện tại ghi rõ ngày snapshot. Đối chiếu từng chỉ số với Gold ở cùng kỳ. Tỷ lệ hết hàng dùng tổng SKU hết hàng chia tổng SKU đã quan sát. Số ngày đủ bán giữ NULL khi thiếu lượng bán trong kỳ tham chiếu.
- [ ] Giới hạn phạm vi đợt đầu vào POS và tồn chi nhánh. Đưa chỉ số đổi trả riêng theo cửa hàng vào đợt sau, khi dữ liệu đã giữ được `store_key` từ đơn gốc trước bước tổng hợp.

[Dataset và biểu đồ hiện có](../infrastructure/superset/retail.py) · [Grain của các mart và công thức KPI](DATA.md#5-dimension-fact-và-14-mart-oltp)

### 2.2. Quyền xem và lọc dữ liệu

- [ ] Bật `DASHBOARD_RBAC` trong `superset_config.py`. Gắn role nghiệp vụ tường minh vào từng dashboard được publish. Gán `bi_store_manager` cho quyền mở **Cửa hàng của tôi** và role như `bi_store_12` cho quyền xem dữ liệu chi nhánh 12.
- [ ] Với mỗi dataset cửa hàng, cấu hình RLS Base `1 = 0` mặc định và RLS Regular `store_key = <id>` cho từng role phạm vi. Chỉ miễn Base cho role đã có rule Regular hợp lệ trên dataset đó. Lấy ID nguyên từ dữ liệu đã xác thực. Cấu hình ban đầu cấp đúng một phạm vi cửa hàng cho mỗi quản lý.
- [ ] Rà toàn bộ biểu đồ và lookup trên dashboard. Quyền dashboard có thể cấp quyền đọc dataset được gắn vào đó. Chặn quyền đọc toàn database, SQL Lab, chỉnh dataset và xuất dữ liệu cho role cửa hàng. Bộ lọc trong trình duyệt chỉ chọn trong tập dữ liệu mà RLS đã cho phép.
- [ ] Giữ đăng nhập Trino của Superset qua HTTPS ở chế độ chỉ đọc. Điểm thực thi phạm vi người dùng của bản này nằm ở Superset. Nếu sau này cấp quyền truy vấn Trino trực tiếp cho người dùng nghiệp vụ, cần thêm chính sách theo danh tính ở tầng truy vấn.

[Cấu hình Superset của repo](../infrastructure/superset/superset_config.py) · [RBAC và RLS trong Superset 6.1](https://superset.apache.org/admin-docs/6.1.0/security/) · [Quyền xuất dữ liệu](https://superset.apache.org/user-docs/6.1.0/security/granular-export-controls/)

### 2.3. Đồng bộ và thu hồi quyền

- [ ] Thêm phần đồng bộ quyền vào luồng `make superset-sync`. Dùng cấu hình ánh xạ do quản trị viên duyệt và nguồn PostgreSQL chỉ đọc các trường cần thiết cho quyền. Ghi nhận số tài khoản được cấp, đổi hoặc thu hồi quyền. Giữ credential trong cấu hình môi trường.
- [ ] Đọc trạng thái hiện tại của từng tài khoản được quản lý. Khi `store_id` đổi, gỡ role cũ và gán role mới trong một giao dịch metadata. Thu hồi các role BI đã quản lý khi tài khoản bị vô hiệu hóa hoặc bị bỏ khỏi ánh xạ. Khi tài khoản đổi vai trò hoặc mất phạm vi bắt buộc, gỡ các role đã mất hiệu lực. Chạy đối chiếu quyền cả khi fingerprint dashboard khớp.
- [ ] Kiểm tra quyền và phạm vi độc lập với truy vấn kiểm tra mart để việc thu hồi vẫn chạy được khi Trino hoặc Gold gặp lỗi. Nếu nguồn danh tính lỗi, báo lần đồng bộ thất bại, dừng cấp quyền mới và ghi cách thu hồi khẩn trong runbook. Thay đổi ở API có hiệu lực với BI sau khi đồng bộ quyền thành công.
- [ ] Khi thay dataset, gắn RLS và xác minh cấu hình trước khi chuyển dashboard sang dùng phiên bản mới. Giữ URL dashboard hiện có khi cập nhật. Rà quyền và đường dẫn của tài sản cũ. Nếu phải phục hồi cấu hình BI, giữ các role giới hạn và kiểm tra lại RLS trước khi publish.

[Luồng đồng bộ hiện tại](../infrastructure/superset/sync_dashboard.py) · [Hướng dẫn vận hành](OPERATIONS.md)

## 3. Đợt 2 — Dashboard phòng ban

- [ ] Dựng dashboard điều hành từ KPI đã có. Giữ cách phân biệt ngày đặt, ngày ghi nhận doanh thu, ngày hoàn và ngày snapshot. So sánh hai kỳ có cùng độ dài. Tỷ lệ và biên lợi nhuận lấy từ tổng tử số và mẫu số.
- [ ] Tách Tồn kho khỏi Vận hành. Giữ các dashboard Sales, POS và Marketing đang có, bổ sung KPI theo ma trận vai trò. Dùng số liệu khách hàng dạng tổng hợp và phân tích chi phí coupon theo đợt sale.
- [ ] Tạo dataset thành phố riêng, giữ `city_key` trước bước tổng hợp. Áp quyền theo `city_id` đã xác thực. Hiển thị rõ cách xác định thành phố của đơn online, đơn POS và vận đơn.
- [ ] Cấp role Superset đúng ma trận. Đăng nhập kiểm tra từng vai trò và đối chiếu số liệu trên cùng phạm vi, ngày và grain với truy vấn Gold. Kiểm tra thêm các dashboard được chia sẻ dataset hoặc có URL từ bản cũ.

## 4. Đợt 3 — Vận hành và bàn giao

- [ ] Bổ sung nguồn tổng hợp trạng thái DAG, độ mới của Gold và kết quả `superset-sync`. Xây dashboard cho `system_admin` từ nguồn kỹ thuật này.
- [ ] Viết runbook cấp quyền, chuyển chi nhánh, thu hồi quyền khẩn và xử lý thất bại khi đồng bộ. Ghi thời điểm đồng bộ quyền thành công gần nhất để người vận hành biết trạng thái đang áp dụng.
- [ ] Bổ sung kiểm thử cục bộ theo quy ước repo. Dùng dữ liệu tổng hợp và trả kết quả đối chiếu cho bản demo.

## 5. Bài nghiệm thu bắt buộc

| Tình huống | Kết quả đạt |
|---|---|
| Quản lý A và B mở cùng URL, hai chi nhánh có chung SKU nhưng doanh số và tồn khác nhau | Mỗi tài khoản thấy đúng số liệu chi nhánh của mình, khớp truy vấn đối soát trong Trino |
| Mở URL dashboard toàn chuỗi hoặc gọi API biểu đồ bằng tài khoản chi nhánh | Truy cập ngoài quyền bị từ chối. API của biểu đồ được cấp chỉ trả dữ liệu đúng phạm vi |
| Đổi tham số bộ lọc sang chi nhánh khác, mở danh sách giá trị bộ lọc và thử xuất | Truy vấn giữ đúng điều kiện RLS. Bộ lọc chỉ hiển thị dữ liệu được phép. Thao tác xuất bị chặn |
| Tài khoản có role nghiệp vụ nhưng thiếu role phạm vi | Truy cập bị từ chối hoặc mọi truy vấn phạm vi trả 0 dòng |
| Chuyển quản lý từ A sang B khi phiên còn mở, rồi đồng bộ quyền | Truy vấn mới trả dữ liệu B và quyền với dữ liệu A đã bị thu hồi |
| Vô hiệu hóa tài khoản hoặc xóa ánh xạ, rồi đồng bộ | Phiên đang mở mất quyền truy vấn dữ liệu BI được quản lý |
| Chạy đồng bộ hai lần hoặc publish lại với dataset mới | Role và rule giữ đúng cấu hình, không sinh bản trùng. Dataset mới áp RLS từ truy vấn đầu tiên |
| `city_planner` ở hai thành phố hoặc người dùng khác phòng ban | Mỗi người thấy đúng thành phố và đúng dashboard nghiệp vụ được cấp |

**Điều kiện mở đợt 2:** hoàn tất nghiệm thu POS, tồn kho, truy cập chéo và thu hồi quyền chi nhánh. **Điều kiện bàn giao:** đối chiếu đầy đủ ma trận vai trò, lặp lại bài kiểm tra trên một lần nâng cấp dataset và bàn giao runbook thu hồi quyền.
