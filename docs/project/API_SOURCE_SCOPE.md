# API nguồn dữ liệu — PostgreSQL

Cập nhật 07/11/2026. FastAPI giữ vai trò tạo/tra cứu dữ liệu nghiệp vụ và quan sát HTTP; Streamlit điều khiển generator, Superset là đầu ra BI dự kiến. Đây là quyết định thu gọn trước phần 3, thay điều kiện giữ API BI tới khi có Superset trong kế hoạch cũ.

## Phạm vi giữ lại

| Nhóm | Vai trò với dữ liệu |
|---|---|
| Auth, phân quyền, CSRF | Tạo khách hàng, xác định chủ giao dịch và phạm vi cửa hàng |
| Catalog, sản phẩm/variant, customer, địa điểm/nhân viên | Dữ liệu tham chiếu và lookup ID cho kịch bản sinh dữ liệu |
| Cart, wishlist, checkout, coupon | Hành vi mua hàng, đơn, payment, coupon redemption, access log |
| Order, giao hàng/COD, return/refund, review | Trạng thái nghiệp vụ, tiền, chất lượng dịch vụ và biến động kho |
| POS, nhập hàng, điều chuyển/điều chỉnh tồn | Bán theo cửa hàng, giá vốn và ledger |
| Store order detail/confirm/cancel | Tra cứu chi tiết và thao tác có quyền theo cửa hàng; bỏ các danh sách trùng |
| Health, HTTP Metrics/Realtime | Quan sát kỹ thuật và đường dữ liệu log |

Giữ toàn bộ 41 khai báo route ghi POST/PUT/PATCH/DELETE có trong bản trước đợt thu gọn, gồm add/update/remove cart và wishlist. GET lookup chỉ giữ để nối các bước nghiệp vụ hoặc đọc lại kết quả; các danh sách quản trị không có vai trò riêng được bỏ. Middleware ghi access log vẫn giữ nguyên. Không xóa bảng, pipeline, mart, publication hay dữ liệu.

## Hai cách tạo dữ liệu cùng được giữ

- Generator SQL tạo dữ liệu nền và lịch sử số lượng lớn.
- FastAPI thực hiện giao dịch và thay đổi trạng thái qua logic nghiệp vụ: add-to-cart, checkout, POS, nhập hàng, giao/hủy đơn, return/refund, review và thay đổi dữ liệu tham chiếu.

Streamlit hiện chạy generator SQL/access log và có thao tác nạp SQL job vào PostgreSQL; chưa có runner gọi API để mô phỏng nghiệp vụ. Bộ kiểm tra đã gọi các luồng này qua HTTP trên database biệt lập, không đồng nghĩa runner mô phỏng đã triển khai. Bỏ storefront không làm mất API nghiệp vụ hoặc các GET sản phẩm/giỏ/đơn phía khách.

## Lookup còn giữ và ID dùng cho bước ghi

Tiền tố `/api/v1`:

| GET | ID/kết quả được dùng |
|---|---|
| `/admin/products?search=<SKU hoặc slug>` | `variants[].variant_id` cho nhập/điều chuyển/POS; `public_id` cho thay đổi sản phẩm/variant |
| `/admin/customers?email=<email>` | Tra cứu đúng email, không phân biệt hoa/thường, tối đa 1 kết quả; thêm `customer_id` cho POS có khách thành viên, giữ UUID cho thay đổi trạng thái |
| `/locations/cities` | Thêm `city_id`, giữ mã thành phố để chọn địa điểm |
| `/locations/cities/{city_code}/stores` | Thêm `store_id`/`city_id`; lấy ID cửa hàng ngay cả khi chưa có tồn kho |
| `/admin/delivery-staff` | `staff_id` cho dispatch khi dùng nhân viên đã có |
| `/admin/branch-inventory` | Số dư đầu vào thao tác và `store_id`/`variant_id`; không thay thế lookup cửa hàng chưa có tồn |
| `/admin/orders/{order_number}` | Dòng hàng, payment/refund và shipment; thay hai GET shipment bị bỏ |
| `/admin/inbound/receipts/{receipt_code}` | Đọc lại phiếu nhập, số lượng và giá vốn |
| `/admin/returns/{return_code}` | Trạng thái và `return_item_id` để kiểm định/hoàn tiền |
| `/admin/store/orders/{order_number}` | Đọc chi tiết theo quyền cửa hàng trước thao tác confirm/cancel |

GET customer yêu cầu `email`: thiếu trả 422, không khớp trả danh sách rỗng, không đúng quyền trả 403. Lookup sản phẩm/cửa hàng/khách được kiểm tra bằng chuỗi HTTP lookup → nhập hàng → điều chuyển → POS thành viên; ID đầu vào lấy từ response API, SQL chỉ dùng đối soát kết quả.

Khi đối tượng vừa được tạo, ưu tiên lưu ID từ response POST thay vì gọi GET danh sách: POST sản phẩm trả UUID và variant ID; POST coupon trả UUID/code; POST delivery staff trả staff ID; POST checkout trả order number; POST review/return trả ID cần cho bước tiếp theo.

## GET quản trị đã bỏ thêm

Mười GET sau được bỏ cùng service/schema chỉ phục vụ danh sách; giữ nguyên POST/PATCH/DELETE trên cùng đường dẫn khi có:

- `/admin/orders`
- `/admin/shipments`
- `/admin/shipments/{order_number}`
- `/admin/inbound/receipts`
- `/admin/returns`
- `/admin/reviews`
- `/admin/coupons`
- `/admin/store/orders`
- `/admin/store/inventory`
- `/admin/store/staff`

GET tới đường dẫn đã bỏ có thể trả 404 hoặc 405 khi đường dẫn vẫn còn POST. Tổng cộng thu gọn 16 GET gồm 6 endpoint BI bên dưới; giữ toàn bộ API ghi. `store` vẫn giữ detail/confirm/cancel và kiểm tra quyền cửa hàng.

## Endpoint BI đã bỏ

Cả sáu đều là GET, tiền tố `/api/v1`:

- `/admin/overview`
- `/admin/analytics/overview`
- `/admin/analytics/role-metrics`
- `/admin/analytics/sales-trend`
- `/admin/analytics/stores`
- `/admin/store/dashboard`

Xóa các handler, schema và service tổng hợp BI không còn người gọi ở backend. Giữ `/admin/analytics/http-metrics` và `/admin/analytics/http-realtime` cho admin/system_admin, cùng reader/cache HTTP và Trino client. Danh sách địa điểm nghiệp vụ lấy từ `/locations/cities` và `/locations/cities/{city_code}/stores`.

**Ảnh hưởng:** các trang tổng quan/BI và danh sách admin Next.js cũ còn trong repo sẽ nhận 404/405 khi gọi endpoint đã bỏ; danh sách customer không có email trả 422. Không có lớp tương thích hoặc dashboard Superset thay thế trong đợt này. Tài liệu thiết kế dashboard cũ mô tả phiên bản trước; không phải hợp đồng serving hiện tại.

## Transaction và số dư kho

- `run_in_transaction` chỉ retry SQLSTATE PostgreSQL 40P01, 40001 và 55P03, mỗi lần dùng session/transaction mới; mặc định tối đa 3 lần thử qua `API_CHECKOUT_MAX_RETRIES` (>=1). Lỗi connection/commit không rõ kết quả không tự retry. Không đặt I/O bên ngoài transaction trong callback được retry.
- Hai API điều chỉnh/điều chuyển kho đã dùng transaction có commit/rollback. Điều chuyển khóa kho tổng trước tồn cửa hàng, ghi hai ledger cùng reference và cập nhật số dư/version atomically. Điều chỉnh ghi delta `adjustment`; gửi lại cùng số dư đích không tạo thêm ledger.
- `opening_on_hand` là mốc tồn ban đầu cố định. Với mỗi variant/location: `on_hand = opening_on_hand + SUM(quantity_delta)`. Nhập hàng, điều chuyển, trả hàng không tăng lại mốc tồn đầu; chúng ghi ledger.
- Bỏ hai CHECK `on_hand <= opening_on_hand` của inventory/store_inventory vì nhập/nhận hàng có thể làm tồn vượt mốc đầu. Giữ CHECK số dư và tồn đầu không âm. Điều chỉnh/điều chuyển giới hạn biến động theo miền INTEGER của ledger.
- POS lookup bỏ hàm MySQL BIN, ưu tiên SKU khớp chính xác không phân biệt hoa/thường; timestamp POS dùng UTC-naive. Tìm kiếm catalog dùng ILIKE. Hai POST store confirm/cancel có CSRF và store manager không chọn được cửa hàng khác.

Schema mới có 185 CHECK, giữ 28 bảng/316 cột/51 FK. `database/schema.sql` vẫn chỉ bootstrap database trống; không có migration tự động. Database cũ cần cập nhật hai CHECK và đối soát mốc tồn đầu với lịch sử trước khi áp dụng công thức trên; không tự sửa số dư hoặc viết lại ledger.

## Kiểm chứng và giới hạn

Chạy từ root, dùng PostgreSQL 16.8 Docker biệt lập qua socket, network none và tmpfs:

```bash
RUN_POSTGRES_SCHEMA_TESTS=1 uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
```

Bộ kiểm tra gọi router HTTP thật với cookie đăng nhập/CSRF, service và transaction PostgreSQL thật. Đọc lại bằng connection mới để xác nhận persistence. Bao phủ giữ API ghi/bỏ GET danh sách, lookup ID qua HTTP, product/coupon/customer mutations, auth/lookup/wishlist, checkout replay/hủy/coupon release, COD giao thành công/thất bại, review, return/refund, nhập hàng, điều chuyển/điều chỉnh và POS. Kiểm tra quyền store, thiếu tồn, rollback khi lỗi ledger, hai checkout tranh một đơn vị cuối và retry SQLSTATE với transaction mới.

Đây là bằng chứng cục bộ cho các kịch bản đã kiểm tra, không phải benchmark, kiểm chứng toàn bộ endpoint hoặc kiểm chứng production. Test không khởi động Kafka lifespan, không xác nhận access log đã tới Kafka/Flink/MinIO; HTTP quan sát chỉ kiểm tra RBAC/contract, chưa gọi Trino thật. Batch JDBC/CDC/provisioning và Superset còn ở bước tiếp theo.

Giữ semantics idempotency hiện có: checkout, một số transition, nhập hàng và hoàn tiền có cơ chế riêng; POS và refill không có bảo đảm dedup request toàn cục. Không tự retry chúng khi chưa biết request trước đã commit hay chưa. Idempotency nhập hàng hiện dùng digest ngắn theo code cũ, chưa phải cơ chế chống xung đột key/payload đầy đủ. Luồng exchange API chưa được kiểm chứng tương đương kịch bản exchange SQL của generator.
