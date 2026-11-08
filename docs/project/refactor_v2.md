# Refactor v2 — Generator Console và BI trên Lakehouse

Cập nhật **05/11/2026**. Quyết định đã chốt: thay toàn bộ Next.js, gồm Storefront/Admin/POS/BI UI, bằng **Streamlit điều khiển generator** và **Superset phục vụ BI qua Trino**. Đây là kiến trúc mục tiêu; đợt 1 chỉ chốt phạm vi và ghi baseline, chưa xóa UI, đổi API, sửa generator hoặc cài hai công cụ mới.

## 1. Quyết định kiến trúc và ranh giới

Giữ bài toán **retail thời trang Việt Nam**, VND, ngày nghiệp vụ Asia/Ho_Chi_Minh. Tập trung vào nguồn dữ liệu giải thích được, nghiệp vụ nhất quán và kết quả phân tích có thể đối soát.

| Thành phần | Trách nhiệm sau chuyển đổi |
|---|---|
| Streamlit | Chọn preset/profile, chỉnh cấu hình, chạy generator, xem/tải kết quả; tác vụ nạp và traffic là bước riêng |
| Generator CLI | Sinh SQL lịch sử và access log tổng hợp; là logic dùng chung cho CLI/console |
| FastAPI + MySQL | Nghiệp vụ nguồn và tra cứu phục vụ mô phỏng; giữ auth, quyền, CSRF/idempotency cần thiết |
| Spark/Airflow | Batch, Silver/Gold/mart, quality gate và publication |
| Debezium/Kafka/Flink | CDC Bronze audit và access log tới Landing; không tự cập nhật OLTP Silver/Gold |
| Trino + Superset | Đọc đầu ra phân tích đã công bố; Superset thay giao diện BI tự viết |
| MinIO/Iceberg/Polaris | Giữ dữ liệu, snapshot, catalog và quyền workload hiện có |

Bối cảnh: BI đang nằm chung trong `services/storefront`, còn nguồn nghiệp vụ và pipeline hoạt động độc lập với Next.js. Hệ quả của quyết định: cần nguồn gọi API thay thao tác UI, dựng lại dashboard và chuyển quyền/phạm vi cửa hàng cùng cơ chế publication; không tự kế thừa các kiểm tra ở FastAPI khi BI truy vấn thẳng Trino. Next.js và API cũ được giữ tới khi có đầu ra thay thế đã kiểm chứng. Volume, checkpoint, bảng nguồn và lịch sử được giữ; ML, marketplace và API/scraping ngoài phạm vi chuyển đổi này.

## 2. Kịch bản dữ liệu phải giữ

API trong bảng có tiền tố `/api/v1`, trừ health và đường internal nêu riêng. “Có API” chỉ xác nhận source code và OpenAPI; chưa chứng minh simulator tự động hóa hoặc giao dịch runtime đúng toàn luồng.

| Mã / kịch bản | Nguồn và thao tác cần giữ | Bảng nguồn chính | Đầu ra phân tích / khoảng trống hiện tại |
|---|---|---|---|
| S00 — Dữ liệu tham chiếu, tài khoản | Auth; catalog; quản trị product/variant, customer; lookup cửa hàng/nhân sự khi cần | customers, customer_credentials, categories, products, product_variants, cities, stores, delivery_staff | Dimension, coverage admin/store. Credentials chỉ phục vụ auth, loại khỏi batch/CDC |
| S01 — Mua online thành công | Cart, checkout/quote, checkout; xác nhận, dispatch, deliver, complete; đọc đơn để kiểm tra | carts, cart_items, orders, order_items, payments, order_status_history, inventory, inventory_transactions, shipments | Order/product/category sales, operations, logistics. Simulator có checkout/confirm/complete, chưa thay toàn bộ fulfillment |
| S02 — Hủy đơn | Cancel của khách/admin; trả tồn, release coupon và refund khi nghiệp vụ yêu cầu | orders, order_status_history, inventory, inventory_transactions, payments, refunds, coupon_redemptions | Tài chính/order mart, operations; cần bổ sung kịch bản tự động và đối soát rollback |
| S03 — COD/giao thất bại | Payment method, dispatch/deliver/failed-delivery; lookup nhân sự/shipment | orders, payments, shipments, shipment_attempts, customers, inventory_transactions | Logistics/COD/operations; chưa có hạn giao dự kiến nên không kết luận on-time SLA |
| S04 — POS | GET pos/products và POST pos/transactions với store/staff hợp lệ | orders, order_items, payments, order_status_history, store_inventory, inventory_transactions | Doanh số kênh/cửa hàng, tồn; generator SQL hiện không ghi channel/store_id và nhận mặc định online |
| S05 — Nhập hàng và giá vốn | POST admin/inbound/receipts; đọc phiếu để kiểm tra | inbound_receipts, inbound_receipt_items, inventory, inventory_transactions, product_variants | Tồn/phiếu nhập và COGS snapshot của lần bán sau; simulator chưa tạo nhập hàng |
| S06 — Điều chuyển/điều chỉnh tồn | admin/inventory/refill, admin/branch-inventory; kiểm tra số dư trước/sau | inventory, store_inventory, inventory_transactions | Current/summary/history/store marts; hai route ghi hiện chỉ flush, chưa commit, là điểm chặn cần sửa trước khi dùng |
| S07 — Trả/đổi/hoàn tiền | Tạo/cancel yêu cầu; admin review, receive, inspect-and-resolve; đọc kết quả | return_requests, return_items, refunds, payments, orders, order_items, inventory, inventory_transactions | Returns, operations và tài chính. Trả hàng online là kịch bản đầu; không mặc định POS walk-in dùng được luồng khách đăng nhập |
| S08 — Coupon | Available; tạo/sửa/archive; redeem/release qua checkout/cancel | coupons, coupon_redemptions, orders, order_items | Coupon/marketing summary, tài chính; chưa suy hiệu quả nhân quả từ tỷ lệ do generator đặt |
| S09 — Review | Tạo sau mua đủ điều kiện; admin moderation | product_reviews, order_items, products, customers | Review/marketing summary; ngưỡng approved 1–2 sao tại mốc đo, không suy nguyên nhân từ số lượng |
| S10 — Tìm kiếm, giỏ, wishlist | Categories/facets/products, cart và wishlist GET/PUT/DELETE | carts, cart_items, wishlist_items; access events | Product demand/funnel. Request không phải session/người/đơn distinct; bỏ các GET sẽ đổi cơ cấu log |
| S11 — Quan sát kỹ thuật | Health, HTTP readouts, truy vấn publication/quality/reconciliation | Landing/log fact/marts, system publications/quality; MySQL để đối soát | HTTP/system; chỉ gọi thêm API khi cần đo vì request quan sát có thể tự phát sinh log |

### Bao phủ 18 mart hiện có

| Nhóm | Mart cần có đầu ra BI/tra cứu sau chuyển đổi |
|---|---|
| Kinh doanh | mart_order_daily, mart_sales_daily, mart_product_sales_daily |
| Kho/cửa hàng | mart_inventory_current, mart_inventory_summary, mart_inventory_health, mart_store_overview |
| Vận hành | mart_operations_summary, mart_logistics_performance, mart_cod_region_daily, mart_product_returns |
| Marketing nghiệp vụ | mart_marketing_summary, mart_coupon_usage, mart_product_review_summary |
| Tổng quan admin | mart_admin_overview; tổng tài chính vẫn lấy order mart |
| Log | mart_hourly_route_metrics, mart_daily_product_demand, mart_marketing_funnel_daily |

CSV [API_TRANSITION_SCOPE.csv](API_TRANSITION_SCOPE.csv) liệt kê đủ **86 endpoint** từ router đăng ký và khớp OpenAPI của image API đang chạy. SQL exporter hiện có 23 đích INSERT; không trực tiếp sinh `cities`, `stores`, `store_inventory`, `inbound_receipts`, `inbound_receipt_items`. Không coi preset lịch sử hiện có là đã bao phủ đủ các nguồn này.

Cơ sở đối chiếu: [router đăng ký](../../services/platform-app/src/platform_app/api/api/router.py), [SQL exporter](../../generator/src/generator/sql_export.py), [simulator](../../scripts/simulate_web_traffic.py), [API kho](../../services/platform-app/src/platform_app/api/modules/admin/router.py), [publication reader](../../services/platform-app/src/platform_app/api/modules/analytics/reader.py) và [định nghĩa KPI](../architecture/METRICS_CATALOG.md).

## 3. Phạm vi backend và điều kiện bỏ endpoint

| Nhãn trong CSV | Số endpoint | Quyết định cho đợt chuyển đổi |
|---|---:|---|
| keep | 65 | Giữ trong hợp đồng nguồn/mô phỏng dự kiến, kể cả GET lookup và auth; không đồng nghĩa đã có automation |
| keep_blocked | 2 | Giữ mục tiêu nghiệp vụ, sửa/kiểm chứng transaction trước khi đưa vào kịch bản |
| review | 12 | Lookup/list hoặc route cửa hàng có thể dư; chỉ bỏ sau khi console/simulator có thay thế và giữ quyền store |
| keep_until_replacement | 2 | HTTP Metrics/Realtime: giữ tới khi có quan sát tương đương đã kiểm chứng |
| retire_after_replacement | 5 | Hai overview, role metrics, sales trend và analytics store selector: dự kiến bỏ sau khi chuyển BI/quyền/publication |

Không xóa endpoint, schema, service hay bảng trong đợt 1. Lúc thu gọn phải kiểm tra consumer và kiểm tra nghiệp vụ; không xóa logic tiền/tồn/permission chỉ vì bỏ response trình bày. GET catalog/cart/order và auth đang được `scripts/simulate_web_traffic.py` dùng. Prefix `/internal/v1/orders/.../confirm` vẫn thuộc mô phỏng fulfillment hiện có.

## 4. Baseline đã ghi nhận và giới hạn

Xem [DATA_BASELINE.md](DATA_BASELINE.md): truy vấn MySQL read-only trong một consistent snapshot và Trino theo các snapshot publication đã chọn. Kết quả local ngày 05/11/2026: **34 đơn = 28 online + 6 POS; tổng giá trị đơn 365.506.100 VND; doanh thu thuần đã công bố 19.405.100 VND**. Hai số tiền có định nghĩa khác nhau. Số dòng 27 nguồn phân tích khớp Silver đã pin; chưa phải kiểm chứng toàn bộ tính đúng nghiệp vụ.

Tất cả đơn có `data_origin=manual`, `generation_run_id=NULL`; đây là baseline local, không chứng minh giao dịch thực hay profile generator đã hiệu chỉnh. Có bảng legacy `alembic_version` ngoài 28 bảng ứng dụng; không đưa vào capture phân tích và không xóa. Không chạy backfill, import, DAG, traffic hoặc sửa bản ghi để tạo baseline này.

Đợt 1 phát hiện tĩnh các lỗi payment, liên kết return, biến động kho và nhãn log. Đợt 2a đã tái hiện và sửa nhóm lỗi này bằng generator **0.8.0**; số liệu baseline ở trên vẫn là quan sát cũ, không bị nạp lại. Chọn variant đều, rescale số đơn/khách, COD/return theo modulo và tồn đầu kỳ cộng buffer vẫn là giả định chưa hiệu chỉnh. Lịch sử kho Lakehouse vẫn chỉ quan sát từng lần chạy, không tự tái dựng tồn quá khứ.

### Đợt 2a — Nhất quán nghiệp vụ generator

| Vấn đề trước sửa | Hành vi ở 0.8 |
|---|---|
| Payment mọi đơn succeeded, COD có thể mang trạng thái paid dù chưa thu | COD checkout confirmed/pending; hoàn tất mới succeeded/paid_at; hủy COD chưa thu không có refund; VietQR hủy giữ mốc đã thanh toán |
| Return lấy variant đầu nhưng order_item cuối; thiếu refund đã chi | Gắn đúng order item; refund theo đơn giá sau phân bổ discount và tạo bản ghi theo payment; trả toàn bộ chuyển returned |
| Kho có inbound 500 không kèm phiếu, thiếu cặp xuất/hoàn và hàng đổi | Bỏ inbound giả; ghi xuất checkout, hoàn hủy/giao thất bại, nhận hàng trả và xuất variant đổi; tồn đầu + biến động ròng = tồn cuối |
| Thời gian trả bị ép về anchor; exchange có thể thiếu variant | Chỉ hoàn tất return nếu đủ cửa sổ sau completion; thiếu variant thay thế thì dùng refund |
| Log tự sinh mang nhãn observed; month-test có pending review không hợp lệ | Log lịch sử synthetic; preset chỉ có approved/rejected theo schema hiện hành |

Đã chạy **15 regression test** bằng unittest, không dùng mạng/dữ liệu thật: kiểm tra tiền, tồn, lineage, trạng thái/thời gian, tái lập SQL, trường hợp đơn một variant/giá 0, nhiều seed và batch vượt 1.000 đơn. Bộ 7 test ban đầu tái hiện 5 failure và 1 error trước sửa; test tái lập đã đạt từ trước. Lệnh chạy trong [generator README](../../generator/README.md#regression-checks).

Đã xuất/nạp `small.yml` (3.000 đơn, 89 yêu cầu đổi/trả, 108 refund) và `month-test.yml` (400 đơn, 4 yêu cầu đổi/trả, 7 refund) vào **MySQL 8.4.5 tạm, không nối mạng**, dùng schema gốc và bật FK/CHECK khi nạp dữ liệu. Cả hai đạt bốn truy vấn đối chiếu: COD chưa thu, liên kết return, refund hợp lệ và phương trình tồn. Refund gồm cả hủy đã thanh toán nên không so số lượng trực tiếp với yêu cầu trả hàng. Container đã được xóa; database/pipeline đang chạy và baseline đợt 1 giữ nguyên. Chưa kiểm chứng dataset mới qua Lakehouse hoặc dashboard.

Tăng phiên bản package/identity lên 0.8.0 và cập nhật lock, không thêm dependency. SQL cũ không được tự sửa; phải xuất bộ SQL/log tương ứng phiên bản mới trước khi mô phỏng. Exchange còn là kịch bản SQL, không khẳng định API có luồng tương đương. Hai endpoint tồn kho thiếu commit đã ghi ở S06 chưa được sửa trong đợt generator này.

## 5. Hợp đồng cải thiện generator ở đợt kế tiếp

Cơ sở thực nghiệm nằm trong [notebook EDA](../../generator/notebooks/reference_datasets_eda.ipynb) và [kế hoạch EDA](GENERATOR_EDA_PLAN.md), sử dụng Olist, H&M, UCI Online Retail và eCommerce Behavior. Generator hiện chỉ sinh SQL; phần hành vi giữ làm tham khảo.

| Đặc trưng | Cách đo phải thống nhất |
|---|---|
| Giỏ hàng | Số sản phẩm khác nhau/đơn có hàng trong Olist; cần ánh xạ sang variant của repo |
| Lượng mua | Quantity cộng theo mã hàng trong invoice UCI; số dòng Olist là tham chiếu bổ sung, cần ánh xạ sang variant |
| Mua lại | Số ngày mua H&M và số invoice UCI theo khách; giữ các invoice cùng ngày |
| Tập trung sản phẩm | Tỷ trọng dòng mua và đường tích lũy theo thứ hạng sản phẩm |

Nguồn → cách tính → phân phối thực nghiệm → quy luật lấy mẫu → dữ liệu sinh. R03 đã áp dụng qua mười nhóm trọng số sản phẩm H&M; phân bố đều trong nhóm và ánh xạ catalog là giả định. Các tham số khác giữ theo kịch bản cũ và được ghi trong [GENERATOR_METRICS](GENERATOR_METRICS.md), gồm giới hạn co giãn số đơn theo tổng quy mô. Các yếu tố Việt Nam được tách khỏi tỷ lệ đo từ dataset; không bổ sung so sánh generator trước/sau.

## 6. Lộ trình và điều kiện hoàn thành

| Đợt | Đầu ra | Điều kiện chuyển bước |
|---|---|---|
| 1 — Phạm vi/baseline | Ma trận S00–S11, inventory API, bao phủ mart và số liệu quan sát | Đã hoàn thành phần tài liệu/quan sát; không đồng nghĩa các scenario đã chạy |
| 2 — Generator | Viết lại sinh SQL theo schema MySQL hiện có; áp dụng trọng số H&M, giữ tham số chưa đủ căn cứ thành giả định | Kiểm tra schema/quan hệ, tiền, kho, lịch sử, deterministic output và tài liệu chỉ số; chưa tự nạp DB |
| 3 — Streamlit | Preset/YAML, validate, job nền và kết quả SQL, chạy lại không tự kích hoạt | Kết quả tương đương CLI; không ghi đè, không tự nạp MySQL |
| 4 — Nạp/mô phỏng | Nạp dataset có nhận diện trùng; script/API cho các scenario đã chọn | Không reset dữ liệu ngầm; kiểm tra postcondition, quyền, CSRF/idempotency |
| 5 — Superset | Một dashboard doanh thu/lợi nhuận trước, rồi đủ nhóm mart | Giữ công thức, publication đồng nhất và quyền; đối chiếu SQL trước khi chuyển toàn bộ |
| 6 — Bỏ Next.js/thu gọn API | Dọn frontend, Compose/CI/env/docs và endpoint đã có thay thế | Nguồn và đầu ra BI chạy khi không bật Next.js; giữ dữ liệu/volume/checkpoint |
| 7 — Kiểm chứng toàn luồng | Generator → MySQL → batch → publication → Trino → Superset; CDC/log kiểm tra riêng | Có bằng chứng từng luồng, nguồn/độ mới rõ; không gán SLA khi chưa đo |

Superset truy vấn trực tiếp không tự kế thừa `PublishedReader` hoặc RBAC tại API. Cần PoC riêng để chốt serving snapshot/version; publication OLTP và log có mốc độc lập. Không thêm database OLAP hoặc semantic layer mới trong bước chốt phạm vi. Upload JSONL lịch sử vào MinIO hiện chưa tự đi Gold; xuất SQL không sinh HTTP access log.

Đợt 1 chốt phạm vi/baseline; đợt 2a sửa nghiệp vụ. Hướng tiếp theo dùng các quy luật từ notebook EDA để xây dựng sampler SQL, giữ rõ đơn vị đo và các ràng buộc tiền/tồn/lineage.
