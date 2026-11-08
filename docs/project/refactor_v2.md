# Refactor v2 — Generator Console và BI trên Lakehouse

Cập nhật **05/11/2026**. Quyết định đã chốt: thay toàn bộ Next.js, gồm Storefront/Admin/POS/BI UI, bằng **Streamlit điều khiển generator** và **Superset phục vụ BI qua Trino**. Đây là kiến trúc mục tiêu; đợt 1 chỉ chốt phạm vi và ghi baseline, chưa xóa UI, đổi API, sửa generator hoặc cài hai công cụ mới.

## 1. Quyết định kiến trúc và ranh giới

**Trạng thái chuyển PostgreSQL ngày 07/11/2026:** phần 1 đã chuyển schema/ORM (28 bảng, UUID native, identity, CHECK và trigger UTC), có kiểm tra PostgreSQL biệt lập; xem [database guide](../../database/README.md) và [OLTP reference](../architecture/OLTP_SCHEMA.md). Phần 2 đã chuyển generator sang SQL PostgreSQL, seed bổ sung bản ghi an toàn và importer có transaction/đối soát/identity restart; generator 0.10 đã đưa thành phố/cửa hàng vào SQL và cho chọn qua YAML/Streamlit; nạp preset small và month-test được kiểm tra trên PostgreSQL biệt lập. Không nạp hoặc migrate database hiện có. Phần 3 đã kiểm chứng các luồng nghiệp vụ chính trên PostgreSQL biệt lập và sửa transaction/ledger; phạm vi và giới hạn tại [API nguồn](API_SOURCE_SCOPE.md). Sáu API BI và mười GET quản trị danh sách/trùng lặp đã bỏ theo quyết định thu gọn mới; giữ toàn bộ API ghi và lookup cần cho mô phỏng, Superset chưa thay thế. Batch JDBC/extraction/đối soát đã chuyển PostgreSQL với reader chỉ đọc; CDC còn chờ chuyển đổi; provisioning MinIO/Polaris đã có lệnh khởi tạo chủ động, đã nghiệm thu batch trên stack biệt lập với Airflow/Polaris/Trino; CDC và Superset còn chờ. Streamlit đã có trong platform-app, gồm nạp SQL job qua importer với checksum/lịch sử riêng; runner mua online API đã có (khách mới → giỏ → checkout → xác minh), các scenario mở rộng và Superset chưa triển khai. Hiệu chỉnh sampler 2c được để cuối theo quyết định sau kế hoạch gốc, không chặn các bước này. Không dùng baseline MySQL cũ để khẳng định trạng thái PostgreSQL hiện tại.

Giữ bài toán **retail thời trang Việt Nam**, VND, ngày nghiệp vụ Asia/Ho_Chi_Minh. Tập trung vào nguồn dữ liệu giải thích được, nghiệp vụ nhất quán và kết quả phân tích có thể đối soát.

| Thành phần | Trách nhiệm sau chuyển đổi |
|---|---|
| Streamlit | Chọn preset/profile, chỉnh cấu hình, chạy generator, xem/tải kết quả; tác vụ nạp và traffic là bước riêng |
| Generator CLI | Sinh SQL lịch sử và access log tổng hợp; là logic dùng chung cho CLI/console |
| FastAPI + PostgreSQL | Nghiệp vụ nguồn và tra cứu phục vụ mô phỏng; giữ auth, quyền, CSRF/idempotency cần thiết |
| Spark/Airflow | Batch, Silver/Gold/mart, quality gate và publication |
| Debezium/Kafka/Flink | CDC Bronze audit và access log tới Landing; không tự cập nhật OLTP Silver/Gold |
| Trino + Superset | Đọc đầu ra phân tích đã công bố; Superset thay giao diện BI tự viết |
| MinIO/Iceberg/Polaris | Giữ dữ liệu, snapshot, catalog và quyền workload hiện có |

Bối cảnh: BI đang nằm chung trong `services/storefront`, còn nguồn nghiệp vụ và pipeline hoạt động độc lập với Next.js. Hệ quả của quyết định: cần nguồn gọi API thay thao tác UI, dựng lại dashboard và chuyển quyền/phạm vi cửa hàng cùng cơ chế publication; không tự kế thừa các kiểm tra ở FastAPI khi BI truy vấn thẳng Trino. Quyết định thu gọn mới đã bỏ sáu API BI cũ trước Superset; các trang Next.js gọi chúng không còn hoạt động. API nghiệp vụ/lookup vẫn giữ. Volume, checkpoint, bảng nguồn và lịch sử được giữ; ML, marketplace và API/scraping ngoài phạm vi chuyển đổi này.

## 2. Kịch bản dữ liệu phải giữ

API trong bảng có tiền tố `/api/v1`, trừ health và đường internal nêu riêng. “Có API” chỉ xác nhận source code và OpenAPI; chưa chứng minh simulator tự động hóa hoặc giao dịch runtime đúng toàn luồng.

| Mã / kịch bản | Nguồn và thao tác cần giữ | Bảng nguồn chính | Đầu ra phân tích / khoảng trống hiện tại |
|---|---|---|---|
| S00 — Dữ liệu tham chiếu, tài khoản | Auth; catalog; quản trị product/variant, customer; lookup cửa hàng/nhân sự khi cần | customers, customer_credentials, categories, products, product_variants, cities, stores, delivery_staff | Dimension, coverage admin/store. Credentials chỉ phục vụ auth, loại khỏi batch/CDC |
| S01 — Mua online thành công | Cart, checkout/quote, checkout; xác nhận, dispatch, deliver, complete; đọc đơn để kiểm tra | carts, cart_items, orders, order_items, payments, order_status_history, inventory, inventory_transactions, shipments | Order/product/category sales, operations, logistics. Runner mới có cart/quote/checkout và hậu kiểm; chưa có confirm/dispatch/deliver/complete |
| S02 — Hủy đơn | Cancel của khách/admin; trả tồn, release coupon và refund khi nghiệp vụ yêu cầu | orders, order_status_history, inventory, inventory_transactions, payments, refunds, coupon_redemptions | Tài chính/order mart, operations; cần bổ sung kịch bản tự động và đối soát rollback |
| S03 — COD/giao thất bại | Payment method, dispatch/deliver/failed-delivery; lookup nhân sự/shipment | orders, payments, shipments, shipment_attempts, customers, inventory_transactions | Logistics/COD/operations; chưa có hạn giao dự kiến nên không kết luận on-time SLA |
| S04 — POS | GET pos/products và POST pos/transactions với store/staff hợp lệ | orders, order_items, payments, order_status_history, store_inventory, inventory_transactions | Doanh số kênh/cửa hàng, tồn; generator SQL hiện không ghi channel/store_id và nhận mặc định online |
| S05 — Nhập hàng và giá vốn | POST admin/inbound/receipts; đọc phiếu để kiểm tra | inbound_receipts, inbound_receipt_items, inventory, inventory_transactions, product_variants | Tồn/phiếu nhập và COGS snapshot của lần bán sau; simulator chưa tạo nhập hàng |
| S06 — Điều chuyển/điều chỉnh tồn | admin/inventory/refill, admin/branch-inventory; kiểm tra số dư trước/sau | inventory, store_inventory, inventory_transactions | Current/summary/history/store marts; hai route đã chuyển sang transaction có commit/rollback và ledger; mốc tồn đầu cố định |
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

Phạm vi endpoint được đối chiếu từ [router đăng ký](../../services/platform-app/src/platform_app/api/routes/router.py). Generator 0.10 hiện có 25 đích INSERT, bao gồm `cities`/`stores` được chọn qua `locations.store_codes` và tái sử dụng theo mã. Chưa sinh `store_inventory`, `inbound_receipts`, `inbound_receipt_items` hoặc gán đơn online sang cửa hàng. Không coi preset lịch sử hiện có là đã bao phủ đủ các nguồn này.

Cơ sở đối chiếu: [router đăng ký](../../services/platform-app/src/platform_app/api/routes/router.py), [SQL exporter](../../generator/src/generator/sql_export.py), simulator cũ `scripts/simulate_web_traffic.py` (đã loại khỏi repo), [API kho](../../services/platform-app/src/platform_app/api/modules/admin/router.py), [publication reader](../../services/platform-app/src/platform_app/api/modules/analytics/reader.py) và [định nghĩa KPI](../architecture/METRICS_CATALOG.md).

## 3. Phạm vi backend và điều kiện bỏ endpoint

Phạm vi mới tại [API_SOURCE_SCOPE](API_SOURCE_SCOPE.md): bỏ sáu endpoint BI và mười GET quản trị danh sách/trùng lặp cùng service/schema không còn dùng; giữ toàn bộ API ghi và các lookup cần thiết, health và HTTP observability. Lookup customer cần email và trả ID số; lookup thành phố/cửa hàng trả ID cho POS/điều chuyển. Không bỏ auth, CSRF, idempotency hoặc quyền cửa hàng. Simulator cũ đã bị xóa; không coi nó là consumer đang chạy. Các trang Next.js cũ còn trong repo nhưng không còn dùng được các endpoint đã bỏ. Không xóa mart hoặc hạ tầng vì thu gọn lớp phục vụ BI.

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

Đợt **2b đã hoàn thành đo nguồn và tạo profile tham chiếu**, xem [GENERATOR_REFERENCE_PROFILE.md](GENERATOR_REFERENCE_PROFILE.md), [spec](../../generator/profiles/uci-reference.yml) và [JSON aggregate](../../generator/profiles/uci-online-retail-uk-v1.json). CLI `profile-reference` đọc artifact được ghim SHA-256, không tự tải hoặc nạp DB. Từ 541.909 dòng raw, chính sách cohort giữ 457.220 dòng trong hai kỳ sáu tháng UK, tương ứng 7.335 invoice fit và 9.783 invoice evaluation. Ghi từng lý do loại, giữ dòng nghi trùng và outlier, cách ly invoice có metadata xung đột; missing CustomerID không bị đưa vào mẫu số mua lại.

Nguồn [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) bán quà tặng UK và có khách sỉ, không đại diện thời trang/POS VN. Profile đã đo bốn đặc trưng và quan hệ giỏ/lượng mua, nhưng **chưa áp dụng vào generator**. Ví dụ median giỏ fit là 15 mã hàng so với giới hạn 1–4 SKU của generator hiện tại; cần chốt cách thích nghi, không sao chép nguyên phân phối nguồn. Attribution/license, timezone chưa xác định, exposure/censoring và giới hạn diễn giải được lưu trong tài liệu.

| Đặc trưng | Cách đo phải thống nhất |
|---|---|
| Giỏ hàng | SKU distinct mỗi invoice hợp lệ, tách với số dòng/tổng lượng; chốt mapping SKU nguồn sang variant |
| Lượng mua | Quantity dương theo SKU trong đơn sau khi gộp dòng đúng quy tắc |
| Mua lại | Số đơn/khách, tỷ lệ có từ hai đơn và khoảng cách mua trong cùng cửa sổ |
| Tập trung sản phẩm | Tỷ trọng lượng bán, đường tích lũy/top share theo thứ hạng hoặc tỷ lệ SKU khi đổi quy mô |

Nguồn → cách làm sạch → thống kê → profile/logic sinh → kiểm tra đầu ra. Mỗi tham số gắn nhãn **đo từ nguồn / đã điều chỉnh / giả định / kịch bản**. Chỉ hiệu chỉnh đặc trưng có nguồn phù hợp; giá VND, Tết, POS, coupon, COD, hủy và review còn thiếu bằng chứng thì giữ giả định/kịch bản.

Baseline ưu tiên đặc trưng tham chiếu; scenario ưu tiên quy mô/kịch bản, công bố phần phải điều chỉnh. Chốt đầu vào giữ cứng khi tổng đơn/số khách/tần suất xung đột. Tách tập hiệu chỉnh/đánh giá theo thời gian, giữ nguyên invoice; kiểm tra quan hệ giữa biến và dữ liệu sau ràng buộc, chạy nhiều seed/quy mô. Đặt ngưỡng theo từng đặc trưng có lý do; không công bố “độ thật 95%”. Ghim profile/reference version vào nhận diện dataset; ID lần chạy job khác ID dataset.

## 6. Lộ trình và điều kiện hoàn thành

| Đợt | Đầu ra | Điều kiện chuyển bước |
|---|---|---|
| 1 — Phạm vi/baseline | Ma trận S00–S11, inventory API, bao phủ mart và số liệu quan sát | Đã hoàn thành phần tài liệu/quan sát; không đồng nghĩa các scenario đã chạy |
| 2 — Generator | Đã xong 2a: sửa lỗi; 2b: đặc tả, audit và profile tham chiếu; tiếp theo 2c: áp dụng/đánh giá | Chưa có sampler hiệu chỉnh hoặc báo cáo trước/sau dữ liệu sinh; chưa nghiệm thu cả đợt 2 |
| 3 — Streamlit | Preset/YAML, validate, job nền và kết quả SQL/log, chạy lại không tự kích hoạt | Kết quả tương đương CLI; không ghi đè, không tự nạp MySQL |
| 4 — Nạp/mô phỏng | Nạp dataset có nhận diện trùng; script/API cho các scenario đã chọn | Không reset dữ liệu ngầm; kiểm tra postcondition, quyền, CSRF/idempotency |
| 5 — Superset | Một dashboard doanh thu/lợi nhuận trước, rồi đủ nhóm mart | Giữ công thức, publication đồng nhất và quyền; đối chiếu SQL trước khi chuyển toàn bộ |
| 6 — Bỏ Next.js/thu gọn API | Dọn frontend, Compose/CI/env/docs và endpoint đã có thay thế | Nguồn và đầu ra BI chạy khi không bật Next.js; giữ dữ liệu/volume/checkpoint |
| 7 — Kiểm chứng toàn luồng | Generator → MySQL → batch → publication → Trino → Superset; CDC/log kiểm tra riêng | Có bằng chứng từng luồng, nguồn/độ mới rõ; không gán SLA khi chưa đo |

Superset truy vấn trực tiếp không tự kế thừa `PublishedReader` hoặc RBAC tại API. Cần PoC riêng để chốt serving snapshot/version; publication OLTP và log có mốc độc lập. Không thêm database OLAP hoặc semantic layer mới trong bước chốt phạm vi. Upload JSONL lịch sử vào MinIO hiện chưa tự đi Gold; xuất SQL không sinh HTTP access log.

Đợt 1 chốt phạm vi/baseline; 2a sửa nghiệp vụ; 2b thêm CLI đo nguồn, spec và profile aggregate nhỏ cùng test không dùng mạng/dữ liệu thật. Raw UCI để ngoài Git, không thêm dependency. Chưa đổi UI, API, schema, dịch vụ hoặc biến môi trường. `FINAL_REPORT.md` giữ nguyên. Bước tiếp theo **2c**: chốt cohort/cách điều chỉnh cho retail, cấu hình baseline/scenario, gắn profile vào dataset identity, thay sampler rồi đánh giá dữ liệu sinh theo cùng grain và các ràng buộc nghiệp vụ.

### Tiến độ runner — 07/11/2026

Đã bổ sung runner mua online qua API trên Streamlit: cấu hình số lượt/số lượng/khoảng nghỉ/COD–VietQR, chạy nền, dừng giữa các lượt, journal riêng và hậu kiểm đơn/giỏ. Chỉ gồm phần đầu S01; chưa fulfillment hay scenario mở rộng. Mỗi lượt là khách mới và một SKU; sampler/phân phối/profile tham chiếu không thay đổi. Không replay checkout khi chưa rõ kết quả. Không tự bật Kafka, nạp log hoặc chạy DAG; provisioning và batch PostgreSQL đã hoàn tất kiểm chứng biệt lập; CDC PostgreSQL, Superset và bỏ Next.js vẫn còn. Xem [console design](GENERATOR_CONSOLE.md#9-runner-api-mua-online).

### Tiến độ batch PostgreSQL — 07/11/2026

Batch dùng PostgreSQL JDBC, schema public, interval UTC có timezone và truy vấn đối soát PostgreSQL; Spark/Airflow nhận cấu hình nguồn/storage/catalog qua Compose. Có lệnh `make source-reader` cấp SELECT 27 bảng, loại credentials và quyền ghi; không tự chạy hoặc migrate schema. Bronze chặn trộn dữ liệu MySQL cũ với nguồn PostgreSQL. Xem [pipeline guide](../../pipelines/README.md#postgresql-batch-source--07112026) cho kiểm chứng biệt lập và giới hạn: kiểm tra batch ban đầu dùng catalog cục bộ; đợt 08/11 bên dưới đã kiểm chứng thêm Polaris/Airflow/Trino thực. CDC PostgreSQL, Superset và dọn Next.js vẫn còn.

Kiểm chứng batch biệt lập đã đạt: 27 nguồn, 30 đối soát nguồn–Silver ở tolerance 0, 27 bảng Gold, 55 quality checks và publication Iceberg. Sửa validation Silver để nhận biến động `order_cancellation` hợp lệ; không nới việc loại movement lạ. Kết quả không đại diện dữ liệu production hay toàn bộ phân phối/kịch bản.

### Tiến độ provisioning — 07/11/2026

Có `make provision-lakehouse`: MinIO dùng `mc` đã có trong image, Polaris dùng CLI trong platform-app qua service one-off scale 0; không thêm dependency/image riêng và không tự chạy startup. Khởi tạo bucket, bốn account MinIO, catalog/sáu namespace, ba principal Polaris và bốn credential output riêng. File credential 0600, thư mục 0700, UID/GID theo workload; state khởi tạo giữ trong volume root riêng, không mount vào API/UI.

Chạy lại giữ secret/policy MinIO hiện có và RBAC Polaris sau khởi tạo; không tự rotate/reset hoặc xóa dữ liệu. Partial bootstrap Polaris có journal để tiếp tục; mất secret cả state lẫn consumer copy thì dừng và yêu cầu khôi phục/rotate chủ động. Đây là công cụ initialization, không phải IAM reconciliation hoặc health check. Thứ tự chạy và giới hạn tại [SETUP](../runbook/SETUP.md#6-explicit-minio-and-polaris-provisioning). Batch trên Polaris/Airflow/Trino đã nghiệm thu biệt lập ngày 08/11; CDC PostgreSQL, Superset và dọn Next.js vẫn còn.

### Quản trị quyền Streamlit — 08/11/2026

Bổ sung màn hình xem và gán/gỡ liên kết quyền hiện có, qua backend nội bộ `lakehouse-access` dùng cùng image platform-app. Auth dùng tài khoản nguồn admin/system_admin, kiểm tra role/trạng thái mỗi request và CSRF khi ghi. Secret quản trị giữ ở backend riêng; không mount Docker socket hoặc credential volume workload vào UI. MinIO dùng binary mc từ image đang có, không thêm Python dependency.

Polaris hỗ trợ principal role, catalog role và các grant catalog được chọn; MinIO hỗ trợ direct-policy binding của bốn workload. Không tạo account/role, sửa policy JSON, chỉnh group hoặc xoay secret. Snapshot revision chặn dữ liệu UI cũ, audit ghi trước thao tác và readback xác minh kết quả; không auto retry trạng thái chưa rõ. YAML/provisioning vẫn giữ semantics khởi tạo; không biến thành đồng bộ quyền liên tục. Chi tiết/giới hạn tại [platform guide](../../services/platform-app/README.md#quản-trị-quyền-trên-streamlit).

Kiểm chứng phần quản trị quyền: bộ platform-app 113 tests đạt, gồm auth/CSRF/thu hồi role trên PostgreSQL biệt lập và thao tác thực trên MinIO/Polaris. Live check xác minh 9 thay đổi với 18 audit record, thêm 4 thay đổi dưới UID 1000; không dùng database/volume production. Source API, schema và sampler giữ nguyên; chưa nghiệm thu triển khai public hay toàn bộ pipeline.

### Nghiệm thu batch trên stack thực — 08/11/2026

Đã kiểm chứng luồng PostgreSQL → Spark do Airflow điều phối → Iceberg trên MinIO/Polaris → publication → Trino bằng stack biệt lập. Hai DAG initial/interval đạt 109/109 task mỗi DAG. Runner tạo hai đơn COD qua API: số đơn ở fact/mart tăng 30 → 32, giá trị đặt hàng tăng 8.800.880 → 9.298.880 đồng; doanh thu thuần giữ 5.303.400 đồng vì COD chưa thanh toán. Đối soát nguồn–Silver đạt 30/30 ở cả hai lượt.

Trino từ chối INSERT/DROP; thu hồi quyền Spark khiến job bị chặn; replay orders không tăng số bản ghi. Cố ý làm lệch tổng tiền bị quality gate chặn, reader vẫn đọc publication thành công trước đó; khôi phục đạt 55/55 kiểm tra và publish lại cùng run ID không nhân đôi publication. Đã sửa ngày initial-load khi Airflow 3 không cung cấp interval và bật Trino `iceberg.security=READ_ONLY`.

Đây là kiểm chứng batch trên dữ liệu nhỏ, chạy DAG thủ công qua scheduler; chưa kiểm chứng lịch cron, CDC PostgreSQL, streaming log, Superset hoặc tải production. Không sửa/reset database và volume hiện có. Xem [cách chạy và bằng chứng](../../pipelines/README.md#full-stack-acceptance-opt-in). Phần tiếp theo là chuyển CDC sang PostgreSQL; Superset, dọn Next.js, scenario mở rộng và hiệu chỉnh generator vẫn nằm trong kế hoạch riêng.
