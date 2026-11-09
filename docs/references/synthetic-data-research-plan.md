# Kế hoạch nghiên cứu sinh dữ liệu SQL

## Mục tiêu và phạm vi

Đợt này tập trung vào dữ liệu lịch sử xuất SQL PostgreSQL, phục vụ batch → Bronze → Silver → Gold → mart. Dữ liệu cần đúng nghiệp vụ, có đặc tính mua hàng có căn cứ và trả lời được các câu hỏi phân tích của dự án.

Giữ schema, importer và phạm vi bảng generator hiện có. Mô phỏng qua API, access log, CDC/streaming và các kịch bản gây lỗi hạ tầng để ở đợt sau. Đã triển khai generator 0.11 theo hướng SQL demo tinh gọn ở mục cuối; các phần nghiên cứu còn lại chỉ mở khi cần quyết định nghiệp vụ cụ thể.

## Cách tiếp cận

**Câu hỏi phân tích → dữ liệu cần có → bằng chứng → quy luật sinh → tiêu chí kiểm tra.**

- Kế thừa notebook Olist, H&M, UCI và nghiên cứu Việt Nam; chỉ phân tích thêm khi có câu hỏi cụ thể chưa trả lời được. eCommerce Behavior tiếp tục là tham khảo cho đợt log sau.
- Sinh lịch sử theo quy tắc và quan hệ nghiệp vụ, rồi xuất SQL. Kế thừa code hiện có; chưa cần framework mô phỏng, ML hoặc sinh song song.
- Ưu tiên phân phối thực nghiệm đơn giản; chỉ fit Zipf, Pareto hoặc phân phối khác khi có lý do và kiểm tra phù hợp.
- Phân biệt số đo thực nghiệm, cách chuyển sang bối cảnh dự án và giả định của nhóm. Dữ liệu nước ngoài không chứng minh tỷ lệ đại diện cho thị trường Việt Nam.

## 1. Đối chiếu câu hỏi, dữ liệu và bằng chứng — 09/11/2026

Đã đối chiếu schema, SQL/builder Gold, generator và notebook. Riêng Q02–Q03 đã bổ sung phân tích, chạy lại toàn bộ notebook từ kernel sạch; chưa chạy pipeline. Các số dưới đây mô tả cohort nguồn, không phải kết quả generator mới hay ước lượng thị trường Việt Nam.

| Câu hỏi và dữ liệu cần | Nơi phân tích hiện có | Bằng chứng đã có | Generator hiện tại và phần còn thiếu |
|---|---|---|---|
| Q01. Doanh số thay đổi theo ngày/tháng thế nào? Cần thời điểm đặt, trạng thái, tiền hàng, giảm giá, hoàn tiền và giá vốn. | `mart_order_daily`, `mart_sales_daily` | R04: H&M đo dòng mua/ngày theo tháng; S05–S06 định hướng giai đoạn Tết, có giới hạn ngành/mẫu. | Lịch mua dùng trọng số thứ/giờ, Tết dương lịch cố định và hệ số chiến dịch giả định. Cần tách nền/chiến dịch, xác định lịch Tết theo năm và giữ hệ số chưa đo dưới nhãn giả định. Mart bán hàng hiện ở cấp ngày; chưa phục vụ phân tích giờ. |
| Q02. Doanh số tập trung vào sản phẩm nào? Cần sản phẩm/variant, số lượt chọn, số lượng và tiền dòng hàng. | `mart_product_sales_daily`, `mart_sales_daily` | R03: top 10% article đã bán của H&M chiếm 64,04% dòng mua; khi gộp product_code là 74,82% (notebook mục 3.3). | Đã dùng mười nhóm thứ hạng H&M, chọn sản phẩm trước variant. Cần xác định cách kiểm tra khi đổi quy mô catalog và chọn nhiều item không hoàn lại; tỷ trọng dòng mua không đồng nghĩa tỷ trọng số lượng hoặc doanh thu. Không mặc định phải fit Zipf. |
| Q03. Giỏ hàng có bao nhiêu mặt hàng và giá trị đơn hình thành thế nào? Cần `order_items`, variant, quantity, giá và giảm giá. | `fact_order_item`/`fact_order`; `mart_order_daily` phục vụ tổng hợp tiền, chưa có mart phân phối giỏ. | R02: toàn Olist có 96,67% giỏ một product; cohort thời trang mới có 1.409 đơn, 92,97% giỏ một product và 97,70% cặp đơn–product một đơn vị. UCI có giỏ lớn và khách sỉ, giữ để đối chiếu. | Bản 0.11 chọn 1–4 product khác nhau, một variant/product và quantity 1–5 theo Olist thời trang; chuyển nguồn, giá VND/giá vốn và chọn variant là giả định. Không gộp hai nguồn hoặc lấy mẫu tổng tiền riêng. |
| Q04. Khách nào mua lại, bao lâu mua một lần? Cần định danh khách, các lần đặt và thời gian có thể quan sát. | `dim_customer`/`fact_order` có dữ liệu; chưa có mart mua lại/RFM. | R01: UCI có 63,99% khách ≥2 invoice; H&M có 66,20% khách ≥2 ngày mua. Notebook đã đo khoảng mua theo nhóm. | Nhóm loyal/regular/one_off là giả định; code co giãn số đơn theo `scale.orders` và rút ngắn khoảng mua để vừa lịch sử. Cần chốt cách bảo toàn quy luật khi đổi quy mô/cửa sổ; không coi nhãn one_off là bảo đảm một đơn. Chưa tự thêm mart khách hàng. |
| Q05. Coupon được dùng thế nào? Cần điều kiện hiệu lực, hạn mức, redemption và giảm giá của đơn. | `mart_coupon_usage`, `mart_marketing_summary` | R07/S08 định hướng động lực khuyến mại; chưa có tỷ lệ dùng voucher phù hợp trên tổng đơn. | Xác suất thử dùng, mức giảm và hiệu ứng theo nhóm/chiến dịch còn là giả định. Giữ kiểm tra đủ điều kiện và tiền giảm; mart hiện đo sử dụng, chưa chứng minh coupon làm tăng doanh số hoặc conversion. |
| Q06. Hủy, giao thất bại và hoàn tiền ảnh hưởng doanh thu ra sao? Cần lịch sử đơn, payment, return và refund. | `mart_order_daily`, `mart_operations_summary`, `mart_product_returns`, `mart_cod_region_daily` | R05/R07: Olist có trạng thái snapshot và cơ cấu giá trị thanh toán; chưa suy được xác suất chuyển trạng thái, COD/VietQR hoặc hoàn hàng của shop VN. | Đã sinh chuỗi trạng thái, liên kết tiền/kho; tỷ lệ kích hoạt và độ trễ là giả định. Ưu tiên đối soát giá trị đơn đặt với doanh thu đã ghi nhận/hoàn thành công; không đổi tỷ lệ bằng snapshot Olist hay invoice hủy UCI. |
| Q07. Thời gian giao và review liên hệ thế nào? Cần thời điểm đặt/xuất/giao, ngày hẹn nếu có và điểm review. | `mart_logistics_performance`, `mart_product_review_summary`; chi tiết ở `fact_shipment` và Silver reviews. | R06: Olist đo thời gian giao, trễ hẹn và điểm theo trải nghiệm; review theo đơn, khác review theo item của repo. | Hiện độ trễ giả định, trọng số rating chưa phụ thuộc trải nghiệm. Schema thiếu ngày hẹn nên SLA giữ null; thời gian vận chuyển tính từ xuất kho khác thời gian đặt→giao trong EDA. Cần chốt cách chuyển đơn→item; mart review chỉ đếm review/negative gần đây, chưa đo quan hệ này. |
| Q08. Tồn kho và lợi nhuận có đối soát được không? Cần tồn đầu, biến động, tồn cuối và giá vốn snapshot lúc bán. | `mart_inventory_current`, `mart_inventory_health`, `mart_inventory_summary`; COGS ở các mart bán hàng. | Ràng buộc nghiệp vụ/schema; EDA hiện chưa đo chính sách tồn, bổ sung hàng hoặc biên lợi nhuận của shop VN. | Tồn đầu = tổng xuất + đệm 80–200; giá vốn là giả định. Bộ SQL này kiểm tra đối soát được nhưng khó tạo thiếu hàng tự nhiên. Muốn kiểm chứng low-stock phải chốt kịch bản riêng; không tự mở rộng nhập hàng/POS. |

### Giới hạn cần giữ khi diễn giải

- SQL hỗ trợ 25/28 bảng; thành phố/cửa hàng là dữ liệu tham chiếu, không chứng minh đã có giao dịch POS, phân phối địa lý hay tồn kho cửa hàng.
- `mart_order_daily` phân biệt giá trị đơn đặt với doanh thu hàng hóa đã ghi nhận/hoàn tiền. Số đơn theo danh mục không cộng thành tổng đơn vì một đơn có thể chứa nhiều danh mục.
- `mart_coupon_usage` đếm redemption gần đây trong 15 phút; `mart_product_review_summary` đếm review tiêu cực approved trong 45 phút, theo `as_of` của batch. SQL lịch sử có anchor cũ có thể cho các bộ đếm này bằng 0 đúng thiết kế; không sửa ngày chỉ để tạo số khác 0.
- Mua lại, phân phối giỏ và quan hệ giao nhận–review có dữ liệu chi tiết để khảo sát nhưng chưa được các mart hiện có biểu diễn đầy đủ. Đây là giới hạn phục vụ phân tích, không tự động là lý do thêm tham số generator hoặc xây mart mới.

### Phần nghiên cứu tiếp theo, theo thứ tự

1. **Sản phẩm/giỏ (Q02–Q03):** dùng các bảng thực nghiệm đã có để chọn đơn vị và cách chuyển sang catalog/variant; bổ sung đo quan hệ giỏ–quantity–giá chỉ khi cần quyết định đó. Đã áp dụng các trọng số vào YAML trong lượt refactor 0.11; các quan hệ chưa đủ căn cứ tiếp tục là giả định.
2. **Mua lại (Q04):** xem cửa sổ quan sát và mối liên hệ số đơn–khoảng mua; đề xuất cách giữ quy luật khi tổng đơn cấu hình xung đột với lịch mua. Giữ riêng invoice UCI và ngày mua H&M.
3. **Thời gian (Q01):** xác định phần nguồn hỗ trợ được; lịch Tết theo năm là yêu cầu lịch, còn biên độ và profile giờ chưa có bằng chứng VN phù hợp thì giữ giả định.
4. **Tiền, hậu mãi và kho (Q05–Q08):** ưu tiên ràng buộc và truy vết; chỉ hiệu chỉnh xác suất khi có bằng chứng tương thích. Ngày hẹn, quan hệ review và thiếu hàng là các giới hạn cần quyết định riêng trước khi mở rộng.

Tham chiếu đối chiếu: [notebook, mục 1–6](../../generator/notebooks/reference_datasets_eda.ipynb), [chỉ số/giả định](../project/GENERATOR_METRICS.md), [nghiên cứu VN, S01–S08](../project/GENERATOR_VIETNAM_RESEARCH.md), [định nghĩa tiền](../architecture/METRICS_CATALOG.md), [Gold builders](../../pipelines/src/lakehouse/oltp/gold/), [SQL schema mart](../../pipelines/src/lakehouse/sql_templates/spark/gold/), [lịch mua và giao dịch](../../generator/src/generator/transactions.py).

### Kết quả đợt sản phẩm/giỏ — Q02–Q03

- Đã bổ sung Olist fashion_ và so sánh nhu cầu H&M theo article_id/product_code; notebook giữ kết quả cùng nhận xét ngắn sau từng cell phân tích. Kiểm chứng: 73 code cell chạy từ kernel sạch không lỗi; 63 code cell cũ giữ nguyên, thêm 10 code cell. Kết quả EDA chỉ nằm trong notebook; runtime cập nhật trong lượt refactor bên dưới.
- Đề xuất số product khác nhau trước khi chọn variant, dùng phân phối product_code cho nhu cầu và phân phối giỏ/quantity Olist thời trang. Một variant/product và chuyển nguồn sang shop VN là giả định; đã áp dụng trong generator 0.11.
- Cohort Olist chủ yếu túi/phụ kiện, chỉ 30 đơn có quần áo nữ; chưa đủ cơ sở lấy làm tỷ lệ đại diện shop nữ VN. Phân tích giá/giỏ chưa đủ căn cứ thêm hệ số quantity có điều kiện.
- Trọng số ứng viên, giới hạn catalog và tiêu chí triển khai được ghi tại [GENERATOR_METRICS, mục 11](../project/GENERATOR_METRICS.md). Người dùng đã yêu cầu triển khai cùng hướng tham số demo đơn giản; chưa mở thêm phân tích mua lại.

## 2. Bổ sung nghiên cứu và EDA đúng khoảng trống

- Đối chiếu các đặc tính trên với kết quả notebook và tham số generator hiện tại; ghi phần đã có căn cứ, phần thiếu và phần không thể suy ra từ nguồn.
- Chỉ nghiên cứu phương pháp cần để giải quyết khoảng trống đã xác định; giải thích vì sao chọn cách đơn giản hơn hoặc cần mô hình khác.
- Mỗi số đo ghi nguồn/cohort, đơn vị quan sát, mẫu số và cửa sổ thời gian. Không đánh đồng ngày mua với đơn hàng, sản phẩm với variant, dòng mua với doanh thu.
- Ưu tiên quan hệ có điều kiện khi nguồn cho phép, thay vì lấy mẫu từng chỉ số độc lập. Giá trị đơn phải được suy từ giỏ, giá và giảm giá.
- Nghiên cứu Việt Nam cung cấp bối cảnh điều chỉnh; hệ số chưa đo được phải ghi giả định. Không chép tỷ lệ khảo sát thành xác suất theo đơn.

Kết quả cần có cho mỗi quy luật: **câu hỏi → số đo/nguồn → cách áp dụng → tham số → giới hạn → kiểm tra**.

## 3. Quy luật và cải thiện generator theo nhu cầu

Thực hiện từng nhóm: sản phẩm/giỏ → khách hàng/mua lại → thời gian → vòng đời đơn và đối soát kho. Giữ quy luật H&M đã áp dụng làm điểm xuất phát, không mặc định đã tái hiện đầy đủ nhu cầu thực.

Mỗi nhóm phải xác định dữ liệu được sinh theo thứ tự nào, trường nào suy ra từ trường khác và ràng buộc nào cần giữ. Tránh ép tổng số đơn hoặc giới hạn thời gian làm méo quy luật mua lại mà không ghi nhận.

R02/R03 và các tỷ lệ demo đã được đưa vào lượt refactor 0.11 theo yêu cầu. Với các phần bổ sung sau, sửa xong từng nhóm phải kiểm tra rồi mới tiếp tục; không viết lại toàn bộ generator chỉ vì đổi cách nghiên cứu.

## 4. Đánh giá vừa đủ cho mục tiêu SQL

- **Đúng nghiệp vụ:** PK/FK, tổng tiền, thanh toán/hoàn tiền, trình tự trạng thái/thời gian và đối soát tồn kho đều hợp lệ. Kịch bản cố tình sai phải tách khỏi bộ SQL chuẩn.
- **Đúng quy luật đã chọn:** xác định trước chỉ số cần đo và sai lệch chấp nhận theo quy mô/đơn vị dữ liệu. Chỉ đối chiếu với nguồn khi tương thích; hình dạng biểu đồ tự nó không chứng minh tính thực tế.
- **Có ích cho phân tích:** nêu kết quả kỳ vọng cho từng câu hỏi; khi kiểm chứng pipeline, đối soát số liệu nguồn với mart. Giới hạn dữ liệu phải được phản ánh trong diễn giải.
- **Tái lập:** cùng seed, config và phiên bản cho cùng SQL. Ghi quy mô/thời gian sinh khi triển khai để biết khả năng chạy local.

Không yêu cầu benchmark generator cũ/mới hoặc thêm bộ đánh giá lớn. Đánh giá nhằm xác nhận đầu ra thực hiện được quy luật và nghiệp vụ đã chốt; không khẳng định đại diện toàn thị trường Việt Nam.

## Tài liệu và trình tự thực hiện

Kế hoạch này quản lý phạm vi; [notebook](../../generator/notebooks/reference_datasets_eda.ipynb) giữ thực nghiệm; [nghiên cứu Việt Nam](../project/GENERATOR_VIETNAM_RESEARCH.md) giữ bằng chứng; [GENERATOR_METRICS](../project/GENERATOR_METRICS.md) tập trung giá trị, ý nghĩa, nguồn/giả định và trạng thái áp dụng. Tham số chạy tiếp tục nằm trong YAML hiện có. Đồng bộ REPORT sau khi có kết quả đã kiểm chứng, không tạo thêm nhiều tài liệu trùng nội dung.

- [x] Chốt đợt đầu chỉ nghiên cứu dữ liệu SQL lịch sử.
- [x] Ánh xạ câu hỏi phân tích với bảng/mart và xác định khoảng trống bằng chứng (Q01–Q08).
- [x] Bổ sung nghiên cứu/EDA và đề xuất quy luật, tiêu chí cho sản phẩm/giỏ (Q02–Q03).
- [ ] Bổ sung nghiên cứu cần thiết cho mua lại, thời gian và hậu mãi/kho theo thứ tự đã chốt.
- [x] Áp dụng R02/R03 và chính sách demo tinh gọn theo yêu cầu; các phần khác giữ giả định.
- [x] Kiểm chứng SQL demo 0.11 và cập nhật tài liệu; nghiên cứu mở rộng chỉ làm khi có câu hỏi cần quyết định.

## Refactor SQL demo đã được yêu cầu — 09/11/2026

Người dùng chọn chủ động tăng ca hủy/hoàn/thất bại vừa đủ cho phân tích, đồng thời triển khai hướng tinh gọn. Từ đợt này không tiếp tục mở rộng EDA để tìm mọi tỷ lệ; dùng kết quả phù hợp làm tham chiếu, phần còn lại là chính sách shop/giả định demo.

1. Áp dụng trọng số product_code H&M và giỏ/quantity Olist thời trang; chọn product khác nhau trước variant. Giữ nguyên các giả định/giới hạn chuyển nguồn.
2. Rút gọn coupon còn thường/chiến dịch; dùng một tỷ lệ viết review; bỏ hệ số theo nhóm khách/coupon của xác suất hủy.
3. Thay modulo bằng xác suất có mẫu số rõ. Mặc định do agent chọn cho demo: hủy 8% đơn đủ tuổi ≥2 giờ; giao thất bại 8% đơn COD chưa hủy đủ tuổi ≥3 ngày; đổi/trả 6% đơn hoàn tất và còn ≥3 ngày xử lý; trong đổi/trả chọn exchange 50% nếu có variant thay thế, còn lại refund. Đây là giả định, không phải tỷ lệ thị trường hay quota bảo đảm mọi quy mô.
4. Giữ lịch mua/campaign hiện có như giả định; không mở thêm nghiên cứu mua lại, lịch Tết, POS, log hoặc thay schema/API/pipeline. Giữ kiểm tra tiền/kho, khóa COD theo thời điểm, địa điểm và hợp đồng import PostgreSQL.
5. Kiểm tra chọn product, tỷ lệ 0/1, lỗi cấu hình, quan hệ tiền/kho và nhập trên PostgreSQL tạm; cập nhật chỉ số/README/notebook theo trạng thái mới. Test bổ sung để ngoài Git; không tự commit/push.

**Hoàn thành lượt refactor 0.11:** 30 test generator + 9 test bổ sung đạt; bộ 113 test platform trên PostgreSQL tạm không lỗi (2 live test hạ tầng bỏ qua). Notebook có 73 code cell đã chạy không lỗi; lượt cập nhật trạng thái chỉ sửa Markdown, giữ source và output phân tích. Rà soát độc lập không tìm thấy lỗi runtime chặn triển khai. Chưa chạy toàn bộ stack.

## Cấu hình YAML tập trung

Theo yêu cầu tiếp theo, đưa các tham số kịch bản còn hard-code vào `policies` trong cùng preset: phí giao, thời gian, số lần giao/COD, đổi trả, tồn đệm, master/customer/catalog, coupon/review, wishlist/cart. Giữ mặc định hiện tại; ràng buộc tiền/FK/kho và giới hạn schema vẫn kiểm tra bằng code. Không đổi API/schema/pipeline hoặc thêm dependency. Xem [nhóm cấu hình](../../generator/README.md#central-yaml-settings).

Kiểm tra trước commit: 50 test generator đạt (30 test hiện có, 20 test bổ sung ngoài Git); Ruff, lockfile và notebook hợp lệ.
