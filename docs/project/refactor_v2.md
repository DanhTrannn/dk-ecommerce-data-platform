# Refactor v2 — Kế hoạch cải thiện Data Generator

Cập nhật: **05/11/2026**. Trạng thái: kế hoạch, chưa sửa code/config generator.

## 1. Mục tiêu và phạm vi

Xây dựng generator cho **retail thời trang Việt Nam** với ba yêu cầu: **tham số giải thích được nguồn gốc, dữ liệu đúng nghiệp vụ và kết quả kiểm chứng tái lập được**.

Đợt đầu tập trung vào bốn đặc trưng của giao dịch: **kích thước giỏ hàng, số lượng mua, tần suất mua lại và mức tập trung sản phẩm**. Giữ mô hình MySQL, đơn vị VND và múi giờ nghiệp vụ hiện có. Chưa tích hợp API/scraping, thay FE, xây trang điều khiển hoặc triển khai ML.

## 2. Điểm cần cải thiện trong generator hiện tại

| Hiện trạng đã kiểm tra | Việc cần làm |
|---|---|
| Các tỷ lệ trong YAML là mô hình giả lập, chưa được hiệu chỉnh từ dataset thực | Lập bảng nguồn → cách đo → tham số → giả định |
| Config `small.yml` phân bổ giỏ 1–4 variant và lượng mua 1–3 mỗi dòng | Kiểm tra miền giá trị với nguồn tham chiếu; không ép dữ liệu thực vào các khoảng hiện có |
| `random.sample` chọn variant đều ngẫu nhiên | Bổ sung lựa chọn có trọng số để thể hiện nhóm bán chạy và phần ít bán; vẫn giữ SKU khác nhau trong một đơn |
| `_order_targets` điều chỉnh số đơn mỗi khách để đạt tổng đơn cấu hình | Chốt đầu vào nào được giữ cứng và đo lại phân phối sau điều chỉnh, kể cả nhóm chỉ mua một lần |
| Dataset identity đã chứa seed/config/version, chưa có tham chiếu profile riêng | Ghim phiên bản/checksum của profile vào nhận diện dataset khi bổ sung profile |

Thành phần liên quan: [config.py](../../generator/src/generator/config.py), [sql_export.py](../../generator/src/generator/sql_export.py), [config mẫu](../../generator/configs/small.yml). Giữ CLI và cách export hiện có, chỉ mở rộng phần cần thiết.

## 3. Nguồn tham chiếu và cách sử dụng

Bắt đầu bằng [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail), có invoice, mã sản phẩm, lượng mua, thời gian và khách hàng. Nguồn này bán quà tặng tại Anh, có khách sỉ, giấy phép CC BY 4.0; chỉ dùng làm căn cứ cho đặc trưng phù hợp, không coi là mẫu đại diện thời trang Việt Nam.

Trước khi đo phải chốt nhóm giao dịch được phân tích, cách xử lý dòng dịch vụ/điều chỉnh, lượng âm, hóa đơn hủy, giá không hợp lệ và bản ghi nghi trùng. Không tự loại đơn lớn để làm kết quả đẹp hơn. Khách thiếu định danh không dùng đo mua lại; phải ghi số mẫu và phạm vi quan sát. Mã sản phẩm nguồn cần được ánh xạ rõ với SKU/variant của generator.

| Đặc trưng ưu tiên | Thống kê cần tạo |
|---|---|
| Kích thước giỏ | Phân bố số SKU khác nhau mỗi hóa đơn bán hàng hợp lệ; tách với số dòng và tổng số lượng |
| Lượng mua | Phân bố quantity dương theo SKU trong đơn, sau khi thống nhất cách gộp dòng |
| Mua lại | Số đơn/khách, tỷ lệ khách có từ hai đơn và khoảng cách mua trong cùng cửa sổ quan sát |
| Độ tập trung sản phẩm | Tỷ trọng lượng bán theo SKU, đường tích lũy và tỷ trọng của nhóm SKU dẫn đầu |

Mỗi tham số có một nhãn: **đo từ nguồn**, **đã điều chỉnh có lý do**, **giả định** hoặc **kịch bản**. Giá VND, cơ cấu thời trang, POS, hiệu ứng Tết/sale, coupon, COD, review và tỷ lệ hủy chưa có bằng chứng phù hợp thì giữ nhãn giả định/kịch bản. Không lấy hóa đơn hủy của UCI làm tỷ lệ hủy trước giao của đồ án.

Nếu một đặc trưng không phù hợp sau phân tích, ghi rõ giới hạn và hoãn hiệu chỉnh đặc trưng đó; không cần buộc cả bốn phải dùng trực tiếp số liệu UCI. Nguồn thời trang khác có thể bổ sung sau, không chặn bản đầu tiên.

## 4. Các bước triển khai và đầu ra

| Bước | Công việc | Đầu ra |
|---|---|---|
| 1. Chốt cách đo | Ghi định nghĩa bốn đặc trưng, nhóm giao dịch, cửa sổ thời gian và quy tắc nghiệp vụ | Đặc tả ngắn và danh sách kiểm tra |
| 2. Làm sạch và phân tích nguồn | Ghim file/checksum; báo số dòng trước/sau từng bộ lọc; tách dữ liệu hiệu chỉnh và đánh giá trước khi tạo tham số | Báo cáo chất lượng và các tập tham chiếu |
| 3. Tạo profile | Tính thống kê, ghi nguồn, đơn vị, số mẫu, cách điều chỉnh và giới hạn | Profile JSON/CSV có phiên bản và bảng ánh xạ tham số |
| 4. Sửa generator | Nạp profile, sửa sampler giỏ/lượng mua/popularity và cách phân bổ đơn; giữ quan hệ giữa các biến khi nguồn hỗ trợ | Cấu hình baseline/scenario và generator được cập nhật |
| 5. Đánh giá | So bản hiện tại với bản mới, chạy nhiều seed/quy mô; kiểm tra dữ liệu sau mọi bước điều chỉnh/ràng buộc | Báo cáo trước/sau, biểu đồ và các trường hợp chưa đạt |

**Baseline:** ưu tiên đặc trưng tham chiếu, suy ra hoặc kiểm tra tính khả thi của số lượng phụ thuộc. **Scenario:** ưu tiên số lượng/kịch bản người dùng yêu cầu, ghi rõ đặc trưng bị thay đổi. Không âm thầm điều chỉnh tổng đơn rồi tuyên bố giữ nguyên phân phối khách hàng.

Khi đổi số SKU, so mức tập trung theo thứ hạng/tỷ lệ SKU thay vì yêu cầu khớp mã sản phẩm thật. Khi đổi lịch sang kỳ giả lập, giữ nhất quán khoảng thời gian quan sát; hiệu ứng sale chèn thêm phải tách khỏi baseline tham chiếu.

## 5. Tiêu chí kiểm chứng

- **Nghiệp vụ:** khóa liên kết hợp lệ; tiền hàng/discount/refund đúng; tồn kho và diễn biến trạng thái/thời gian nhất quán. Tăng độ giống thống kê không được làm sai các ràng buộc này.
- **Thống kê:** so phân bố, trung vị/phân vị, mua lại và mức tập trung theo cùng định nghĩa/cửa sổ. Kiểm tra cả quan hệ như kích thước giỏ với lượng mua; không chỉ so từng cột độc lập.
- **Đánh giá riêng:** không dùng lại chính mẫu đã hiệu chỉnh làm bằng chứng độc lập. Tách theo thời gian, giữ nguyên từng hóa đơn và xử lý khác biệt thời gian quan sát của khách; báo cả thay đổi của dữ liệu nguồn giữa hai kỳ.
- **Ngưỡng đạt:** xác định trước khi đánh giá, dựa trên biến động nguồn và sai lệch chấp nhận được của từng đặc trưng. Không đặt một mức “giống thật 95%” chung.
- **Tái lập:** cùng code/config/seed/profile cho cùng dataset; lưu phiên bản và checksum nguồn. Profile mới phải tạo phiên bản dữ liệu mới.
- **Tích hợp:** import vào MySQL kiểm tra biệt lập, đối soát số lượng/tiền qua Lakehouse và đầu ra phân tích. Hiện SQL không hỗ trợ import lặp vào cùng DB; không dùng việc chạy lại import làm tiêu chí idempotency đã có.

Khi sửa hành vi, thêm/cập nhật kiểm tra phù hợp theo AGENTS.md và chạy lint hiện có. Dữ liệu thô, SQL/log sinh ra và kết quả lớn để ngoài Git; code, cấu hình và tài liệu nhỏ có thể review trong repo.

## 6. Mốc hoàn thành và bước làm đầu tiên

Bản đầu tiên hoàn thành khi có **tài liệu nguồn/giả định, profile có phiên bản, config baseline/scenario, generator đã hiệu chỉnh và báo cáo kiểm chứng trước/sau**. Đặc trưng nào chưa đạt phải được nêu rõ; không tuyên bố dữ liệu đại diện thị trường Việt Nam.

**Bước tiếp theo:** hoàn thành bước 1–2 trước: định nghĩa bốn đặc trưng, làm sạch UCI và lập báo cáo phân tích. Chỉ sau khi xem được kết quả mới chốt tham số và sửa sampler.

**Hiện trạng:** UCI đã được kiểm tra dữ liệu thô sơ bộ, chưa hoàn tất làm sạch, tạo profile hoặc hiệu chỉnh generator. Đợt cập nhật kế hoạch này giữ nguyên code, pipeline và `FINAL_REPORT.md`.
