# Kế hoạch notebook EDA cho data generator

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Thực hiện phạm vi từng đợt đã được người dùng duyệt; đợt mới nhất đã cho phép viết lại generator. Không tự commit/push.

**Goal:** Tạo một notebook khảo sát Olist, H&M, eCommerce Behavior Data from Multi Category Store và UCI Online Retail, rút ra bằng chứng để đề xuất quy luật sinh dữ liệu và phục vụ REPORT.

**Architecture:** Một notebook trong module generator, tổ chức theo hành vi cần mô phỏng. Mỗi nhóm nối câu hỏi nghiệp vụ → căn cứ nghiên cứu → EDA → đánh giá giữa nguồn → quy luật → ứng dụng. Đọc dữ liệu local độc lập với DB/pipeline bằng Polars; kết quả và diễn giải giữ trong notebook.

**Tech Stack:** Python 3.11; dependency group `eda` gồm Polars, Seaborn, Matplotlib, ipykernel và nbconvert đã khóa trong `uv.lock`. Notebook chạy trực tiếp trên máy người dùng bằng VS Code hoặc Jupyter với kernel Python local; dữ liệu đọc từ ổ đĩa local.

**Spec:** Phạm vi đã chốt trong trao đổi: bốn dataset trên, một notebook EDA, bằng chứng thực nghiệm kết hợp bối cảnh Việt Nam trước khi hiệu chỉnh generator. UCI được bổ sung trực tiếp vào notebook; không khôi phục profile hoặc giao diện tham chiếu cũ.

## Đợt tổ chức nghiên cứu theo hành vi — 08/11/2026

Người dùng đã duyệt cách tổ chức và yêu cầu triển khai. Thực hiện các bước trong đợt này; các checklist phía dưới ghi lại quá trình EDA trước đó. Chưa sửa generator, cấu hình lấy mẫu hoặc dữ liệu nguồn; không tự commit/push.

1. [x] Sắp xếp lại notebook: chuẩn bị dữ liệu; khách hàng/mua lại; giỏ hàng; nhu cầu sản phẩm; thời điểm mua; giao dịch/hậu mãi; bảng quy luật. Giữ eCommerce Behavior ở phụ lục vì sinh log ngoài phạm vi.
2. [x] Nối nghiên cứu Việt Nam và kết quả EDA ngay trong từng nhóm; đánh giá khác biệt giữa nguồn, ghi lựa chọn/giới hạn và ánh xạ sang bảng nghiệp vụ. Bảng cuối phân biệt đề xuất có căn cứ, tham chiếu và phần chưa chốt tham số.
3. [x] Đồng bộ phần phương pháp ở báo cáo nghiên cứu, mục 4.7 của REPORT, README generator và mục lục docs; giữ nguyên các phần không liên quan.

**Kiểm tra:** giữ đủ code cell cũ, không thay phép tính/cohort; chạy notebook theo thứ tự mới từ kernel sạch, không có lỗi; kiểm tra các số liệu tổng hợp và liên kết; `git diff --check` đạt. Chỉ lưu kết quả trong notebook, kiểm tra tạm nằm ngoài Git.

**Kết quả kiểm tra đợt này:** 63 code cell chạy tuần tự từ kernel sạch, không có error output; source của từng code cell giữ nguyên. Các kết quả text/plain đã có khớp bản trước đợt tổ chức lại; bảng giỏ Olist trước đó chưa có output nay được tính đầy đủ. Notebook hợp lệ theo nbformat, render HTML và liên kết nội bộ đạt; review độc lập không có phát hiện cần sửa. Không thêm dependency hoặc test vào Git, chưa commit/push.

## Đợt viết lại generator từ quy luật đã đánh giá

**Phạm vi được duyệt:** người dùng chọn viết lại generator tối giản theo `database/schema.sql` MySQL hiện có, không chuyển schema nguồn sang PostgreSQL trong đợt này. Áp dụng bằng chứng đủ rõ; các giá trị chưa đủ căn cứ giữ từ kịch bản cũ và ghi là giả định. Tài liệu chỉ số tập trung tại [GENERATOR_METRICS](GENERATOR_METRICS.md).

- [x] Viết lại cấu hình và lấy mẫu: giữ giao diện CLI/UI cần dùng, áp dụng trọng số sản phẩm H&M từ nhóm 3 notebook, đánh dấu phép ánh xạ catalog là giả định; giữ các tỷ lệ khác.
- [x] Sinh dữ liệu theo khóa/quan hệ và ràng buộc schema đã chốt; bảo đảm tiền, thanh toán/hoàn tiền, lịch sử trạng thái và tồn kho nhất quán. Không khôi phục profile UCI hoặc sinh log.
- [x] Kiểm tra deterministic output, quan hệ dữ liệu và lựa chọn sản phẩm có trọng số bằng các kiểm tra tạm ngoài Git; chạy các kiểm tra hiện có còn phù hợp. Đồng bộ chỉ số, README, REPORT và trạng thái áp dụng R01–R07.

**Tổ chức:** cấu hình/validation, dữ liệu catalog, giao dịch và xuất SQL/CLI. Không thêm framework lấy mẫu, benchmark trước/sau hoặc dependency nghiên cứu vào runtime. Bỏ Faker/HTTPX khỏi dependency generator vì không dùng, cập nhật lock; platform-app vẫn giữ HTTPX theo nhu cầu riêng.

**Kiểm tra cuối:** Ruff đạt; 23 test hiện có đạt; sáu kịch bản tổng hợp kiểm tra quan hệ/tiền/kho/lịch sử đạt. Review độc lập tìm ba vấn đề (khóa COD theo thời điểm, tài khoản inactive và hết trọng số dương); bốn test tái hiện đã được chạy lỗi rồi chạy đạt sau sửa. File SQL thực sinh chạy qua schema/ràng buộc tương đương trong SQLite; kiểm tra FK, tổng tiền, bộ đếm coupon/COD, tính tái lập và xuất lỗi không ghi đè đầu ra đều đạt. Đây không phải bằng chứng đã import trên MySQL thật. Test bổ sung và SQL kiểm tra nằm trong `/tmp`, không đưa vào commit. Commit/push chỉ thực hiện khi người dùng yêu cầu.

## Tiến độ triển khai

**Phần 1 — dữ liệu và notebook: đã có tổng quan phạm vi dữ liệu, sẵn sàng để người dùng kiểm tra.**

- [x] Notebook chỉ giữ imports/đọc dữ liệu tối thiểu và nội dung EDA; bỏ hướng dẫn môi trường, kiểm tra tệp, phiên bản thư viện, checksum và tiến độ.
- [x] Khai báo/khóa/cài nhóm thư viện EDA; kiểm tra vẽ bằng Polars + Seaborn + Matplotlib.
- [x] Tải Olist và chạy notebook với năm bảng Olist thật; bản execute và manifest nằm trong `data/generator/eda/`.
- [x] Tải H&M từ repo Hugging Face người dùng cung cấp, ghim commit `acde210b0090be673abdba5f989c48925c856f78`; xác minh kích thước/SHA-256 hai CSV với con trỏ Git LFS. Nguồn và metadata tải giữ ngoài notebook.
- [x] Chạy lại notebook bằng kernel sạch với Olist và H&M thật; manifest đọc đủ hai CSV H&M và khớp checksum đã xác minh.
- [x] Hoàn tất tải eCommerce Behavior: `2019-Oct.csv` và `2019-Nov.csv`; kiểm tra kích thước, ZIP CRC và SHA-256, lưu metadata nguồn.
- [x] Xác minh đủ ba dataset thật; snapshot manifest nguồn giữ riêng trong thư mục dữ liệu.
- [x] Tổng quan EDA: số bản ghi, đơn vị quan sát, khoảng thời gian và số ngày thiếu/sai định dạng của ba nguồn; chưa triển khai phân phối nghiệp vụ hoặc quy luật sinh dữ liệu.
- [x] Rút gọn phần tổng quan/chất lượng về `describe()`, `null_count()`, khoảng thời gian và kiểm tra trùng `order_id`; bỏ bảng audit tự xây, kiểm tra join chi tiết và toàn bộ nhận xét của phần này.
- [x] Phần H&M 2019: ngày mua/khách, khoảng cách mua lại, tập trung sản phẩm, tỷ trọng nhóm hàng và nhu cầu/ngày theo tháng. Đã chạy dữ liệu thật và kiểm tra bốn tình huống nhỏ ngoài Git; giữ các cell người dùng đã bổ sung.
- [x] Phần Olist 2017: giỏ hàng/giá trị đơn, trạng thái/thanh toán, giao nhận và liên hệ điểm đánh giá với giao trễ. Đã chạy dữ liệu thật và bốn kiểm tra nhỏ ngoài Git; các trường thiếu bảng con giữ null.
- [x] Phần session/funnel: cohort tháng 10/2019 có ngày 1/11 làm biên quan sát, thống kê phiên/event, funnel đúng thứ tự/cùng sản phẩm, timestamp trùng và thời gian đường đi hoàn thành. Bốn nhóm kiểm tra nhỏ ngoài Git đã qua; không thêm timeout hoặc chia lại session.
- [x] Phần tổng hợp đã được rút gọn theo yêu cầu mới: kết quả giữ trong notebook; 11 mục nghiệp vụ có nhận xét/quy luật Markdown, bỏ bảng diễn giải bằng Python và toàn bộ lệnh xuất file. Chưa hiệu chỉnh generator hoặc sửa REPORT.
- [x] Dọn `data/` theo yêu cầu: chỉ giữ 13 CSV nguồn của ba dataset; xóa bản nén, clone trùng, metadata tải và kết quả EDA ngoài notebook. Nguồn tải giữ trong README.
- [x] Phạm vi generator hiện chỉ sinh SQL; đã gỡ sinh log theo yêu cầu. Phần EDA hành vi vẫn giữ để tham khảo, chưa triển khai cách sinh log thay thế.
- [x] Bổ sung phân phối giỏ/số lượng, giá trị đơn theo giỏ, khoảng mua theo nhóm tần suất, trọng số sản phẩm/nhóm và xác suất điểm theo giao nhận. Đã chạy notebook với nguồn thật, năm kiểm tra nhỏ ngoài Git và Ruff đạt; kết quả giữ trong notebook.
- [x] Theo phạm vi mới, không bổ sung so sánh generator trước/sau hoặc benchmark holdout. Phân phối/quy luật là kết quả thực nghiệm; dữ liệu sinh khi triển khai là đầu ra phương pháp.
- [x] Bổ sung UCI UK trong 12 tháng đầy đủ: giỏ theo invoice, Quantity theo mã hàng và mua lại theo đơn; giữ dữ liệu lớn và đơn cùng ngày. Đã chạy đủ 541.909 dòng nguồn, sáu kiểm tra nhỏ ngoài Git; giữ 14 CSV nguồn, không phục hồi module profile.

## Ràng buộc chung

- Đợt tổ chức nghiên cứu chỉ thay notebook/tài liệu; đợt viết lại generator đã được duyệt riêng ở trên. Không tự khảo sát thêm dataset mới.
- Notebook chạy trên máy người dùng; không mount vào container, không thay Dockerfile hoặc Docker Compose cho phần EDA.
- Đợt viết lại chỉ sửa generator/cấu hình và tài liệu liên quan; không sửa source DB, DAG, schema hoặc hạ tầng. REPORT chỉ đồng bộ phần nghiên cứu liên quan.
- Không nối ID giữa các dataset; mỗi kết quả phải ghi nguồn, grain, cửa sổ và mẫu số.
- Phân biệt kết quả đo, điều chỉnh Việt Nam có nguồn và giả định chưa kiểm chứng.
- Không đặt trước kết luận, tỷ lệ hay ngưỡng chấp nhận để ép EDA khớp generator.
- Không commit dữ liệu thô, cache, dữ liệu định danh, notebook đã execute hoặc test tạm.
- Viết phép lọc, join và tổng hợp bằng biểu thức Polars ngay trong notebook. Không tạo framework hay module helper riêng.

## Cách viết notebook

- Notebook chỉ chứa nội dung phân tích và code cần để thực hiện phân tích. Hướng dẫn môi trường, nguồn tải/điều khoản, checksum, manifest và tiến độ để trong README/metadata/kế hoạch; không tạo các mục này trong notebook.
- Tổng quan/chất lượng dùng hàm có sẵn như `describe()`, `null_count()`, `is_duplicated()`. Theo yêu cầu mới, thêm nhận xét Markdown ngắn sau cell có kết quả để nêu ý nghĩa hoặc ảnh hưởng đến phép đo; cell chỉ đọc dữ liệu/bước trung gian không thêm diễn giải thừa.
- Chia phần theo hành vi; trong mỗi phần ghi rõ dataset/cohort/đơn vị đo. Mỗi nhóm theo thứ tự **câu hỏi → nghiên cứu → cách tính/code → kết quả → đánh giá → quy luật/ứng dụng Markdown ngắn**. Code không chứa diễn giải hoặc xuất file.
- Markdown ngắn, nhận xét đặt ngay sau bảng/biểu đồ và dựa trên số liệu đã đo. Cuối mục giữ quy luật áp dụng, tránh lặp lại toàn bộ số liệu đã nhận xét ở từng cell.
- Cell code ngắn, làm một bước dễ theo dõi; dùng tên biến mô tả dữ liệu. Tách đọc, lọc, tổng hợp và vẽ thành các cell khi cần; tránh chuỗi biểu thức quá dài hoặc logic lồng nhiều tầng.
- Viết trực tiếp bằng Polars. Chỉ tạo hàm khi một phép xử lý thực sự lặp lại hoặc khó đọc; không tạo class, registry, wrapper, pipeline tổng quát hay cơ chế tự phát hiện schema.
- Chỉ đọc cột/bảng phục vụ câu hỏi đã nêu. Chỉ làm sạch lỗi ảnh hưởng phép đo; ghi rõ dữ liệu bị loại. Không thêm biến, phân khúc, mô hình hoặc kiểm định thống kê khi chưa có câu hỏi cần chúng.
- Mỗi câu hỏi dùng bảng hoặc biểu đồ đủ để trả lời; chỉ dùng cả hai khi chúng bổ sung thông tin. Không vẽ mọi cột, mọi cặp biến hoặc nhiều biểu đồ lặp cùng kết luận.
- Kiểm tra số dòng, khóa join, mẫu số và thứ tự thời gian bằng các phép đếm/assert rõ ngay gần bước xử lý. Chỉ dùng ví dụ nhỏ kiểm tra riêng khi phép tính cần đối chiếu; không xây bộ framework test cho notebook.
- Dùng toàn bộ dữ liệu trước nếu máy đáp ứng. Chỉ thêm lấy mẫu hoặc xử lý tài nguyên khi cần; ghi chính sách ngay trong phần sử dụng.

## Cấu trúc sử dụng

| File/thư mục | Trách nhiệm |
|---|---|
| `generator/notebooks/reference_datasets_eda.ipynb` | Notebook duy nhất; câu hỏi, phương pháp, code EDA, kết quả và nhận xét |
| `docs/project/GENERATOR_VIETNAM_RESEARCH.md` | Hồ sơ nguồn Việt Nam và cách dùng nguồn cho từng nhóm hành vi |
| `docs/FINAL_REPORT.md`, mục 4.7 | Tóm tắt phương pháp, kết quả và quy luật đề xuất; không thay thế notebook |
| `data/generator/eda/raw/{olist,hm,ecommerce_behavior,uci}/` | CSV nguồn; đã nằm dưới đường dẫn bị Git bỏ qua |

Khi triển khai cập nhật `pyproject.toml`, `uv.lock`, `.gitignore`, `generator/README.md` và `docs/README.md` đúng phạm vi: dependency phân tích, bỏ qua notebook checkpoints, hướng dẫn chạy và liên kết notebook. Không tạo notebook khác hay thay cơ chế khởi động ứng dụng.

## Các bước EDA ban đầu theo dataset

### 1. Chuẩn bị dữ liệu và khả năng tái lập

- [ ] Đọc quy định từng nguồn; ghi URL, phiên bản/tệp, giấy phép/điều khoản sử dụng, SHA-256 và ngày thu thập trong README/metadata ngoài notebook. Không lưu token Kaggle.
- [ ] Notebook chỉ có imports, đường dẫn và lệnh đọc cần cho mục phân tích hiện tại. Chỉ thêm seed/cửa sổ/lấy mẫu khi phép phân tích thực sự dùng đến.
- [x] Chuẩn bị môi trường local bằng `uv sync --locked --group eda`; hướng dẫn mở notebook trong VS Code và chọn kernel `.venv/bin/python` của repo. Nếu dùng giao diện Jupyter, dùng bản cài local sẵn có và chọn cùng kernel.
- [ ] Với file lớn, dùng Polars lazy scan, chọn cột và lọc trước khi tổng hợp; dùng streaming cho các phép toán được hỗ trợ. Kiểm tra mức dùng RAM thực tế, không coi lazy scan là bảo đảm mọi join/group/sort đều vừa RAM. Chỉ collect kết quả cần thiết và truyền các cột/mảng tổng hợp cho Seaborn/Matplotlib; nếu vượt tài nguyên máy thì dùng chế độ sample theo nhóm đã quy định.
- [ ] Nếu cần mẫu, chọn xác định theo customer/user và giữ toàn bộ sự kiện của nhóm được chọn trong cửa sổ; không lấy N dòng đầu hay sample từng event. Ghi tỷ lệ và chỉ số chỉ có ý nghĩa trong mẫu.

**Kiểm tra:** file đầu vào không bị sửa; thiếu file/cột thì dừng ở lệnh đọc tương ứng, không cần cơ chế bỏ qua dataset. Nếu dùng mẫu, đọc lại cùng cấu hình phải có cùng tập ID.

### 2. Audit chất lượng các dataset

- [x] Dùng `describe()` cho cột số/ngày, `null_count()` cho các trường cần phân tích; kiểm tra trùng `order_id` bằng `is_duplicated()`. Không xây bộ đếm lỗi hoặc ma trận audit riêng.
- [x] Giữ giờ Olist theo nguồn, H&M ở cấp ngày, parse sự kiện theo UTC; không chuyển thành hành vi giờ Việt Nam.
- [x] Cố định cohort và đơn vị quan sát trước khi tính phân phối; không diễn giải số ngày mua thành số đơn. Phạm vi mới không yêu cầu tách fit/evaluation để đánh giá generator.
- [x] H&M dùng năm 2019 làm cohort khảo sát đầy đủ 12 tháng; dữ liệu ngoài năm vẫn giữ ở nguồn, không thêm phần benchmark.
- [x] Giữ nguyên nguồn, không cắt outlier hoặc xóa dòng ở bước tổng quan; chỉ chuẩn hóa kiểu số/ngày để thống kê.
- [x] Hiển thị bảng thống kê và missing value trực tiếp. Điều kiện lọc nghiệp vụ hoặc kiểm tra join chỉ thêm tại phép phân tích sử dụng chúng.

**Kết quả:** bảng chất lượng dữ liệu ngay trong notebook; không xuất file. Metadata nguồn tải giữ riêng ngoài notebook.

### 3. EDA H&M: nhu cầu thời trang

- [x] Đọc lịch sử mua và metadata nhóm sản phẩm; không nạp metadata khách khi chưa dùng.
- [x] Đo ngày mua/khách và khoảng cách giữa các ngày mua; tính tập trung sản phẩm, tỷ trọng nhóm hàng và chỉ số dòng mua/ngày theo tháng trong 2019.
- [x] Gộp customer/ngày chỉ cho phép đo ngày mua; giữ từng dòng khi đo nhu cầu sản phẩm. Không diễn giải ngày mua hoặc dòng mua thành số đơn/giỏ/AOV.
- [x] Dùng `describe()`, phân vị và đường tích lũy; không đổi giá nguồn sang VND hoặc gán mùa vụ H&M thành mùa vụ Việt Nam.

**Kiểm tra:** một khách mua nhiều món cùng ngày không bị tính thành nhiều ngày mua; kiểm tra tổng lượng trước/sau join metadata; đối chiếu top sản phẩm bằng fixture nhỏ.

### 4. EDA Olist: đơn hàng và vận hành

- [x] Dùng năm 2017 đầy đủ để trả lời bốn nhóm câu hỏi: giỏ/giá trị đơn, trạng thái/thanh toán, thời gian giao/trễ và đánh giá theo trễ.
- [x] Gộp item, payment và review về cấp đơn trước khi join `1:1`; tiền hàng, phí và thanh toán là các cột riêng. Bảng con thiếu giữ null, không lấp 0.
- [x] Đo giao nhận trên đơn `delivered`, đủ ngày và thời gian không âm; so sánh ngày giao với ngày hẹn. Trạng thái chỉ là snapshot, không suy transition hoặc hoàn tiền.
- [x] Điểm lấy trung bình trong từng đơn, so sánh trên đơn giao hợp lệ có đánh giá; ghi rõ quan hệ quan sát. Không đổi payment sang COD/VietQR.

Mua lại đo theo ngày ở H&M và theo invoice ở UCI; nếu bổ sung cho Olist thì dùng `customer_unique_id`, không dùng `customer_id` riêng theo từng đơn.

**Kiểm tra:** fixture một đơn có nhiều item, payment và review không làm nhân số đơn/tiền hàng; khách có hai customer_id nhưng cùng customer_unique_id được nhận diện đúng; thời gian thiếu/âm không được biến thành 0.

### 5. EDA eCommerce Behavior: session và funnel

- [x] Dùng UTC, đếm loại sự kiện trên đúng cohort phiên; không giả định tệp có đủ mọi loại event.
- [x] Đo event/sản phẩm xem/khoảng thời gian quan sát mỗi phiên; đếm funnel và profile thời gian giữa các bước.
- [x] Dùng `(user_id, user_session)` nguồn; loại định danh/thời gian thiếu cho phép đo session, không tách bằng timeout mới.
- [x] Funnel dùng view đầu tiên, cart đầu tiên sau view, purchase đầu tiên sau cart trên cùng sản phẩm/phiên; đếm phiên distinct, mẫu số là phiên có view. Tỷ lệ phiên có cart/purchase báo riêng.
- [x] Không diễn giải purchase thành số đơn hoặc khoảng cách event thành HTTP latency; thời gian hoàn thành đo theo session/sản phẩm, chỉ trong nhóm có đủ chuỗi.
- [x] Quan sát `[2019-10-01, 2019-11-02)`; giữ phiên có start từ 2/10 và end trước 1/11. Phiên qua hai tệp được ghép trước khi lọc biên; báo riêng timestamp trùng, không tự suy thứ tự hoặc gọi phiên thiếu bước là bỏ giỏ.

Khoảng thời gian phiên là chênh lệch giữa event đầu/cuối quan sát, không chứng minh thời gian thực ở lại trang hoặc session đã kết thúc. Ngày 1/11 làm biên quan sát; phần hành vi giữ làm tham khảo, chưa triển khai sinh log.

**Kiểm tra:** session nằm qua hai chunk/tệp không bị tách; purchase trước cart không đạt funnel; session thiếu ID, cùng timestamp và chạm biên được xử lý theo chính sách; các tầng funnel có thứ tự không tăng số session.

### 6. EDA UCI: giỏ, số lượng và mua lại theo đơn

- [x] Dùng UK, `[2010-12-01, 2011-12-01)`; metadata invoice thống nhất, bỏ C/Quantity và giá không dương/mã ngoài dạng đã nêu; giữ dòng trùng và đuôi lớn.
- [x] Gộp Quantity và giá trị theo invoice–mã hàng; thiếu CustomerID vẫn giữ cho giỏ/số lượng. Mua lại chỉ dùng khách có ID, không gộp các invoice cùng ngày.
- [x] Đo phân phối giỏ/số lượng, giá trị theo nhóm giỏ, số đơn/khách và khoảng mua theo nhóm tần suất; giữ riêng đơn vị và giới hạn nguồn có khách sỉ.

### 7. Nhận xét và tổng hợp quy luật trong notebook

- [x] Giữ số liệu và biểu đồ ngay trong notebook; không xuất CSV, ảnh hoặc file khác.
- [x] Sau mỗi mục nghiệp vụ, nhận xét kết quả và rút ra quy luật bằng cell Markdown ngắn; code chỉ đọc, tính toán và vẽ.
- [x] Mục cuối tổng hợp ngắn các quy luật và giới hạn, không dựng bảng chữ bằng Python. Đã đối chiếu generator hiện tại; chưa sửa chúng.
- [x] Phần Việt Nam ghi nhu cầu nguồn cho lịch Tết, giá VND, địa lý và COD/VietQR. Chưa có nguồn thì để trống bằng chứng và ghi giả định hoặc thiếu dữ liệu.
- [x] Giữ kết quả thực nghiệm và quy luật trong notebook; không thêm phần so sánh generator trước/sau. Chưa thay sampler thì không ghi đã áp dụng hoặc cải thiện đầu ra.

**Kết quả:** số liệu, biểu đồ, nhận xét và quy luật nằm trong notebook; đủ trích vào REPORT sau khi được xem xét.

## Review Focus

1. Join Olist nhiều-nhiều làm tăng tiền/số đơn: kiểm tra fixture ở bước 4.
2. H&M không có grain đơn đáng tin cậy: kiểm tra ngày mua ở bước 3, không báo AOV/giỏ hàng suy diễn.
3. Session qua biên và thứ tự event không xác định: kiểm tra fixture ở bước 5.
4. Thiếu file, null, outlier hoặc dữ liệu rỗng: kiểm tra audit ở bước 1–2; không đưa ra kết luận từ phần không có dữ liệu.
5. File hành vi lớn và mẫu làm sai thống kê: dùng Polars lazy scan, kiểm tra tài nguyên và lấy mẫu theo nhóm ở bước 1; luôn công bố chế độ, cửa sổ và giới hạn.

## Tiêu chí hoàn thành

- [ ] Notebook chạy lại từ kernel sạch, từ trên xuống, không cần DB hay stack batch/streaming. Chạy bằng `uv run --group eda jupyter nbconvert --to notebook --execute generator/notebooks/reference_datasets_eda.ipynb --inplace --ExecutePreprocessor.timeout=1800` từ repo root trên máy người dùng; exit 0 và đủ output cho bốn nguồn mới được gọi là hoàn thành.
- [ ] Kiểm tra các grain, audit và biên đã nêu bằng phép đếm/assert trong notebook; dùng ví dụ nhỏ cho join/funnel khi cần đối chiếu. Không commit test tạm.
- [ ] Đọc notebook theo thứ tự để kiểm tra mỗi mục có câu hỏi, code dễ theo dõi, kết quả và nhận xét; bỏ các cell/biểu đồ không phục vụ đề xuất quy luật hoặc giải thích giới hạn dữ liệu.
- [ ] Chạy lại cùng input/config cho cùng số liệu aggregate và cùng cohort; không yêu cầu timestamp metadata hoặc byte biểu đồ trùng tuyệt đối.
- [ ] Notebook trong Git đã xóa output/execution count; `git diff --check` đạt; không có raw data, cache, credential hay notebook execute trong danh sách thay đổi.
- [ ] README có nguồn, bố trí tệp, cách chuẩn bị môi trường Python local/chọn kernel và lệnh chạy tại chỗ. Dependency mới chỉ nằm trong nhóm EDA và lock file.
- [ ] Mỗi đề xuất hiệu chỉnh truy được về kết quả đo, nguồn Việt Nam và giới hạn; REPORT phân biệt quy luật đã áp dụng với các tham số còn là giả định.

## Nguồn dự kiến

- [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
- [H&M Personalized Fashion Recommendations](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data).
- [eCommerce Behavior Data from Multi Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
- [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail), Chen, D. (2015), DOI `10.24432/C5BW33`, CC BY 4.0.

## Tích hợp vào dev PostgreSQL

Theo quyết định tiếp theo của người dùng ngày 08/11/2026, giữ PostgreSQL và các chức năng mới của `dev`, tích hợp sampler H&M và EDA. Generator dùng hợp đồng SQL `postgresql-v1`, hỗ trợ lựa chọn địa điểm và importer hiện có. Quyết định MySQL ở trên mô tả phạm vi triển khai ban đầu, đã được thay thế khi merge.

Kiểm chứng bản tích hợp: 30 test generator và 113 test platform đạt (2 live test provisioning ngoài phạm vi được bỏ qua), với importer, API và quyền nguồn chạy trên PostgreSQL 16.8 biệt lập. Bốn test hồi quy nghiệp vụ cùng kiểm tra YAML số nguyên dạng float đạt; test bổ sung nằm ngoài Git. Chưa chạy lại toàn bộ pipeline/stack.

## Hướng tinh gọn và SQL demo — 09/11/2026

Người dùng chọn dùng nghiên cứu vừa đủ, các chính sách shop/tỷ lệ thiếu bằng chứng được chủ động đặt làm giả định. Generator 0.11 áp dụng R02 Olist thời trang và R03 H&M product_code; coupon thường/chiến dịch, một tỷ lệ review và các xác suất hủy/COD/đổi trả rõ mẫu số thay modulo/hệ số phụ. Không mở rộng EDA để fit mọi tham số; giữ các phân tích đã có làm tham khảo. [Phạm vi/tiến độ](../references/synthetic-data-research-plan.md) và [chỉ số runtime](GENERATOR_METRICS.md) là nơi theo dõi hiện tại. Chưa mở rộng API/log/streaming.
