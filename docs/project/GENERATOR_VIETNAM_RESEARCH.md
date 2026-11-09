# Bằng chứng nghiên cứu cho sinh dữ liệu mua hàng tại Việt Nam

Ngày tra cứu: **08/11/2026**. Phạm vi: hành vi mua hàng của người Việt nói chung, gồm trực tuyến và tại cửa hàng; phân biệt kết quả theo ngành hàng khi áp dụng cho dự án.

Tài liệu bổ sung căn cứ cho [notebook EDA](../../generator/notebooks/reference_datasets_eda.ipynb) và việc thiết kế generator. R02 từ Olist thời trang và R03 từ H&M product_code đã được áp dụng làm tham chiếu trong generator 0.11; các tỷ lệ nghiên cứu Việt Nam chưa được chuyển trực tiếp thành tham số. Giá trị runtime, ý nghĩa và giả định nằm trong [GENERATOR_METRICS](GENERATOR_METRICS.md).

## 1. Kết quả chính

Các nguồn dưới đây củng cố việc mô hình hóa khách hàng có mức chi tiêu khác nhau, mua đa kênh, chịu ảnh hưởng của khuyến mại, mua sắm trước Tết và quan tâm đến trải nghiệm giao nhận. Chúng giúp giải thích **vì sao chọn các hành vi này** trong dữ liệu tổng hợp.

Tuy nhiên, chưa có nguồn đã kiểm tra nào cung cấp đầy đủ lịch sử đơn hàng của một nhà bán lẻ Việt Nam để suy ra đồng thời giỏ hàng, mua lại, thanh toán và đổi trả. Cần kết hợp ba loại căn cứ:

| Loại căn cứ | Vai trò trong đồ án |
|---|---|
| EDA dữ liệu giao dịch thực tế | Rút ra phân phối và quan hệ quan sát được trong chính dataset; ghi rõ thị trường, ngành hàng và thời gian |
| Thống kê và khảo sát Việt Nam | Bổ sung bối cảnh địa phương, khác biệt giữa nhóm khách và đặc điểm mua sắm |
| Giả định của nhóm | Hoàn thiện kịch bản khi thiếu số liệu phù hợp; ghi rõ là giả định, chưa coi là tỷ lệ thực tế |

Các đề xuất áp dụng dưới đây là **suy luận thiết kế của nhóm từ nguồn**, không phải kết quả thực nghiệm trực tiếp của nguồn đó trên generator.

### Cách nối nghiên cứu với thực nghiệm và ứng dụng

Notebook là nơi trình bày chuỗi lập luận theo hành vi; tài liệu này giữ hồ sơ nguồn để tra cứu. Mỗi nhóm bắt đầu bằng câu hỏi nghiệp vụ, đặt bằng chứng Việt Nam cạnh phép đo từ dataset, rồi đánh giá trước khi chọn quy luật. Nguồn chưa đủ mẫu/phương pháp hoặc khác đơn vị đo chỉ dùng trong giới hạn đã nêu.

| Nhóm trong notebook | Nguồn Việt Nam liên quan | Thực nghiệm dùng để đánh giá | Kết quả/ứng dụng |
|---|---|---|---|
| 1. Khách hàng/mua lại | S02 tần suất, S03 đa kênh | UCI theo invoice; H&M theo ngày mua | R01: khoảng mua theo nhóm; lịch đơn của khách, chưa chốt tỷ trọng nhóm tại shop VN |
| 2. Giỏ hàng | S01 chi tiêu; S07 dải giá theo ngành | Olist product/đơn; UCI mã/đơn và Quantity | R02: tách kích thước giỏ, số lượng, giá trị; chưa gộp trọng số hoặc quy đổi sang VND |
| 3. Nhu cầu sản phẩm | S07 tập trung doanh số theo shop | H&M tập trung theo sản phẩm và nhóm | R03: trọng số sản phẩm, xác suất có điều kiện; ánh xạ catalog mới còn là giả định |
| 4. Thời điểm mua | S05 Tết; S06 FMCG | H&M nhu cầu/ngày theo tháng | R04: lịch theo năm, tách nền/chiến dịch; hệ số VN chưa chốt |
| 5. Giao dịch/hậu mãi | S04 thanh toán; S08 khuyến mại, giao nhận, đổi trả | Olist trạng thái, giá trị thanh toán, giao hàng và đánh giá | R05–R07: ràng buộc kênh/phương thức, điểm theo trải nghiệm; tỷ lệ VN còn thiếu |

S09–S11 là nguồn có thể nghiên cứu thêm, chưa đưa thành kết quả EDA hoặc tham số. Hành trình eCommerce Behavior giữ ở phụ lục vì sinh log ngoài phạm vi hiện tại.

**Đầu ra:** bảng R01–R07 ở mục 6 notebook ghi kết quả đo, nguồn Việt Nam, quy luật/cách lấy mẫu, phần chưa chốt và bảng nghiệp vụ sử dụng. REPORT tóm tắt từ chuỗi lập luận này, không trình bày kết quả rời theo từng nguồn hoặc ghi quy luật đề xuất thành tính năng đã triển khai.

## 2. Nguồn thống kê và khảo sát có thể sử dụng

### S01. Cục Thống kê — Khảo sát mức sống dân cư 2024

[Thông cáo kết quả KSMS 2024](https://www.nso.gov.vn/tin-tuc-thong-ke/2025/05/thong-cao-bao-chi-ket-qua-khao-sat-muc-song-dan-cu-nam-2024/), công bố năm 2025.

- **Mẫu:** 46.995 hộ tại 63 tỉnh/thành theo địa giới năm khảo sát; thông tin chi tiêu đại diện đến cấp toàn quốc, thành thị/nông thôn và sáu vùng.
- **Bằng chứng:** chi tiêu bình quân đầu người/tháng gần 3 triệu đồng; thành thị gần 3,8 triệu, nông thôn khoảng 2,5 triệu. Chi đời sống của nhóm thu nhập cao nhất gấp 3,1 lần nhóm thấp nhất.
- **Dùng được:** căn cứ cho khác biệt về khả năng chi tiêu giữa các nhóm khách, thay vì dùng một mức chi tiêu giống nhau.
- **Giới hạn:** đây là tổng chi tiêu đầu người, gồm nhiều nhu cầu. Không chuyển trực tiếp thành giá sản phẩm, giá trị đơn hay ngân sách tại một shop; số bình quân chưa đủ để xác định toàn bộ phân phối.

### S02. TGM — Vietnam E-Commerce Insights 2025

[Thông cáo và phương pháp khảo sát](https://tgmresearch.com/press-tgm-vietnam-ecommerce-insights-2025.html), công bố 20/06/2025, trang cập nhật 30/06/2026.

- **Mẫu:** 1.120 người, 18–54 tuổi; CAWI từ 18/12/2024 đến 22/01/2025. Nhà cung cấp công bố hiệu chỉnh theo tuổi và giới.
- **Bằng chứng:** 50% mua online hằng tuần; 92% người mua online sử dụng smartphone. Các ngành được mua gồm quần áo/phụ kiện 63%, chăm sóc cá nhân 55%, làm đẹp 48%.
- **Dùng được:** căn cứ cho sự khác nhau về tần suất mua và bối cảnh mua nhiều ngành hàng.
- **Giới hạn:** tần suất trên nhiều nơi bán không phải số đơn tại một shop. Tỷ lệ người mua ngành hàng không phải tỷ trọng doanh thu hoặc xác suất chọn SKU. Không coi cập nhật trang năm 2026 là khảo sát mới.

### S03. PwC — Voice of the Consumer 2024, Việt Nam

[Kết quả công bố tại Việt Nam](https://www.pwc.com/vn/vn/publications/vietnam-publications/voice-of-consumer-2024.html).

- **Mẫu:** 515 người tại Việt Nam, thuộc khảo sát hơn 7.000 người ở 11 thị trường châu Á–Thái Bình Dương.
- **Bằng chứng:** các kênh mua sắm phổ biến gồm điện thoại 67%, cửa hàng 63%, máy tính 44%; 71% cho biết đã từng mua trực tiếp qua mạng xã hội.
- **Dùng được:** căn cứ cho một khách có thể mua cả online và tại cửa hàng, đồng thời tiếp cận sản phẩm qua nhiều kênh.
- **Giới hạn:** các nhóm có thể trùng nhau. Không dùng 67%/63% làm tỷ trọng đơn online/POS hoặc coi 71% là tỷ trọng đơn social commerce. Trang tóm tắt chưa cung cấp đủ thiết kế mẫu để khẳng định đại diện mọi nhóm dân cư.

### S04. TGM — Ưu tiên phương thức thanh toán năm 2025

[Bài phân tích COD và BNPL](https://tgmresearch.vn/thanh-toan-thuong-mai-dien-tu-viet-nam-cod-bnpl.html) và [câu hỏi về phương thức được ưu tiên nhất](https://tgmstatbox.com/stats/preferred-payment-methods-for-online-shopping-vietnam/).

- **Bằng chứng:** bài phân tích công bố 35% người trả lời ưu tiên COD, với mẫu ghi tại kết quả là 1.142 người.
- **Dùng được:** củng cố việc giữ COD bên cạnh thanh toán điện tử.
- **Giới hạn:** cùng bài lại ghi phương pháp với mẫu 1.120; chưa giải thích được khác biệt. Câu hỏi đo sở thích, không đo số giao dịch thực tế. Chưa dùng 35% để thay tỷ trọng thanh toán theo đơn; phần còn lại cũng không đồng nghĩa toàn bộ là VietQR. Chỉ đọc phần công khai, chưa truy cập bộ bảng đầy đủ.

### S05. Ipsos — Mua sắm Tết 2026

[TẾT 2026 in Vietnam: Festive hearts, sharper carts](https://www.ipsos.com/en-vn/tet-2026-vietnam-festive-hearts-sharper-carts), công bố 06/04/2026.

- **Mẫu:** 500 người, 18–55 tuổi, tại TP.HCM, Hà Nội, Đà Nẵng và Cần Thơ.
- **Bằng chứng:** 53% cho biết phần lớn việc mua sắm Tết diễn ra trong 1–2 tuần trước Tết.
- **Dùng được:** căn cứ cho giai đoạn mua sắm trước Tết. Khi thiết kế lịch, dùng ngày Tết của từng năm và khoảng cách đến Tết, thay vì cố định một khoảng ngày dương lịch cho mọi năm.
- **Giới hạn:** khảo sát bốn đô thị, không đại diện đầy đủ cả nước. 53% người có cửa sổ mua chính trước Tết không phải 53% tổng đơn; chưa suy ra được hệ số tăng đơn từng ngày.

### S06. NIQ — Mùa Tết và hàng tiêu dùng nhanh

[Winning Vietnam’s Tet Holiday — FMCG 2025](https://nielseniq.com/global/en/insights/analysis/2025/winning-vietnams-tet-holiday-optimizing-fmcg-growth-for-the-next-season/), công bố 30/06/2025.

- **Bằng chứng:** NIQ cho biết mùa Tết đóng góp khoảng 20% giá trị bán hàng FMCG cả năm.
- **Dùng được:** bằng chứng bổ sung rằng mức độ mùa vụ phụ thuộc ngành hàng; nhu cầu tích trữ, biếu tặng có thể quan trọng với FMCG.
- **Giới hạn:** không áp tỷ lệ này cho thời trang hoặc toàn bộ bán lẻ. Giá trị bán hàng không phải số đơn; trang tóm tắt không công bố đủ mẫu và cách xác định cửa sổ mùa Tết để tái lập phép tính. Chỉ dùng định hướng mùa vụ, chưa dùng làm trọng số.

### S07. Metric — Thị trường sàn thương mại điện tử 2025

[Công bố báo cáo TMĐT Việt Nam 2025](https://metric.vn/insights/cong-bo-bao-cao-thuong-mai-dien-tu-viet-nam-2025/), ngày 28/01/2026; kỳ thống kê 01/01–31/12/2025 trên Shopee, TikTok Shop, Lazada và Tiki.

- **Bằng chứng:** riêng ngành Sức khỏe, nhóm giá 200–500 nghìn đồng đóng góp 25% doanh số. Riêng Shopee và TikTok Shop, 2,12% shop thuộc Mall đóng góp 32,6% doanh số.
- **Dùng được:** tham khảo dải giá theo đúng ngành; ghi nhận sự tập trung doanh số giữa người bán.
- **Giới hạn:** tỷ trọng doanh số theo giá không phải tỷ trọng số lượng sản phẩm. Tập trung theo shop không chứng minh phân phối bán chạy theo SKU. Đây là thống kê của nền tảng nghiên cứu, chưa có đơn hàng gốc để tái lập; chưa đủ căn cứ đặt giá, AOV hoặc quy luật Pareto cho repo.

### S08. Milieu — Khuyến mại, giao nhận và đổi trả năm 2025

[Vietnam’s online shoppers demand speed and reliability in 2025](https://mili.eu/vietnams-online-shoppers-demand-speed-and-reliability-in-2025/), ngày 08/09/2025.

- **Bằng chứng:** 64% nêu giảm giá, 55% nêu phí giao hàng thấp/miễn phí là động lực mua. Giao trễ được 37% nhắc đến trong các phàn nàn. Trong trải nghiệm đổi trả gần nhất, 60% liên quan hàng lỗi/sai, 23% thiếu/không nhận được, 12% đổi ý.
- **Dùng được:** chọn các tình huống khuyến mại và nguyên nhân hậu mãi có ý nghĩa nghiệp vụ.
- **Giới hạn:** bài không công bố cỡ mẫu, thời gian thu thập và trọng số. Các tỷ lệ trên không phải tỷ lệ dùng voucher, giao trễ hoặc hoàn hàng trên tổng đơn. Cơ cấu lý do mới liệt kê 95%; chưa chuyển thành phân phối đầy đủ. Chỉ dùng định hướng, chưa chốt tham số.

## 3. Bài báo và dữ liệu Việt Nam có thể phân tích thêm

### S09. Hành vi mua ngẫu hứng trên TikTok Shop — Data in Brief 2024

[Bài báo, DOI 10.1016/j.dib.2024.111047](https://pmc.ncbi.nlm.nih.gov/articles/PMC11546432/) · [Mendeley Data, phiên bản 1](https://data.mendeley.com/datasets/46bxcjgws4/1).

- **Dữ liệu:** 361 phản hồi hợp lệ của sinh viên tại 10 tỉnh/thành Đồng bằng sông Cửu Long, thu thập 01–03/2024; chọn mẫu thuận tiện. Kho dữ liệu công bố giấy phép CC BY 4.0.
- **Dùng được:** tham khảo bảng hỏi và phân tích yếu tố như khan hiếm, cách trình bày sản phẩm, niềm tin, động cơ mua ngẫu hứng.
- **Giới hạn:** bảng hỏi tự khai, không phải lịch sử giao dịch. Không suy ra tỷ lệ đặt hàng tức thời, xác suất dùng voucher hay quan hệ nhân quả cho mọi người Việt. Mới kiểm tra bài báo và metadata kho dữ liệu, chưa EDA dữ liệu gốc.

### S10. Social commerce của sinh viên Gen Z — Data in Brief 2026

[Bài báo, DOI 10.1016/j.dib.2026.112529](https://pmc.ncbi.nlm.nih.gov/articles/PMC12907679/) · [Mendeley Data, phiên bản 2](https://data.mendeley.com/datasets/g59k85hrd5/2).

- **Dữ liệu:** 757 phản hồi hợp lệ, thu thập 10/2024–03/2025, chọn mẫu thuận tiện; có biến thu nhập, giới tính, tần suất mua, nơi cư trú và các thang đo niềm tin/hành vi. Kho dữ liệu công bố CC BY 4.0.
- **Dùng được:** ứng viên để khảo sát thêm khác biệt giữa các nhóm khách và cách đặt câu hỏi tần suất mua; kiểm tra bảng hỏi trước khi tính chỉ số.
- **Giới hạn:** chỉ bao phủ sinh viên Gen Z; thang điểm Likert về hành vi không tương đương số đơn hoặc số tiền đã mua. Mới kiểm tra bài báo và metadata, chưa EDA dữ liệu gốc.

### S11. UIT-ViOCD — Phàn nàn trong đánh giá sản phẩm tiếng Việt

[Vietnamese Complaint Detection on E-Commerce Websites](https://arxiv.org/abs/2104.11969), bản sửa năm 2021.

- **Dữ liệu được bài báo mô tả:** 5.485 đánh giá được gán nhãn thủ công, thuộc bốn nhóm sản phẩm.
- **Dùng được:** tài liệu tham khảo cho nội dung và cách diễn đạt phàn nàn bằng tiếng Việt, nếu sau này cần sinh nội dung review.
- **Giới hạn:** chưa xác minh quyền truy cập/giấy phép dữ liệu gốc. Tập gán nhãn không mặc nhiên đại diện tỷ lệ phàn nàn, phân phối sao hoặc tỷ lệ hoàn hàng ngoài thị trường. Không cần thêm mô hình NLP vào repo chỉ để sử dụng ý tưởng này.

## 4. Chuyển bằng chứng thành quyết định cho generator

R02 đã dùng giỏ/quantity Olist thời trang, R03 dùng mười nhóm thứ hạng H&M product_code; cách chuyển nguồn/catalog/variant là giả định. Các hướng khác bên dưới là tham khảo; chính sách shop và tỷ lệ cần cho demo được chủ động đặt và ghi rõ giả định. Việc mở rộng nghiên cứu ra nhiều ngành không đồng nghĩa phải mở rộng danh mục sản phẩm hoặc schema của repo.

| Thành phần | Hướng sử dụng căn cứ | Phần chưa đủ dữ liệu |
|---|---|---|
| Nhóm khách và mua lại | Giữ khác biệt giữa khách mua ít/nhiều; phân phối quan sát trong notebook là tham chiếu theo từng dataset. S01–S02 bổ sung bối cảnh VN | Tỷ trọng từng nhóm, số đơn và khoảng cách mua tại riêng shop mô phỏng |
| Giá và giỏ hàng | Tách giá một sản phẩm, giá trị một đơn và chi tiêu tháng; chọn tham chiếu phù hợp ngành từ EDA và S01/S07 | Phân phối giá/giỏ hàng chính xác cho khách Việt của shop |
| Sản phẩm bán chạy | EDA H&M hỗ trợ mô hình nhu cầu không đồng đều trong dataset đó | Chưa có tỷ trọng SKU bán chạy tại VN; S07 không lấp được khoảng trống này |
| Tết và chiến dịch | Dùng lịch theo năm; phân biệt trước/trong/sau Tết. S05–S06 hỗ trợ hướng mùa vụ | Hệ số tăng đơn, hình dạng theo ngày/giờ và khác biệt cụ thể giữa ngành |
| Khuyến mại | Có nhóm nhạy với giảm giá/phí giao hàng, tham khảo S08 | Xác suất dùng voucher, mức giảm và mức tăng mua thực tế |
| Thanh toán | Có COD và thanh toán điện tử, dựa trên S04 | Tỷ trọng theo đơn; tỷ lệ COD thất bại và thanh toán thất bại |
| Giao nhận, review, đổi trả | S08 hỗ trợ chọn tình huống. Quan hệ giao trễ–đánh giá trong EDA Olist là căn cứ quan sát ở Brazil | Phân phối thời gian giao, tổng tỷ lệ đổi trả, xác suất review và mức ảnh hưởng tại VN |
| Online và POS | Cho phép cùng khách xuất hiện ở cả hai kênh nếu nghiệp vụ cần, tham khảo S03 | Tỷ trọng đơn theo kênh và mức chuyển đổi kênh của shop |

**Quyết định áp dụng:** dùng H&M product_code cho nhu cầu và Olist fashion_ cho giỏ/quantity. Các ước lượng VN chưa đủ căn cứ để trống ở mức nghiên cứu; generator chọn giá trị kịch bản demo có mẫu số rõ và ghi giả định, không tìm thêm dữ liệu để chứng minh mọi tham số. Chưa chuyển tỷ lệ sở thích, phàn nàn hoặc lý do đổi trả thành tỷ lệ theo tổng đơn.

Chưa cần thêm SEM, mô hình dự đoán hay pipeline nghiên cứu riêng. Các dataset bảng hỏi chỉ nên bổ sung vào notebook khi trả lời được một câu hỏi cụ thể còn thiếu.

## 5. Cách ghi căn cứ trong báo cáo đồ án

Mỗi quy luật khi đưa vào phương pháp sinh dữ liệu cần ghi ngắn gọn: **chỉ số → nguồn/kỳ dữ liệu → phạm vi áp dụng → cách dùng hoặc giả định còn lại**. Chỉ mô tả là đã áp dụng sau khi generator thực sự dùng quy luật đó.

Đoạn có thể dùng cho phần nghiên cứu:

> Nhóm thực hiện EDA trên các bộ dữ liệu thương mại thực tế để xác định những phân phối và quan hệ quan sát được trong hoạt động mua hàng. Bên cạnh đó, nhóm khảo sát thống kê mức sống, báo cáo tiêu dùng và nghiên cứu tại Việt Nam nhằm bổ sung căn cứ về khả năng chi tiêu, mua đa kênh, thanh toán, mùa vụ Tết và trải nghiệm giao nhận. Các bằng chứng được xem xét theo đối tượng khảo sát, ngành hàng, thời gian và đơn vị đo trước khi sử dụng. Những tham số chưa đủ dữ liệu được xác định là giả định của kịch bản mô phỏng.

Phần nghiên cứu này củng cố **cơ sở lựa chọn quy luật và tính hợp lý của mô hình sinh**. Nó chưa chứng minh dữ liệu tổng hợp đại diện toàn thị trường Việt Nam hoặc dự báo chính xác hành vi khách hàng. Không suy diễn thêm các tỷ lệ chỉ để làm đủ tham số.

## 6. Phạm vi đã kiểm tra

- Ưu tiên nội dung do cơ quan thống kê, đơn vị khảo sát hoặc tác giả nghiên cứu công bố; các số liệu trên đều có đường dẫn trực tiếp.
- Với báo cáo thương mại, chỉ sử dụng phần công khai đã đọc; chưa coi đó là đã tiếp cận toàn bộ bảng số liệu hay dữ liệu giao dịch gốc.
- Chưa tải thêm dữ liệu hoặc chạy EDA các dataset VN. Notebook nối nguồn nghiên cứu với các phép đo hiện có; trạng thái áp dụng R02/R03 và các giá trị giả định được ghi riêng trong danh mục chỉ số.
