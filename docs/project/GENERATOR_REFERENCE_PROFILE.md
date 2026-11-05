# Hồ sơ tham chiếu cho data generator

## Mục đích và nguồn

Đợt 2b tạo công cụ đo và profile tham chiếu; **chưa thay sampler, chưa chứng minh dữ liệu sinh đã giống thị trường**. Quyết định kiến trúc tiếp nối [refactor_v2](refactor_v2.md): sửa tính đúng nghiệp vụ trước, đo nguồn tham chiếu rồi mới chọn cách hiệu chỉnh.

Nguồn: Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. DOI [10.24432/C5BW33](https://doi.org/10.24432/C5BW33), giấy phép [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). [Trang dữ liệu chính thức](https://archive.ics.uci.edu/dataset/352/online+retail) mô tả nhà bán quà tặng trực tuyến tại UK, có khách sỉ, giai đoạn 01/12/2010–09/12/2011. Không coi đây là mẫu đại diện thời trang Việt Nam, POS hoặc sàn đa nhà bán.

- [Spec](../../generator/profiles/uci-reference.yml): nguồn, checksum, cohort, cửa sổ và quy tắc mã hàng có phiên bản.
- [Profile JSON](../../generator/profiles/uci-online-retail-uk-v1.json): phân phối/count tổng hợp, không chứa InvoiceNo, CustomerID, danh sách StockCode hoặc dữ liệu thô.
- Mỗi lần chạy chỉ đọc file người dùng cung cấp; không tự tải Internet hoặc ghi vào MySQL/Lakehouse. Raw ZIP/XLSX/CSV đặt ngoài Git.

Artifact SHA-256: `f5385cbb54bbebf7196389109c6b0621faab0c304e3702548165e71c84aede8b`.

Profile ID: `d65da964de4c14794f013c8b3aedd0e4f073b5853927f69d9e247866a03c0969`. ID được tính từ JSON chuẩn hóa gồm số liệu, nguồn, spec và policy; không chứa thời gian chạy hoặc đường dẫn máy. `policy_version=uci-merchandise-v1`.

## Định nghĩa bốn đặc trưng

| Đặc trưng | Grain, công thức và giới hạn |
|---|---|
| Kích thước giỏ | Số StockCode distinct mỗi invoice bán hợp lệ sau các bộ lọc; không phải số dòng hoặc tổng lượng |
| Lượng mua | Tổng Quantity dương của cùng invoice/StockCode, kể cả khác đơn giá; mỗi cặp có một quan sát |
| Mua lại | Đếm invoice mỗi CustomerID có định danh trong từng kỳ; tỷ lệ khách có ≥2 invoice trên khách có ≥1 invoice; khoảng cách giữa hai invoice liên tiếp, tính ngày thập phân |
| Tập trung sản phẩm | Xếp SKU theo tổng lượng bán dương; top x% dùng ceil(x × số SKU) và chia lượng nhóm này cho tổng lượng; không dùng doanh thu |

`StockCode` là mã hàng tham chiếu, chưa được ánh xạ tới product/variant của generator. Histogram giữ số đếm để xây sampler sau; `sku_units_ranked` chỉ lưu lượng theo thứ hạng, không lưu mã SKU. Phân vị dùng nearest rank `sorted[ceil(q*n)-1]`. Profile cũng đo lượng mua theo nhóm kích thước giỏ để tránh hiệu chỉnh các cột hoàn toàn độc lập.

## Cohort, tách kỳ và xử lý chất lượng

Chọn `Country=United Kingdom` để giữ một địa bàn nguồn thống nhất. Fit là **[01/12/2010, 01/06/2011)**, evaluation là **[01/06/2011, 01/12/2011)**: hai kỳ sáu tháng, lần lượt 182 và 183 ngày. Loại phần tháng 12/2011 chưa đủ kỳ. Timestamp giữ nguyên giờ địa phương không timezone của nguồn, không tự gán UTC hoặc đổi sang giờ Việt Nam. Không chia ngẫu nhiên dòng của một invoice sang hai tập.

Quy tắc được chốt trong spec/code và lưu trong profile:

1. Kiểm tra SHA-256 trước khi đọc; yêu cầu đủ đúng tám cột nguồn. Adapter nhận ZIP chứa một XLSX, XLSX một worksheet dùng Excel epoch 1900, hoặc CSV UTF-8 với InvoiceDate dạng ISO không timezone.
2. Loại invoice bắt đầu C, ID invoice không hợp lệ, ngày/số không hợp lệ, Quantity không dương hoặc không nguyên, UnitPrice không dương, mã hàng không theo pattern, CustomerID sai định dạng và country thiếu. Mỗi dòng chỉ ghi một lý do loại chính theo thứ tự xử lý.
3. Pattern `^[0-9]{5}[A-Za-z]{0,2}$` là lựa chọn cohort bảo thủ. Nó loại mã dịch vụ/điều chỉnh và cả mã không chuẩn; không chứng minh mọi mã ngoài pattern đều không phải hàng hóa. Không cắt outlier theo giá trị hoặc kích thước giỏ.
4. Gộp Quantity theo invoice/StockCode. Nếu các dòng ứng viên của một invoice khác timestamp, customer hoặc country, cách ly toàn invoice; sau đó mới lọc địa bàn và cửa sổ. Bộ lọc dòng có thể làm giỏ còn lại là giỏ hàng hóa hợp lệ, không phải toàn bộ chứng từ ban đầu.
5. Giữ dòng nghi trùng tám trường vì không có receipt-line ID để kết luận lỗi trùng. Đếm nghi trùng ở raw và trong phần giữ lại. Thiếu Description vẫn được giữ nếu mã hàng hợp lệ. Thiếu CustomerID được giữ cho giỏ/lượng/popularity, chỉ loại khỏi phép đo mua lại.

| Kiểm kê raw | Số lượng |
|---|---:|
| Dòng nguồn | 541.909 |
| Invoice ID distinct, gồm credit và ID ngoài cohort | 25.900 |
| Thiếu CustomerID | 135.080 |
| Thiếu Description | 1.454 |
| Dòng nghi trùng ở raw | 5.268 |
| Dòng nghi trùng giữ trong hai kỳ | 4.828 |
| Dòng giữ trong hai kỳ | 457.220 |

| Lý do loại chính | Dòng |
|---|---:|
| credit_or_cancellation | 9.288 |
| invalid_invoice_id | 3 |
| invoice_metadata_conflict | 3.332 |
| nonmerchandise_or_nonstandard_code | 2.378 |
| nonpositive_price | 1.179 |
| nonpositive_quantity | 1.336 |
| other_country | 43.607 |
| outside_windows | 23.566 |

Tổng dòng raw = 457.220 giữ + 84.689 loại. Có 42 invoice ứng viên bị cách ly do metadata xung đột, tương ứng 3.332 dòng. Số thiếu và số nghi trùng là kiểm kê chồng lấp, không cộng thêm vào các lý do loại.

## Kết quả đo

| Chỉ số | Fit | Evaluation |
|---|---:|---:|
| Invoice hợp lệ | 7.335 | 9.783 |
| Dòng hợp lệ | 185.449 | 271.771 |
| Invoice thiếu CustomerID | 675 | 596 |
| CustomerID có mua hàng | 2.459 | 3.133 |
| SKU distinct | 3.354 | 3.454 |
| Giỏ: median / p90 / max | 15 / 50 / 673 | 16 / 54 / 1108 |
| Lượng mỗi invoice/SKU: median / p90 / max | 3 / 19 / 74215 | 3 / 24 / 4800 |
| Khách có ít nhất hai invoice | 50.75% | 57.01% |
| Khoảng cách mua quan sát: median (ngày) | 18.19 | 18.08 |
| Tỷ trọng lượng bán top 10% SKU | 62.56% | 57.98% |

## Ý nghĩa đối với generator và giới hạn

Generator hiện chọn 1–4 SKU/đơn và 1–3 đơn vị/SKU theo YAML, chọn variant đều và rescale số đơn mỗi nhóm khách. Trong nguồn, giỏ và lượng mua có đuôi dài lớn hơn nhiều; top SKU tập trung lượng bán. Đây là bằng chứng cho việc cần cấu hình phân phối và popularity, chưa phải lý do sao chép nguyên phân phối UK vào thời trang Việt Nam.

Profile giữ nguyên đơn lớn, trong đó có thể có khách sỉ; không tự nhận diện hoặc loại khách sỉ bằng ngưỡng tùy ý. Trước khi dùng làm sampler cần chọn cohort phù hợp hoặc công bố cách điều chỉnh/capping và phần khối lượng bị ảnh hưởng. Khi đổi số SKU phải ánh xạ thứ hạng/tỷ trọng, không gắn mã nguồn vào catalog giả lập. Chỉ dùng kỳ fit để chọn tham số; kỳ evaluation dùng đối chiếu theo cùng định nghĩa.

Tỷ lệ mua lại chỉ xét khách đã mua có ID trong mỗi kỳ, không bao gồm người đăng ký chưa mua. Lần mua đầu quan sát không phải ngày gia nhập, khoảng cách mua chỉ có ở khách quay lại, và các lần mua sau cuối kỳ chưa quan sát được. Profile lưu thời gian từ lần mua đầu thấy đến cuối kỳ để mô tả exposure; chưa ước lượng churn hoặc thời gian mua lại đã hiệu chỉnh censoring. Hai kỳ có mùa vụ và độ dài khác nhau; chênh lệch không phải thước đo generator vì chưa sinh dữ liệu từ profile.

Invoice C/Quantity âm không được chuyển thành tỷ lệ hủy trước giao, hoàn tiền hoặc đổi trả của hệ thống. Giá VND, cấu trúc thời trang, Tet, POS, COD, coupon và review vẫn là giả định/kịch bản chưa hiệu chỉnh từ nguồn này. Profile chứa thống kê tham chiếu; chưa có profile runtime hoặc thay đổi logical identity của dataset SQL. Bước sau mới chốt baseline/scenario, ánh xạ profile và đo đầu ra sau các ràng buộc nghiệp vụ.

## Tái lập và kiểm tra

Đặt artifact chính thức ngoài repo; lệnh dưới dùng biến shell trỏ tới file đã có. Công cụ không tự tải dataset:

```bash
export UCI_RETAIL_SOURCE=/path/to/online_retail.zip
uv run --locked --package data-generator -- generator profile-reference \
  --input "$UCI_RETAIL_SOURCE" \
  --spec generator/profiles/uci-reference.yml \
  --output data/generator/reference/uci-online-retail-uk-v1.json

uv run --locked --package data-generator -- python -m unittest discover -s generator/tests -v
```

Kết quả CLI phải có cùng profile ID như bản đã lưu khi dùng cùng artifact/spec/policy. Đổi một lựa chọn cohort/cửa sổ hoặc byte nguồn sẽ đổi ID; checksum sai bị từ chối. Output không được ghi đè input/spec. Không sửa/xóa source, không chạy pipeline hoặc nạp DB.

Các fixture nhỏ kiểm tra parser CSV/XLSX/ZIP, ngày Excel, checksum, bảo toàn dòng, chính sách dòng trùng, gộp SKU, missing ID, invoice xung đột, chia kỳ, giá trị không hữu hạn, grain và mẫu số, null khi rỗng, ID/nội dung tái lập và CLI. Chúng không cần mạng hoặc dataset UCI. Đã chạy CLI trên artifact ghim checksum và đối chiếu số dòng/thiếu/duplicate với audit raw trước đó; file kết quả chỉ là aggregate theo chính sách đã nêu.
