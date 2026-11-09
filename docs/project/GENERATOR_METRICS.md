# Chỉ số và giả định của generator

## Trạng thái và cách đọc

Generator 0.11 dùng schema PostgreSQL (`database/schema.sql`). YAML `generator/configs/small.yml` là cấu hình chạy; tài liệu này ghi giá trị, ý nghĩa và căn cứ. Xuất SQL và nạp qua importer/console vẫn là hai thao tác riêng.

**Mục tiêu:** dữ liệu đúng nghiệp vụ, có đặc tính mua hàng hợp lý và đủ ca hủy/đổi trả/giao thất bại để trình diễn phân tích. R02 dùng Olist thời trang, R03 dùng H&M product_code làm tham chiếu; các chính sách shop và tỷ lệ demo được chủ động đặt. Không cần tìm dataset hoặc fit mô hình cho mọi hằng số, không khẳng định khớp thị trường VN 100%.

- **Thực nghiệm:** số đo trực tiếp từ cohort EDA, chỉ có ý nghĩa trong phạm vi nguồn.
- **Giả định:** giá trị nhóm chủ động chọn cho kịch bản, gồm cả giá trị được giữ từ bản trước; không nhận là thống kê thị trường.
- **Kỹ thuật/nghiệp vụ:** điều khiển quy mô, tái lập hoặc bảo đảm quan hệ dữ liệu; không phải thống kê thị trường.

Trọng số được chuẩn hóa khi lấy mẫu: `p_i = w_i / tổng w`. Một trọng số không tự là tỷ lệ kết quả cuối cùng khi còn có điều kiện đủ tuổi đơn, giá tối thiểu, cắt cửa sổ quan sát hoặc giới hạn số mặt hàng.

## Nơi cấu hình tập trung

[small.yml](../../generator/configs/small.yml) là nguồn cấu hình chạy. `distributions` giữ trọng số/tỷ lệ đã có; `policies` chứa các giá trị kịch bản vừa chuyển khỏi code. Các con số trong tài liệu là mặc định tham khảo; khi chỉnh kịch bản, sửa YAML.

| Khối | Nội dung |
|---|---|
| `policies.history` | Ngày/tháng quy ước và số ngày tạo master trước lịch sử |
| `policies.customers`, `policies.catalog` | Lấy mẫu tên/trạng thái/địa chỉ, tuổi tài khoản, archive, bước giá và giá vốn |
| `policies.shipping` | Phí/miễn phí ship, số lần giao lỗi, khoảng thử giao, thời gian thất bại và ngưỡng khóa COD |
| `policies.timing` | Tuổi đơn đủ điều kiện, thời gian xác nhận/hoàn tất/hủy, cửa sổ kéo về chiến dịch và phút mua |
| `policies.returns` | `first`/`random` cho item/variant thay thế, quantity và độ trễ yêu cầu/xử lý |
| `policies.inventory` | Khoảng tồn đệm |
| `policies.coupons`, `policies.reviews` | Hạn mức/hiệu lực coupon, giờ chờ review và kiểm duyệt |
| `policies.wishlist`, `policies.carts` | Tỷ lệ tạo/xóa, số item/quantity và các khoảng thời gian |

Khoảng `[min, max]` lấy cả hai đầu. Số lần giao phải nằm trong 1–3 theo schema; thời gian phải hợp lệ. Quantity trả giới hạn theo số đã mua; tiền hoàn không vượt payment. Danh sách tên/mẫu sản phẩm, kiểu trạng thái, mã định danh và ràng buộc nhất quán vẫn thuộc code. YAML này chỉ điều khiển generator, không tự đổi chính sách API nguồn.

## 1. Quy mô và thời gian

| Chỉ số | Giá trị preset | Ý nghĩa | Phân loại |
|---|---|---|---|
| `scenario_id` | `small-repurchase-v2` | Tên kịch bản, dùng để nhận diện đầu ra | Kỹ thuật |
| `dataset_size` | `small` | Nhãn quy mô; không tự suy ra số dòng | Kỹ thuật |
| `seed` | 20260724 | Hạt giống để tái lập các lựa chọn ngẫu nhiên | Kỹ thuật |
| `anchor_time` | `2026-07-01T00:00:00+00:00` | Mốc cuối lịch sử được sinh | Kỹ thuật |
| `history_months` | 12 | Cửa sổ lịch sử; code cũ quy ước một tháng = 30 ngày | Giả định |
| `modes` | `seed_master`, `historical_transactions`, `repurchase_history` | Nhãn kịch bản trong cấu hình/manifest; chưa tự chứng minh có ba đường chạy độc lập | Kỹ thuật |
| `scale.customers` | 500 | Số khách mua hàng; tài khoản phục vụ quản trị có thể là dòng bổ sung | Quy mô |
| `scale.products` | 60 | Số sản phẩm trong catalog | Quy mô |
| `scale.variants` | 240 | Số tổ hợp size/màu của sản phẩm | Quy mô |
| `scale.orders` | 3000 | Tổng đơn mục tiêu; có thể xung đột khoảng đơn theo nhóm khách | Quy mô |
| `business_timezone` | `Asia/Ho_Chi_Minh` | Múi giờ dùng cho giờ mua và ngày chiến dịch; timestamp lưu theo UTC | Ngữ cảnh VN |

## 2. Thời điểm mua và chiến dịch — giả định

`day_of_week`, từ thứ Hai đến Chủ nhật: **[0,9; 0,95; 1; 1,05; 1,2; 1,5; 1,4]**. Giá trị lớn làm ngày đó dễ được chọn hơn; chưa đo được đường theo thứ tại shop VN.

Hai bộ trọng số theo giờ, thứ tự từ 00h đến 23h:

| Giờ | Ngày thường | Ngày chiến dịch |
|---|---:|---:|
| 00 | 0,05 | 2,80 |
| 01 | 0,02 | 1,20 |
| 02 | 0,02 | 0,22 |
| 03 | 0,02 | 0,08 |
| 04 | 0,03 | 0,05 |
| 05 | 0,05 | 0,06 |
| 06 | 0,09 | 0,10 |
| 07 | 0,14 | 0,16 |
| 08 | 0,18 | 0,22 |
| 09 | 0,20 | 0,28 |
| 10 | 0,22 | 0,34 |
| 11 | 0,26 | 0,55 |
| 12 | 0,28 | 1,35 |
| 13 | 0,26 | 0,72 |
| 14 | 0,24 | 0,36 |
| 15 | 0,24 | 0,30 |
| 16 | 0,27 | 0,36 |
| 17 | 0,34 | 0,48 |
| 18 | 0,50 | 0,72 |
| 19 | 0,72 | 1,10 |
| 20 | 0,95 | 1,65 |
| 21 | 1,10 | 2,05 |
| 22 | 1,00 | 1,80 |
| 23 | 0,60 | 1,30 |

| Chỉ số | Giá trị giữ từ bản cũ | Ý nghĩa |
|---|---|---|
| `tet.month_start/day_start` | 1/25 | Bắt đầu cửa sổ Tết giả định: 25/01 dương lịch |
| `tet.month_end/day_end` | 2/18 | Kết thúc ngày 18/02 dương lịch; chưa phải lịch Tết từng năm |
| `tet.peak` | 2,8 | Hệ số trọng số ở giữa cửa sổ, giảm về hai đầu |
| Ngày đôi 1/1–8/8 | `boost=4`, `after_days=0` | Tăng trọng số đúng ngày chiến dịch |
| Ngày đôi 9/9–12/12 | `boost=4,8`, `after_days=0` | Tăng trọng số đúng ngày chiến dịch |
| Black Friday | Thứ Sáu thứ tư tháng 11; `boost=3,2`, `after_days=2` | Chiến dịch kéo dài ngày chính và hai ngày sau |
| Cửa sổ kéo lần mua về chiến dịch | ±7 ngày | Tìm ngày chiến dịch gần lần mua dự kiến; phụ thuộc `campaign_affinity` |
| Phút mua | 0–59, đồng đều | Độ phân tán timestamp trong giờ đã chọn |

Nghiên cứu Việt Nam hỗ trợ xem xét giai đoạn trước Tết, chưa xác định các hệ số trên. Trong bản cũ, trọng số ngày nhân theo thứ × Tết × chiến dịch; các chiến dịch trùng ngày có thể nhân chồng. Đây là cơ chế kịch bản, chưa phải các tác động độc lập đã đo.

## 3. Catalog, giá và giỏ hàng — chính sách và tham chiếu

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| Tỷ trọng tạo catalog `categories` | áo 22; đầm 18; váy 15; quần 14; khoác 12; phụ kiện 9; giày 5; túi 5 | Xác suất gán nhóm khi tạo sản phẩm; khác tỷ trọng đơn hoặc doanh thu |
| `price_bands[0]` | 79.000–199.000 VND; trọng số 30 | Dải giá nền sản phẩm |
| `price_bands[1]` | 199.000–399.000 VND; trọng số 40 | Dải giá nền sản phẩm |
| `price_bands[2]` | 399.000–699.000 VND; trọng số 20 | Dải giá nền sản phẩm |
| `price_bands[3]` | 699.000–2.500.000 VND; trọng số 10 | Dải giá nền sản phẩm |
| Bước giá | 1.000 VND | Độ chia khi lấy giá trong dải |
| Chênh giá variant | +5.000 VND theo thứ tự variant, chặn ở giá tối đa | Cách tạo giá khác nhau giữa size/màu |
| Giá vốn variant | 35%–45% giá bán theo thứ tự variant | Giá vốn mô phỏng, chưa phải số liệu biên lợi nhuận thực tế |
| `order_size` | 1/2/3/4 product: trọng số [1310, 87, 8, 4] | Tham chiếu 1.409 giỏ Olist thời trang; chọn product khác nhau trước variant, giới hạn theo số product có trọng số dương |
| `quantity_per_item` | 1/2/3/4/5 đơn vị: trọng số [1489, 29, 3, 2, 1] | Tham chiếu cặp đơn–product Olist; áp vào một variant/product là giả định |
| Phí giao hàng | 30.000 VND; miễn nếu subtotal ≥500.000 VND | Phí tính theo tiền hàng trước giảm giá |
| Size/màu | 5 size, 12 màu; tối đa 60 variant/sản phẩm | Tổ hợp hợp lệ của catalog; không phải tỷ lệ nhu cầu theo size/màu |
| Phân bổ variant | Chia đều cho sản phẩm, phần dư theo thứ tự | Đáp ứng đúng tổng variant cấu hình |
| Lưu trữ sản phẩm | `floor(products/20)` | Khoảng 5% catalog được archive cuối cửa sổ; lịch sử giao dịch vẫn giữ |

Giỏ/quantity dùng Olist thời trang làm tham chiếu như mục 9; UCI có khách sỉ nên chỉ đối chiếu. Chuyển sang product/variant của repo vẫn là giả định. Giá trị đơn được tính từ dòng hàng, không lấy riêng một AOV cố định.

## 4. Khách hàng và mua lại — giả định

| Nhóm | Tỷ trọng | Khoảng đơn cấu hình/năm | Khoảng mua | `campaign_affinity` |
|---|---:|---|---|---:|
| `loyal` | 15% | 4–10 | 20–60 ngày | 0,72 |
| `regular` | 35% | 2–4 | 45–120 ngày | 0,48 |
| `one_off` | 50% | 1 | Không cấu hình khoảng mua lại | 0,38 |

`campaign_affinity` là xác suất thử kéo lần mua về chiến dịch gần đó, không phải tỷ lệ đơn dùng coupon. Các nhãn nhóm là lựa chọn kịch bản, chưa được phân loại từ khách Việt.

**Giới hạn được giữ thành giả định:** số đơn được lấy trong khoảng rồi co giãn theo tỷ lệ để khớp `scale.orders`; vì vậy các khoảng trên là đầu vào phân bổ, không bảo đảm số đơn cuối cùng hoặc nhóm `one_off` chỉ có một đơn. Lịch mua còn bị giới hạn bởi cửa sổ lịch sử, thời điểm tài khoản ngừng hoạt động và số đơn còn phải sinh; khoảng mua thực tế có thể ngắn hơn khoảng cấu hình. Tài khoản đã inactive trước cửa sổ không được phân bổ đơn; tổng đơn phân bổ cho các tài khoản còn thời gian hợp lệ.

| Chỉ số bổ sung | Giá trị bản cũ | Ý nghĩa |
|---|---|---|
| Tỷ lệ tên nữ | 80% | Cách tạo tên cho bối cảnh catalog; không phải cơ cấu khách Việt đã đo |
| Khách inactive | Khoảng 4%, khách đầu tiên luôn active | Trạng thái tài khoản mô phỏng |
| Tuổi tài khoản trước cửa sổ | 1–180 ngày | Tạo tài khoản trước giao dịch |
| Thời điểm chuyển inactive | 1–60 ngày trước anchor | Giới hạn cuối lịch mua; thời điểm tạo tài khoản được giữ trước mốc này |
| Địa chỉ | Số nhà 1–999; danh sách đường/phường/quận chọn độc lập tại TP.HCM | Địa chỉ tổng hợp; chưa bảo đảm ánh xạ địa giới hợp lệ |
| Tài khoản bổ sung | Một người kiểm duyệt; 10 nhân viên giao hàng | Phục vụ quan hệ FK và kịch bản giao nhận/review |

## 5. Coupon — chính sách và giả định demo

Chỉ sinh coupon thường và coupon chiến dịch, đều giảm phần trăm. Không còn welcome/midnight hoặc hệ số theo nhóm khách.

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| `usage_rate` / `campaign_usage_rate` | 0,25 / 0,45 | Xác suất dùng trên đơn có coupon đủ điều kiện; ưu tiên coupon chiến dịch nếu có |
| `percentage` / `campaign_percentage` | 10 / 15 | Mức giảm % của hai loại coupon, là chính sách shop |
| `minimum_subtotal_vnd` | 199.000 VND | Ngưỡng tiền hàng trước giảm giá |
| Hạn mức mỗi khách | 10 lần coupon thường; 1 lần mỗi coupon chiến dịch | Kiểm tra số lần đã redeemed, released không chiếm hạn mức |
| Tổng hạn mức mỗi coupon | `max(100, orders+100)` | Hạn mức demo, không phải số liệu nhu cầu |
| Thời gian | Coupon thường: trước cửa sổ 1 ngày đến anchor +365 ngày; chiến dịch: ngày chính và `after_days` | Coupon hết hạn được archive; giao dịch lịch sử vẫn xét hiệu lực tại thời điểm đặt |

Tỷ lệ cuối cùng còn phụ thuộc ngưỡng tiền, thời gian và hạn mức; không bảo đảm 25%/45% mọi đơn. Tiền giảm = subtotal × phần trăm chia nguyên 100. Mức giảm bằng 0 thì không gắn coupon. Phí giao là 30.000 VND, miễn khi subtotal ≥500.000 VND — chính sách shop, không lấy từ EDA.

## 6. Vòng đời đơn — giả định demo có mẫu số rõ

| Chỉ số/điều kiện | Giá trị | Mẫu số hoặc quy tắc |
|---|---|---|
| `outcomes.cancellation_rate` | 0,08 | Đơn đủ tuổi ≥2 giờ; một xác suất chung, không cộng hệ số nhóm khách/coupon |
| `outcomes.cod_rate` | 0,60 | Chọn COD khi khách chưa bị khóa tại thời điểm đặt; còn lại VietQR |
| `outcomes.cod_failure_rate` | 0,08 | Đơn COD chưa hủy, đủ tuổi ≥3 ngày; mỗi đơn lỗi có ba lần giao thất bại |
| `outcomes.return_rate` | 0,06 | Đơn đã hoàn tất, còn ít nhất 3 ngày để tạo và xử lý đổi/trả trước anchor |
| `outcomes.exchange_rate` | 0,50 | Trong các ca đổi/trả có variant thay thế; không có thì refund |
| `cancellation_reasons` | nhầm size/màu 28; địa chỉ 18; lựa chọn khác 18; nhầm mã 14; hết nhu cầu 22 | Trọng số lý do, chỉ áp dụng cho đơn đã chọn hủy |
| Hoàn tất | Đơn ≥5 ngày nếu không hủy/giao thất bại | Đơn mới hơn còn paid/confirmed; không còn xác suất 3% giữ đơn cũ chưa hoàn tất |
| Xác nhận | COD ngay khi đặt; VietQR sau 2–18 giờ nếu đơn ≥8 giờ hoặc đã hoàn tất | Mốc giới hạn bởi anchor |
| Hoàn tất / hủy | Xác nhận +24–96 giờ / đặt +15–480 phút | Mốc giả định, giới hạn bởi anchor |
| Giao thất bại | Hai ngày sau xác nhận/đặt; ba lần thử cách nhau 6 giờ | Ghi cả shipment và attempt, mẫu số tỷ lệ là đơn, không phải số lần thử |
| Khóa COD | Ba đơn giao thất bại đã xảy ra trước/tại thời điểm đặt | Đơn tiếp theo dùng VietQR; giữ snapshot boom_count/is_cod_blocked đến anchor |
| Kênh | Online | Địa điểm tham chiếu không đồng nghĩa đã sinh giao dịch POS |

Các tỷ lệ là xác suất lấy mẫu theo seed, không phải quota và không cộng thẳng do khác mẫu số. Chúng được chọn để demo có tình huống phân tích, không phải ước lượng thị trường. COD chỉ có payment succeeded/paid_at khi hoàn tất; VietQR thanh toán lúc đặt và có refund nếu hủy sau thanh toán. Không tự thêm mô phỏng lỗi cổng thanh toán.

## 7. Review và đổi trả — giả định demo

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| `reviews.write_rate` | 0,35 | Xác suất thử tạo review trên từng item đã hoàn tất, dùng chung mọi nhóm khách |
| `rating_weights` | [2, 5, 13, 38, 42] | Trọng số điểm 1–5; chưa phụ thuộc giao nhận |
| `status_weights` | approved 94; rejected 6 | Kịch bản kiểm duyệt |
| `delay_days` | 1–14 ngày, cộng 0–23 giờ | Sau hoàn tất; vượt anchor hoặc từ lúc hoàn trả toàn bộ được xử lý thì bỏ review |
| Kiểm duyệt từ chối | Sau review 1–48 giờ | Có metadata người kiểm duyệt, chặn bởi anchor |
| Item/số lượng trả | Mặc định `first`, một đơn vị | Chỉnh qua `policies.returns`; quantity không vượt số đã mua, exchange chọn variant khác cùng product |
| Tạo/xử lý yêu cầu | Hoàn tất +2 ngày, thêm 1 ngày xử lý | Không vượt anchor |
| Tiền hoàn | Giá một đơn vị sau phân bổ giảm giá, làm tròn VND × quantity trả | Giới hạn bởi payment; refund chỉ gắn payment succeeded, đổi hàng không hoàn tiền |

Hoàn toàn bộ hàng chuyển đơn thành returned; đổi hàng hoặc hoàn một phần vẫn giữ completed. Nguồn Olist có review theo đơn và ngày hẹn giao; repo review theo item và chưa có ngày hẹn. SLA giữ null; chưa ánh xạ quan hệ giao trễ→điểm review hay thêm schema để lấp thiếu dữ liệu.

## 8. Wishlist, giỏ chưa checkout và tồn kho — giả định

| Chỉ số | Giá trị bản cũ | Ý nghĩa |
|---|---|---|
| Khách có wishlist | 55% | Xác suất tạo wishlist |
| Sản phẩm/wishlist | 1–5, không vượt catalog | Số sản phẩm của mỗi khách được chọn |
| Phần wishlist gắn với giao dịch | Mục tiêu 40%, ít nhất một khi có lịch sử đơn chưa hủy; chặn bởi số sản phẩm có giao dịch | Các lựa chọn còn lại vẫn có thể trùng lịch sử, nên tỷ lệ cuối không bảo đảm 40%; chưa đo chuyển đổi |
| Thêm trước khi mua | 1–30 ngày và 0–23 giờ | Timestamp wishlist, chặn đầu cửa sổ |
| Thêm lại trước khi mua | 0–7 ngày sau lần đầu | Giữ thời điểm không vượt lần mua |
| Thêm lại khi chưa mua | 0–29 ngày sau lần đầu | Giữ thời điểm không vượt anchor |
| Xóa wishlist chưa mua | 18% | Chọn `is_present=false` |
| Thời gian xóa | 0–14 ngày sau lần thêm cuối | Giữ thứ tự thời gian và không vượt anchor |
| Giỏ gắn với đơn | Tạo trước đặt 5–10.080 phút | Giỏ đã checkout, khác giỏ active |
| Giỏ active | Một giỏ cho mỗi khách active | Tạo dữ liệu giỏ chưa checkout |
| Giỏ active cũ | 65% chọn cập nhật 2–20 ngày trước anchor | Proxy giỏ ít hoạt động; không phải funnel bỏ giỏ đã đo |
| Giỏ active gần đây | Cập nhật 1–12 giờ trước anchor | Nhóm còn lại |
| Tuổi giỏ so với cập nhật | 1–72 giờ | Thời gian tạo giỏ active |
| Item giỏ active | 1–3 variant, mỗi variant 1–3 đơn vị | Số lượng giỏ chưa checkout |
| Tồn đệm mỗi variant | 80–200 đơn vị | Giả định đủ hàng cho lịch sử; không phải chính sách bổ sung hàng |
| Tồn đầu | Tổng xuất kho, gồm hàng đổi, + tồn đệm | Bảo đảm lịch sử không âm |
| Tồn cuối | Tồn đầu + tổng biến động | Ràng buộc đối soát tồn kho |

## 9. Tham chiếu thực nghiệm đã áp dụng

| Thành phần | Nguồn và giá trị | Phần chuyển nguồn là giả định |
|---|---|---|
| `product_popularity` | H&M 2019, product_code: `[12268604, 2147935, 947086, 482513, 264938, 146864, 79113, 38985, 16192, 4700]` | Product_code→product giả lập; chuyển catalog và chọn variant |
| `order_size` | 1.409 đơn Olist 2017 thuần fashion_: [1310, 87, 8, 4] cho 1–4 product | Marketplace Brazil, chủ yếu túi/phụ kiện; chưa đại diện riêng shop nữ VN |
| `quantity_per_item` | 1.524 cặp đơn–product Olist thời trang: [1489, 29, 3, 2, 1] cho quantity 1–5 | Dùng một variant/product và một phân phối quantity chung |

70.739 article H&M đã bán thuộc 33.324 product_code. Top 10% article chiếm 64,04% dòng mua; top 10% product_code chiếm 74,82%. Bản 0.11 dùng cấp product_code. Hai con số khác mẫu số định danh, không phải đo mức cải thiện generator.

Trọng số nhóm thứ hạng được chuẩn hóa rồi tích theo khoảng thứ hạng của catalog đích; thứ hạng product được xáo bằng seed. Chọn product trước rồi một variant hợp lệ/product; số variant không nhân thêm nhu cầu product. Một đơn không lặp product. Product trọng số 0 bị loại; số product được giới hạn bởi tập khả dụng. Chọn nhiều product không hoàn lại hoặc giới hạn catalog có thể đổi tỷ trọng đầu ra, không buộc doanh thu/số lượng top 10% phải đạt 74,82%.

Mẫu H&M chỉ có mã đã bán, chưa đo tỷ lệ product không có nhu cầu. Olist thời trang có 92,97% giỏ một product và 97,70% cặp đơn–product một đơn vị; chỉ 30 đơn có quần áo nữ. Chưa có cơ sở thêm hệ số giá/giỏ→quantity hoặc quy đổi giá BRL sang VND. UCI tiếp tục là nguồn đối chiếu, không dùng làm phân phối giỏ bán lẻ.

Chi tiết cohort/phép tính ở [notebook, mục 2.4 và 3.3](../../generator/notebooks/reference_datasets_eda.ipynb). R01 và R04–R07 giữ vai trò tham khảo; các giá trị runtime tương ứng là giả định/chính sách demo, không chờ nghiên cứu mọi hằng số mới chạy được.

## 10. Chỉ số kỹ thuật và giới hạn phạm vi

- UUIDv5, logical identity và generation run ID dùng để nhận diện dataset và các bản ghi liên quan; không phải checkpoint fetch hoặc tỷ lệ nghiệp vụ. Namespace `dk-generator-v2` phân biệt bản viết lại; identity lấy từ toàn bộ cấu hình và phiên bản. Khối khóa số được suy từ identity; importer PostgreSQL từ chối dataset đã nạp; không tự ghi đè hoặc reset dữ liệu.
- Tổng tiền, thanh toán, hoàn tiền, trạng thái và biến động kho phải nhất quán; seed không thay thế các kiểm tra này.
- INSERT ghi tối đa 1.000 dòng/lệnh; buffer giao dịch xả sau 1.000 đơn. Đây là kích thước ghi SQL, không phải chunk extract của pipeline. File SQL ghi tạm rồi thay thế đầu ra khi hoàn tất; không phải checkpoint/retry theo chunk.
- Generator hỗ trợ 25/28 bảng, gồm `cities`/`stores` tham chiếu theo `locations.store_codes` (mặc định 5 thành phố, 6 cửa hàng; có thể chọn rỗng). Không sinh `store_inventory`, `inbound_receipts`, `inbound_receipt_items`. Không tự thêm phân phối địa lý/POS/nhập hàng khi chưa có căn cứ hoặc quyết định nghiệp vụ.
- Chưa có chỉ số hiệu quả dự báo, mức tương đồng toàn thị trường VN hoặc tỷ lệ cải thiện generator được đo. Các phân phối EDA và giả định có vai trò riêng.

## 11. Trạng thái triển khai và kiểm chứng

Đã áp dụng quy luật product/giỏ ở trên và rút gọn coupon/review/outcomes trong generator 0.11. Không thay schema, importer, API hoặc pipeline; không thêm dependency. Cấu hình cũ cần chuyển sang các khối `outcomes`, `coupons`, `reviews` hiện tại, không có lớp tương thích tự động.

Tiêu chí: SQL tái lập; product không lặp và giới hạn theo catalog; quantity nguyên dương; tỷ lệ 0/1 đúng điều kiện tuổi đơn, COD và khóa COD; bảo toàn tiền/FK/kho; nạp được bằng importer PostgreSQL. Test bổ sung giữ ngoài Git, không làm benchmark cũ/mới.

Kiểm chứng đợt 09/11/2026: 30 test generator hiện có và 9 test bổ sung đạt; bộ 113 test platform trên PostgreSQL 16.8 biệt lập không lỗi (2 live test hạ tầng bỏ qua). Với preset 500 khách/60 product/240 variant/3.000 đơn, seed 20260724 và anchor 01/07/2026: 221 đơn hủy, 117 giao thất bại, 150 yêu cầu đổi/trả (82 exchange, 68 refund). Đây là kết quả một lượt demo tái lập, không phải tỷ lệ thị trường hay benchmark cải thiện.

Lượt tập trung YAML: các giá trị mặc định trên được giữ trong `policies`; các test bổ sung kiểm tra thay đổi cấu hình thực sự tác động đầu ra, kể cả trả nhiều đơn vị, wishlist nhỏ và chiến dịch ở biên ngày. Các tham số chính sách cũng tham gia identity và config đóng băng của job.
