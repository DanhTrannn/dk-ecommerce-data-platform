# Chỉ số và giả định của generator

## Trạng thái và cách đọc

Generator đã được viết lại theo schema MySQL hiện tại (`database/schema.sql`), theo quyết định ngày 08/11/2026. Giá trị đối chiếu nằm trong `generator/configs/small.yml` và `generator/src/generator/`; các tham số giả định được giữ từ bản trước (`6284896`). Chưa chuyển schema nguồn sang PostgreSQL hoặc tự nạp dữ liệu.

**Đã cải thiện R03:** chọn sản phẩm theo trọng số mười nhóm thứ hạng H&M 2019, sau đó chọn variant. Các tỷ lệ khác giữ nguyên giá trị kịch bản. Việc viết lại đồng thời bảo đảm tên trường/trạng thái khớp schema, hạn mức coupon, thứ tự thời gian và cập nhật snapshot theo anchor.

- **Thực nghiệm:** số đo trực tiếp từ cohort EDA, chỉ có ý nghĩa trong phạm vi nguồn.
- **Giả định:** giá trị kịch bản giữ từ generator cũ vì chưa có bằng chứng phù hợp để thay.
- **Kỹ thuật/nghiệp vụ:** điều khiển quy mô, tái lập hoặc bảo đảm quan hệ dữ liệu; không phải thống kê thị trường.

Trọng số được chuẩn hóa khi lấy mẫu: `p_i = w_i / tổng w`. Một trọng số không tự là tỷ lệ kết quả cuối cùng khi còn có điều kiện đủ tuổi đơn, giá tối thiểu, cắt cửa sổ quan sát hoặc giới hạn số mặt hàng.

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

## 3. Catalog, giá và giỏ hàng — giả định

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
| `order_size` | 1/2/3/4 variant: 55%/30%/12%/3% | Số variant khác nhau mục tiêu mỗi đơn; giới hạn bởi các variant có trọng số sản phẩm dương |
| `quantity_per_item` | 1/2/3 đơn vị: 70%/25%/5% | Số lượng của mỗi variant trong đơn |
| Phí giao hàng | 30.000 VND; miễn nếu subtotal ≥500.000 VND | Phí tính theo tiền hàng trước giảm giá |
| Size/màu | 5 size, 12 màu; tối đa 60 variant/sản phẩm | Tổ hợp hợp lệ của catalog; không phải tỷ lệ nhu cầu theo size/màu |
| Phân bổ variant | Chia đều cho sản phẩm, phần dư theo thứ tự | Đáp ứng đúng tổng variant cấu hình |
| Lưu trữ sản phẩm | `floor(products/20)` | Khoảng 5% catalog được archive cuối cửa sổ; lịch sử giao dịch vẫn giữ |

Phân phối giỏ Olist và Quantity UCI chưa thay thế các trọng số trên vì khác grain và phạm vi bán lẻ/bán sỉ. Giá trị đơn được tính từ dòng hàng, không lấy riêng một AOV cố định.

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

## 5. Coupon — giả định

| Chỉ số | Giá trị | Ý nghĩa |
|---|---|---|
| `base_usage_rate` | 0,14 | Xác suất thử dùng coupon thường khi đủ điều kiện |
| `campaign_usage_rate` | 0,64 | Xác suất thử dùng coupon chiến dịch |
| `midnight_usage_rate` | 0,82 | Xác suất thử dùng coupon 0h–1h khi có coupon hợp lệ |
| `first_order_usage_rate` | 0,42 | Xác suất thử dùng coupon chào mừng ở lần mua đầu |
| `customer_multipliers` | loyal 1,25; regular 1; one_off 0,8 | Nhân xác suất theo nhóm, chặn tối đa 1 |
| `campaign_percentage_values` | 10%, 12%, 15% | Mức giảm chiến dịch |
| `midnight_fixed_values_vnd` | 30.000; 50.000; 70.000 | Mức giảm cố định cho khung nửa đêm |
| `everyday_percentage` | 8% | Mức giảm coupon thường |
| `welcome_fixed_vnd` | 50.000 | Mức giảm coupon chào mừng |
| `everyday_minimum_subtotal_vnd` | 299.000 | Ngưỡng tiền hàng coupon thường |
| `campaign_minimum_subtotal_vnd` | 199.000 | Ngưỡng tiền hàng coupon chiến dịch, midnight và welcome |
| Phân bổ mức giảm chiến dịch | Luân phiên theo thứ tự ngày chiến dịch | Chọn giá trị trong các danh sách giảm giá; chưa phải phân phối nhu cầu khuyến mại |
| Cửa sổ coupon 0h | Hai giờ | Chỉ dùng trong đầu ngày chiến dịch |
| Giới hạn mỗi khách | 1 cho welcome/campaign/midnight; 10 cho everyday | Quy tắc sử dụng coupon |
| Tổng giới hạn coupon | `max(100, orders+100)` | Hạn mức kịch bản; không đo nhu cầu thị trường |

Ưu tiên coupon theo điều kiện: midnight → campaign → welcome ở lần đầu → everyday. Tỷ lệ sử dụng cuối cùng còn phụ thuộc đủ giá tối thiểu, cửa sổ hiệu lực, hạn mức và nhóm khách. Coupon cố định không vượt subtotal; giảm phần trăm tính từ subtotal. Coupon cho mức giảm bằng 0 không được gắn vào đơn.

## 6. Trạng thái, thanh toán và giao nhận — giả định

| Chỉ số/điều kiện | Giá trị bản cũ | Ý nghĩa |
|---|---|---|
| COD/VietQR | Công thức modulo hướng tới 60%/40% | Lựa chọn theo vị trí khách/lần mua, sau đó xét điều kiện khóa COD; không bảo đảm tỷ lệ toàn bộ đơn |
| Hủy đơn thường/chiến dịch | 3,5% / 6,5% | Xác suất nền khi đơn đủ tuổi ≥2 giờ |
| Hủy theo nhóm | loyal −1,2 điểm %; regular 0; one_off +1,8 điểm % | Cộng vào xác suất nền |
| Hủy có coupon | +1,2 điểm % | Cộng vào xác suất hủy; tổng chặn trong [0,1] |
| Lý do hủy | nhầm size/màu 28; địa chỉ 18; lựa chọn khác 18; nhầm mã 14; hết nhu cầu 22 | Trọng số lý do trong nhóm đơn hủy |
| Hoàn tất | 97% khi chưa hủy và đơn ≥5 ngày | Xác suất có điều kiện, chưa trừ tình huống boom COD |
| Xác nhận | Đơn chưa hủy/chưa hoàn tất và ≥8 giờ | Điều kiện chọn trạng thái snapshot |
| Thời gian xác nhận VietQR | 2–18 giờ sau đặt | Mốc nghiệp vụ, giới hạn bởi anchor |
| Thời gian hoàn tất | 24–96 giờ sau xác nhận | Thời gian mô phỏng hiện tại, chưa dùng trung vị Olist |
| Thời gian hủy | 15–480 phút sau đặt | Mốc hủy của đơn đã được chọn hủy |
| Khách rủi ro COD | `customer_index % 8 == 3` | Khoảng một phần tám khách theo thứ tự |
| Boom COD | Đơn COD chưa hủy, ≥3 ngày; nhóm rủi ro mỗi vị trí chia hết cho 3, nhóm khác chia hết cho 31 | Kịch bản theo vị trí, không phải xác suất thất bại thị trường |
| Thời điểm giao thất bại | Hai ngày sau xác nhận/đặt | Mốc thất bại, chặn bởi anchor |
| Số lần giao | Thành công 1, thất bại 3 | Tạo shipment attempt hợp lệ |
| Khoảng lần giao thất bại | 6 giờ | Các lần thử liên tiếp; chặn ở thời điểm thất bại |
| Khóa COD | Ba lần giao thất bại đã xảy ra trước hoặc tại lúc đặt | Sau mốc này đơn mới dùng VietQR; chưa tính các thất bại vẫn còn ở tương lai tại lúc đặt |
| Kênh bán | Chỉ sinh online trong bản cũ | Chưa có phân phối POS; không suy từ khảo sát đa kênh |

COD chỉ được ghi nhận đã trả tiền khi hoàn tất; đơn COD chưa hoàn tất giữ payment pending. VietQR ghi nhận thanh toán khi đặt, hủy sau thanh toán có refund. Snapshot `boom_count`/`is_cod_blocked` tổng hợp đến anchor; cập nhật bộ đếm trong file SQL đặt `updated_at` theo anchor. Vì thế `customers.updated_at` cuối cùng có thể muộn hơn mốc inactive được lấy mẫu nội bộ. Đây là tính nhất quán nghiệp vụ, không phải bằng chứng tỷ lệ thanh toán thực tế.

## 7. Review và đổi trả — giả định

| Chỉ số | Giá trị bản cũ | Ý nghĩa |
|---|---|---|
| Xác suất thử tạo review | loyal 58%; regular 38%; one_off 24% | Áp dụng trên từng item của đơn hoàn tất; tên cấu hình `completed_order_rates` không có nghĩa mẫu số là đơn |
| `rating_weights` | Điểm 1–5: [2, 5, 13, 38, 42] | Phân phối điểm độc lập với giao nhận trong bản cũ |
| `status_weights` | approved 94; rejected 6 | Trọng số trạng thái kiểm duyệt |
| `delay_days` | 1–14 ngày, cộng 0–23 giờ | Thời gian viết review sau hoàn tất; vượt anchor hoặc từ lúc hoàn trả toàn bộ được xử lý thì không tạo |
| Thời gian kiểm duyệt từ chối | 1–48 giờ sau review | Tạo metadata kiểm duyệt, chặn bởi anchor |
| Chọn đơn đổi trả | `order_index % 25 == 7`, đã hoàn tất và còn ≥3 ngày trước anchor | Kịch bản theo thứ tự; không phải tỷ lệ hoàn hàng quan sát |
| Chọn exchange | `order_index % 10 < 7`, đồng thời có variant thay thế | Điều kiện modulo tương tác với chọn đơn; không khẳng định 70% các ca đổi trả |
| Item/số lượng trả | Item đầu tiên, một đơn vị | Phạm vi đổi trả đơn giản |
| Ngày yêu cầu/xử lý | Hoàn tất +2 ngày; xử lý thêm +1 ngày | Trình tự đổi trả |
| Tiền hoàn một đơn vị | `round(unit_price × (1 − discount/subtotal))` | Phân bổ giảm giá theo tỷ lệ; không hoàn tiền khi đổi hàng |

Nguồn Olist mô tả review theo đơn và có ngày hẹn giao; repo review theo item và chưa có ngày hẹn. Chưa thay trọng số review bằng phân phối Olist khi chưa giải quyết ánh xạ này. Thiếu bằng chứng VN cho xác suất viết review, kiểm duyệt và đổi trả.

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

## 9. Quy luật thực nghiệm đủ căn cứ để cải thiện

`distributions.product_popularity` chứa số dòng mua H&M 2019 theo mười nhóm thứ hạng sản phẩm, đã được áp dụng cho lựa chọn sản phẩm. Đây không phải tỷ trọng SKU của thị trường Việt Nam.

| Nhóm thứ hạng | Dòng mua | Tỷ trọng xấp xỉ |
|---|---:|---:|
| 0–10% | 10.500.637 | 64,04% |
| 10–20% | 2.943.373 | 17,95% |
| 20–30% | 1.411.092 | 8,61% |
| 30–40% | 717.882 | 4,38% |
| 40–50% | 386.309 | 2,36% |
| 50–60% | 215.783 | 1,32% |
| 60–70% | 120.385 | 0,73% |
| 70–80% | 63.178 | 0,39% |
| 80–90% | 28.810 | 0,18% |
| 90–100% | 9.481 | 0,06% |

**Ánh xạ đã triển khai:** chuẩn hóa số dòng mua thành trọng số của từng nhóm; phân bổ đều khối xác suất bên trong mỗi nhóm và tích phần giao với khoảng thứ hạng của từng sản phẩm giả lập. Xáo thứ hạng bằng seed để tránh gắn mức bán chạy cố định với giá/thứ tự tạo. Mỗi lượt chọn sản phẩm theo trọng số rồi chọn đều một variant còn lại trong đơn; variant đã chọn không lặp lại. Khi một sản phẩm hết variant khả dụng trong đơn thì bỏ khỏi tập chọn.

Sản phẩm có trọng số 0 không được chọn; nếu hết variant mang trọng số dương thì giỏ được giới hạn theo số variant khả dụng, không quay về chọn đều các sản phẩm bị loại. Số variant không tự nhân trọng số sản phẩm ở đầu lượt chọn. Mẫu nhiều item không hoàn lại có thể làm thay đổi tỷ trọng cuối cùng so với khối xác suất đầu vào. Phân bố đều trong nhóm, ánh xạ thứ hạng và chọn đều variant là giả định mô hình; không khẳng định tái hiện toàn bộ phân phối H&M hoặc thị trường VN.

Nguồn và phép tính nằm trong [notebook EDA](../../generator/notebooks/reference_datasets_eda.ipynb), nhóm 3. Các quy luật R01, R02 và R04–R07 chưa xác định đủ tham số áp dụng trực tiếp cho shop; giữ giá trị cũ dưới nhãn giả định, không thay bằng tỷ lệ khảo sát khác mẫu số.

## 10. Chỉ số kỹ thuật và giới hạn phạm vi

- UUIDv5, logical identity và generation run ID dùng để nhận diện dataset và các bản ghi liên quan; không phải checkpoint fetch hoặc tỷ lệ nghiệp vụ. Namespace `dk-generator-v2` phân biệt bản viết lại; identity lấy từ toàn bộ cấu hình và phiên bản. Khối khóa số được suy từ identity; nhập lại cùng dataset cần database trống hoặc cách xử lý trùng riêng.
- Tổng tiền, thanh toán, hoàn tiền, trạng thái và biến động kho phải nhất quán; seed không thay thế các kiểm tra này.
- INSERT ghi tối đa 1.000 dòng/lệnh; buffer giao dịch xả sau 1.000 đơn. Đây là kích thước ghi SQL, không phải chunk extract của pipeline. File SQL ghi tạm rồi thay thế đầu ra khi hoàn tất; không phải checkpoint/retry theo chunk.
- Bản viết lại sinh dữ liệu cho 23 bảng như phạm vi cũ; không sinh `cities`, `stores`, `store_inventory`, `inbound_receipts`, `inbound_receipt_items`. Không tự thêm phân phối địa lý/POS/nhập hàng khi chưa có căn cứ hoặc quyết định nghiệp vụ.
- Chưa có chỉ số hiệu quả dự báo, mức tương đồng toàn thị trường VN hoặc tỷ lệ cải thiện generator được đo. Các phân phối EDA và giả định có vai trò riêng.
