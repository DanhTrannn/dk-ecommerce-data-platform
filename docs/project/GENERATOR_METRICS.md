# Quy luật và cấu hình sinh dữ liệu

Generator **0.11.0** sinh dữ liệu lịch sử dưới dạng SQL cho [schema PostgreSQL](../../database/schema.sql). Cấu hình chạy tập trung tại [small.yml](../../generator/configs/small.yml); tài liệu này ghi giá trị preset, cách áp dụng, căn cứ và giới hạn hiện tại.

Generator kết hợp quy tắc nghiệp vụ, lấy mẫu theo trọng số, xác suất có điều kiện và lịch mua mô phỏng. Runtime không đọc CSV nghiên cứu hay chạy notebook.

## Cách đọc cấu hình

- **Trọng số:** chuẩn hóa theo `p_i = w_i / tổng w`, không bắt buộc đầu ra đạt đúng tỷ trọng.
- **Tỷ lệ:** xác suất lấy mẫu trên tập bản ghi đủ điều kiện, không phải quota.
- **Khoảng `[min, max]`:** lấy cả hai đầu.
- **Thực nghiệm:** số đo từ cohort EDA, chỉ có ý nghĩa trong phạm vi nguồn.
- **Giả định/chính sách:** giá trị chủ động chọn cho shop và demo; chưa phải thống kê thị trường Việt Nam.
- **Kỹ thuật/nghiệp vụ:** quy mô, định danh và ràng buộc nhất quán.

`distributions` chứa trọng số/tỷ lệ; `policies` chứa chính sách và khoảng lấy mẫu. Khi đổi kịch bản, sửa YAML. YAML chỉ điều khiển generator, không tự thay chính sách API nguồn.

## 1. Quy mô và cửa sổ lịch sử

| Cấu hình | Giá trị | Ý nghĩa |
|---|---|---|
| `scenario_id` | `small-repurchase-v2` | Tên kịch bản |
| `dataset_size` | `small` | Nhãn quy mô; số dòng do `scale` quyết định |
| `seed` | `20260724` | Tái lập lựa chọn ngẫu nhiên |
| `anchor_time` | `2026-07-01T00:00:00+00:00` | Mốc cuối lịch sử |
| `history_months` | 12 | Số tháng quy ước |
| `policies.history.days_per_month` | 30 | Số ngày mỗi tháng quy ước |
| `policies.history.master_data_lead_days` | 30 | Tạo master trước đầu lịch sử |
| `distributions.business_timezone` | `Asia/Ho_Chi_Minh` | Giờ mua/ngày chiến dịch; timestamp lưu UTC |
| `scale.customers` | 500 | Khách, chưa gồm tài khoản kiểm duyệt bổ sung |
| `scale.products` | 60 | Sản phẩm |
| `scale.variants` | 240 | Variant |
| `scale.orders` | 3.000 | Tổng đơn mục tiêu |
| `modes` | `seed_master`, `historical_transactions`, `repurchase_history` | Nhãn cấu hình/manifest, chưa phải ba luồng xuất độc lập |

Lịch sử mặc định dài **360 ngày: 06/07/2025 00:00 → 01/07/2026 00:00 UTC**. Tài khoản, master và giỏ checkout có thể được tạo trước cửa sổ giao dịch; coupon thường có hiệu lực vượt anchor. Ngày dữ liệu phụ thuộc anchor, không phụ thuộc ngày chạy lệnh.

## 2. Khách hàng và mua lại — giả định

Các nhóm thuộc `distributions.customers`:

| Nhóm | `share` | `orders_per_year` | `interval_days` | `campaign_affinity` |
|---|---:|---|---|---:|
| `loyal` | 15% | `[4, 10]` | `[20, 60]` | 0,72 |
| `regular` | 35% | `[2, 4]` | `[45, 120]` | 0,48 |
| `one_off` | 50% | `[1, 1]` | `[1, 1]` | 0,38 |

Chia khách theo tỷ trọng, lấy số đơn trong khoảng rồi **co giãn phân bổ để khớp `scale.orders`**. Khoảng đơn không bảo đảm số đơn cuối cùng; khách `one_off` vẫn có thể có nhiều đơn. Khoảng mua có thể bị rút ngắn để xếp đủ đơn trong cửa sổ và trước thời điểm inactive. Khách inactive trước cửa sổ không được phân bổ đơn.

`campaign_affinity` là xác suất thử kéo lần mua về chiến dịch gần đó, không phải xác suất dùng coupon.

Các khóa trong `policies.customers`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `female_name_rate` | 0,8 | Chọn tên nữ; không phải cơ cấu giới tính đã đo |
| `inactive_rate` | 0,04 | Xác suất inactive; khách đầu tiên luôn active |
| `created_before_history_days` | `[1, 180]` | Tạo tài khoản trước đầu lịch sử |
| `inactive_before_anchor_days` | `[1, 60]` | Inactive trước anchor; giữ thời điểm tạo trước inactive |
| `house_number_range` | `[1, 999]` | Số nhà |

Tên và địa chỉ ghép từ danh sách cố định. Đường/phường/quận được chọn độc lập trong bối cảnh TP.HCM, chưa bảo đảm mọi tổ hợp đúng địa giới. Ngoài khách cấu hình có **một tài khoản kiểm duyệt và 10 nhân viên giao hàng**. Khách active có thông tin đăng nhập demo, mật khẩu được băm.

## 3. Catalog, variant và giá — chính sách mô phỏng

`distributions.categories` là trọng số tạo catalog, không phải tỷ trọng đơn hàng/doanh thu:

| Danh mục | Trọng số |
|---|---:|
| `ao` — Áo | 22 |
| `dam` — Đầm | 18 |
| `vay` — Váy | 15 |
| `quan` — Quần | 14 |
| `khoac` — Khoác | 12 |
| `phu-kien` — Phụ kiện | 9 |
| `giay` — Giày | 5 |
| `tui-xach` — Túi xách | 5 |

`distributions.price_bands` chọn dải theo `weight`, rồi lấy giá nền trong `min_vnd`–`max_vnd`:

| `min_vnd` | `max_vnd` | `weight` |
|---:|---:|---:|
| 79.000 | 199.000 | 30 |
| 199.000 | 399.000 | 40 |
| 399.000 | 699.000 | 20 |
| 699.000 | 2.500.000 | 10 |

Các khóa trong `policies.catalog`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `archive_rate` | 0,05 | Archive `floor(products × archive_rate)` tại anchor; giữ lịch sử |
| `price_step_vnd` | 1.000 | Bước lấy giá nền |
| `variant_price_step_vnd` | 5.000 | Tăng giá theo thứ tự variant, chặn tại giá tối đa |
| `cost_ratio_base` | 0,35 | Tỷ lệ giá vốn khởi đầu |
| `cost_ratio_step` | 0,01 | Bước tăng tỷ lệ giá vốn |
| `cost_ratio_levels` | 11 | Số mức trước khi lặp |

```text
Giá variant = min(giá tối đa, giá nền + thứ tự variant × 5.000)
Giá vốn = int(giá variant × (0,35 + (thứ tự variant % 11) × 0,01))
```

Thứ tự variant bắt đầu từ 0. Variant chia đều cho sản phẩm, phần dư theo thứ tự. Từ vựng cố định gồm size `XS, S, M, L, XL` và màu `BLACK, WHITE, RED, GREEN, BLUE, YELLOW, PINK, PURPLE, ORANGE, BROWN, GRAY, BEIGE`; tối đa 60 tổ hợp/sản phẩm.

**Giới hạn preset:** 240 variant/60 product = 4 variant/product. Code lấy tổ hợp theo thứ tự size rồi màu nên **cả 240 variant đều là XS**, với bốn màu BLACK/WHITE/RED/GREEN. Giá vốn tương ứng là **35–38% giá bán**; 35–45% là toàn bộ dải tỷ lệ công thức có thể tạo.

## 4. Nhu cầu sản phẩm và giỏ hàng — tham chiếu EDA

Luồng chọn hàng: **product khác nhau theo trọng số → một variant ngẫu nhiên/product → quantity**. Chọn product không hoàn lại; số variant không nhân thêm nhu cầu product.

`distributions.product_popularity` dùng số dòng mua của 10 nhóm thứ hạng H&M 2019 ở cấp `product_code`:

```text
[12268604, 2147935, 947086, 482513, 264938,
 146864, 79113, 38985, 16192, 4700]
```

Code chuẩn hóa và tích trọng số theo khoảng thứ hạng catalog đích; xáo thứ hạng bằng seed. Product trọng số 0 bị loại. Nguồn có 70.739 article thuộc 33.324 product_code; top 10% product chiếm **74,82% dòng mua**. Đây là số đo nguồn, không buộc đầu ra hoặc doanh thu generator đạt cùng tỷ trọng.

`distributions.order_size` lấy từ 1.409 giỏ Olist 2017 thuần thời trang:

| Product khác nhau/đơn | Trọng số | Xác suất lấy mẫu |
|---|---:|---:|
| 1 | 1.310 | 92,97% |
| 2 | 87 | 6,17% |
| 3 | 8 | 0,57% |
| 4 | 4 | 0,28% |

Số product thực tế bị chặn theo số product khả dụng có trọng số dương.

`distributions.quantity_per_item` lấy từ 1.524 cặp đơn–product Olist thời trang:

| Quantity | Trọng số | Xác suất lấy mẫu |
|---|---:|---:|
| 1 | 1.489 | 97,70% |
| 2 | 29 | 1,90% |
| 3 | 3 | 0,20% |
| 4 | 2 | 0,13% |
| 5 | 1 | 0,07% |

**Giả định chuyển nguồn:** áp H&M/Olist sang shop của repo, chọn variant đồng đều và dùng chung phân phối quantity. Olist cohort chủ yếu túi/phụ kiện, chỉ 30 đơn có quần áo nữ; chưa đại diện riêng shop nữ Việt Nam. UCI chỉ đối chiếu, không trực tiếp điều khiển giỏ runtime. Chưa thêm quan hệ thực nghiệm giá/giỏ→quantity hoặc quy đổi giá BRL sang VND.

Cohort và phép tính nằm ở [notebook EDA, mục 2.4 và 3.3](../../generator/notebooks/reference_datasets_eda.ipynb). Phạm vi nằm ở [kế hoạch EDA](GENERATOR_EDA_PLAN.md).

## 5. Thời điểm mua và chiến dịch — giả định

`distributions.day_of_week`, thứ Hai → Chủ nhật:

```text
[0.9, 0.95, 1.0, 1.05, 1.2, 1.5, 1.4]
```

Trọng số giờ trong `distributions.hour_of_day` và `distributions.campaign_hour_of_day`:

| Giờ | Ngày thường | Chiến dịch |
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

`distributions.seasonality`:

| Chiến dịch | Cấu hình |
|---|---|
| `tet` | `month_start=1`, `day_start=25`, `month_end=2`, `day_end=18`, `peak=2.8` |
| `sales`: ngày đôi 1/1–8/8 | `name=1-1`…`8-8`, `month=day=1`…`8`, `boost=4.0`, `after_days=0` |
| `sales`: ngày đôi 9/9–12/12 | `name=9-9`…`12-12`, `month=day=9`…`12`, `boost=4.8`, `after_days=0` |
| `sales`: Black Friday | `name=black_friday`, `month=11`, `day=null`, `weekday=4`, `week_index=4`, `boost=3.2`, `after_days=2` |

Ngày mua đầu được lấy theo **thứ × hệ số Tết × các hệ số chiến dịch đang áp dụng**. Hệ số Tết cao nhất giữa cửa sổ rồi giảm về hai đầu; chiến dịch trùng ngày có thể nhân chồng. Các lần mua sau dựa vào khoảng mua của nhóm và có thể kéo về chiến dịch.

`policies.timing.campaign_pull_days=7` tìm trong khoảng ±7 ngày; `policies.timing.minute_of_hour=[0,59]` chọn phút đồng đều trong giờ.

**Giới hạn:** Tết dùng 25/01–18/02 dương lịch cố định, chưa theo lịch âm từng năm. Black Friday dùng thứ Sáu thứ tư tháng 11, chưa bảo đảm đúng ngày thực tế mọi năm. Nghiên cứu Việt Nam cung cấp bối cảnh cho mùa vụ, chưa xác định các hệ số trên.

## 6. Coupon, phí ship và tổng tiền — chính sách shop

Chỉ sinh coupon thường và coupon chiến dịch, đều giảm phần trăm. Các khóa trong `distributions.coupons`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `usage_rate` | 0,25 | Xác suất dùng coupon thường khi đủ điều kiện |
| `campaign_usage_rate` | 0,45 | Xác suất dùng coupon chiến dịch khi đủ điều kiện |
| `percentage` | 10 | Phần trăm giảm coupon thường |
| `campaign_percentage` | 15 | Phần trăm giảm coupon chiến dịch |
| `minimum_subtotal_vnd` | 199.000 | Ngưỡng tiền hàng trước giảm giá |

Các khóa trong `policies.coupons`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `start_before_history_days` | 1 | Coupon thường bắt đầu trước đầu lịch sử |
| `valid_after_anchor_days` | 365 | Coupon thường hết hạn sau anchor |
| `minimum_total_usage_limit` | 100 | Sàn hạn mức tổng |
| `extra_total_usage_limit` | 100 | Cộng vào số đơn để tính hạn mức tổng |
| `ordinary_customer_usage_limit` | 10 | Lần dùng/khách/coupon thường |
| `campaign_customer_usage_limit` | 1 | Lần dùng/khách/coupon chiến dịch |

Hạn mức tổng mỗi coupon = `max(minimum_total_usage_limit, scale.orders + extra_total_usage_limit)`; preset là **3.100**. Coupon chiến dịch hiệu lực ngày chính và `after_days`.

Ưu tiên coupon chiến dịch đủ điều kiện. Nếu lần lấy mẫu không dùng thì không thử tiếp coupon thường. Hiệu lực xét tại thời điểm đặt, kể cả coupon hiện đã archive. Hủy/giao thất bại giải phóng redemption; released không chiếm hạn mức. Đổi/trả không tự giải phóng.

Các khóa tính phí trong `policies.shipping`: `fee_vnd=30000`, `free_from_subtotal_vnd=500000`.

```text
subtotal = tổng(đơn giá × quantity)
discount = subtotal × phần trăm giảm, chia nguyên 100
shipping = 0 nếu subtotal ≥ 500.000đ; ngược lại 30.000đ
total = subtotal − discount + shipping
```

Mức giảm bằng 0 thì không gắn coupon. Miễn ship xét subtotal trước giảm giá. AOV hình thành từ dòng hàng, không lấy mẫu riêng.

## 7. Vòng đời đơn, thanh toán và giao hàng — giả định demo

Các khóa trong `distributions.outcomes`:

| Cấu hình | Giá trị | Tập áp dụng |
|---|---|---|
| `cancellation_rate` | 0,08 | Đơn đủ ít nhất 2 giờ |
| `cod_rate` | 0,60 | Khách chưa bị khóa COD tại thời điểm đặt; còn lại VietQR |
| `cod_failure_rate` | 0,08 | Đơn COD chưa hủy, đủ ít nhất 3 ngày |
| `return_rate` | 0,06 | Đơn hoàn tất còn đủ thời gian xử lý đổi/trả |
| `exchange_rate` | 0,50 | Ca đổi/trả có variant thay thế; không có thì refund |

`cancellation_reasons` là trọng số trên các đơn đã chọn hủy:

| Lý do | Trọng số |
|---|---:|
| Đặt nhầm size hoặc màu | 28 |
| Muốn thay đổi địa chỉ nhận hàng | 18 |
| Tìm thấy lựa chọn phù hợp hơn | 18 |
| Áp nhầm mã giảm giá | 14 |
| Không còn nhu cầu mua | 22 |

Các khóa thời gian còn lại trong `policies.timing`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `cancellation_min_age_hours` | 2 | Tuổi đơn tối thiểu để xét hủy |
| `completion_min_age_days` | 5 | Đủ tuổi thì hoàn tất nếu không hủy/giao thất bại |
| `confirmation_min_age_hours` | 8 | Ngưỡng xét xác nhận VietQR chưa hoàn tất |
| `failure_min_age_days` | 3 | Tuổi đơn tối thiểu để xét giao thất bại |
| `confirmation_delay_hours` | `[2, 18]` | Xác nhận VietQR sau đặt; COD xác nhận khi đặt |
| `completion_delay_hours` | `[24, 96]` | Hoàn tất sau xác nhận |
| `cancellation_delay_minutes` | `[15, 480]` | Hủy sau đặt |

Các khóa giao nhận còn lại trong `policies.shipping`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `failed_attempts` | 3 | Số attempt của đơn giao thất bại |
| `attempt_interval_hours` | 6 | Khoảng giữa các lần thử, tính từ dispatch |
| `failure_after_days` | 2 | Thất bại sau xác nhận/đặt |
| `cod_block_after_failures` | 3 | Khóa COD sau số đơn giao thất bại đã xảy ra tại thời điểm đặt |

Các mốc được chặn để không vượt anchor. Đơn gần anchor có thể còn `paid`/`confirmed`; không dùng xác suất riêng để giữ đơn cũ chưa hoàn tất. Trạng thái được sinh gồm `paid`, `confirmed`, `completed`, `cancelled`, `failed_delivery`, `returned`.

- Mỗi đơn có một payment. VietQR thành công lúc đặt; hủy sau thanh toán có refund. COD chỉ thành công khi hoàn tất; COD chưa giao thành công giữ payment pending.
- Sinh shipment cho đơn hoàn tất hoặc giao thất bại. Thành công có một attempt; thất bại theo `failed_attempts`. Nhân viên phân công luân phiên.
- Chỉ đếm thất bại đã xảy ra để khóa COD, không dùng thất bại tương lai. Đơn sau khi bị khóa dùng VietQR.
- Có `order_status_history` với các mốc tương ứng. Kênh giao dịch hiện là online.

Các tỷ lệ có mẫu số khác nhau, không cộng thành một phân phối trạng thái. Chưa mô phỏng lỗi cổng thanh toán hay mọi trạng thái trung gian của API.

## 8. Đổi/trả và hoàn tiền — giả định demo

Các khóa trong `policies.returns`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `item_selection` | `first` | Chọn item đầu; hỗ trợ `random` |
| `replacement_selection` | `first` | Chọn variant thay thế đầu; hỗ trợ `random` |
| `quantity` | 1 | Chặn theo quantity đã mua |
| `request_delay_days` | 2 | Tạo yêu cầu sau hoàn tất |
| `processing_delay_days` | 1 | Xử lý sau yêu cầu |

Chỉ xét xác suất 6% khi đơn hoàn tất còn đủ **2 + 1 ngày** trước anchor. Mỗi yêu cầu chọn một item. Đổi hàng chọn variant khác cùng product; không có variant thay thế thì refund. Yêu cầu sinh ra được xử lý hoàn tất, kiểm tra hàng đạt.

- Nhập lại hàng trả; đổi hàng xuất thêm variant thay thế.
- Tiền hoàn = `round(đơn giá × (1 − discount/subtotal)) × quantity trả`, chặn theo payment; subtotal bằng 0 thì tiền hoàn bằng 0.
- Refund tiền chỉ gắn payment succeeded. Đổi hàng không hoàn tiền và chưa mô phỏng thanh toán chênh lệch giá.
- Hoàn toàn bộ hàng chuyển đơn thành `returned`; hoàn một phần/đổi hàng giữ `completed`.

Refund do hủy VietQR được sinh riêng, nên số refund không nhất thiết bằng số yêu cầu trả hàng.

## 9. Review — giả định demo

Các khóa trong `distributions.reviews`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `write_rate` | 0,35 | Xác suất thử viết trên từng item đã hoàn tất |
| `rating_weights` | `[2, 5, 13, 38, 42]` | Trọng số điểm 1–5 |
| `status_weights` | `approved: 94`, `rejected: 6` | Trọng số trạng thái kiểm duyệt |
| `delay_days` | `[1, 14]` | Số ngày sau hoàn tất |

Các khóa trong `policies.reviews`: `extra_delay_hours=[0,23]` cộng vào thời gian viết; `moderation_delay_hours=[1,48]` cho review bị từ chối, chặn tại anchor và gắn người kiểm duyệt.

Bỏ review nếu vượt anchor hoặc thời điểm viết từ lúc đơn đã hoàn trả toàn bộ. Nội dung chọn từ mẫu theo điểm. Điểm chưa phụ thuộc trải nghiệm giao nhận. Schema chưa có ngày hẹn giao để suy ra SLA; phần thiếu dữ liệu giữ null.

## 10. Wishlist và giỏ hàng — giả định

Các khóa trong `policies.wishlist`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `creation_rate` | 0,55 | Xác suất tạo wishlist/khách |
| `item_count` | `[1, 5]` | Số product, chặn theo catalog |
| `purchased_share` | 0,4 | Mục tiêu chủ động chọn từ lịch sử đơn chưa hủy |
| `minimum_purchased_items` | 1 | Tối thiểu khi có product phù hợp, chặn theo số item/tập khả dụng |
| `added_before_purchase_days` | `[1, 30]` | Thêm trước lần mua, chặn đầu lịch sử |
| `added_before_purchase_hours` | `[0, 23]` | Giờ cộng thêm vào khoảng trên |
| `readded_after_days` | `[0, 7]` | Thêm lại sau lần đầu, không vượt lần mua |
| `unpurchased_readded_after_days` | `[0, 29]` | Thêm lại product chưa mua, không vượt anchor |
| `removal_rate` | 0,18 | Xác suất xóa product chưa mua |
| `removed_after_days` | `[0, 14]` | Xóa sau lần thêm cuối, không vượt anchor |

Product gắn với giao dịch được đánh dấu bỏ khỏi wishlist tại lần mua đã lưu. Phần chọn ngẫu nhiên còn lại vẫn có thể trùng lịch sử mua nên **40% không phải tỷ lệ chuyển đổi đầu ra**. Wishlist là trạng thái/timestamp, chưa phải đầy đủ chuỗi sự kiện hành vi.

Các khóa trong `policies.carts`:

| Cấu hình | Giá trị | Cách áp dụng |
|---|---|---|
| `checkout_lead_minutes` | `[5, 10080]` | Giỏ checkout tạo trước đặt |
| `stale_rate` | 0,65 | Xác suất giỏ active cũ |
| `stale_before_anchor_days` | `[2, 20]` | Giỏ cũ cập nhật trước anchor |
| `recent_before_anchor_hours` | `[1, 12]` | Giỏ còn lại cập nhật trước anchor |
| `created_before_update_hours` | `[1, 72]` | Tạo giỏ trước cập nhật |
| `active_item_count` | `[1, 3]` | Số variant, chặn theo catalog |
| `active_quantity` | `[1, 3]` | Quantity mỗi variant |

Mỗi khách active có một giỏ active, ngoài các giỏ đã checkout theo đơn. Tỷ lệ giỏ cũ chưa phải tỷ lệ bỏ giỏ đã đo từ thực nghiệm.

## 11. Tồn kho và biến động kho — giả định đủ hàng

`policies.inventory.buffer_units=[80,200]` là tồn đệm ngẫu nhiên mỗi variant.

- Checkout xuất kho, kể cả đơn còn chờ xử lý.
- Hủy/giao thất bại nhập bù.
- Trả hàng nhập lại quantity trả; đổi hàng xuất variant thay thế.

```text
Tồn đầu = tổng lượng xuất trong lịch sử, gồm hàng đổi + tồn đệm
Tồn cuối = tồn đầu + tổng biến động kho
```

Cách này tính ngược tồn đầu để bảo đảm đủ hàng, chưa mô phỏng nhập bổ sung hay mất doanh thu do hết hàng. Giá vốn là giả định tại variant, chưa phải lịch sử nhập hàng/giá vốn bình quân được sinh từ thực nghiệm.

## 12. Địa điểm và phạm vi bảng

`locations.store_codes` là cấu hình tùy chọn:

| Thành phố | Mã cửa hàng |
|---|---|
| TP.HCM | `HCM-CENTRE`, `HCM-NORTHEAST` |
| Hà Nội | `HN-CENTRE` |
| Đà Nẵng | `DN-CENTRE` |
| Cần Thơ | `CT-CENTRE` |
| Hải Phòng | `HP-CENTRE` |

Không khai báo thì chọn cả 6 cửa hàng/5 thành phố. Dùng `locations: {store_codes: []}` để không chọn; chỉ sinh thành phố có cửa hàng được chọn. Mã được chuẩn hóa; mã không biết hoặc trùng bị từ chối. Dữ liệu địa điểm có sẵn được tái sử dụng theo mã.

Generator hỗ trợ **25/28 bảng nguồn**. Chưa sinh `store_inventory`, `inbound_receipts`, `inbound_receipt_items`. Chọn địa điểm chưa tự phân bổ khách/đơn theo vùng, giao dịch POS hoặc tồn cửa hàng. Phần sinh log đã bỏ; access log từ API là luồng riêng.

## 13. Tái lập, xuất SQL và ranh giới cấu hình

- Seed, toàn bộ cấu hình và phiên bản tham gia dataset identity. UUIDv5/`generation_run_id` nhận diện bộ dữ liệu và bản ghi; không phải checkpoint pipeline. Namespace là `dk-generator-v2`.
- Cùng cấu hình/seed/phiên bản tái lập được dữ liệu và SQL; `generated_at` của manifest vẫn thay đổi. ID địa điểm được giải quyết theo mã trong database đích.
- SQL dùng PostgreSQL, tiền nguyên VND, timestamp UTC. INSERT tối đa 1.000 dòng/lệnh; buffer xả sau 1.000 đơn. Đây là kích thước ghi SQL, không phải chunk extract.
- File SQL ghi tạm rồi thay thế khi hoàn tất. Import có transaction, khóa bảng, kiểm tra quan hệ/tiền/refund/kho và cập nhật sequence. Xuất và import là hai bước riêng; importer từ chối dataset đã nạp hoặc xung đột khóa, không tự reset/ghi đè.
- Validation yêu cầu khách/product ≥1; đơn ≥0; product ≤ variant ≤ product ×60; trọng số hữu hạn không âm và tổng dương; tỷ lệ hợp lệ; khoảng thời gian hợp lệ. Số lần giao từ 1–3 và phải khớp độ trễ; quantity trả/tiền hoàn bị giới hạn theo dữ liệu đã mua/thanh toán.
- Các khóa `policies` bắt buộc đọc từ YAML, không có giá trị dự phòng trong Python. Console giữ chúng trong cấu hình đóng băng; trình sửa nâng cao hiện chỉ sửa `distributions`, nên sửa `policies` tại preset.
- Danh sách tên, mẫu sản phẩm/review, size/màu, trạng thái schema, ID, SQL, băm mật khẩu và ràng buộc nhất quán vẫn thuộc code. Không phải mọi chi tiết kỹ thuật đều là cấu hình kịch bản.

Căn cứ trực tiếp đang áp dụng là **nhu cầu theo thứ hạng H&M và giỏ/quantity Olist**. Các tỷ lệ còn lại là giả định/chính sách demo. Chưa có phép đo chứng minh mức tương đồng toàn thị trường Việt Nam hoặc tỷ lệ cải thiện generator. Cách xuất/nạp xem [README generator](../../generator/README.md); hồ sơ bối cảnh Việt Nam xem [nghiên cứu Việt Nam](GENERATOR_VIETNAM_RESEARCH.md).
