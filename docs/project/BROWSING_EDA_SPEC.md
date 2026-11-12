# Dự án A — EDA hành vi duyệt web cho log tổng hợp

**Ngày:** 2026-11-12 · **Trạng thái:** chờ duyệt spec

## 1. Bối cảnh

Hệ thống log truy cập hiện có hợp đồng `ecommerce.access:1.0.0` (trường `data_origin` cho phép `synthetic`), luồng Kafka → Flink → Landing → Bronze → Silver → Gold và 3 mart: chỉ số route theo giờ, nhu cầu sản phẩm theo ngày, phễu marketing theo ngày. Bộ sinh log tổng hợp cũ đã bị xóa (commit `6284896`). Dữ liệu OLTP được sinh theo quy luật trong `generator/notebooks/reference_datasets_eda.ipynb` và các preset `small/10m/50m.yml`.

Log tổng hợp mới được chia thành ba dự án con:

| # | Dự án con | Phụ thuộc |
|---|---|---|
| **A** | EDA hành vi duyệt web → quy luật và `access_logs.yml` (**spec này**) | Không phụ thuộc |
| B | Bộ sinh log lịch sử: đọc OLTP từ PostgreSQL, dựng hành trình quanh từng đơn trong N ngày cuối, ghi JSONL và đẩy lại vào Kafka | A, module OLTP |
| C | Chế độ phát liên tục: mở rộng "Mô phỏng API", khách ảo duyệt web và mua qua FastAPI thật | A |

### Quyết định đã chốt

1. Log **khớp với OLTP**: mỗi đơn có hành trình log tương ứng (cùng khách, sản phẩm, thời điểm), kèm các phiên không mua.
2. Hai chế độ: backfill lịch sử vào Kafka (giữ timestamp gốc) và phát liên tục.
3. Chế độ phát liên tục **gọi API thật**; middleware tự sinh log, API tự tạo đơn.
4. Hành vi duyệt web lấy căn cứ từ **EDA trên eCommerce Behavior**, trình bày gọn như notebook OLTP.
5. Log lịch sử chỉ phủ **N ngày cuối** của lịch sử OLTP (`window_days`).
6. Dự án B đọc OLTP **từ PostgreSQL sau khi nạp**, là package riêng.

### Giả định

- Giữ nguyên hợp đồng và pipeline log hiện có.
- Log mô phỏng request HTTP tới FastAPI, không phải click trên trình duyệt.
- Hợp đồng không có session ID. Phiên chỉ là khái niệm bên trong bộ sinh; log chỉ mang `actor.key`.

## 2. Mục tiêu và tiêu chí thành công

**Mục tiêu:** đo các quy luật để B và C dựng được (1) hành trình quanh mỗi đơn OLTP và (2) lưu lượng duyệt web không mua, đủ để 3 mart log cho số hợp lý.

**Tiêu chí thành công của dự án A:**
- Notebook chạy lại từ đầu bằng `nbconvert` không lỗi; mọi con số trong nhận xét khớp với output.
- Mỗi quy luật L1–L6 có biểu đồ, một câu quy luật và bảng kiểm tra "mô phỏng so với thực tế" tính bằng điểm %.
- `access_logs.yml` đọc được bằng PyYAML; phân phối tổng bằng 1; tỷ lệ nằm trong [0, 1]; mỗi khối ghi `source`.

## 3. Dữ liệu và cohort

- **Nguồn:** `data/generator/eda/raw/ecommerce_behavior/2019-Oct.csv` (khoảng 42,4 triệu sự kiện, 9,24 triệu phiên). Chỉ dùng tháng 10; tháng 11 nằm ngoài phạm vi.
- **Sự kiện:** chỉ có `view`, `cart`, `purchase`. Một phiên là một `user_session`.
- **Biên:** loại các phiên có sự kiện ngày 1/10 hoặc 1/11 (giờ UTC), để không cắt phiên.
- **Hai nhóm so sánh:** toàn bộ dữ liệu, và nhóm `apparel` (`category_code` bắt đầu bằng `apparel`; khoảng 381 nghìn phiên, 7.233 phiên có mua). Quy luật giống nhau giữa hai nhóm thì coi là chuyển được; khác nhau thì ưu tiên số của `apparel`, trừ L3 (xem §3.1).
- **Đọc dữ liệu:** Polars streaming trực tiếp từ CSV gốc, không dùng file trung gian.

### 3.1. Sự kiện giỏ hàng bị ghi nhận thiếu

Mọi lượt mua đều phải đi qua giỏ, nhưng tỷ lệ lượt mua (cặp phiên × sản phẩm) có sự kiện `cart` tương ứng chỉ là:

- Theo nhóm hàng: điện tử khoảng 60%, đồ gia dụng khoảng 38%, `apparel` khoảng 0,1% (26 sự kiện giỏ so với 8.002 lượt mua).
- Theo ngày: nhảy theo từng khối, khoảng 33% (1–3/10, 7–13/10, 28–31/10) và khoảng 53% (4–6/10, 14–27/10).

Đây là dấu hiệu thay đổi trong hệ thống thu thập, không phải hành vi khách. Hệ quả: L3 không đo trực tiếp được và phải hiệu chỉnh (§4, L3); nhóm `apparel` không dùng được cho L3.

## 4. Quy luật cần đo

| Mã | Cách đo | Quy luật dự kiến | Kiểm tra |
|---|---|---|---|
| **L1 Lưu lượng** | Số phiên không mua ÷ số phiên mua, theo từng ngày | Phiên không mua = r × phiên mua, r gần như không đổi giữa các ngày | Độ dao động của r theo ngày; so sánh toàn bộ với `apparel` |
| **L2 Độ dài phiên** | Phân phối số lượt `view` mỗi phiên, tách phiên mua và phiên không mua | Mỗi loại phiên theo một phân phối 1 tham số (hình học hoặc log-series, chọn cái lệch ít hơn) | Bảng % theo 1 / 2 / 3–4 / 5–9 / 10+ lượt xem: thực tế so với mô hình |
| **L3 Phễu (có hiệu chỉnh)** | (1) Tỷ lệ ghi nhận giỏ `q` = % lượt mua có sự kiện `cart`. (2) `purchase_given_cart` = % giỏ *được ghi nhận* mà sản phẩm đó được mua trong cùng phiên; tỷ lệ này không bị ảnh hưởng nếu việc ghi thiếu xảy ra ngẫu nhiên. (3) Tỷ lệ phiên không mua có thêm giỏ = tỷ lệ quan sát được ÷ `q` | Hai hằng số: `purchase_given_cart` và `non_buying_cart_rate` | Tính riêng trên khối ngày khoảng 33% và khối khoảng 53%. **Nếu hai khối lệch nhau quá 5 điểm % ở bất kỳ tham số nào, L3 chuyển sang `assumption`** và ghi lý do |
| **L4 Nhịp thời gian** | Số giây giữa hai sự kiện liên tiếp trong phiên | Log-normal (trung vị, σ) | Bảng % theo khoảng < 10 giây / 10–60 giây / 1–5 phút / > 5 phút |
| **L5 Sản phẩm được xem** | (a) Hồi quy log(lượt xem) theo log(lượt mua) của từng sản phẩm có ít nhất 1 lượt mua. (b) Trong phiên mua, tỷ lệ lượt xem là sản phẩm đã mua | (a) Lượt xem ∝ lượt mua^b. (b) Một tỷ lệ cố định | (a) Tỷ trọng lượt xem của top 10% sản phẩm: mô phỏng so với thực tế |
| **L6 Hành trình nhiều phiên** | Với mỗi phiên mua: số phiên trước đó của cùng `user_id` có xem chính sản phẩm đã mua, và số ngày từ lần xem đầu tới lúc mua | Số phiên trước theo một phân phối 1 tham số; số ngày theo phân phối mũ | Bảng % số phiên trước (0 / 1 / 2 / 3+) và trung vị số ngày: thực tế so với mô hình |

**Giới hạn đo của L6:** tháng 10 chỉ có 31 ngày quan sát, nên hành trình bắt đầu từ tháng 9 bị cắt. Chỉ đo các lượt mua từ ngày 15/10 trở đi, nhìn lại tối đa 14 ngày. Đây là giả định đo lường và được ghi trong notebook.

## 5. Giả định (không có trong dữ liệu)

Ghi trong mục §7 của notebook và khối `assumptions` của YAML, với `source: assumption`:

- Request `category_list` / `catalog_search` trước lượt xem đầu tiên của phiên; `cart_read` và `checkout_quote` trước `checkout_submit`.
- Phiên mua cần đăng nhập; tỷ lệ phiên không mua ở trạng thái ẩn danh là tham số kịch bản.
- Lưu lượng ngày sale tăng cùng hệ số với số đơn OLTP (eCommerce Behavior không có hiệu ứng Black Friday để đo).
- Giờ của phiên lấy theo phân bố giờ đặt hàng của OLTP (không đo lại).

## 6. Đầu ra

### 6.1. Notebook `generator/notebooks/browsing_behavior_eda.ipynb`

- §0 Dữ liệu và cohort (`describe()`, `null_count()`, cách loại phiên chạm biên, định nghĩa `apparel`).
- §0.1 Giỏ hàng bị ghi nhận thiếu (bảng theo nhóm hàng và theo ngày).
- §1–§6: L1–L6.
- §7 Giả định.
- §8 Tổng hợp: bảng quy luật, mức chuyển nguồn, ô in tham số theo khóa YAML.

### 6.2. `generator/configs/access_logs.yml`

Không phụ thuộc preset quy mô; B và C cùng đọc.

```yaml
schema_version: 1
window_days: 30                        # assumption
sessions:
  non_buying_per_buying: {value: …, source: transfer}              # L1
  views_per_session:                                                # L2
    buying: {model: …, …, source: transfer}
    non_buying: {model: …, …, source: transfer}
funnel:                                                             # L3
  purchase_given_cart: …
  non_buying_cart_rate: …
  source: transfer | assumption
timing:
  event_gap_seconds: {model: lognormal, median: …, sigma: …, source: transfer}   # L4
products:                                                           # L5
  views_vs_purchases_exponent: …
  purchased_product_view_share: …
  source: transfer
journey:                                                            # L6
  prior_sessions: {model: …, …}
  days_first_view_to_purchase: {model: exponential, mean: …}
  source: transfer
assumptions:                                                        # §5
  anonymous_non_buying_share: …
  request_steps: …
  sale_day_traffic_follows_orders: true
  source: assumption
```

Mọi tham số đo được mang nhãn `transfer`, vì dữ liệu đến từ một cửa hàng đa ngành ở Nga. Ghi chú bên cạnh cho biết quy luật có giống nhau giữa toàn bộ dữ liệu và `apparel` không.

## 7. Ngoài phạm vi

Code bộ sinh log (B), mở rộng mô phỏng API (C), thay đổi pipeline hoặc hợp đồng log, dữ liệu tháng 11/2019, và tài liệu README/`docs/` hiện có.
