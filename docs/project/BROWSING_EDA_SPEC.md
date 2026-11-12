# Dự án A — EDA hành vi duyệt web cho log tổng hợp

**Ngày:** 2026-11-12 · **Trạng thái:** đã triển khai, rút gọn còn 3 quy luật đo (12/11/2026)

## 1. Bối cảnh

Hệ thống log truy cập có hợp đồng `ecommerce.access:1.0.0` (trường `data_origin` cho phép `synthetic`), luồng Kafka → Flink → Landing → Bronze → Silver → Gold và 3 mart: chỉ số route theo giờ, nhu cầu sản phẩm theo ngày, phễu marketing theo ngày. Các mart chỉ **đếm request**, không theo dõi phiên hay khách quay lại. Dữ liệu OLTP theo quy luật trong `generator/oltp/notebooks/purchase_behavior_eda.ipynb` và các preset `generator/oltp/configs/{1m,10m,50m}.yml`.

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
7. Chỉ đo những quy luật làm thay đổi số liệu trong mart log; phần còn lại là giả định.

### Giả định

- Giữ nguyên hợp đồng và pipeline log hiện có.
- Log mô phỏng request HTTP tới FastAPI, không phải click trên trình duyệt.
- Hợp đồng không có session ID. Phiên chỉ là khái niệm bên trong bộ sinh; log chỉ mang `actor.key`.

## 2. Mục tiêu và tiêu chí thành công

**Mục tiêu:** đo các quy luật để B và C dựng được lưu lượng duyệt web quanh mỗi đơn OLTP và lưu lượng không mua, đủ để 3 mart log cho số hợp lý.

**Tiêu chí thành công:**
- Notebook chạy lại từ đầu bằng `nbconvert` không lỗi; mọi con số trong nhận xét khớp với output.
- Mỗi quy luật đo có biểu đồ, một câu quy luật và bảng kiểm tra "mô hình so với thực tế" tính bằng điểm %.
- Ô cuối notebook khẳng định `access_logs.yml` khớp với số đo và giả định; mỗi khối YAML ghi `source`.

## 3. Dữ liệu và cohort

- **Nguồn:** `data/generator/eda/raw/ecommerce_behavior/2019-Oct.csv`; sự kiện ngày 1/11 trong `2019-Nov.csv` chỉ được đọc để nhận ra phiên vắt qua nửa đêm 31/10.
- **Sự kiện:** chỉ có `view`, `cart`, `purchase`. Một phiên là một `user_session`.
- **Biên:** loại các phiên có sự kiện ngày 1/10 hoặc 1/11 (giờ UTC). Cohort còn 2–31/10 (30 ngày), 8.974.352 phiên, 613.292 phiên có mua.
- **Hai nhóm so sánh:** toàn bộ dữ liệu, và nhóm `apparel` (mọi sự kiện có `category_code` đều thuộc `apparel`; 303.167 phiên, 7.152 phiên có mua). Dạng quy luật chọn trên toàn bộ dữ liệu; tham số đưa vào YAML lấy theo `apparel`.

**Giỏ hàng bị ghi nhận thiếu:** chỉ 32–53% lượt mua có sự kiện `cart` đi kèm, nhảy theo khối ngày (do hệ thống thu thập, không phải hành vi khách); `apparel` gần như không có sự kiện giỏ. Phép hiệu chỉnh cho kết quả lệch 7,1 điểm % giữa hai khối ngày, nên phễu giỏ hàng là giả định.

## 4. Quy luật đo

| Mã | Cách đo | Quy luật | Kết quả (`apparel`) |
|---|---|---|---|
| **L1 Lưu lượng** | Số phiên không mua ÷ số phiên mua, theo từng ngày; độ nhạy theo 3 định nghĩa `apparel` | Phiên không mua = r × phiên mua, r không đổi theo ngày | r = 41,39 (khoảng hợp lý 31,3–51,6) |
| **L2 Lượt xem mỗi phiên** | Phân phối số `view` mỗi phiên, tách phiên mua và không mua; chọn giữa hình học và log-series | Có mua: hình học; không mua: log-series | p = 0,1044 (lệch 6,4 điểm %); θ = 0,9239 (lệch 1,3 điểm %) |
| **L3 Nhịp thời gian** | Số giây giữa hai sự kiện liên tiếp trong phiên | Log-normal | Trung vị 36,2 giây, σ = 0,938 (lệch 4,4 điểm %) |

**Đã bỏ khỏi phạm vi đo:**
- **Hành trình nhiều phiên** (khách xem trước rồi quay lại mua): log không có session ID và không mart nào theo dõi khách quay lại.
- **Phễu giỏ hàng:** chuyển thành giả định vì ghi nhận thiếu không hiệu chỉnh ổn định.
- **Sản phẩm được xem:** số mũ đo được ≈ 1,03, nên giả định lượt xem tỷ lệ thuận với lượt mua.

## 5. Giả định (không đo)

Ghi ở mục 4 của notebook và các khối `source: assumption` của YAML:

- Phễu: 7,5% phiên không mua thêm 1 sản phẩm vào giỏ rồi bỏ; mỗi phiên có mua bỏ lại trung bình 0,09 sản phẩm trong giỏ (làm tròn từ phép đo gộp cũ).
- Lượt xem của sản phẩm tỷ lệ thuận với lượt mua OLTP; trong phiên có mua, 32% lượt xem là sản phẩm được mua.
- Request `category_list` / `catalog_search` trước lượt xem đầu tiên; `cart_read` và `checkout_quote` trước `checkout_submit`.
- Phiên mua cần đăng nhập; 60% phiên không mua ở trạng thái ẩn danh.
- Lưu lượng ngày sale tăng cùng hệ số với số đơn OLTP; giờ của phiên theo phân bố giờ đặt hàng OLTP.

## 6. Đầu ra

### 6.1. Notebook `generator/logs/notebooks/browsing_behavior_eda.ipynb`

- §0 Dữ liệu và cohort.
- §1–§3: L1–L3.
- §4 Giả định.
- §5 Tổng hợp, ô in tham số theo khóa YAML và ô kiểm tra YAML.

### 6.2. `generator/logs/configs/access_logs.yml`

Không phụ thuộc preset quy mô; B và C cùng đọc.

```yaml
schema_version: 1
window_days: 30
sessions:     {source: transfer, non_buying_per_buying, views_per_session: {buying, non_buying}}  # L1, L2
timing:       {source: transfer, event_gap_seconds: {model: lognormal, median, sigma}}           # L3
funnel:       {source: assumption, non_buying_cart_rate, abandoned_items_per_buying_session}
products:     {source: assumption, views_proportional_to_purchases, purchased_product_view_share}
assumptions:  {source: assumption, anonymous_non_buying_share, request_steps, sale_day_traffic_follows_orders, session_hour_follows_order_hour}
```

Tham số đo được mang nhãn `transfer`, vì dữ liệu đến từ một cửa hàng đa ngành ở Nga và `apparel` chỉ là ngành phụ của cửa hàng đó.

## 7. Ngoài phạm vi

Code bộ sinh log (B), mở rộng mô phỏng API (C), thay đổi pipeline hoặc hợp đồng log.
