# Baseline trước chuyển đổi Generator Console và BI

Ghi nhận ngày **05/11/2026**, cho đợt 1 của [refactor_v2](refactor_v2.md). Đây là quan sát môi trường local để đối chiếu chuyển đổi; không phải dataset tham chiếu thực tế, kết quả hiệu chỉnh generator hoặc chứng nhận chất lượng toàn hệ thống.

## 1. Phạm vi và cách đo

- Source checkout: `f9ca33fbf7ea364ecfd5f539a98300850c233446`; nhánh `feature/data-platform-transition-scope` khi bắt đầu ghi nhận.
- MySQL: `2026-11-05 09:24:27.491464 UTC` (16:24:27 Asia/Ho_Chi_Minh), một transaction `REPEATABLE READ`, `READ ONLY`, `WITH CONSISTENT SNAPSHOT`; kết thúc bằng rollback.
- Trino: `2026-11-05T09:25:42.412280+00:00` → `2026-11-05T09:25:45.261257+00:00`. Dùng `PublishedReader` chọn publication hoàn tất và pin từng bảng bằng `FOR VERSION AS OF`; không đọc đầu bảng đang thay đổi.
- Tổng kho/HTTP bổ sung: `2026-11-05T09:27:16.025460+00:00` → `2026-11-05T09:27:16.310730+00:00`, dùng lại snapshot map đã lưu, không chọn publication mới.
- Có 17 dịch vụ Compose đang chạy. Không khởi động/rebuild dịch vụ, chạy DAG, import, backfill hoặc traffic để tạo baseline. OpenAPI được đọc từ ứng dụng trong container, không gửi request nghiệp vụ.
- 86 cặp method/path khớp giữa router source và OpenAPI image API đang chạy. Điều này không xác minh toàn bộ code trong image trùng checkout.
- Chỉ lưu số tổng hợp và metadata tại đây; không lưu credential, dữ liệu cá nhân hoặc bản dump database trong Git. JSON và script capture gốc nằm trong cache cục bộ ngoài repo.

Hash SHA-256 của DDL vật lý (nối `SHOW CREATE TABLE` theo tên bảng đã sắp xếp):

`715fe68e620b4fb5980603bf4e760c91315af40419c2d59ff805a2ff2acec265`

## 2. Publication được sử dụng

Các thời điểm trong phần này là UTC. Thời gian publication không phải thời gian sự kiện nghiệp vụ mới nhất.

| Thuộc tính | OLTP | Access log |
|---|---|---|
| Run ID | `scheduled__2026-11-03T20:00:00+00:00` | `logs-scheduled__2026-11-05T08:00:00+00:00` |
| Published at | `2026-11-05 06:02:00.247 UTC` | `2026-11-05 09:04:11.880 UTC` |
| Source event at | NULL | `2026-11-05 08:59:50.855 UTC` |
| Schema version | `3` | `1` |
| Source mode | `batch` | `logs_rollup` |

Log observed_at: `2026-11-05 09:00:00.055 UTC`. OLTP chứa 54 entry snapshot: 27 Silver và 27 Gold. Hai entry Silver không có snapshot được reader diễn giải là tập rỗng, không lấy dữ liệu hiện tại thay thế.

## 3. Số dòng nguồn và Silver đã công bố

Bảng Silver có tên `lakehouse.silver.silver_<bảng nguồn>`. Cột snapshot ghi đúng publication ở phần 2.

| Bảng nguồn | MySQL | Silver | Snapshot Silver |
|---|---:|---:|---|
| cart_items | 1 | 1 | 7251793109734817332 |
| carts | 1 | 1 | 3468077179543570795 |
| categories | 3 | 3 | 9167938941898304920 |
| cities | 3 | 3 | 7729183271340250559 |
| coupon_redemptions | 7 | 7 | 8740480746088998381 |
| coupons | 3 | 3 | 2487803529578506760 |
| customers | 14 | 14 | 7889431581865413945 |
| delivery_staff | 1 | 1 | 3756162889366718704 |
| inbound_receipt_items | 12 | 12 | 8401130582223629553 |
| inbound_receipts | 3 | 3 | 8165832491468178454 |
| inventory | 30 | 30 | 2953034428069995657 |
| inventory_transactions | 46 | 46 | 6040371610907084268 |
| order_items | 48 | 48 | 8740285537807652546 |
| order_status_history | 2 | 2 | 8142723153582006491 |
| orders | 34 | 34 | 459707355250945563 |
| payments | 34 | 34 | 3805237735710611781 |
| product_reviews | 13 | 13 | 2653276288723320541 |
| product_variants | 30 | 30 | 2412213552017236740 |
| products | 13 | 13 | 8907603048414057561 |
| refunds | 1 | 1 | 2950389399551067949 |
| return_items | 2 | 2 | 4721404998578461874 |
| return_requests | 2 | 2 | 1278051194796399344 |
| shipment_attempts | 0 | 0 | NULL — rỗng |
| shipments | 26 | 26 | 2444442525032917519 |
| store_inventory | 11 | 11 | 6670905441874345356 |
| stores | 2 | 2 | 3694864024592962887 |
| wishlist_items | 0 | 0 | NULL — rỗng |

**27/27 bảng khớp số dòng** tại các mốc quan sát này. `customer_credentials` có 14 dòng, chỉ dùng auth và bị loại khỏi batch/CDC: tổng cộng 28 bảng ứng dụng. Môi trường còn `alembic_version` legacy có 1 dòng, ngoài schema ứng dụng/capture phân tích; đợt này không xóa hoặc chuyển đổi bảng đó.

## 4. Đơn hàng và tài chính

Tất cả 34 đơn mang `data_origin=manual`, `generation_run_id=NULL`. Nhãn này không chứng minh giao dịch thực. Có 14 bản ghi customers, gồm 6 role customer và 8 tài khoản nhân sự/quản trị; không xem toàn bộ 14 là người mua.

Đơn được tạo từ `2026-10-09 08:46:12.480607` đến `2026-11-03 06:40:48.097295` UTC; mốc cập nhật cuối `2026-11-04 15:57:58.489040` UTC.

| Kênh / trạng thái nguồn | Số đơn | Tổng giá trị đơn (VND) |
|---|---:|---:|
| online / delivered | 17 | 14.820.100 |
| online / failed_delivery | 3 | 2.204.000 |
| online / paid | 2 | 1.387.000 |
| online / returned | 1 | 1.650.000 |
| online / shipping | 5 | 340.350.000 |
| pos / completed | 6 | 5.095.000 |

Các tổng sau lấy từ `mart_order_daily` đã pin, không cộng order count qua mart có grain category. Định nghĩa đầy đủ ở [Metrics Catalog](../architecture/METRICS_CATALOG.md).

| Chỉ số | Giá trị |
|---|---:|
| Tổng đơn | 34 |
| Tổng giá trị đơn, mọi trạng thái (VND) | 365.506.100 |
| Đơn thuộc trạng thái ghi nhận doanh thu | 24 |
| Doanh thu hàng hóa trước hoàn (VND) | 21.025.100 |
| Hoàn tiền được khấu trừ trong mart (VND) | 1.620.000 |
| Doanh thu thuần (VND) | 19.405.100 |
| Giá vốn sau điều chỉnh trả hàng (VND) | 8.176.985 |
| Lợi nhuận gộp (VND) | 11.228.115 |

Tổng giá trị đơn **365.506.100 VND** gồm mọi trạng thái và phí vận chuyển; không phải doanh thu thuần **19.405.100 VND**. Đơn được ghi nhận có trạng thái delivered/completed/returned. Lợi nhuận gộp chưa trừ chi phí vận hành.

| Kênh | Số đơn | Tổng giá trị đơn (VND) | Doanh thu thuần (VND) |
|---|---:|---:|---:|
| online | 28 | 360.411.100 | 14.310.100 |
| pos | 6 | 5.095.000 | 5.095.000 |

Nguồn payments có 26 succeeded (22.952.100 VND), 5 pending (340.350.000 VND), 3 failed (2.204.000 VND); refunds có 1 succeeded (1.620.000 VND). Tổng payment không đồng nghĩa doanh thu do khác trạng thái/phí/thời điểm ghi nhận. Có 2 yêu cầu trả hàng: 1 approved và 1 completed, đều action refund.

## 5. Kho và HTTP

| Chỉ số | Giá trị |
|---|---:|
| Kho trung tâm: dòng số dư / tổng đơn vị | 30 / 2.249 |
| Kho cửa hàng: dòng số dư / tổng đơn vị | 11 / 865 |
| Giá trị tồn theo mart (VND) | 304.689.765 |
| SKU hết hàng theo mart | 0 |
| Phiếu nhập theo mart | 3 |
| Mốc quan sát kho | 2026-11-05 01:38:16.519 UTC |

Có 46 giao dịch kho: 12 inbound (+915 đơn vị), 25 outbound_order (−41), 9 outbound_pos (−15). Đây là tổng quan sát; chưa đối soát đầy đủ ledger từ số dư đầu kỳ. `fact_inventory_daily_snapshot` chứa các lần quan sát, không bảo đảm tái dựng được mọi tồn kho quá khứ.

HTTP tổng hợp toàn bộ snapshot `mart_hourly_route_metrics` đã chọn, metric_date từ **22/10/2026 đến 05/11/2026 UTC**; ngày cuối chưa mặc định đủ 24 giờ:

| Chỉ số HTTP | Số request |
|---|---:|
| Tổng request | 3.688 |
| Success 2xx/3xx | 2.693 |
| 4xx | 995 |
| 5xx | 0 |

Tên cột legacy `success_2xx_count` hiện bao gồm 2xx/3xx. Không cộng/đếm các request thành người, session hoặc đơn distinct. Không lấy trung bình percentile từng giờ/route để suy p95 toàn cục. Không suy nguyên nhân 4xx hoặc sức khỏe pipeline chỉ từ tổng này.

## 6. Số dòng Gold và snapshot

Tên bảng trong cột đầu thuộc `lakehouse.gold`. Đây là số dòng theo grain từng bảng, không phải các tổng có thể cộng lẫn nhau.

| Bảng | Dòng | Snapshot |
|---|---:|---|
| dim_customer | 14 | 6112727618587170538 |
| dim_date | 1095 | 5917436445513175768 |
| dim_delivery_staff | 1 | 5372279205850675567 |
| dim_product | 13 | 8473348831958323457 |
| dim_store | 2 | 7644313032808537750 |
| dim_variant | 30 | 8893542141792814477 |
| fact_inventory_current | 41 | 7095953115161379136 |
| fact_inventory_daily_snapshot | 492 | 5829181875885606530 |
| fact_order | 34 | 208165037922333194 |
| fact_order_item | 48 | 3427726227913068856 |
| fact_return_exchange | 2 | 6231082585726858450 |
| fact_shipment | 26 | 4815481059118694371 |
| mart_admin_overview | 1 | 921494728263135925 |
| mart_cod_region_daily | 10 | 8579802735707303538 |
| mart_coupon_usage | 3 | 4903737389425085610 |
| mart_inventory_current | 41 | 842561490802253779 |
| mart_inventory_health | 72 | 1626943468825436041 |
| mart_inventory_summary | 1 | 4449746990544933871 |
| mart_logistics_performance | 12 | 6157912417660152656 |
| mart_marketing_summary | 1 | 5747329850756001037 |
| mart_operations_summary | 1 | 911081939517466315 |
| mart_order_daily | 18 | 6383592560377095773 |
| mart_product_returns | 2 | 3199226624914527287 |
| mart_product_review_summary | 5 | 6268020787960313381 |
| mart_product_sales_daily | 27 | 5692280977852442718 |
| mart_sales_daily | 18 | 8931632716661036983 |
| mart_store_overview | 2 | 6567502886099290878 |
| mart_daily_product_demand | 2 | 4080833320340462573 |
| mart_hourly_route_metrics | 231 | 7725544213000117948 |
| mart_marketing_funnel_daily | 9 | 7031929310601059372 |

## 7. Cách đối chiếu ở các đợt sau

Giữ cùng bộ lọc, grain, tiền tệ và định nghĩa; chọn một publication hoàn tất cho mỗi nguồn, lưu snapshot map rồi dùng lại map cho tất cả truy vấn. Đọc MySQL trong một transaction read-only nhất quán; ghi riêng mốc nguồn và mốc publication. Ví dụ Trino tái đọc tổng của baseline này, khi snapshot còn tồn tại:

```sql
SELECT SUM(total_orders) AS total_orders,
       SUM(order_total_vnd) AS order_total_vnd,
       SUM(net_revenue_vnd) AS net_revenue_vnd
FROM lakehouse.gold.mart_order_daily
FOR VERSION AS OF 6383592560377095773;
```

Khi thay UI/serving mà không đổi dữ liệu, tổng phải khớp trên cùng snapshot. Khi sửa generator hoặc sinh thêm dữ liệu, phải ghi dataset/config/seed/profile và phần chênh lệch dự kiến; không yêu cầu số lượng mới luôn bằng baseline cũ. Chỉ đánh giá scenario sau khi kiểm tra postcondition tiền/tồn/trạng thái và đầu ra pipeline tương ứng.

MySQL và Iceberg được quan sát ở các thời điểm khác nhau, không có một transaction chung xuyên hai hệ thống. Khớp row count và tổng tiền không chứng minh từng dòng/quan hệ/logic đều đúng, không đo độ trễ CDC và không bảo đảm SLA. Maintenance có thể hết hạn snapshot; ghi ID không giữ snapshot vĩnh viễn.

Hash SHA-256 của các kết quả capture gốc (không đưa nội dung thô vào Git):

| Capture | SHA-256 |
|---|---|
| mysql.json | `b7fb4b85c37f240ed09f73d9a1be472d9f8a62c549a437e7cb98cdabf2139007` |
| trino.json | `a11e19eb8558bea9bd85376e0685589f1a4dcbc7720b9412c42ed37d04401307` |
| pinned_totals.json | `4b386c07f85c1d9a6c34131c5be3a9291167b0b99e827202a3354e395681d4d6` |
