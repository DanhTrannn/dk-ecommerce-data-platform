# Phạm vi đồ án

## 1. Mục tiêu

Xây dựng Data Lakehouse cho thương mại điện tử: thu thập, lưu trữ, chuẩn hóa, đối soát và cung cấp dữ liệu phân tích. Website/POS tạo nguồn giao dịch và access log; phần DE là trọng tâm.

## 2. Nguồn dữ liệu

| Nguồn | Nội dung và biên giới |
|---|---|
| MySQL | 28 bảng ứng dụng; batch/CDC lấy 27 bảng, loại `customer_credentials` |
| Access log FastAPI | HTTP events theo JSON contract; không ghi password/token/cookie/raw IP |
| Generator | Dữ liệu giao dịch và log tổng hợp theo cấu hình |
| Website/API | Giao dịch và access log phát sinh khi sử dụng hệ thống |

Dữ liệu tổng hợp cần được ghi nhận là dữ liệu tổng hợp khi trình bày kết quả. Số dòng và tốc độ xử lý phải lấy từ lần chạy thực tế.

## 3. Phần đã có trong mã nguồn

- Batch MySQL → Landing → Bronze → Silver → Gold bằng Spark/Airflow.
- Delta theo time interval Airflow, staging ghi mới, Bronze MERGE theo PK/thời gian thay đổi để giữ lịch sử không trùng và Silver MERGE theo PK.
- Chuẩn hóa, khử trùng, MERGE, pseudonymization và quarantine.
- Đối soát MySQL/Silver và kiểm tra Gold; lưu quality results.
- Binlog → Debezium → Kafka → Flink → Bronze CDC.
- Access log → Kafka → Flink append Landing; Spark/Airflow mỗi giờ xử lý tuần tự Bronze → Silver → Gold fact → ba mart và publication. Raw không hợp lệ được giữ ở Landing/Bronze; realtime Landing dashboard còn chờ.
- Trino SQL và BI Hub; RBAC và phạm vi cửa hàng tại API.
- Bảo trì Iceberg, snapshot/file và retention quarantine.
- CLI sinh dữ liệu nguồn; CI kiểm tra cấu hình, lint, typecheck và build.

## 4. Đầu ra phân tích

Gold OLTP gồm 6 dimension, 6 fact, 15 mart. Nhóm log gồm 1 fact, 3 mart. Bronze CDC là log sự kiện riêng, không phải bảng trạng thái hiện tại.

Chi tiết grain ở [Lakehouse Design](LAKEHOUSE_DESIGN_PLAN.md). Công thức và khác biệt Gold/OLTP ở [Metrics Catalog](../architecture/METRICS_CATALOG.md).

## 5. Giới hạn

- CDC chưa tự cập nhật Silver/Gold OLTP theo sự kiện.
- DAG manual hard delete chỉ đồng bộ CDC delete của cart_items/wishlist_items đã bỏ, có dấu soft delete và đã bị xóa thật ở MySQL ít nhất 30 ngày, tính theo source_ts của CDC; trigger là thực hiện đồng bộ Silver bằng tập khóa đầy đủ, không chia chunk; giữ Bronze/CDC lịch sử; không kiểm tra lại source/phiên bản Silver, không lưu ledger hoặc chặn backfill. Dữ liệu cũ có thể được nạp lại; khi cần phải chạy DAG xóa lại. Không rebuild Gold/mart hoặc tạo publication vì không có phụ thuộc vào hai bảng này. Purge MySQL vẫn là quyết định/thao tác chủ động riêng.
- Batch không có lookback; timestamp nguồn phải phản ánh update. Late commits/missed intervals cần rerun/backfill chủ động, không tự lùi cursor. Payments dùng updated_at.
- Quality gate sau Gold không tạo transaction/rollback chung cho mọi bảng; maintenance chỉ áp dụng danh sách bảng cấu hình.
- Upload JSONL vào MinIO chưa có job tự động đọc file thô sang Gold.
- Tài liệu A–D đã tách chức năng hiện có và phần chưa triển khai; không dùng chúng thay bằng chứng runtime.
- Chưa có bảng cấu hình mục tiêu doanh số; target/run-rate để không có dữ liệu thay vì dùng số minh họa.
- Chưa có benchmark end-to-end hoặc cam kết latency/throughput production.
- HTTPS và secret manager production chưa được đóng gói đầy đủ.
- UI/API còn lệch quyền operations_manager; route sửa/cấp tồn chi nhánh mới flush chưa commit. Chi tiết và nguồn code nằm trong [kiến trúc](../architecture/SYSTEM_ARCHITECTURE_VISUAL.md).
- Machine Learning/RAG/fine-tuning ngoài phạm vi hiện tại.

## 6. Bằng chứng trình bày

Dùng log DAG/Flink, snapshot Iceberg, Trino SQL và `system.data_quality_results` của lần chạy cụ thể. Ghi kèm dataset, thời điểm, số dòng từng tầng và môi trường thực thi.

[Kiến trúc hệ thống](../architecture/SYSTEM_ARCHITECTURE_VISUAL.md) là tham chiếu triển khai. A–D được giữ để phân tích nghiệp vụ và viết tiểu luận.
