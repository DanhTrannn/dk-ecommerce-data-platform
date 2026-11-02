# Ghi chú refactor repo

**Các quyết định bên dưới là phạm vi refactor đã chốt. Tiến độ triển khai được ghi riêng theo từng đợt.**

## Đã chốt hướng thay đổi

1. **Tách Compose profile:** không bắt buộc chạy toàn bộ stack; tách nhóm dịch vụ ứng dụng, batch, streaming và công cụ quản trị. Cập nhật launcher, Makefile và hướng dẫn theo profile mới. Đợt 1 đã chốt `core` mặc định, `batch`, `streaming`, `tools`; `lakehouse-tools` giữ Spark SQL client dùng một lần. Launcher nhận danh sách profile chính xác; Compose trực tiếp vẫn áp dụng quy tắc cộng profile của Docker.
2. **Spark chạy local:** bỏ Spark master/worker; job Airflow và Spark SQL chạy local trong container thực thi. Đồng bộ connection `spark_default`, biến môi trường, bootstrap và tài liệu.
3. **Polaris RBAC chỉ bootstrap lần đầu từ YAML:** giữ identity và phân quyền hiện có. Sau khởi tạo, người dùng thêm/sửa/thu hồi quyền trên Console; YAML không còn đồng bộ trạng thái đang chạy. Init chạy lại không được ghi đè thay đổi thủ công, purge identity/role hay tự reset credential. Lưu trạng thái hoàn tất bootstrap cùng môi trường để restart không khởi tạo lại.
4. **MinIO đồng bộ mỗi lần init:** giữ 7 tài khoản và policy; cấu hình repo là nguồn chuẩn. Tạo phần thiếu, cập nhật phần khác và thu hồi quyền gắn dư trên tài khoản được quản lý; không xóa/tạo lại toàn bộ hoặc tác động tài khoản ngoài phạm vi repo. Giữ credential ổn định khi cấu hình không đổi; thay đổi secret trong cấu hình phải được áp dụng. Thay đổi thủ công trên tài nguyên được quản lý sẽ bị ghi đè ở lần init sau. Không xóa bucket/dữ liệu; chỉ báo hoàn tất khi đồng bộ thành công. Cách xử lý tài khoản bị bỏ khỏi cấu hình sẽ chốt trước triển khai.
5. **Giữ bảng nguồn MySQL:** dữ liệu phát sinh từ web vẫn được lưu đầy đủ; tinh gọn analytics không đồng nghĩa xóa bảng nghiệp vụ nguồn.
6. **Tách nguồn dashboard:** dashboard phân tích đọc datamart, không fallback MySQL hoặc truy vấn fact để thay thế mart. Ngoại lệ đã chốt: dashboard realtime access log đọc Landing qua lớp API/query có phân quyền. Hiển thị rõ nguồn và thời điểm cập nhật; thiếu dữ liệu thì để trống.
7. **Mart hiện có phải có dashboard:** bổ sung biểu đồ/góc nhìn cho các mart chưa được sử dụng, gồm HTTP metrics, product demand và marketing funnel. Không loại mart chỉ vì hiện chưa có dashboard.
8. **Loại bảng analytics không có mục đích được chứng minh:** lập ánh xạ câu hỏi → dữ liệu → mart → dashboard. Bảng trung gian/dimension được giữ nếu cần cho transformation, chất lượng, truy vết hoặc RBAC; danh sách bỏ phải kiểm tra phụ thuộc trước. Chưa chốt tên bảng và chưa xóa dữ liệu đang lưu.
9. **Thiếu dữ liệu thì để trống:** để trống phần dashboard tương ứng; không thay bằng 0, số liệu giả hoặc fallback MySQL. Phần có dữ liệu vẫn hiển thị bình thường.
10. **Bỏ KPI không rõ căn cứ:** loại khỏi dashboard và logic tính nếu chưa có định nghĩa hoặc dữ liệu đủ tin cậy; không giữ dưới nhãn ước tính để lấp chỗ trống. Giữ dữ liệu nguồn.
11. **Hoãn chốt danh sách bảng:** tạm ghi nhận tiêu chí giữ/bỏ; người dùng sẽ chỉ rõ khi triển khai. Giữ bảng trả lời câu hỏi hoặc phục vụ pipeline/quality/truy vết/RBAC; kiểm tra phụ thuộc và xin duyệt danh sách trước khi xóa.
12. **Maintenance thủ công trong local/demo:** bỏ lịch tự động cho maintenance OLTP và streaming; gom compact, expire snapshots và orphan cleanup trong một lượt Spark. Giữ báo lỗi khi thao tác thất bại.
13. **Rollup mart log mỗi giờ:** tách khỏi luồng maintenance; lỗi compaction không chặn cập nhật mart/dashboard.
14. **Đồng bộ phạm vi maintenance:** sau khi chốt bảng giữ/bỏ, cập nhật danh sách bảo trì, bao gồm các bảng Bronze batch/CDC và mart log được giữ; không tiếp tục dùng danh sách thiếu hiện tại.
15. **Retention khi chạy maintenance thủ công:** giữ snapshot 7 ngày và tối thiểu 10 snapshot/bảng; chỉ dọn orphan cũ hơn 3 ngày; giữ bản ghi quarantine 30 ngày. Không dọn orphan trên staging/raw Landing hoặc checkpoint. Đồng bộ tham số maintenance và thuộc tính retention của bảng theo quy tắc này.
16. **Batch tối giản là luồng cập nhật chính:** Airflow xác định khoảng lấy dữ liệu theo lịch → staging ghi mới delta từ source theo created_at/updated_at phù hợp, không lookback → Bronze append delta gốc → Silver chỉ đọc delta mới, transform/dedup và upsert theo khóa chính → Gold build fact/dim/mart. Staging dùng lại và overwrite/truncate mỗi lượt; không truncate Bronze/Silver. Không đọc lại toàn bộ lịch sử Bronze để rebuild Silver. Giữ run_id đơn giản để truy vết và xử lý retry không nhân dữ liệu.
17. **CDC phục vụ audit và đồng bộ hard delete riêng:** lưu insert/update/delete, before/after và tọa độ binlog/Kafka vào Bronze CDC; không dùng CDC để upsert thường ngày. DAG chạy thủ công chỉ đọc sự kiện delete chưa xử lý để đồng bộ việc xóa khỏi dữ liệu hiện hành trong Lakehouse; chạy lại không gây sai lệch.
18. **Bỏ cơ chế batch phức tạp:** manifest/MD5, high watermark và cursor lưu riêng từng bảng, completion marker, đọc/hash/đếm lặp. Dùng khoảng thời gian và dependency/retry sẵn có của Airflow; giữ kiểm tra dữ liệu cơ bản, PII và RBAC. Rà soát các quality/reconciliation gate để giảm phần trùng lặp, không biến pipeline thành chuỗi kiểm tra nhiều tầng.
19. **Query delta nhận interval từ Airflow:** truyền data_interval_start/end thành interval_start/end; lọc đúng `change_time >= interval_start AND change_time < interval_end`, không lookback. Dùng updated_at cho bảng có update, created_at cho bảng chỉ insert. Retry/backfill dùng đúng interval của run, không lấy thời gian hiện tại làm mốc. Module truyền tham số đã xử lý an toàn; query ngắn/nội bộ có thể để inline theo quy tắc SQL bên dưới. Silver dedup và upsert theo PK; source phải cập nhật change_time khi dữ liệu đổi. Hard delete xử lý riêng.
20. **Soft delete mặc định, hard delete thủ công:** thiết kế source dùng soft delete cho thao tác xóa nghiệp vụ; batch truyền trạng thái này xuống Lakehouse. Chỉ purge source chủ động khi có quyết định xóa, không tự purge theo tuổi dữ liệu. Luồng: purge source → CDC ghi nhận delete → DAG được gọi thủ công đồng bộ xóa dữ liệu hiện hành và cập nhật Gold/mart liên quan. Không xóa source lần nữa khi đọc CDC. Luôn giữ lịch sử giao dịch, Bronze lịch sử và CDC audit; không cascade xóa lịch sử hoặc làm mất doanh thu quá khứ. Retry/backfill không được hồi sinh bản ghi đã purge. Danh sách bảng được phép purge phải được duyệt trước triển khai.
21. **SQL dùng chung:** đặt DDL/schema và query lớn có mục đích sử dụng riêng trong thư mục `sql/` cấp repo để bootstrap/infrastructure và pipeline cùng gọi, không nhân bản. SQL ngắn hoặc chỉ phục vụ xử lý nội bộ pipeline không bắt buộc tách file; bỏ việc tách từng fragment vụn. Phạm vi trước mắt là SQL lakehouse; không tự chuyển DDL MySQL đang có ở `database/schema.sql`.
22. **Access log có hai luồng phục vụ:** Flink ghi nối tiếp Landing liên tục, không truncate như staging OLTP; dashboard realtime đọc từ Landing để giảm độ trễ, số liệu chưa qua đầy đủ bước làm sạch. Airflow chạy mỗi giờ: Landing mới → Bronze → Silver → Gold fact → datamart; dashboard phân tích sâu đọc mart. Đầu ra tầng trước là đầu vào tầng sau, không ghi song song bốn tầng từ cùng parsed stream. Landing nhận raw, Bronze giữ dữ liệu gốc kèm lineage, Silver chuẩn hóa/làm sạch, Gold xây fact/mart. Giữ đủ tầng; không cần Flink xử lý liên tục tới Gold. Pipeline giờ này độc lập maintenance.
23. **Dashboard access log:** cả HTTP metrics (request, lỗi, độ trễ) và sự kiện nghiệp vụ (xem sản phẩm, giỏ, checkout...) đều phục vụ góc nhìn vận hành realtime và báo cáo phân tích. Realtime đọc Landing, refresh mỗi 10 giây; báo cáo phân tích sâu đọc mart được cập nhật mỗi giờ. Không diễn giải số sự kiện log như số giao dịch/người dùng duy nhất khi chưa có căn cứ.
24. **Generator khớp chính xác nghiệp vụ:** đồng bộ với schema và hành vi ứng dụng, gồm trạng thái, timestamp, payment/COD, return/refund, soft delete và quan hệ dữ liệu; không sinh kịch bản trái nghiệp vụ để lấp dashboard.
25. **Dọn code và test cục bộ:** loại code/config không dùng, logic trùng và fallback không cần thiết. Có thể thêm test để kiểm tra từng phần, nhưng chỉ lưu/chạy cục bộ; không đưa test vào Git, commit hoặc push lên remote.
26. **Đồng bộ docs cuối đợt refactor:** sau khi sửa và kiểm tra xong toàn bộ phạm vi đã chốt, cập nhật README, hướng dẫn chạy, kiến trúc, mô hình dữ liệu và báo cáo liên quan theo code mới; bỏ mô tả cũ không còn đúng.

## Tiến độ triển khai

**Đợt 1 — hoàn thành mục 1–2 (2026-11-02):**

- `core` chỉ chạy MySQL/API/Storefront; `batch` không kéo Kafka/Flink; `streaming` không kéo Airflow; các giao diện quản trị nằm trong `tools`.
- Bỏ Spark master/worker; SparkSubmit của Airflow, Spark SQL và CDC materializer hiện tại chạy `local[2]` trong container thực thi. `SPARK_MASTER_URL` cho phép chỉnh số thread local; connection Airflow không dùng địa chỉ cluster cũ.
- Cập nhật launcher, Makefile, `.env.example`, CI và tài liệu vận hành. Kafka tắt chủ động thì API không ghi DLQ.
- Kiểm tra cả 15 tổ hợp nhóm chính; chạy riêng core đạt đúng 3 dịch vụ healthy; Spark SQL và toàn bộ DAG batch chạy local đạt 13/13 task, 26/26 quality checks.
- CDC local đồng bộ được thay đổi và hoàn nguyên tới API; 10 endpoint analytics đạt; 328 dòng tồn kho lịch sử và tổng tài chính giữ nguyên. Test/log/dữ liệu kiểm tra chỉ lưu cục bộ ngoài Git.

Các quyết định 3–26 không thuộc phạm vi đợt 1; tiến độ các đợt sau được ghi dưới đây.

**Đợt 2a — Polaris, hoàn thành mục 3 (2026-11-02):**

- YAML chỉ là mẫu bootstrap lần đầu. Sau khi hoàn tất, init dùng state đã lưu để kiểm tra identity/credential và khôi phục file đầu ra; không áp lại YAML, purge, thu hồi grant, cập nhật catalog hay tự reset credential.
- Tận dụng volume `polaris-credential-state`: `ready` là JSON ghi nhận trạng thái initializing/complete, realm, catalog/creation timestamp, client ID và cấu hình khởi tạo. Ghi file atomically, lưu từng credential mới trước bước API tiếp theo; khóa cục bộ bảo vệ init đồng thời.
- Cài đặt cũ được tiếp nhận mà không sửa catalog/RBAC. State sai, metadata thiếu, principal bị thay thế hoặc secret không hợp lệ làm init thất bại rõ ràng; không tự tạo lại identity.
- 15 kiểm tra tích hợp đạt trên catalog/principal tạm riêng, gồm giữ thay đổi quyền thủ công, không khôi phục quyền đã thu hồi, khôi phục file mất, retry và init đồng thời. Trên hệ thống đang chạy, 26 bản ghi catalog/RBAC và nội dung credential trước/sau giữ nguyên; sáu output có đúng UID/GID và mode 0600.
- Spark SQL, Spark trong Airflow và Trino đọc được dữ liệu; 10 endpoint analytics đạt; Flink/CDC tiếp tục hoạt động. Test/log chỉ lưu cục bộ ngoài Git.

**Tiếp theo:** mục 4 (đồng bộ MinIO tại chỗ) chưa triển khai. Các mục 5–26 vẫn theo kế hoạch; đợt Polaris không thay đổi luồng batch/CDC hoặc cơ chế MinIO hiện tại.

## Chi tiết để chốt khi triển khai

- Danh sách bảng giữ/bỏ và bảng được phép purge do người dùng chỉ rõ, duyệt trước khi xóa.
- Cách xử lý tài khoản MinIO bị bỏ khỏi cấu hình. Tên/default Compose profile đã chốt ở đợt 1.

## Quy tắc thực hiện

Chỉ triển khai khi người dùng yêu cầu; sửa từng phần, kiểm tra sau mỗi phần. Không mở rộng phạm vi hoặc thêm cơ chế phức tạp ngoài các quyết định trên.
