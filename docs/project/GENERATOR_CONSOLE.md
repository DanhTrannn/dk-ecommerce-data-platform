# Generator Console

## 1. Mục tiêu và phạm vi

Console tạo job xuất SQL PostgreSQL, kiểm tra cấu hình, theo dõi tiến trình và tải artifact. Import PostgreSQL là thao tác riêng, không tự chạy khi sinh file. Giữ màn hình mô phỏng API và quản trị quyền của `dev`. Sampler dùng trọng số nhóm thứ hạng H&M; các tỷ lệ khác là giả định tại [GENERATOR_METRICS](GENERATOR_METRICS.md). Sinh log tổng hợp và profile UCI runtime đã được bỏ.

## 2. Cấu hình

Preset tại `generator/configs/`; form chỉnh scenario, seed, anchor, tháng lịch sử, customers/products/variants/orders và cửa hàng tham chiếu. YAML nâng cao chỉnh `distributions`. Dùng chung validation với generator; cấu hình cuối đóng băng trong `config.yml` và tham gia dataset identity.

## 3. Thực thi và trạng thái

Worker `python -m platform_app.ui.worker <job_dir>` gọi CLI `run --json` để ghi manifest, rồi `export-sql --json`. Một job sinh dữ liệu active trên một console; đóng tab không dừng worker. Trạng thái: queued/running/succeeded/failed/interrupted; stage validate/sql. PID không xác minh được chuyển interrupted.

## 4. Tổ chức

`ui/config_builder.py` quản lý form/config; `job_store.py` lưu trạng thái/lock; `worker.py` gọi CLI; `artifacts.py` quản lý file/checksum; `import_store.py` và `import_worker.py` quản lý nạp. Giao diện nằm trong `ui/views/`, kịch bản HTTP trong `ui/scenarios/`. Logic sinh dữ liệu thuộc package generator.

## 5. Lưu trữ

`data/generator/jobs/<job_id>/` chứa config/request/state, manifest, checksums, `logs/`, `outputs/sql/` và journal `imports/<attempt_id>.json`. State ghi atomic và còn sau restart. Artifact collector bỏ qua journal import. File lớn hiển thị đường dẫn/dung lượng thay vì đọc toàn bộ vào UI.

## 6. Cách chạy

Chạy local: `uv run --locked --package platform-app platform-app`; Compose: `docker compose --profile core up -d --build platform-app`. Mở cổng 8501. API và console dùng chung container platform-app.

## 7. Kiểm tra

Test generator bao phủ validation, SQL/JSON, quan hệ tiền/kho và địa điểm. Test console bao phủ form, trạng thái, checksum, worker và import; kiểm thử PostgreSQL dùng database tạm riêng. Xem [hướng dẫn platform](../../services/platform-app/README.md) cho lệnh chạy và biến môi trường.

## 8. Quyết định mở rộng: nạp SQL PostgreSQL từ console

Nạp là tác vụ nền độc lập với sinh file, không thêm stage tự chạy vào pipeline generation. Chỉ job terminal succeeded/partial với SQL succeeded và artifact hợp lệ được chọn; kiểm tra checksum trên bản sao riêng và identity trước khi gọi `platform_app.api.db.import_dataset.import_dataset`. Giữ nguyên transaction, schema gate, đối soát và identity restart của importer; không tạo một cơ chế nạp SQL thứ hai.

`ui/import_store.py` quản lý khóa, chống khởi chạy trùng, nhận diện đích không chứa password và journal `job/imports/<attempt_id>.json`. `ui/import_worker.py` thực thi nạp ngoài tiến trình Streamlit; `ui/views/imports.py` hiển thị nút và receipt trong kết quả job. Dùng hai flock riêng: khóa ngắn cho journal và khóa execution cho toàn tác vụ; tối đa một import trên một thư mục jobs. Import không sửa generation state hoặc checksum, và artifact collector bỏ qua thư mục imports.

Trạng thái bền vững: queued/running/succeeded/failed/already_present/unknown. Thành công chỉ ghi sau khi importer trả về từ commit. Lỗi driver được rút gọn, không ghi raw SQL/credential. Nếu commit chưa rõ kết quả hoặc worker mất, không giả định rollback: ghi unknown và chặn tự nạp lại cùng dataset/đích. Database đã có generation run được ghi already_present, không nhận là một lần import mới thành công. Thay đổi đích sau khi queue được phát hiện trước khi ghi. Trạng thái failed có thể thử lại bằng thao tác rõ ràng; không auto retry.

Đích lấy từ API_DATABASE_URL hiện có, không nhận connection string qua form và không lưu credential vào state/command. Mật khẩu/user thay đổi không làm mất nhận diện cùng host/port/database; alias host vẫn có thể khác, nên chống trùng trong PostgreSQL của importer là lớp quyết định cuối. Không tự sửa schema/seed, xóa volume, chạy pipeline hoặc thay API nghiệp vụ. Xem [hướng dẫn sử dụng và giới hạn](../../services/platform-app/README.md#nạp-sql-vào-postgresql).

## 9. Runner API mua online

Quyết định: giữ runner trong `ui/scenarios/` với cấu hình, journal store, luồng HTTP purchase và worker riêng; view tại `ui/views/scenarios.py`. Dùng HTTP qua API nguồn, không import service nghiệp vụ hoặc ghi DB trực tiếp. Dùng lại httpx đã có, không thêm dependency/hàng đợi. Một run API active theo thư mục `data/generator/scenarios/`; generation và import là tác vụ độc lập, không tự nối chuỗi.

Mỗi lượt tạo khách mới, đăng nhập, khám phá variant đủ tồn, PUT giỏ, quote và checkout. Worker ghi key trước khi gọi, lưu mã đơn trước đọc kiểm chứng; lỗi ghi không rõ kết quả hoặc mất worker chuyển unknown và không tự replay. Password/cookie không lưu; email/key/mã đơn đủ truy vết cục bộ. Nút dừng chỉ ngăn lượt tiếp theo, không hủy giao dịch đang chạy. HTTP 4xx giữ failed riêng, thành công/thất bại hỗn hợp là partial. Khoá file chỉ phù hợp worker cục bộ dùng chung filesystem.

Cấu hình HTTP server qua `SCENARIO_API_BASE_URL` và `SCENARIO_HTTP_TIMEOUT_SECONDS`; form chỉ điều chỉnh kịch bản. Không tự cập nhật lineage API hoặc triển khai nguồn đa khách mua lại; mỗi lượt một khách/một SKU, chọn variant đầu đủ tồn nên không coi là phân phối bán hàng thực. Chưa tự động hóa fulfillment/POS/đổi trả, chưa chứng minh Kafka/Lakehouse delivery. Hướng dẫn đầy đủ tại [platform guide](../../services/platform-app/README.md#mô-phỏng-mua-online-qua-api).
