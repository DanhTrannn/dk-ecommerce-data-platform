# Thiết kế đồng bộ nguồn

`lakehouse_oltp_sync` xử lý hai loại thay đổi mà incremental (lọc theo `updated_at`/`created_at`) không thấy: thay đổi cột và dòng bị xoá cứng ở nguồn. Cách chạy, cột và log ở [airflow/README.md](../airflow/README.md#deleted-rows); cách xử lý lỗi ở [OPERATIONS.md](OPERATIONS.md#4-xử-lý-sự-cố). File này ghi các quyết định thiết kế và lý do.

## Ràng buộc chung

- Chỉ đọc database nguồn: không trigger, không bảng bia mộ, không CDC.
- Idempotent ở mọi tầng: retry, clear, chạy hỏng rồi chạy tiếp hay backfill đều cho cùng bảng.
- Init và sync sửa schema; incremental chỉ nạp.
- Sync chạy sau mỗi lần incremental thành công, qua Asset `lakehouse_oltp_loaded`. `max_active_runs` không giới hạn giữa hai DAG, nên khi backfill phải tạm dừng sync rồi trigger tay một lần sau đó.

## Dòng bị xoá

| Quyết định | Lý do | Đánh đổi |
|---|---|---|
| Phát hiện bằng đối chiếu toàn bộ khoá chính nguồn với khoá sống ở Bronze | Không sửa nguồn, bắt mọi kiểu xoá kể cả `TRUNCATE`; chỉ đọc cột khoá qua index nên đủ rẻ ở quy mô 10m | `_deleted_at` là lúc phát hiện, không phải lúc xoá thật. CDC theo WAL là hướng phát triển |
| Xoá mềm ở mọi tầng (`_is_deleted`, `_deleted_at` ở Bronze, Silver, dimension và fact Gold) | Giữ dấu vết "dòng từng có, bị xoá lúc nào"; mart vẫn thấy ngày của dòng đã xoá để tính lại | Ai đọc thẳng dimension/fact phải lọc `NOT _is_deleted`; mart đọc qua macro `live()` |
| `detect` ghi dấu xoá thẳng vào Bronze, mỗi bảng một task | Nhiều task commit vào một bảng chung sẽ xung đột | |
| Dòng có lại ở nguồn (hoặc Bronze còn thiếu) được nạp lại như `restored` | Khôi phục tự sống lại; dòng mới mà incremental bỏ sót cũng được bắt | Lần sửa bị incremental bỏ sót thì vẫn chưa bắt được |
| Init giữ dấu xoá cũ và đánh dấu dòng nguồn không còn | Một lần nạp lại toàn bộ không xoá sạch lịch sử | |
| `fact_inventory_daily` đánh dấu cả các ngày trước chuyển động còn sống đầu tiên | Purge xong phải cho đúng bảng mà một lần dựng lại sẽ cho | |
| Xoá cứng bằng DAG chạy tay `lakehouse_oltp_purge`, Gold → Silver → Bronze, không điều kiện chặn | Người vận hành chủ động; mart không đổi vì vốn đã bỏ dòng xoá | Chạy khi sync chưa xong có thể để sót dòng; sửa bằng init |

## Thay đổi cột

| Quyết định | Lý do | Đánh đổi |
|---|---|---|
| `sources.py` là danh sách cho phép: `columns` được nạp, `excluded` cố ý bỏ (gồm dữ liệu cá nhân) | Cột nhạy cảm mới không lọt vào trước khi có người duyệt (giống "Block new columns" của Fivetran) | Cột mới cần một commit để duyệt |
| Phân loại sáu loại: `unlisted`, `approved`, `widened`, `narrowed`, `dropped`, `incompatible` | Tự làm phần an toàn, dừng rõ ở phần có thể làm sai số | Đổi tên cột được coi là `dropped` cộng `unlisted` |
| Cột `unlisted` chỉ làm `schema.<bảng>` của sync lỗi; incremental vẫn chạy | Một cột chưa duyệt không được làm ngừng nạp dữ liệu | Sync dừng cả lượt, nên đồng bộ xoá của mọi bảng hoãn tới khi duyệt |
| Kiểu chỉ tính cho cột trong `columns`; cột khác so theo tên | Có cột kiểu extract không đọc được (`jsonb`) | |
| Incremental chỉ đọc các cột vừa trong `columns` vừa có ở Bronze; giá trị vượt kiểu cũ báo lỗi nhắc trigger sync | Incremental không phụ thuộc việc sync đã chạy hay chưa | |
| Thêm cột `approved` và cờ Bronze `lakehouse.reload-pending` trong cùng một commit | Không có trạng thái "đã thêm cột mà quên nạp lại" | |
| Nếu còn file dữ liệu thiếu cột thì ghi lại Bronze một lần; quyết định đọc từ metadata file | PyIceberg 0.12 không upsert được vào bảng có file cũ thiếu cột; retry luôn làm tiếp được | Bronze bị ghi lại hai lần khi duyệt cột (lần này và lần nạp lại) |
| Khi cờ bật, `detect` đưa mọi dòng nguồn vào staging và `bronze` thay cả bảng trong một commit, rồi mới xoá cờ; mọi full load (kể cả init) cũng xoá cờ | Không có trạng thái điền dở; hỏng trước khi xoá cờ thì lần sau nạp lại từ đầu; không cần truy vấn thêm vào nguồn | Nạp lại cả bảng chỉ vì một cột; chấp nhận vì duyệt cột hiếm |
| `silver_missing` chỉ là dòng log | Không làm sai số liệu hiện có; Airflow không có trạng thái "cảnh báo" | Dễ bị bỏ qua |
| `incompatible` cần xoá bảng Bronze rồi chạy init | Iceberg không đổi được loại cột | Mất lịch sử xoá mềm của bảng đó; việc xoá bảng luôn cần duyệt |
| Bronze giữ cột đã bị bỏ ở nguồn | Giữ lịch sử | Duyệt lại cột đó không kích hoạt nạp lại; dòng cũ chỉ có giá trị khi đổi ở nguồn |
