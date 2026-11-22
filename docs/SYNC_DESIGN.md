# Đồng bộ nguồn

DAG `lakehouse_oltp_sync` chạy sau mỗi lần incremental thành công. Nó xử lý hai loại thay đổi mà incremental (lọc theo `updated_at`) không thấy.

## 1. Thay đổi cột (nhóm `schema`, chạy trước)

`sources.py` là danh sách cho phép: `columns` được nạp, `excluded` cố ý bỏ. So nguồn với danh sách và Bronze:

- **Cột mới chưa duyệt:** sync báo lỗi, chờ người duyệt; incremental vẫn chạy.
- **Cột đã duyệt:** tự thêm vào staging và Bronze, bật cờ `reload-pending`, rồi nạp lại cả bảng trong một commit.
- **Kiểu được nới** (int → long…): tự nới.
- **Cột bị bỏ hoặc đổi loại:** mọi DAG dừng, kèm thông báo cách xử lý.

## 2. Dòng bị xoá (nhóm `detect`, chạy sau)

So toàn bộ khoá chính nguồn với khoá còn sống ở Bronze:

- **Khoá mất ở nguồn:** xoá mềm (`_is_deleted`, `_deleted_at`) qua Bronze, Silver, Gold; mart không tính.
- **Khoá có lại ở nguồn:** dòng sống lại.
- **Xoá hẳn:** trigger tay DAG `lakehouse_oltp_purge`.

## Lưu ý

- Cả hai luồng idempotent: chạy lại, hỏng rồi chạy tiếp hay chạy init đều cho cùng kết quả.
- Khi backfill incremental: tạm dừng sync, backfill xong thì trigger tay sync một lần.
