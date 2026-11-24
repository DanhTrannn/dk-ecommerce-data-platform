# Đồng bộ nguồn

DAG `lakehouse_oltp_sync` chạy sau incremental thành công qua Asset `lakehouse_oltp_loaded`, hoặc chạy tay khi cần. Nó xử lý schema và so khóa. Incremental thông thường đọc theo `updated_at` hoặc `created_at` trong cửa sổ của lần chạy.

## 1. Thay đổi cột (nhóm `schema`, chạy trước)

`sources.py` là danh sách cho phép: `columns` được nạp, `excluded` cố ý bỏ. So nguồn với danh sách và Bronze:

- **Cột mới chưa duyệt:** sync báo lỗi, chờ người duyệt; incremental vẫn chạy.
- **Cột đã duyệt:** tự thêm vào staging và Bronze, đặt `lakehouse.reload-pending` trên Bronze, rồi nạp lại cả bảng trong một commit.
- **Kiểu được nới** (int → long…): tự nới.
- **Cột đã duyệt bị bỏ hoặc chuyển sang kiểu không tương thích:** init, incremental hoặc sync sẽ báo lỗi tại bước kiểm tra hoặc trích xuất của bảng đó.

## 2. Dòng bị xoá (nhóm `detect`, chạy sau)

So toàn bộ khoá chính nguồn với khoá còn sống ở Bronze:

- **Khoá mất ở nguồn:** xóa mềm qua Bronze, Silver và dimension/fact Gold. `_deleted_at` ghi thời điểm phát hiện. Mart chỉ tính dòng còn hiệu lực.
- **Khoá có lại ở nguồn:** dòng sống lại.
- **Xoá hẳn:** trigger tay DAG `lakehouse_oltp_purge`.

## Lưu ý

- Các bước dùng khóa và cờ nạp lại để có thể retry. Snapshot Bronze rỗng được bỏ qua khi kiểm tra file cần viết lại sau thay đổi cột.
- So khóa phát hiện dòng mất hoặc xuất hiện lại. Một lần cập nhật nội dung trên khóa còn tồn tại có thể bị bỏ sót nếu commit sau cửa sổ trích xuất. Trường hợp này cần đọc lại cửa sổ hoặc nạp toàn bộ.
- Khi backfill incremental: tạm dừng sync, backfill xong thì trigger tay sync một lần.
