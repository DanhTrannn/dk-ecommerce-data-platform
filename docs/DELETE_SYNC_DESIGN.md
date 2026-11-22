# Thiết kế: đồng bộ dòng bị xoá ở nguồn (phần 1/2)

Phần 2 (thay đổi cột ở nguồn) thiết kế sau, dùng lại DAG sync của phần này.

## 1. Mục đích

Luồng incremental lọc theo `updated_at`, nên không thấy dòng bị xoá cứng ở nguồn: dòng đó nằm mãi ở Bronze, Silver, Gold. Lakehouse cần xử lý việc xoá giống một hệ thống thật, và giữ lại dấu vết để trả lời "dòng này từng có, bị xoá lúc nào".

## 2. Mục tiêu, không làm, phản mục tiêu

| | |
|---|---|
| Mục tiêu | Phát hiện dòng bị xoá ở 19 bảng nguồn; đánh dấu xoá mềm ở Bronze, Silver, Gold; mart không tính dòng đã xoá; dòng khôi phục lại ở nguồn thì sống lại; có DAG chạy tay để xoá cứng |
| Không làm | Thay đổi cột ở nguồn (phần 2); CDC theo WAL; sửa database nguồn (trigger, bảng bia mộ) |
| Phản mục tiêu | Mất dấu vết xoá (kể cả khi chạy lại init); mart sai số ở ngày có dòng bị xoá; một lần chạy lại hay backfill cho ra kết quả khác |

## 3. Ràng buộc

- Không sửa database nguồn; chỉ đọc.
- Idempotent ở mọi tầng: retry, clear, chạy hỏng rồi chạy tiếp, backfill đều ra cùng bảng.
- Theo quy ước Airflow của dự án: `PythonOperator` + `PostgresHook`, `BashOperator` chỉ cho dbt, không custom operator.
- Init vẫn sở hữu schema: cột mới do `create_schema` thêm.

## 4. Các cách đã cân nhắc

**Cách phát hiện xoá:**

| Cách | Đánh đổi | Chọn |
|---|---|---|
| Đối chiếu khoá định kỳ | Không đụng nguồn, không thêm dịch vụ, bắt mọi kiểu xoá; phải quét toàn bộ khoá; thời điểm xoá là lúc phát hiện | **Chọn** |
| Trigger ghi bia mộ ở nguồn | Chính xác, nhẹ; phải sửa schema nguồn, sót khi trigger tắt hoặc `TRUNCATE` | Không |
| CDC theo WAL (Debezium) | Thấy mọi thay đổi tức thì; thêm dịch vụ, `wal_level = logical`, thay cả luồng batch | Hướng phát triển |

**Cách thể hiện ở Gold:**

| Cách | Chọn |
|---|---|
| Xoá mềm ở mọi tầng, kể cả Gold; xoá cứng bằng DAG chạy tay | **Chọn** |
| Giữ dấu tạm ở Gold, dọn sau khi mart xử lý xong | Không |
| Xoá cứng ngay ở Gold | Không: mart mất thông tin ngày cần tính lại |

## 5. Thiết kế

### 5.1 Cột mới

| Cột | Kiểu | Ở đâu | Ý nghĩa |
|---|---|---|---|
| `_is_deleted` | boolean | Bronze, Silver, dimension và fact Gold | Dòng không còn ở nguồn |
| `_deleted_at` | timestamp(6) with time zone | như trên | Lúc pipeline phát hiện; giữ nguyên ở các lần phát hiện sau |

Staging và mart không có hai cột này. `create_schema` thêm chúng vào Bronze; Silver và Gold nhận chúng qua model dbt.

### 5.2 DAG `lakehouse_oltp_sync`

- **Lịch:** chạy sau mỗi lần `lakehouse_oltp_incremental` thành công, qua một Asset mà task cuối của incremental phát ra. Hai DAG vì thế không chạy chồng nhau theo lịch thường.
- **Luồng:** `detect >> bronze >> silver >> gold >> maintenance`. Ba nhóm sau dùng lại đúng task của incremental.
- **`detect.<bảng>`**, mỗi bảng một task:
  1. Trong một transaction đọc của nguồn, đọc toàn bộ khoá chính (chỉ cột khoá).
  2. Đọc khoá và `_is_deleted` của Bronze.
  3. `deleted` = khoá sống ở Bronze nhưng không còn ở nguồn.
  4. `restored` = khoá có ở nguồn nhưng Bronze không có hoặc đang đánh dấu xoá.
  5. Đánh dấu xoá thẳng các dòng `deleted` trong Bronze của bảng đó: `_is_deleted = true`, `_deleted_at` = thời điểm chạy, `_ingested_at` mới. Mỗi bảng chỉ có một task ghi, vì nhiều task cùng commit vào một bảng chung sẽ xung đột.
  6. Ghi các dòng `restored` (đọc theo khoá từ nguồn) vào staging của bảng đó.
- **`bronze.<bảng>`:** upsert các dòng `restored` như incremental, với `_is_deleted = false`. Nhờ `_ingested_at` mới, Silver và Gold nhận cả dấu xoá lẫn dòng khôi phục qua mốc như mọi dòng khác.

### 5.3 Bronze

- Mọi dòng đến từ staging (incremental, init, `restored`) được ghi với `_is_deleted = false`, `_deleted_at = NULL`. Staging không có hai cột này, nên Bronze tự gán thay vì để `align_arrow` điền `NULL`.
- `filter_current` coi việc đổi `_is_deleted` là một thay đổi, để dòng khôi phục có cùng `updated_at` không bị bỏ qua.
- **Init (full load) phải giữ dấu vết xoá.** Bronze sau init = dòng từ nguồn + các dòng đã đánh dấu xoá trước đó + các dòng Bronze có mà nguồn không còn (đánh dấu xoá luôn). Nếu không, một lần init sẽ xoá sạch lịch sử.

### 5.4 Silver

Mỗi model chuyển nguyên `_is_deleted`, `_deleted_at` từ Bronze. Test `matches_bronze` giữ nguyên, vì Silver vẫn giữ mọi khoá của Bronze.

### 5.5 Gold

- **Dimension, fact:**
  - mang `_is_deleted`, `_deleted_at` của dòng Silver ứng với grain của nó;
  - mọi phép join và tổng hợp từ bảng con loại dòng con đã xoá. Ví dụ `fact_order` không cộng dòng hàng đã xoá.
  - Dòng đã xoá vẫn giữ các cột ngày, nên `changed_days` vẫn đưa ngày của nó cho mart tính lại.
- **`fact_inventory_daily`:** sổ kho bỏ dòng đã xoá. Một vị trí kho không còn chuyển động nào thì các dòng của nó được đánh dấu xoá.
- **Mart:** mọi nguồn fact và dimension lọc `NOT _is_deleted`. Cơ chế tập ngày và `_mart_state` giữ nguyên.

### 5.6 Reconcile của init

Không đổi: reconcile so phần extract trong staging với nguồn, và staging không bao giờ chứa dòng đã xoá.

### 5.7 DAG `lakehouse_oltp_purge`

- `schedule=None`, chỉ chạy tay.
- Xoá cứng mọi dòng `_is_deleted` ở Gold, rồi Silver, rồi Bronze, theo thứ tự từ dưới lên để không tầng nào đọc lại dòng đã mất.
- Không có điều kiện chặn: người vận hành tự trigger khi incremental và sync đang nghỉ.
- Mart không đổi số liệu sau khi purge, vì chúng vốn đã bỏ dòng đã xoá.

## 6. Idempotency

| Tình huống | Vì sao giữ được |
|---|---|
| Chạy lại `detect` | Cùng nguồn và cùng Bronze cho cùng hai tập khoá |
| Chạy lại Bronze | Dòng đã đánh dấu không đổi; `_deleted_at` giữ giá trị cũ |
| Sync hỏng giữa chừng rồi chạy lại | Bronze đã đánh dấu thì lần sau `deleted` rỗng; Silver và Gold bắt kịp qua mốc |
| Init sau khi đã có dòng xoá | Init giữ và bổ sung dấu xoá (5.3) |
| Purge chạy lại | Không còn dòng `_is_deleted` thì không làm gì |

## 7. Kiểm chứng

- **Unit test, không cần stack:**
  - phép tính `deleted` và `restored`;
  - upsert dấu xoá giữ `_deleted_at`;
  - `filter_current` với cờ;
  - init giữ dấu xoá;
  - dòng nạp thường có `_is_deleted = false`.
- **Chạy thật trên bộ 100k:**
  1. Xoá ở nguồn một dòng hàng, một đánh giá, một dòng sổ kho, một khách (khoá ngoại `SET NULL`).
  2. Chạy incremental rồi sync.
  3. So với init (cả hai giữ dấu xoá): mọi bảng phải trùng.
  4. Khôi phục một dòng: cờ được gỡ.
  5. Chạy purge: dòng xoá biến mất, mart không đổi.
  6. Chạy lại từng bước: kết quả không đổi.

## 8. Quyết định của người thiết kế (chưa hỏi người dùng)

- Sync chạy sau **mỗi** lần incremental, không phải hằng tuần. Quét khoá chỉ đọc cột khoá qua index, đủ rẻ ở quy mô 10m.
- Sync nhận as-of là lúc nó chạy, nên Gold có thể nối dài tới ngày hôm đó; các macro ngày cuối đã dùng `greatest` cho trường hợp này.
- Dòng mới mà incremental bỏ sót (transaction commit sau khi cửa sổ đã đóng) cũng được sync nạp như dòng `restored`. Đây là lợi ích phụ; lần sửa dòng bị bỏ sót thì vẫn chưa bắt được.
- Superset sau này phải lọc `NOT _is_deleted` khi đọc thẳng fact và dimension.

## 9. Để người làm tự quyết

- Cách đọc khoá Bronze hiệu quả (chỉ đọc cột khoá và cờ).
- Chia lô khi đọc khoá nguồn để giới hạn bộ nhớ.
- Cách phát Asset từ incremental: `outlets` trên task cuối.
- Tên và cách sắp các task trong hai DAG mới.
