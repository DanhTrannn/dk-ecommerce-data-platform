# Thiết kế: thay đổi cột ở nguồn (phần 2/2)

Phần 1 ([DELETE_SYNC_DESIGN.md](DELETE_SYNC_DESIGN.md)) đồng bộ dòng bị xoá. Phần này dùng lại DAG `lakehouse_oltp_sync`.

## 1. Mục đích

Ngoài thực tế, schema nguồn đổi theo thời gian: thêm cột, bỏ cột, đổi tên, đổi kiểu. Hiện pipeline đọc một danh sách cột cố định. Cột mới bị bỏ qua mà không báo gì. Cột bị bỏ làm extract lỗi bằng một thông báo SQL khó hiểu. Muốn nạp cột mới thì phải sửa code rồi chạy lại init. Lakehouse cần phản ứng như một hệ thống thật: tự làm phần an toàn, dừng rõ ràng ở phần có thể làm sai số liệu, và không bao giờ nhận một cột chưa ai duyệt.

## 2. Mục tiêu, không làm, phản mục tiêu

| | |
|---|---|
| Mục tiêu | Phát hiện mọi khác biệt cột giữa nguồn và Bronze cho 19 bảng. Cột mới chưa duyệt làm sync báo lỗi cho tới khi có người duyệt. Cột đã duyệt (thêm vào `sources.py`) được tự thêm vào staging và Bronze, và bảng được nạp lại. Kiểu được nới thì Bronze nới theo. Thay đổi phá vỡ dừng pipeline với thông báo rõ |
| Không làm | Tự sửa model Silver hay Gold; tự đổi tên cột; CDC theo WAL |
| Phản mục tiêu | Một cột chưa duyệt (có thể là dữ liệu cá nhân) vào lakehouse. Báo cáo sai số mà không ai biết vì một cột bị bỏ. Incremental hỏng vì một thay đổi an toàn, khiến sync không bao giờ được chạy. Một lần chạy lại cho kết quả khác |

## 3. Ràng buộc

- Không sửa database nguồn; chỉ đọc.
- `sources.py` là danh sách cột được phép nạp. Hiện nó cố ý bỏ 17 cột, trong đó có dữ liệu cá nhân (`receiver_name`, `receiver_phone`, `shipping_address_text`, `delivery_staff.phone`).
- **Init và sync sở hữu schema; incremental chỉ nạp.** Đây là quy tắc mới, thay cho "chỉ init sở hữu schema".
- Idempotent ở mọi tầng, như phần 1.
- Quy ước Airflow và code của dự án (CLAUDE.md).

## 4. Các cách đã cân nhắc

**Ai được sửa schema:**

| Cách | Chọn |
|---|---|
| Sync sửa schema; incremental chỉ nạp | **Chọn** |
| Incremental tự sửa ở đầu mỗi lần chạy | Không: DAG nạp đi sửa schema |
| Chỉ phát hiện và báo; người vận hành chạy init | Không: thay đổi an toàn cũng phải chờ người |

**Cột mới ở nguồn:**

| Cách | Chọn |
|---|---|
| Danh sách cho phép: phát hiện và cảnh báo; nạp sau khi người vận hành thêm vào `sources.py` | **Chọn** (giống tuỳ chọn "Block new columns" của Fivetran) |
| Danh sách chặn: mọi cột không bị chặn đều tự vào | Không: cột nhạy cảm mới lọt vào trước khi có người chặn |
| Nạp mọi cột | Không: dữ liệu cá nhân hiện có vào lakehouse ngay |

## 5. Thiết kế

### 5.1 Phân loại khác biệt

Với mỗi bảng, so ba nguồn thông tin: cột ở nguồn (đọc `LIMIT 0`, kiểu Arrow như extract), cột trong `sources.py`, và cột của Bronze.

| Loại | Điều kiện | Phản ứng |
|---|---|---|
| `unlisted` | Có ở nguồn, không có trong `sources.py`, không thuộc 17 cột đang cố ý bỏ | **Dừng sync** (incremental vẫn chạy) |
| `approved` | Có trong `sources.py` và ở nguồn, chưa có ở Bronze | Sync thêm vào staging và Bronze, rồi nạp lại bảng |
| `widened` | Kiểu ở nguồn rộng hơn kiểu Bronze và Iceberg cho nới (`int` → `long`, `float` → `double`, `decimal` tăng độ chính xác cùng scale) | Sync nới kiểu ở staging và Bronze |
| `narrowed` | Kiểu ở nguồn hẹp hơn mà vẫn ghi được vào kiểu Bronze | Không làm gì |
| `dropped` | Có trong `sources.py` nhưng không còn ở nguồn | **Dừng** |
| `incompatible` | Đổi loại (số ↔ chữ, chữ → ngày…) | **Dừng** |

Đổi tên cột là `dropped` cộng một cột mới (`unlisted`), nên dừng ở phần `dropped`.

Để `unlisted` không báo mãi 17 cột đang bỏ, `sources.py` có thêm danh sách cột cố ý bỏ của từng bảng (`excluded`). Một cột chỉ là `unlisted` khi không nằm ở cả hai danh sách. Lỗi vì vậy chỉ xuất hiện với cột thật sự mới.

Mọi phép so sánh nằm trong một hàm dùng chung; hàm này không ghi gì.

### 5.2 Incremental: chỉ nạp, không hỏng vì thay đổi an toàn

`staging.<bảng>` so khác biệt trước khi đọc:

- `dropped` hoặc `incompatible`: báo lỗi rõ, nêu cột và cách xử lý (mục 5.5), và không đọc gì.
- Còn lại: đọc các cột vừa có trong `sources.py` vừa có ở Bronze. Cột `approved` chưa có ở Bronze thì chờ sync. Giá trị được ép về kiểu Bronze; một giá trị `widened` vượt kiểu cũ thì báo lỗi rõ, bảo trigger sync bằng tay. Trường hợp này hiếm, chỉ xảy ra trong khoảng giữa lúc nguồn đổi kiểu và lần sync tiếp theo.

### 5.3 Sync: nhóm `schema` chạy đầu tiên

Luồng: `schema` → `detect` → `bronze` → `silver` → `gold` → `maintenance`.

**`schema.<bảng>`**, mỗi bảng một task, là người ghi duy nhất vào schema của bảng đó:

1. So khác biệt.
2. `dropped` hoặc `incompatible`: báo lỗi như incremental. Lỗi này chỉ gặp khi trigger sync bằng tay, vì incremental đã dừng trước.
3. `approved` và `widened`: sửa schema staging và Bronze bằng `update_schema().union_by_name`. Sau khi thêm cột, Bronze được ghi lại một lần (một commit, đọc dạng stream), để mọi file đều có cột mới mang giá trị NULL. Lý do: PyIceberg 0.12 không upsert được vào bảng có file cũ thiếu cột.
4. `unlisted`: báo lỗi `SCHEMA table=<bảng> unlisted=[...]` sau khi đã sửa xong các thay đổi an toàn, nêu cách duyệt (mục 5.5). Sync dừng; incremental vẫn nạp các cột đã duyệt.
5. Cột của Bronze mà Silver chưa có (đọc metadata bảng Silver, không đọc dữ liệu): ghi cảnh báo `silver_missing=[...]`.

**`detect.<bảng>`** quyết định có nạp lại cả bảng hay không, dựa trên chính dữ liệu thay vì truyền cờ giữa các task:

- **Nạp lại** khi có một cột của `sources.py` mà ở Bronze **toàn NULL** (đọc từ thống kê `null_value_counts` trong metadata Iceberg, không quét dữ liệu), trong khi nguồn có ít nhất một giá trị khác NULL (`SELECT 1 … WHERE <cột> IS NOT NULL LIMIT 1`). Khi đó mọi dòng của bảng vào staging. Bronze upsert chỉ ghi lại những dòng có giá trị ở cột mới; dòng NULL không đổi, đúng với nguồn.
- **Không nạp lại:** chỉ đưa dòng khôi phục vào staging, như phần 1.

Vì tín hiệu nằm trong dữ liệu, một lần chạy hỏng giữa chừng, hay một lần clear cả run, vẫn nạp lại đúng ở lần sau. Một cột thật sự toàn NULL ở cả nguồn lẫn Bronze không bao giờ gây nạp lại.

### 5.4 Init

`create_schema` vốn thêm các cột của `sources.py` còn thiếu. Nó còn nới kiểu `widened` như sync (dùng chung hàm sửa schema), và báo lỗi `dropped`, `incompatible` như mục 5.2.

### 5.5 Quy trình cho người vận hành

| Tình huống | Việc cần làm |
|---|---|
| Lỗi `unlisted` | Quyết định có nạp hay không. Nạp thì thêm cột vào `sources.py` **và** model Silver trong cùng một commit; không nạp thì thêm vào `excluded`. Rồi Clear task `schema.<bảng>`; sync tự làm phần còn lại |
| Cảnh báo `silver_missing` | Thêm cột vào model Silver, rồi `dbt build --full-refresh` model đó (hoặc chạy init) để các dòng cũ có giá trị |
| Lỗi `dropped` | Bỏ cột khỏi `sources.py`, model Silver và mọi model Gold dùng nó, rồi Clear task. Bronze giữ cột làm lịch sử; dòng mới để trống |
| Lỗi `incompatible` | Sửa model Silver theo kiểu mới. Iceberg không đổi được loại cột, nên phải xoá bảng Bronze đó rồi chạy init; việc xoá bảng cần duyệt, và mất lịch sử xoá mềm của bảng đó |
| Lỗi giá trị vượt kiểu cũ (`widened`) | Trigger `lakehouse_oltp_sync` bằng tay, rồi Clear task incremental |

## 6. Idempotency

| Tình huống | Vì sao giữ được |
|---|---|
| Chạy lại `schema` | Lần sau không còn khác biệt nên không sửa gì |
| `schema` xong, `detect` hoặc `bronze` hỏng, rồi chạy lại hay clear cả run | Cột mới ở Bronze vẫn toàn NULL nên `detect` vẫn nạp lại cả bảng; khi đã nạp xong thì không còn toàn NULL |
| Incremental trong khoảng chờ sync | Chỉ đọc các cột đã có ở Bronze nên kết quả không phụ thuộc vào việc sync đã chạy hay chưa |

## 7. Kiểm chứng

- **Unit test** cho hàm phân loại, mỗi loại ở mục 5.1 một test. Thêm các test:
  - incremental bỏ qua cột `approved`;
  - incremental báo lỗi `dropped`;
  - `schema` thêm cột;
  - `detect` nạp lại cả bảng khi cột toàn NULL ở Bronze mà nguồn có giá trị, và không nạp lại khi cả hai đều NULL.
- **Chạy thật trên bộ 100k:**
  1. `ALTER TABLE customers ADD COLUMN loyalty_tier text` và điền giá trị: incremental vẫn xanh, `schema.customers` của sync báo lỗi `unlisted`, lakehouse không đổi.
  2. Thêm `loyalty_tier` vào `sources.py` và model Silver: sync thêm cột và nạp lại bảng, Silver có giá trị. So với init: trùng.
  3. `ALTER TABLE ... ALTER COLUMN ... TYPE bigint` cho một cột `integer`: sync nới kiểu.
  4. Bỏ một cột được nạp: incremental dừng với thông báo rõ. Làm theo mục 5.5 thì chạy tiếp được.
  5. Trả nguồn về như cũ sau khi thử.

## 8. Quyết định của người thiết kế (chưa hỏi người dùng)

- Cột chưa duyệt làm sync báo lỗi (theo yêu cầu người dùng). Cảnh báo `silver_missing` vẫn chỉ là dòng log và task vẫn xanh, vì nó không làm sai số liệu hiện có; Airflow không có trạng thái "cảnh báo".
- `excluded` nằm trong `sources.py`, cạnh `columns`, để quyết định duyệt cột nằm một chỗ.
- Đổi tên cột không được nhận diện riêng; nó là `dropped` cộng `unlisted`.
- `incompatible` cần xoá và dựng lại bảng Bronze của bảng đó, vì Iceberg không đổi được loại cột. Việc xoá bảng luôn cần người duyệt.

## 9. Cài đặt

- Hàm phân loại nằm ở `airflow/lakehouse/oltp/drift.py` (`classify`, `Drift`, `SchemaChanged`).
- Kiểu được so ở dạng Arrow. Kiểu của Bronze (`schema().as_arrow()`) được chuẩn hoá trước khi so: `large_string` thành `string`, `large_binary` thành `binary`, và múi giờ của timestamp thành `UTC`.
- Metadata Silver được đọc bằng `catalog.load_table(...).schema()`, không quét dữ liệu.
