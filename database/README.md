# PostgreSQL source schema

`schema.sql` khởi tạo **28 bảng nguồn trên PostgreSQL 16**, không chứa dữ liệu. Schema/ORM đã chuyển ở phần 1; phần 2 bổ sung generator/seed/importer PostgreSQL; generator hiện ở 0.10 và đã tích hợp địa điểm vào SQL, được kiểm tra bằng database biệt lập. Các luồng API nghiệp vụ chính đã kiểm chứng trên PostgreSQL biệt lập; batch reader đã chuyển PostgreSQL; CDC và provisioning Lakehouse vẫn còn bước riêng. Xem [phạm vi API nguồn](../docs/project/API_SOURCE_SCOPE.md). Không nạp SQL MySQL cũ vào schema này.

## Định nghĩa và tái lập

- [ORM](../services/platform-app/src/platform_app/api/models/) định nghĩa cột, default, CHECK, FK và index.
- [schema.sql](schema.sql) là bản SQL bootstrap được render từ ORM, bổ sung trigger `updated_at`. `Base.metadata.create_all()` riêng lẻ không cài các trigger này.
- [OLTP_SCHEMA](../docs/architecture/OLTP_SCHEMA.md) mô tả hợp đồng dữ liệu, cursor và giới hạn chuyển đổi.
- [seeds/](seeds/) chạy thủ công, bổ sung từng bản ghi tham chiếu còn thiếu và giữ dữ liệu đã có; không tự chạy khi API khởi động.

Render hoặc kiểm tra file SQL từ thư mục gốc:

```bash
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Hai lệnh này chỉ đọc ORM và ghi/so sánh file, không kết nối database. Sau khi đổi ORM phải render lại và review diff. Cột/constraint/index được sắp xếp ổn định để kết quả tái lập qua process.

## Bootstrap database trống

Compose init chỉ tạo database/user `ecommerce`, `airflow`, `polaris` và role replication; không tự tạo bảng nghiệp vụ. Để nạp schema vào database nguồn trống của Compose:

```bash
docker compose exec -T postgres sh -c \
  'PGPASSWORD="$POSTGRES_ECOMMERCE_PASSWORD" exec psql -X -v ON_ERROR_STOP=1 --host=127.0.0.1 --username="$POSTGRES_ECOMMERCE_USER" --dbname="$POSTGRES_ECOMMERCE_DB"' \
  < database/schema.sql
```

DDL chạy trong một transaction: tạo đủ bảng trước, thêm FK sau để xử lý quan hệ vòng, rồi tạo index/function/trigger. Không tắt FK, không DROP bảng, không seed hoặc chạy pipeline. File không phải migration và không dùng để nâng cấp database đã có dữ liệu. Với database khác, dùng psql chọn đúng database/user tương ứng.

`API_DATABASE_URL` là cấu hình bắt buộc, dùng `postgresql+psycopg2`. `.env.example` có URL cho host; Compose inject URL riêng dùng hostname `postgres`. Session API đặt UTC và search_path public. Không chạy script nâng cấp timestamp MySQL cũ trên PostgreSQL.

## Seed và nạp lịch sử

Từ root repo, cấu hình `.env` cho `API_DATABASE_URL` host và `API_SECRET_KEY` như `.env.example`. Sau bootstrap schema:

```bash
# Seed catalog/admin trong một transaction; địa điểm nằm trong SQL generator
make seed
uv run --locked --package data-generator -- generator export-sql \
  --config generator/configs/small.yml --output data/generator/small.sql
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.import_dataset data/generator/small.sql
```

Trong container đã build mới, có thể dùng `docker compose exec -T platform-app python -m database.seeds.seed_catalog`. Seed địa điểm cũ đã được loại bỏ; `cities` và `stores` do generator xuất, mặc định 5 thành phố/6 cửa hàng. Chọn danh sách bằng `locations.store_codes` trong YAML hoặc trường Cửa hàng tham chiếu trên Streamlit; xem [generator guide](../generator/README.md#cities-and-stores). Nạp SQL từ container cần đường dẫn artifact bên trong container; job console ở `/data/generator/jobs/<job_id>/outputs/sql/`.

Importer chỉ nhận SQL tin cậy do generator xuất, không phải sandbox hay công cụ chạy SQL tùy ý. Kiểm tra table/cột/type/nullability/identity, index, FK/CHECK và trigger đang bật trước khi ghi. Sau đó khóa nạp, từ chối generation run đã có, thực thi và đối soát trong một transaction. JSON receipt ghi SHA-256 artifact, SHA-256 schema **kỳ vọng từ ORM**, identity, số khách/đơn và tổng giá trị đơn đã đặt; đây không phải doanh thu thực thu. Khách được đếm theo generation run, không gồm moderator tổng hợp phụ trợ.

Các cột identity thuộc 25 bảng được generator sử dụng được khởi động lại ít nhất ở MAX(id)+1, giữ mức sequence cao hơn nếu đã dùng trước đó. Dùng [`ALTER SEQUENCE RESTART`](https://www.postgresql.org/docs/16/sql-altersequence.html) vì rollback được cùng transaction; `setval` không có đặc tính đó. Bulk import chặn writer trên các bảng này và cần quyền sở hữu sequence; nên nạp trước khi chạy tác vụ ghi đồng thời. Lỗi SQL/đối soát hoặc file thiếu COMMIT sẽ rollback. Không đổi/xóa volume, sửa dữ liệu cũ hay tự chạy DAG. Import lại cùng generation run bị từ chối. Các dataset khác dùng chung địa điểm theo code, bổ sung bản ghi thiếu và giữ thông tin cũ; sai quan hệ store/city sẽ rollback. Địa điểm không được đếm là bản ghi mới: receipt ghi số địa điểm được chọn và xác minh. SQL 0.9 cũ vẫn nạp được nhưng không tự thêm địa điểm.

Schema hiện bỏ hai CHECK tồn hiện tại không vượt tồn đầu: `opening_on_hand` là mốc cố định, nhập/nhận/trả hàng đi qua ledger. Database đã có schema cũ cần được cập nhật và đối soát riêng; bootstrap không tự nâng cấp hay sửa dữ liệu.

Console cũng có thể nạp artifact của job: mở **Kết quả & lịch sử → Nạp vào PostgreSQL** sau khi sinh SQL thành công và job hoàn tất. Worker kiểm tra bản sao SQL theo checksum/dataset identity rồi gọi chính importer này; lưu receipt/lịch sử riêng, không ghi credential. Xem [hướng dẫn console](../services/platform-app/README.md#nạp-sql-vào-postgresql). Không tự tạo schema/seed hoặc tự nạp khi sinh file; trạng thái kết quả chưa xác định chặn tự nạp lại cùng dataset/đích.

## Hợp đồng dữ liệu

- UUID native, nhận `uuid.UUID` hoặc chuỗi UUID qua ORM; không lưu binary 16 byte.
- Thời gian `TIMESTAMP WITHOUT TIME ZONE` chứa UTC, tương thích cách API hiện tạo datetime UTC không tzinfo. Default/trigger dùng `timezone('UTC', statement_timestamp())`, không phụ thuộc timezone session.
- Identity dùng `GENERATED BY DEFAULT`: nhận ID tường minh khi nạp lịch sử. Importer PostgreSQL đã đồng bộ identity sau khi nạp ID tường minh bằng `ALTER SEQUENCE … RESTART`, không giảm sequence. Các công cụ nạp ID tường minh khác vẫn phải tự xử lý sequence.
- Trường unsigned-32 cũ dùng BIGINT kèm CHECK 0..4294967295; các PK/tiền/unsigned-64 cũ dùng BIGINT không âm, giới hạn tối đa 2^63−1. VND vẫn là số nguyên.
- Giữ computed column và unique guard cho mỗi khách chỉ một giỏ active. Khóa chuỗi unique dùng lower để giữ không phân biệt hoa/thường; không mô phỏng toàn bộ collation accent-insensitive của MySQL. Hành vi tra cứu/tìm kiếm thuộc phần nghiệm thu API sau.
- `updated_at` tự tăng khi dữ liệu đổi mà timestamp không được đổi tường minh; timestamp lịch sử khác giá trị cũ được giữ. No-op không tăng, kể cả bảng có cột generated. Trigger timestamp không tự tạo ledger/history nghiệp vụ.
- Có 316 cột và 51 FK, gồm `orders.staff_id` đã có trong ORM nhưng thiếu ở DDL MySQL cũ; `refunds.updated_at` cũng được đồng bộ từ ORM. Batch/CDC vẫn loại customer_credentials khỏi 27 nguồn phân tích; chuyển reader/consumer còn chờ.

## Kiểm tra

```bash
# Contract tests; PostgreSQL integration tests được skip mặc định
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v

# Có Docker và image postgres:16.8-alpine đã được tải
RUN_POSTGRES_SCHEMA_TESTS=1 uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
```

Integration tests tự tạo PostgreSQL 16.8 biệt lập với `--network none`, dữ liệu tmpfs và Unix socket riêng; không publish port, không dùng database/volume thật và không tự pull image. Container và thư mục tạm được dọn sau khi chạy. Có thể chọn image PostgreSQL 16 đã có bằng `POSTGRES_TEST_IMAGE`.

Đã kiểm tra DDL/ORM reflection, identity cả cột không phải PK, UUID/JSON roundtrip, CHECK số/boolean/tiền/tồn, FK, unique hoa-thường, giỏ active và timestamp. Phần 2 kiểm tra nạp `small`/`month-test`, rollback dữ liệu và sequence khi lỗi cuối file/thiếu COMMIT, từ chối trùng, đối soát nghiệp vụ, seed trước/sau import và seed chạy lại. Phần 3 bổ sung HTTP workflow test checkout/POS/nhập–xuất–hoàn kho và ledger, retry, quyền store và tranh tồn. Chưa kiểm chứng pipeline đầu cuối hoặc dữ liệu thật.

## Tài khoản đọc cho batch PostgreSQL

Sau bootstrap đủ bảng, đặt `POSTGRES_ECOMMERCE_READER_USER=ecommerce_reader` và password riêng trong `.env`; Compose truyền chúng vào PostgreSQL/Spark/Airflow. Cập nhật container PostgreSQL để nhận biến/mount mới rồi chạy `make source-reader`. Lệnh gọi `infrastructure/postgres/configure-source-reader.sh` bằng tài khoản quản trị PostgreSQL trong container, tạo/cấu hình role chuyên dụng trong một transaction; không tự chạy khi startup và không thay schema hay dữ liệu nghiệp vụ.

Role chỉ SELECT 27 bảng nguồn của batch, không có quyền đọc `customer_credentials` hoặc quyền ghi bảng/sequence. Không dùng role ứng dụng/admin; script từ chối role có đặc quyền, membership hoặc sở hữu database/schema/relation. Mặc định phiên là UTC/read-only. Không grant mặc định cho bảng mới; khi whitelist thay đổi phải review và chạy lại. Script có thể áp lại password cấu hình khi chạy lại; đây không phải công cụ migrate database cũ. Hướng dẫn kết nối và kiểm chứng tại [pipeline guide](../pipelines/README.md#postgresql-batch-source--07112026).
