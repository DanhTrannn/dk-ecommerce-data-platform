# Platform App

Package Python `platform_app` tách `api/` (FastAPI) và `ui/` (Streamlit), cùng dùng một `pyproject.toml`.

Container Python slim chạy chung FastAPI (`API_PORT`, mặc định 8000) và Streamlit (`PLATFORM_APP_PORT`, mặc định 8501). Chuẩn bị thư mục `data/generator` với quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Khởi động bằng `bash /app/start.sh`; nếu một process thoát thì process còn lại cũng dừng. Healthcheck kiểm tra cả `/health/live` của FastAPI và `/_stcore/health` của Streamlit. Schema/ORM đã chuyển PostgreSQL và được kiểm tra biệt lập, xem [database guide](../../database/README.md). Startup không tự tạo schema hoặc seed; seed PostgreSQL đã được kiểm tra biệt lập; các luồng API nghiệp vụ chính đã được kiểm chứng trên PostgreSQL biệt lập; batch và log API thực đã có nghiệm thu stack biệt lập; xem phạm vi bằng chứng trong [operations](../../docs/operations.md#8-bằng-chứng-nghiệm-thu-đã-ghi-nhận).

`API_DATABASE_URL` bắt buộc khi chạy API, dùng `postgresql+psycopg2`. Không còn fallback URL/mật khẩu MySQL trong code. Session dùng UTC và schema public; `/health/ready` hiện chỉ kiểm tra kết nối, chưa kiểm tra schema nghiệp vụ.

Streamlit **Platform Console** có hai màn hình: **Mô phỏng API** (mua online qua API thật) và **Quản trị quyền** (binding Polaris/MinIO hiện có). Chức năng sinh và nạp dataset SQL của console cũ đã bị gỡ cùng package `data-generator`.

Thiết kế và quyết định kỹ thuật: [docs/setup.md](../../docs/setup.md).

## API nguồn

FastAPI giữ nghiệp vụ và lookup phục vụ dữ liệu; sáu API BI và mười GET danh sách/trùng lặp đã bỏ cùng service/schema riêng; giữ toàn bộ API ghi, gồm add-to-cart/checkout. Lookup customer yêu cầu email và trả customer_id; lookup địa điểm trả store_id/city_id. Các trang Next.js cũ gọi chúng nhận 404; Superset đã có PoC doanh thu dùng hai mart; các dashboard khác và quyền viewer/store chưa triển khai. Hai API quan sát HTTP vẫn giữ quyền admin/system_admin. Chi tiết endpoint, transaction, hợp đồng tồn đầu/ledger và giới hạn kiểm chứng tại [API_SOURCE_SCOPE](../../docs/architecture.md#1-phạm-vi-và-hiện-trạng).

## Chạy cục bộ

Từ thư mục gốc repository:

```bash
uv lock
uv run --locked --package platform-app platform-app
```

Mở `http://localhost:8501`. Run mô phỏng API được lưu dưới `data/generator/scenarios/`; đổi thư mục bằng `GENERATOR_CONSOLE_DATA_DIR` (xem bảng biến môi trường).

Có thể chạy trực tiếp Streamlit nếu package đã được cài:

```bash
uv run --locked --package platform-app -- \
  streamlit run services/platform-app/src/platform_app/ui/app.py
```

## Chạy bằng Docker Compose

Console nằm trong profile `core`:

```bash
# Cùng các dịch vụ core
make build
make up PROFILES=core

# Hoặc chỉ console
docker compose --profile core up -d --build platform-app
```

Mở `http://localhost:8501` (đổi `PLATFORM_APP_PORT` nếu cần). Thư mục host `./data` được mount vào `/data`; run mô phỏng nằm tại `./data/generator/scenarios/`. Container chạy bằng UID/GID cấu hình trong `.env` (`PLATFORM_APP_UID`, `PLATFORM_APP_GID`) để file trên host không bị root sở hữu.

Healthcheck dùng `http://localhost:8501/_stcore/health`.

Có thể build riêng image ứng dụng bằng `docker compose --profile core build platform-app`, không cần build toàn bộ hạ tầng.

## Biến môi trường

Tên biến giữ tiền tố `GENERATOR_CONSOLE_` từ console cũ.

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `GENERATOR_CONSOLE_DATA_DIR` | `<repo>/data/generator/jobs` | Mốc thư mục dữ liệu; run mô phỏng API lưu ở thư mục `scenarios/` cùng cấp |
| `GENERATOR_CONSOLE_REPO_ROOT` | suy ra từ package | Working directory cho worker mô phỏng |

## Kiểm tra

Test chỉ giữ local (mọi thư mục `tests/` nằm trong `.gitignore`):

```bash
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
uvx ruff check .
```

## Giới hạn

- Mô phỏng API chỉ gồm mua online; chưa upload log hoặc trigger Airflow/Superset.
- Mỗi thư mục dữ liệu có tối đa một run API đồng thời; không có queue phân tán (không Celery/Redis).
- Không còn sinh hoặc nạp dataset SQL; generator mới đang chờ triển khai, xem [generator](../../generator/README.md).

## Mô phỏng mua online qua API

Mở **Mô phỏng API**, chọn số lượt mua (1–1.000), số lượng mỗi đơn (1 SKU, 1–20 sản phẩm), thời gian nghỉ giữa các lượt, COD/VietQR và thông tin người nhận mô phỏng. Bấm **Bắt đầu mô phỏng**; đây là thao tác ghi dữ liệu vào API đích.

Điều kiện: API đã chạy, schema đã khởi tạo, catalog có sản phẩm active và đủ tồn kho trung tâm. Khi chạy cục bộ, khởi động API ở terminal riêng bằng `uv run --locked --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log`; cấu hình `API_DATABASE_URL` và `API_SECRET_KEY` như phần API. Compose chạy sẵn API và console trong cùng container.

- `SCENARIO_API_BASE_URL`: origin API, mặc định `http://127.0.0.1:8000`; không có credential, query hoặc path. Không chỉnh đích trên form. Nếu API chạy cổng khác trên host, sửa biến này; Compose dùng cổng nội bộ 8000.
- `SCENARIO_HTTP_TIMEOUT_SECONDS`: timeout HTTP mặc định 15 giây (1–120). Không tự retry request; không dùng proxy môi trường hoặc tự theo redirect.
- Cookie name lấy từ cấu hình `API_CSRF_COOKIE_NAME` của API; origin/domain/Secure phải cho phép cookie hoạt động. API từ xa cần cùng cấu hình cookie ở worker.
- Run JSON lưu tại thư mục `scenarios/` cùng cấp với `GENERATOR_CONSOLE_DATA_DIR` (mặc định `data/generator/scenarios/<run_id>.json`).

Mỗi lượt có email riêng `scenario-<run_id>-<index>@example.com`, tạo tài khoản rồi đăng nhập bằng API. Mật khẩu ngẫu nhiên, cookie và token chỉ ở bộ nhớ; không ghi journal. Runner đọc catalog qua API, chọn variant đầu tiên đủ tồn trong tối đa 20 trang sản phẩm, PUT giỏ hàng, quote và checkout với idempotency key riêng đã lưu trước khi gọi. Một client/cookie jar riêng cho mỗi lượt. Khoảng nghỉ áp dụng giữa hai lượt hoàn tất, không phải giới hạn request/giây.

Sau checkout, runner lưu mã đơn rồi đọc lại đơn và giỏ. Chỉ ghi `succeeded` khi mã đơn/tổng tiền khớp quote, SKU/số lượng khớp giỏ, payment amount/status đúng và giỏ rỗng. COD tạo đơn `confirmed`/payment `pending`; VietQR theo logic API hiện có tạo `paid`/`succeeded`, không xác minh thanh toán ngân hàng thực và không tương đương đơn đã giao hay doanh thu thuần.

Mỗi thư mục chỉ một run API active. Nút **Yêu cầu dừng** chờ lượt hiện tại xong rồi dừng trước lượt sau; không rollback hoặc xóa khách/giỏ/đơn đã tạo. Đóng tab không dừng worker. Refresh không tạo run mới. Các trạng thái run: `queued`, `running`, `succeeded`, `partial`, `failed`, `stopped`, `unknown`.

HTTP 4xx là lượt thất bại (408 được coi chưa xác định); runner có thể chuyển sang lượt mới nhưng không retry lượt lỗi. Lỗi kết nối hoặc HTTP 5xx khi ghi, lỗi xác minh sau checkout hay worker mất đều có thể để lại thay đổi: đánh dấu `unknown`, dừng các lượt tiếp theo và giữ email/mã đơn/idempotency key để tra cứu. Không có chức năng resume hoặc tự gửi lại checkout; bắt đầu lần nữa tạo run mới, không giải quyết kết quả cũ. Lỗi chỉ lưu endpoint/mã HTTP, không lưu response body hay credential.

Runner hiện là kịch bản kiểm chứng nghiệp vụ, không phải sampler đã hiệu chỉnh: mỗi lượt là khách mới, một SKU và chọn hàng theo thứ tự catalog. API vẫn ghi `data_origin=manual`/`generation_run_id=NULL`; HTTP middleware ghi `observed` cho request thực sự nhận, không chứng minh hành vi người dùng thật. Run journal liên kết các giao dịch mô phỏng bằng email và idempotency key; chưa bổ sung lineage mô phỏng vào schema/API. Không suy tỷ lệ mua lại, độ phổ biến sản phẩm hay conversion tự nhiên từ kịch bản này.

Compose nhận `KAFKA_ENABLED` từ môi trường, mặc định `false`; bật `true` khi dùng streaming để ghi request thực qua Kafka/Flink. Runner không tự bật producer, tải log lịch sử, gửi trực tiếp Kafka hoặc kích hoạt DAG. Producer tự thử kết nối lại khi Kafka khởi động muộn và giữ lượt gửi chưa xác nhận trong DLQ dưới `data/events/`; replay DLQ vẫn là thao tác riêng. Xem [hướng dẫn log thực](../../docs/setup.md#log-api-thực). Chưa có fulfillment, hủy đơn, coupon, POS, nhập kho, đổi trả hoặc tự khôi phục mật khẩu tài khoản mô phỏng.

## Công cụ provisioning Lakehouse

Package `platform_app.provisioning` cung cấp CLI Polaris, tách khỏi API/UI startup và không cần `API_DATABASE_URL` hoặc `API_SECRET_KEY`. `make provision-polaris` chạy `python -m platform_app.provisioning.polaris` bằng image platform-app trong service `lakehouse-provision` (`scale: 0`, root cho việc chown file). Service này không mở port hay chạy web server; chỉ nó mount cả bốn credential output và state quản trị. API/UI bình thường không nhận những volume/secret quản trị này.

`make provision-minio` gọi script dùng `mc` có sẵn trong container MinIO. `make provision-lakehouse` chạy MinIO trước rồi Polaris; phải build image platform-app mới và chờ storage/catalog sẵn sàng trước. Không khởi tạo business schema, nạp SQL, gọi nghiệp vụ hoặc chạy DAG. Xem [hướng dẫn provisioning](../../docs/setup.md#2-build-và-chọn-profile) cho biến môi trường, khôi phục state, quyền output và kiểm chứng.

## Quản trị quyền trên Streamlit

Mở **Quản trị quyền** và đăng nhập bằng tài khoản nguồn có role `admin` hoặc `system_admin` (schema và seed admin phải có trước). Chỉ khu vực này có bước đăng nhập mới; các màn hình generator giữ cơ chế hiện tại. Cookie/token lưu trong bộ nhớ của từng session Streamlit, không cache chung hoặc ghi file; password form được xóa sau lần submit. Đăng xuất xóa session cục bộ và cookie phía client quản trị.

Streamlit gọi service nội bộ `lakehouse-access:8010`, tách khỏi source API ở 8000. Backend dùng cùng PostgreSQL/JWT của ứng dụng để kiểm tra role/trạng thái tài khoản ở **mỗi request**, và kiểm tra CSRF khi ghi. Source API giữ nguyên router/endpoint cũ. `lakehouse-access` không publish port ra host và không mount Docker socket, state provisioning hoặc credential volume của workload. Chỉ service quản trị giữ MinIO/Polaris admin credentials. Nó chạy bằng UID/GID của platform-app, không cần root Linux.

Phạm vi:

- Polaris: xem ba workload principal, principal role, catalog role và grant hiện tại; gán/gỡ principal role, catalog role, hoặc chín quyền catalog đã được hỗ trợ. Root, `service_admin`, `catalog_admin` và role `system$...` không là đối tượng sửa trên giao diện.
- MinIO: xem bốn workload account cấu hình qua `MINIO_*_ACCESS_KEY`, trạng thái, direct policy và group membership; xem JSON của bốn policy `lakehouse-{polaris,batch,flink,trino}` và gán/gỡ chúng. Policy JSON là read-only; không có API tạo/xóa account, sửa policy, đổi password/secret hoặc chỉnh group.
- Hiển thị **liên kết trực tiếp**, không tính đầy đủ quyền hiệu lực từ mọi role/group/policy. Gỡ một grant/policy không chứng minh toàn bộ quyền truy cập đã mất nếu còn đường kế thừa khác.

Quyền hiển thị là snapshot của lần tải; dùng **Tải lại quyền** khi có thao tác từ công cụ khác. Backend đọc lại trạng thái và so revision trước khi ghi; revision lệch trả 409 và yêu cầu tải lại. Một thay đổi chỉ tác động một liên kết, sau đó đọc lại để xác minh; không tự retry khi mất kết nối hoặc chưa rõ kết quả. Khóa file serialize các thao tác của backend dùng chung audit path, không phải khóa mọi Console/API bên ngoài.

Audit append-only cục bộ tại `data/access-admin/audit.jsonl` (0600) ghi actor ID/role, loại thao tác, đối tượng, operation ID và trạng thái `started`, `verified`, `noop`, `unknown`; không ghi password/token/admin secret. Không ghi được audit trước thao tác thì không gọi upstream. Nếu ghi thành công nhưng chưa lưu/xác minh kết quả cuối, phải kiểm tra trạng thái thật và operation ID trước khi thao tác tiếp. Audit không phải kho chống sửa/xóa hoặc giao dịch phân tán với Polaris/MinIO.

Chạy Docker:

```bash
docker compose --profile core build platform-app
docker compose --profile core up -d platform-app lakehouse-access
```

Cần MinIO/Polaris đã provisioning và `data/` có quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Image platform-app copy `mc` từ chính image MinIO đang dùng, không thêm Python dependency/image runtime mới. MinIO adapter chỉ chạy các subcommand đọc và attach/detach cố định; credential truyền qua môi trường tiến trình ngắn hạn, không qua argv hay output UI.

Chạy console trên host dùng `CONSOLE_ACCESS_ADMIN_URL` (mặc định `http://127.0.0.1:8010`), nhưng service Compose cố ý không publish cổng này. Nếu chạy API quản trị riêng trên host: `uv run --locked --package platform-app -- uvicorn platform_app.access_admin.app:app --host 127.0.0.1 --port 8010 --no-access-log`; cần cung cấp `mc` qua `ACCESS_ADMIN_MC_BINARY`, URL MinIO/Polaris host qua `ACCESS_ADMIN_MINIO_URL`/`ACCESS_ADMIN_POLARIS_URL`, các root credential và cấu hình PostgreSQL/JWT. Quy trình Docker ở trên là cách có sẵn binary/client đầy đủ.

API nội bộ (prefix `/api/v1/access`):

| Method | Path | Chức năng |
|---|---|---|
| POST | `/login`, `/logout` | Đăng nhập/đăng xuất quản trị |
| GET | `/me` | Kiểm tra danh tính và role hiện tại |
| GET | `/polaris`, `/minio` | Đọc snapshot quyền và revision |
| POST | `/{provider}/bindings` | Gán/gỡ một liên kết có revision; cần CSRF |
| GET | `/audit` | Đọc tối đa 50 audit record gần nhất |

Mỗi request ghi chứa `kind`, `subject`, `target`, `granted`, `revision`; dữ liệu thừa bị từ chối. API không nhận admin credential, command tùy ý hay policy JSON từ form.

Kiểm tra biệt lập:

```bash
uv run --locked --package platform-app -- python -m unittest discover -s services/platform-app/tests -p test_access.py -v
RUN_POSTGRES_SCHEMA_TESTS=1 uv run --locked --package platform-app -- python -m unittest discover -s services/platform-app/tests -p test_auth_postgres.py -v
docker build -f services/platform-app/Dockerfile -t tlcn-access-check:local .
RUN_ACCESS_TESTS=1 uv run --locked --package platform-app -- python -m unittest discover -s services/platform-app/tests -p test_live_access.py -v
```

Live check tái sử dụng stack provisioning tạm, thêm business schema public vào database fixture riêng để kiểm tra auth; không thay đổi source/metadata đang chạy. Kiểm chứng này không nghiệm thu deployment public, toàn pipeline hoặc tính bất biến của audit.

Kết quả kiểm chứng ngày 08/11/2026: 113 tests platform-app đạt với các cờ PostgreSQL/provisioning/access bật; riêng stack quyền xác minh 9 thay đổi/18 audit record và 4 thay đổi nữa bằng UID 1000. MinIO trả thứ tự action/resource không ổn định nên policy được chuẩn hóa thứ tự khi tính revision; việc đảo thứ tự JSON không bị coi là thay đổi quyền. Source/volume đang dùng không được chỉnh bởi các kiểm tra này.
