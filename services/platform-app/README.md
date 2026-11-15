# Platform App

Package Python `platform_app` tách `api/` (FastAPI) và `ui/` (Streamlit), cùng dùng một `pyproject.toml`.

Container Python slim chạy chung FastAPI (`API_PORT`, mặc định 8000) và Streamlit (`PLATFORM_APP_PORT`, mặc định 8501). Chuẩn bị thư mục `data/scenarios` với quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Khởi động bằng `bash /app/start.sh`; nếu một process thoát thì process còn lại cũng dừng. Healthcheck kiểm tra cả `/health/live` của FastAPI và `/_stcore/health` của Streamlit. Schema/ORM đã chuyển PostgreSQL và được kiểm tra biệt lập, xem [database guide](../../database/README.md). FastAPI không tự tạo schema hoặc seed; Compose chạy `platform-admin-init` trước để khởi tạo schema trống và admin bootstrap; các luồng API nghiệp vụ chính đã được kiểm chứng trên PostgreSQL biệt lập.

`API_DATABASE_URL` bắt buộc khi chạy API, dùng `postgresql+psycopg2`. Session dùng UTC và schema public; `/health/ready` hiện chỉ kiểm tra kết nối, chưa kiểm tra schema nghiệp vụ.

Streamlit **Platform Console** có hai màn hình: **Mô phỏng API** (mua online qua API thật) và **Quản trị quyền** (CRUD, binding Polaris/MinIO và cấp credential workload). Chức năng sinh và nạp dataset SQL của console cũ đã bị gỡ cùng package `data-generator`.

Thiết kế và quyết định kỹ thuật: [docs/setup.md](../../docs/setup.md).

## API nguồn

FastAPI giữ nghiệp vụ và lookup phục vụ dữ liệu; sáu API BI và mười GET danh sách/trùng lặp đã bỏ cùng service/schema riêng; giữ toàn bộ API ghi, gồm add-to-cart/checkout. Lookup customer yêu cầu email và trả customer_id; lookup địa điểm trả store_id/city_id. Các trang Next.js cũ gọi chúng nhận 404; Superset đã có PoC doanh thu dùng hai mart; các dashboard khác và quyền viewer/store chưa triển khai. Hai API quan sát HTTP vẫn giữ quyền admin/system_admin. Chi tiết endpoint, transaction, hợp đồng tồn đầu/ledger và giới hạn kiểm chứng tại [API_SOURCE_SCOPE](../../docs/architecture.md#1-phạm-vi-và-hiện-trạng).

## Chạy cục bộ

Từ thư mục gốc repository:

```bash
uv lock
uv run --locked --package platform-app platform-app
```

Mở `http://localhost:8501`. Run mô phỏng API được lưu dưới `data/scenarios/`; đổi thư mục bằng `PLATFORM_APP_SCENARIOS_DIR` (xem bảng biến môi trường).

Có thể chạy trực tiếp Streamlit nếu package đã được cài:

```bash
uv run --locked --package platform-app -- \
  streamlit run services/platform-app/src/platform_app/ui/app.py
```

## Chạy bằng Docker Compose

Console nằm trong profile `core`:

```bash
# Console/API cùng dependency quản trị
make build
make up

# Lệnh Compose tương đương
docker compose --profile core up -d --build platform-app
```

Mở `http://localhost:8501` (đổi `PLATFORM_APP_PORT` nếu cần). Thư mục host `./data` được mount vào `/data`; run mô phỏng nằm tại `./data/scenarios/`. Container chạy bằng UID/GID cấu hình trong `.env` (`PLATFORM_APP_UID`, `PLATFORM_APP_GID`) để file trên host không bị root sở hữu.

Healthcheck kiểm tra cả FastAPI `/health/live` ở cổng 8000 và Streamlit `/_stcore/health` ở cổng 8501.

Có thể build riêng image ứng dụng bằng `docker compose --profile core build platform-app`, không cần build toàn bộ hạ tầng.

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PLATFORM_APP_SCENARIOS_DIR` | `<repo>/data/scenarios` | Thư mục lưu run mô phỏng API |
| `PLATFORM_APP_REPO_ROOT` | suy ra từ package | Working directory cho worker mô phỏng |

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
- Run JSON lưu tại `PLATFORM_APP_SCENARIOS_DIR` (mặc định `data/scenarios/<run_id>.json`).

Mỗi lượt có email riêng `scenario-<run_id>-<index>@example.com`, tạo tài khoản rồi đăng nhập bằng API. Mật khẩu ngẫu nhiên, cookie và token chỉ ở bộ nhớ; không ghi journal. Runner đọc catalog qua API, chọn variant đầu tiên đủ tồn trong tối đa 20 trang sản phẩm, PUT giỏ hàng, quote và checkout với idempotency key riêng đã lưu trước khi gọi. Một client/cookie jar riêng cho mỗi lượt. Khoảng nghỉ áp dụng giữa hai lượt hoàn tất, không phải giới hạn request/giây.

Sau checkout, runner lưu mã đơn rồi đọc lại đơn và giỏ. Chỉ ghi `succeeded` khi mã đơn/tổng tiền khớp quote, SKU/số lượng khớp giỏ, payment amount/status đúng và giỏ rỗng. COD tạo đơn `confirmed`/payment `pending`; VietQR theo logic API hiện có tạo `paid`/`succeeded`, không xác minh thanh toán ngân hàng thực và không tương đương đơn đã giao hay doanh thu thuần.

Mỗi thư mục chỉ một run API active. Nút **Yêu cầu dừng** chờ lượt hiện tại xong rồi dừng trước lượt sau; không rollback hoặc xóa khách/giỏ/đơn đã tạo. Đóng tab không dừng worker. Refresh không tạo run mới. Các trạng thái run: `queued`, `running`, `succeeded`, `partial`, `failed`, `stopped`, `unknown`.

HTTP 4xx là lượt thất bại (408 được coi chưa xác định); runner có thể chuyển sang lượt mới nhưng không retry lượt lỗi. Lỗi kết nối hoặc HTTP 5xx khi ghi, lỗi xác minh sau checkout hay worker mất đều có thể để lại thay đổi: đánh dấu `unknown`, dừng các lượt tiếp theo và giữ email/mã đơn/idempotency key để tra cứu. Không có chức năng resume hoặc tự gửi lại checkout; bắt đầu lần nữa tạo run mới, không giải quyết kết quả cũ. Lỗi chỉ lưu endpoint/mã HTTP, không lưu response body hay credential.

Runner hiện là kịch bản kiểm chứng nghiệp vụ, không phải sampler đã hiệu chỉnh: mỗi lượt là khách mới, một SKU và chọn hàng theo thứ tự catalog. API vẫn ghi `data_origin=manual`/`generation_run_id=NULL`; HTTP middleware ghi `observed` cho request thực sự nhận, không chứng minh hành vi người dùng thật. Run journal liên kết các giao dịch mô phỏng bằng email và idempotency key; chưa bổ sung lineage mô phỏng vào schema/API. Không suy tỷ lệ mua lại, độ phổ biến sản phẩm hay conversion tự nhiên từ kịch bản này.

Compose nhận `KAFKA_ENABLED` từ môi trường, mặc định `false`; bật `true` khi dùng streaming để ghi request thực qua Kafka/Flink. Runner không tự bật producer, tải log lịch sử, gửi trực tiếp Kafka hoặc kích hoạt DAG. Producer tự thử kết nối lại khi Kafka khởi động muộn và giữ lượt gửi chưa xác nhận trong DLQ dưới `data/events/`; replay DLQ vẫn là thao tác riêng. Xem [hướng dẫn log thực](../../docs/setup.md#log-api-thực). Chưa có fulfillment, hủy đơn, coupon, POS, nhập kho, đổi trả hoặc tự khôi phục mật khẩu tài khoản mô phỏng.

## Bootstrap ứng dụng

`platform_app.provisioning.admin` chạy trong service one-off `platform-admin-init` trước API/exporter. Nó tạo schema nguồn nếu trống, tạo admin từ `API_BOOTSTRAP_ADMIN_*`, chuẩn bị storage secret/audit và khóa ký cho exporter. Khi schema đã đủ, bootstrap giữ dữ liệu/password; schema dở dang cần migration riêng. Startup không chạy seed catalog hoặc tạo account/role/policy workload mẫu. `provisioning.trino` vẫn chuẩn bị TLS/password cho Trino khi khởi động dịch vụ đó.

## Quản trị quyền trên Streamlit

Mở **Quản trị quyền** và đăng nhập bằng admin bootstrap cấu hình ở `API_BOOTSTRAP_ADMIN_EMAIL`. API kiểm tra tài khoản active, role `admin` và đúng danh tính bootstrap ở mỗi request quản trị; admin khác hoặc `system_admin` bị từ chối. Cookie/token nằm trong bộ nhớ từng session Streamlit, không cache chung hoặc ghi file; form xóa password sau submit. Đăng xuất xóa cookie và session quản trị của client này.

Streamlit gọi FastAPI tại `http://127.0.0.1:8000/api/v1/access` trong cùng container `platform-app`. Router nằm trong `access_admin/router.py`, dùng chung cơ chế auth và xử lý lỗi với API nghiệp vụ; validation quản trị được rút gọn để không phản hồi dữ liệu bí mật. Request ghi cần CSRF. Không có API quản trị riêng ở cổng 8010.

`platform-app` chạy bằng `PLATFORM_APP_UID`/`PLATFORM_APP_GID`, giữ credential admin MinIO/Polaris và mount `access-admin-secrets`. FastAPI và Streamlit dùng chung môi trường container; việc gộp giảm một container/Uvicorn nhưng cũng đưa credential quản trị vào container ứng dụng. Không mount Docker socket hoặc bốn volume credential consumer vào `platform-app`.

Phạm vi CRUD:

- **MinIO:** tạo/xem user, bucket và policy; bật/tắt user; sửa policy bằng form bucket/prefix/action; sửa tag bucket; xóa tài nguyên khi các điều kiện phụ thuộc/dữ liệu cho phép. JSON policy hiển thị trên UI và được lưu trong MinIO; các file `infrastructure/minio/policies/*.json` chỉ là ví dụ, không tự cập nhật khi sửa trên UI. Có gán/gỡ policy trực tiếp; không quản trị group hoặc đổi password/secret user đã có.
- **Polaris:** tạo/xem/sửa thuộc tính/xóa principal, principal role, catalog role và catalog; gán/gỡ liên kết và các privilege được hỗ trợ ở cấp catalog/namespace/table. Danh tính/role hệ thống được bảo vệ. Credential của principal do UI tạo được lưu trong `access-admin-secrets` và có thể xem lại bằng thao tác được audit; không tự lấy lại secret của principal bên ngoài hoặc xoay secret.
- UI hiển thị **liên kết trực tiếp**, không tính đầy đủ quyền hiệu lực từ mọi role/group/policy. Gỡ một grant/policy chưa chứng minh toàn bộ quyền đã mất nếu còn đường kế thừa khác.

Quyền hiển thị là snapshot của lần tải. Dùng **Tải lại quyền** sau thao tác từ công cụ khác. Backend đọc lại trạng thái và so revision trước khi ghi; revision lệch trả 409. Sửa policy cần xác nhận tác động; xóa yêu cầu nhập đúng tên, có kiểm tra tài nguyên còn được tham chiếu hoặc đang cấp cho workload. Backend đọc lại sau thay đổi để xác minh, không tự retry thao tác có kết quả chưa rõ. Khóa file chỉ serialize các thao tác dùng cùng audit path, không khóa công cụ quản trị bên ngoài.

### Cấp credential Polaris cho workload

Tạo principal chưa tự cấu hình consumer. Trong tab **Cấp cho workload**, chọn principal có credential nguồn đã lưu và cấp cho `batch-airflow`, `flink`, `trino-reader` hoặc `trino-writer`. API ký yêu cầu gửi tới `credential-exporter:8011`; exporter chạy root riêng, đọc `access-admin-secrets` ở chế độ read-only và ghi file `clients.env` với UID/GID tương ứng vào bốn volume consumer. Manifest/audit exporter nằm trong `credential-export-state`.

UI cho xem trạng thái file/ownership và principal đã cấp. Thay file đã có cần xác nhận; gỡ cần nhập đúng workload. Principal còn được phân công bị chặn xóa. Consumer giữ credential trong bộ nhớ cần khởi động lại hoặc submit lại job để áp dụng. Gỡ file không tự thu hồi quyền/secret trên Polaris.

Cấp file chỉ xử lý **credential Polaris**. MinIO user/secret phải khớp các biến `MINIO_*` consumer nhận từ `.env`; thao tác trên Streamlit không tự sửa `.env` hay cấu hình môi trường của container đã chạy. Xem [setup](../../docs/setup.md#2-build-và-chọn-profile) cho volume và thứ tự khởi động.

### Audit và chạy ứng dụng

Audit append-only cục bộ tại `data/access-admin/audit.jsonl` (0600) ghi actor, thao tác, đối tượng, operation ID và kết quả; không ghi password/token/admin secret. Không ghi được audit trước thao tác thì không gọi upstream. Nếu kết quả chưa xác minh, kiểm tra trạng thái thật và operation ID trước khi thao tác tiếp. Audit không phải kho chống sửa/xóa hoặc giao dịch phân tán với MinIO/Polaris.

```bash
make up
# Nâng cấp từ bản có container quản trị riêng, giữ named volumes:
docker compose --profile core up -d --build --wait --remove-orphans platform-app
```

`make up` chạy console/API và các dependency quản trị; chỉ dùng `make up-all` sau khi cấu hình quyền/credential cho workload. Image mang sẵn binary `mc`; adapter gọi các subcommand cố định, không nhận command tùy ý. Credential MinIO được truyền qua môi trường tiến trình ngắn hạn thay vì argv.

Khi chạy API trên host, dùng `uv run --locked --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log`, rồi chạy Streamlit riêng. `CONSOLE_ACCESS_ADMIN_URL` mặc định là `http://127.0.0.1:8000`; nếu `.env` cũ còn đặt cổng 8010 thì đổi sang cổng API chung. Host cần binary `mc` (`ACCESS_ADMIN_MC_BINARY`), URL host MinIO/Polaris, root credential, PostgreSQL/JWT và thư mục secret/audit có quyền phù hợp. Cấp credential cho volume consumer qua exporter là luồng Compose; không tự hoạt động chỉ bằng cách chạy API trên host.

### API quản trị

Các path dưới đây có prefix `/api/v1/access` trên FastAPI cổng 8000:

| Method | Path | Chức năng |
|---|---|---|
| POST | `/login`, `/logout` | Đăng nhập/đăng xuất quản trị |
| GET | `/me` | Kiểm tra danh tính và role hiện tại |
| GET | `/polaris`, `/minio` | Đọc snapshot quyền và revision |
| POST | `/{provider}/resources` | Tạo tài nguyên |
| PATCH | `/{provider}/resources` | Sửa hoặc xóa qua trường `operation` |
| POST | `/{provider}/bindings` | Gán/gỡ một liên kết |
| POST | `/polaris/credentials/{name}` | Xem lại credential principal đã lưu; cần CSRF, được audit |
| GET | `/polaris/workload-credentials` | Xem trạng thái các consumer |
| POST | `/polaris/workload-credentials` | Cấp/thay/gỡ file credential workload |
| GET | `/audit` | Đọc tối đa 50 audit record gần nhất |

Payload được kiểm tra theo từng loại tài nguyên; dữ liệu thừa bị từ chối. Thao tác thay đổi tài nguyên/liên kết/workload cần revision hiện tại và CSRF. API không nhận credential quản trị upstream, command tùy ý hoặc policy JSON tự do từ form.

Kiểm chứng ngày 15/11/2026: suite local 86 test (5 test opt-in bỏ qua), bốn test tích hợp riêng đạt trên stack PostgreSQL/MinIO/Polaris/API/exporter cô lập. Các test kiểm tra CRUD, quyền MinIO thực, cấp/gỡ credential và thao tác Streamlit qua API chung. Build image, Ruff và cấu hình Compose đạt. Stack/image/volume kiểm thử đã được dọn; kiểm chứng này không bao gồm toàn pipeline hoặc triển khai public.
