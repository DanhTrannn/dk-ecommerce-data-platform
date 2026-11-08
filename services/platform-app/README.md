# Platform App

Package Python `platform_app` tách `api/` (FastAPI) và `ui/` (Streamlit), cùng dùng một `pyproject.toml`.

Container Python slim chạy chung FastAPI (`API_PORT`, mặc định 8000) và Streamlit (`PLATFORM_APP_PORT`, mặc định 8501). Chuẩn bị thư mục `data/generator/jobs` với quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Khởi động bằng `bash /app/start.sh`; nếu một process thoát thì process còn lại cũng dừng. Healthcheck kiểm tra cả `/health/live` của FastAPI và `/_stcore/health` của Streamlit. Schema/ORM đã chuyển PostgreSQL và được kiểm tra biệt lập, xem [database guide](../../database/README.md). Startup không tự tạo schema hoặc seed; generator 0.10, seed và importer PostgreSQL đã được kiểm tra biệt lập; các luồng API nghiệp vụ chính đã được kiểm chứng trên PostgreSQL biệt lập; chưa chứng minh pipeline đầu cuối.

`API_DATABASE_URL` bắt buộc khi chạy API, dùng `postgresql+psycopg2`. Không còn fallback URL/mật khẩu MySQL trong code. Session dùng UTC và schema public; `/health/ready` hiện chỉ kiểm tra kết nối, chưa kiểm tra schema nghiệp vụ.

Streamlit console điều khiển generator 0.10: chọn preset, chỉnh cấu hình, tạo job sinh SQL chạy nền, theo dõi theo công đoạn và tải kết quả. Console sinh file và cho nạp SQL của job vào PostgreSQL bằng thao tác riêng tại Kết quả & lịch sử. Không tự nạp khi sinh file, có màn hình Mô phỏng API cho mua online; chưa upload log hoặc kích hoạt Airflow; sampler dùng trọng số nhóm thứ hạng sản phẩm H&M, các tỷ lệ khác là giả định.

Thiết kế và quyết định kỹ thuật: [docs/project/GENERATOR_CONSOLE.md](../../docs/project/GENERATOR_CONSOLE.md).

## API nguồn

FastAPI giữ nghiệp vụ và lookup phục vụ dữ liệu; sáu API BI và mười GET danh sách/trùng lặp đã bỏ cùng service/schema riêng; giữ toàn bộ API ghi, gồm add-to-cart/checkout. Lookup customer yêu cầu email và trả customer_id; lookup địa điểm trả store_id/city_id. Các trang Next.js cũ gọi chúng nhận 404; Superset chưa triển khai. Hai API quan sát HTTP vẫn giữ quyền admin/system_admin. Chi tiết endpoint, transaction, hợp đồng tồn đầu/ledger và giới hạn kiểm chứng tại [API_SOURCE_SCOPE](../../docs/project/API_SOURCE_SCOPE.md).

## Chạy cục bộ

Từ thư mục gốc repository:

```bash
uv lock
uv run --locked --package platform-app platform-app
```

Mở `http://localhost:8501`. Mặc định job được lưu dưới `data/generator/jobs/`; đổi bằng `GENERATOR_CONSOLE_DATA_DIR`:

```bash
GENERATOR_CONSOLE_DATA_DIR=/tmp/generator-jobs \
  uv run --locked --package platform-app platform-app
```

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

Mở `http://localhost:8501` (đổi `PLATFORM_APP_PORT` nếu cần). Thư mục host `./data` được mount vào `/data`; job nằm tại `./data/generator/jobs/`. Container chạy bằng UID/GID cấu hình trong `.env` (`PLATFORM_APP_UID`, `PLATFORM_APP_GID`) để file trên host không bị root sở hữu.

Healthcheck dùng `http://localhost:8501/_stcore/health`.

## Cấu hình và validate

- Preset lấy từ `generator/configs/*.yml`.
- Các trường thường dùng: scenario, seed, anchor time, số tháng lịch sử, customers, products, variants, orders, cửa hàng tham chiếu. Chọn cửa hàng để SQL bổ sung địa điểm còn thiếu; danh sách rỗng không sinh địa điểm. Cấu hình `locations.store_codes` được đóng băng cùng job và tham gia dataset identity.
- Phân phối nâng cao sửa trong expander YAML. Chỉ khối `distributions` được thay; các trường khác lấy từ preset và form.
- Submit mới đọc form. Cấu hình cuối được ghi nguyên văn vào `config.yml` của job và là file được generator thực thi.
- Validation dùng chung `generator.config.config_from_dict` và `generator.validation.validate_scale`: timezone bắt buộc, scale hợp lệ, `variants >= products`, giới hạn tổ hợp size/color, phân phối/trọng số/tỷ lệ.
- `scenario_id` chỉ nhận chữ, số, `.`, `_`, `-` và tối đa 64 ký tự để tránh ghi ra ngoài thư mục job.

## Job và trạng thái

Mỗi lần bấm Tạo job sinh một Job ID mới dạng `<UTC timestamp>-<8 hex>`. Chỉ một job `queued`/`running` trên một console; submit trùng bị chặn bằng lock và kiểm tra PID. Refresh hoặc chuyển màn hình không tạo job; đóng tab không dừng worker.

Job status: `queued`, `running`, `succeeded`, `failed`, `interrupted`.

- `interrupted`: console/worker restart nhưng PID hoặc `/proc/<pid>/cmdline` không còn xác minh được. Artifact đã có vẫn được giữ.

Công đoạn: `validate` (ghi manifest), sau đó `sql`. Sinh log tổng hợp và profile UCI đã được bỏ; log HTTP thực từ API vẫn là luồng riêng.

## Nạp SQL vào PostgreSQL

1. Bootstrap schema nguồn trống theo [database guide](../../database/README.md), cấu hình `API_DATABASE_URL` và `API_SECRET_KEY` như `.env.example`. Container dùng URL nội bộ đã inject; chạy host dùng URL host.
2. Sinh job SQL, chờ job hoàn tất. Job cũ có trạng thái partial vẫn nạp được nếu bước SQL thành công.
3. Mở **Kết quả & lịch sử**, chọn job, xem đích database và bấm **Nạp vào PostgreSQL**.
4. Worker kiểm tra đường dẫn/checksum/dataset identity trên bản sao SQL riêng rồi gọi importer có sẵn. Xem số khách, số đơn, giá trị đơn đã đặt (VND), checksum và receipt sau commit.

Một import chạy tại một thời điểm trên thư mục console dùng chung; generation có thể chạy độc lập. Lần nạp có lịch sử riêng `imports/<attempt_id>.json`, không đổi generation state, manifest hoặc checksum sinh file. Refresh/đổi tab không tạo tác vụ nạp; đóng tab không dừng worker. Lịch sử nạp tự cập nhật mỗi 2 giây trên màn hình kết quả.

Trạng thái: `queued`, `running`, `succeeded`, `failed`, `already_present`, `unknown`. `failed` có thể thử lại bằng nút nạp sau khi sửa nguyên nhân. Dataset đã nạp thành công hoặc đã có được chặn; đổi mật khẩu/user cùng database không xóa nhận diện đích. `already_present` nghĩa là database đã có generation run, không phải lần này đã ghi thành công hoặc đã đối soát lại toàn bộ dữ liệu.

Nếu worker mất, kết nối lỗi lúc commit hoặc không lưu được receipt sau commit, trạng thái `unknown` chặn nạp lại cùng dataset/đích để người vận hành xác minh. Chưa có nút tự giải quyết trạng thái này. Lỗi kết nối trước commit cho phép thử lại; không tự retry tác vụ ghi. Thông báo không lưu SQL, credential hoặc chi tiết lỗi driver. Receipt thành công là kết quả lúc nạp, không phải đếm database realtime.

Chỉ nhận SQL trong artifact của job, không có upload SQL hoặc nhập URL database tùy ý. Bản sao tạm cần dung lượng xấp xỉ file SQL và được dọn khi worker kết thúc bình thường. Không tự tạo/sửa schema, seed, xóa dữ liệu hoặc chạy DAG. Import cần quyền bảng/sequence và chặn writer theo hợp đồng importer. CLI nạp độc lập vẫn dùng được; nếu CLI đã nạp dataset, console ghi `already_present`.

## Thư mục job

```text
data/generator/jobs/
├── .lock
├── .active.json
└── <job_id>/
    ├── imports/<attempt_id>.json  # lịch sử nạp riêng, không nằm trong checksum generation
    ├── config.yml
    ├── request.json
    ├── state.json
    ├── checksums.sha256
    ├── <scenario>-<identity>.manifest.json
    ├── logs/
    │   ├── worker.log
    │   ├── validate.log
    │   └── sql.log
    └── outputs/
        └── sql/<scenario>-<identity>.sql
```

`state.json` là nguồn sự thật; Session State chỉ giữ màn hình và job đang chọn. State ghi atomic; lịch sử còn sau restart.

## Tệp và checksum

- `checksums.sha256` chứa SHA-256 của config, request, manifest, SQL và log thực thi worker.
- UI hiển thị dung lượng/checksum; file lớn không được nạp để preview.
- File quá lớn để tải qua Streamlit sẽ hiển thị đường dẫn trên host.

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `GENERATOR_CONSOLE_DATA_DIR` | `<repo>/data/generator/jobs` | Thư mục job |
| `GENERATOR_CONFIGS_DIR` | `<repo>/generator/configs` | Thư mục preset |
| `GENERATOR_CONSOLE_REPO_ROOT` | suy ra từ package | Working directory cho worker |

## Kiểm tra

```bash
uv run --locked --package data-generator -- \
  python -m unittest discover -s generator/tests -v
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
uvx ruff check .
```

CLI nạp SQL PostgreSQL vẫn có thể chạy riêng: `uv run --locked --package platform-app -- python -m platform_app.api.db.import_dataset <sql_file>`. Xem [hướng dẫn bootstrap và nạp](../../database/README.md); console dùng lại cùng importer.

## Giới hạn

- Chỉ nạp PostgreSQL bằng thao tác riêng cho job SQL đã hoàn tất; mô phỏng API chỉ gồm mua online, chưa upload log hoặc trigger Airflow/Superset.
- Mỗi thư mục console có tối đa một job sinh file, một tác vụ nạp và một run API đồng thời; không có queue phân tán (không Celery/Redis).
- Không áp profile UCI vào sampler trong phiên bản này; profile chỉ để xem tham chiếu.

## Mô phỏng mua online qua API

Mở **Mô phỏng API**, chọn số lượt mua (1–1.000), số lượng mỗi đơn (1 SKU, 1–20 sản phẩm), thời gian nghỉ giữa các lượt, COD/VietQR và thông tin người nhận mô phỏng. Bấm **Bắt đầu mô phỏng**; đây là thao tác ghi dữ liệu vào API đích, độc lập với job SQL và bước nạp database.

Điều kiện: API đã chạy, schema đã khởi tạo, catalog có sản phẩm active và đủ tồn kho trung tâm. Khi chạy cục bộ, khởi động API ở terminal riêng bằng `uv run --locked --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log`; cấu hình `API_DATABASE_URL` và `API_SECRET_KEY` như phần API. Compose chạy sẵn API và console trong cùng container.

- `SCENARIO_API_BASE_URL`: origin API, mặc định `http://127.0.0.1:8000`; không có credential, query hoặc path. Không chỉnh đích trên form. Nếu API chạy cổng khác trên host, sửa biến này; Compose dùng cổng nội bộ 8000.
- `SCENARIO_HTTP_TIMEOUT_SECONDS`: timeout HTTP mặc định 15 giây (1–120). Không tự retry request; không dùng proxy môi trường hoặc tự theo redirect.
- Cookie name lấy từ cấu hình `API_CSRF_COOKIE_NAME` của API; origin/domain/Secure phải cho phép cookie hoạt động. API từ xa cần cùng cấu hình cookie ở worker.
- Run JSON lưu tại thư mục `scenarios/` cùng cấp với `GENERATOR_CONSOLE_DATA_DIR` (mặc định `data/generator/scenarios/<run_id>.json`), tách khỏi checksum/artifact của job generator.

Mỗi lượt có email riêng `scenario-<run_id>-<index>@example.com`, tạo tài khoản rồi đăng nhập bằng API. Mật khẩu ngẫu nhiên, cookie và token chỉ ở bộ nhớ; không ghi journal. Runner đọc catalog qua API, chọn variant đầu tiên đủ tồn trong tối đa 20 trang sản phẩm, PUT giỏ hàng, quote và checkout với idempotency key riêng đã lưu trước khi gọi. Một client/cookie jar riêng cho mỗi lượt. Khoảng nghỉ áp dụng giữa hai lượt hoàn tất, không phải giới hạn request/giây.

Sau checkout, runner lưu mã đơn rồi đọc lại đơn và giỏ. Chỉ ghi `succeeded` khi mã đơn/tổng tiền khớp quote, SKU/số lượng khớp giỏ, payment amount/status đúng và giỏ rỗng. COD tạo đơn `confirmed`/payment `pending`; VietQR theo logic API hiện có tạo `paid`/`succeeded`, không xác minh thanh toán ngân hàng thực và không tương đương đơn đã giao hay doanh thu thuần.

Mỗi thư mục chỉ một run API active. Nút **Yêu cầu dừng** chờ lượt hiện tại xong rồi dừng trước lượt sau; không rollback hoặc xóa khách/giỏ/đơn đã tạo. Đóng tab không dừng worker. Refresh không tạo run mới. Các trạng thái run: `queued`, `running`, `succeeded`, `partial`, `failed`, `stopped`, `unknown`.

HTTP 4xx là lượt thất bại (408 được coi chưa xác định); runner có thể chuyển sang lượt mới nhưng không retry lượt lỗi. Lỗi kết nối hoặc HTTP 5xx khi ghi, lỗi xác minh sau checkout hay worker mất đều có thể để lại thay đổi: đánh dấu `unknown`, dừng các lượt tiếp theo và giữ email/mã đơn/idempotency key để tra cứu. Không có chức năng resume hoặc tự gửi lại checkout; bắt đầu lần nữa tạo run mới, không giải quyết kết quả cũ. Lỗi chỉ lưu endpoint/mã HTTP, không lưu response body hay credential.

Runner hiện là kịch bản kiểm chứng nghiệp vụ, không phải sampler đã hiệu chỉnh: mỗi lượt là khách mới, một SKU và chọn hàng theo thứ tự catalog. API vẫn ghi `data_origin=manual`/`generation_run_id=NULL`; HTTP middleware ghi `observed` cho request thực sự nhận, không chứng minh hành vi người dùng thật. Run journal liên kết các giao dịch mô phỏng bằng email và idempotency key; chưa bổ sung lineage mô phỏng vào schema/API. Không suy tỷ lệ mua lại, độ phổ biến sản phẩm hay conversion tự nhiên từ kịch bản này.

Compose hiện đặt `KAFKA_ENABLED=false` cho platform-app. Runner phát sinh HTTP thật nhưng không tự bật producer, tải log lịch sử, gửi trực tiếp Kafka hoặc kích hoạt DAG. Cần bước kiểm chứng log/pipeline riêng. Chưa có fulfillment, hủy đơn, coupon, POS, nhập kho, đổi trả hoặc tự khôi phục mật khẩu tài khoản mô phỏng.

## Công cụ provisioning Lakehouse

Package `platform_app.provisioning` cung cấp CLI Polaris, tách khỏi API/UI startup và không cần `API_DATABASE_URL` hoặc `API_SECRET_KEY`. `make provision-polaris` chạy `python -m platform_app.provisioning.polaris` bằng image platform-app trong service `lakehouse-provision` (`scale: 0`, root cho việc chown file). Service này không mở port hay chạy web server; chỉ nó mount cả bốn credential output và state quản trị. API/UI bình thường không nhận những volume/secret quản trị này.

`make provision-minio` gọi script dùng `mc` có sẵn trong container MinIO. `make provision-lakehouse` chạy MinIO trước rồi Polaris; phải build image platform-app mới và chờ storage/catalog sẵn sàng trước. Không khởi tạo business schema, nạp SQL, gọi nghiệp vụ hoặc chạy DAG. Xem [hướng dẫn provisioning](../../docs/runbook/SETUP.md#6-explicit-minio-and-polaris-provisioning) cho biến môi trường, khôi phục state, quyền output và kiểm chứng.

## Quản trị quyền trên Streamlit

Mở **Quản trị quyền** và đăng nhập bằng tài khoản nguồn có role `admin` hoặc `system_admin` (schema và seed admin phải có trước). Chỉ khu vực này có bước đăng nhập mới; các màn hình generator giữ cơ chế hiện tại. Cookie/token lưu trong bộ nhớ của từng session Streamlit, không cache chung hoặc ghi file; password form được xóa sau lần submit. Đăng xuất xóa session cục bộ và cookie phía client quản trị.

Streamlit gọi service nội bộ `lakehouse-access:8010`, tách khỏi source API ở 8000. Backend dùng cùng PostgreSQL/JWT của ứng dụng để kiểm tra role/trạng thái tài khoản ở **mỗi request**, và kiểm tra CSRF khi ghi. Source API giữ nguyên router/endpoint cũ. `lakehouse-access` không publish port ra host và không mount Docker socket, state provisioning hoặc credential volume của workload. Chỉ service quản trị giữ MinIO/Polaris admin credentials. Nó chạy bằng UID/GID của platform-app, không cần root Linux.

Phạm vi:

- Polaris: xem ba workload principal, principal role, catalog role và grant hiện tại; gán/gỡ principal role, catalog role, hoặc chín quyền catalog đã được hỗ trợ. Root, `service_admin`, `catalog_admin` và role `system$...` không là đối tượng sửa trên giao diện.
- MinIO: xem bốn workload account cấu hình qua `MINIO_*_ACCESS_KEY`, trạng thái, direct policy và group membership; xem JSON của bốn policy `lakehouse-{polaris,spark,flink,trino}` và gán/gỡ chúng. Policy JSON là read-only; không có API tạo/xóa account, sửa policy, đổi password/secret hoặc chỉnh group.
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
