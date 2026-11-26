# Platform App

Package Python `platform_app` tách `api/` (FastAPI) và `ui/` (Streamlit), cùng dùng một `pyproject.toml`.

Container Python slim chạy chung FastAPI (`API_PORT`, mặc định 8000) và Streamlit (`PLATFORM_APP_PORT`, mặc định 8501). Chuẩn bị thư mục `data/scenarios` với quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Khởi động bằng `bash /app/start.sh`; nếu một process thoát thì process còn lại cũng dừng. Healthcheck kiểm tra cả `/health/live` của FastAPI và `/_stcore/health` của Streamlit. Schema/ORM đã chuyển PostgreSQL và được kiểm tra biệt lập, xem [database guide](../../database/README.md). FastAPI không tự tạo schema hoặc seed; Compose chạy `platform-admin-init` trước để khởi tạo schema trống và admin bootstrap; việc có code hoặc kiểm tra bootstrap không thay thế nghiệm thu đầy đủ mọi nghiệp vụ.

`API_DATABASE_URL` bắt buộc khi chạy API, dùng `postgresql+psycopg2`. Session dùng UTC và schema public; `/health/ready` hiện chỉ kiểm tra kết nối, chưa kiểm tra schema nghiệp vụ.

Streamlit **Platform Console** có hai màn hình: **Mô phỏng API** (mua online qua API thật) và **Quản trị quyền** (CRUD, binding Polaris và cấp credential workload). Console không còn chức năng sinh/nạp SQL của giao diện cũ. Package workspace `data-generator` hiện cung cấp CLI OLTP/log riêng, không chạy qua Streamlit.

Thiết kế và quyết định kỹ thuật: [docs/SETUP.md](../../docs/SETUP.md).

## API nguồn

FastAPI cung cấp nghiệp vụ như giỏ hàng, checkout, POS, kho, giao vận và đổi trả cùng các lookup phục vụ mô phỏng. Bốn dashboard Superset đọc 14 mart OLTP Gold qua Trino. Vận hành mặc định ghi access log ra stdout. [Hợp đồng API](src/platform_app/api/README.md) · [Danh sách dashboard](../../docs/SETUP.md#6-dashboard-superset)

## Chạy cục bộ

Từ thư mục gốc repository:

```bash
uv sync --locked --package platform-app
uv run --locked --env-file .env --package platform-app platform-app
```

Mở `http://localhost:8501`. Run mô phỏng API được lưu dưới `data/scenarios/`; đổi thư mục bằng `PLATFORM_APP_SCENARIOS_DIR` (xem bảng biến môi trường).

Có thể chạy trực tiếp Streamlit nếu package đã được cài:

```bash
uv run --locked --package platform-app -- \
  streamlit run services/platform-app/src/platform_app/ui/app.py
```

## Chạy bằng Docker Compose

Dùng `make up` để mở toàn bộ stack. Để mở riêng Console/API khi thiết lập quyền lần đầu, chạy lệnh sau.

```bash
make up SERVICES=platform-app
```

Mở `http://localhost:8501` (đổi `PLATFORM_APP_PORT` nếu cần). Thư mục host `./data` được mount vào `/data`; run mô phỏng nằm tại `./data/scenarios/`. Container chạy bằng UID/GID cấu hình trong `.env` (`PLATFORM_APP_UID`, `PLATFORM_APP_GID`) để file trên host không bị root sở hữu.

Healthcheck kiểm tra cả FastAPI `/health/live` ở cổng 8000 và Streamlit `/_stcore/health` ở cổng 8501.

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PLATFORM_APP_SCENARIOS_DIR` | `<repo>/data/scenarios` | Thư mục lưu run mô phỏng API |
| `PLATFORM_APP_REPO_ROOT` | suy ra từ package | Working directory cho worker mô phỏng |

## Kiểm tra

```bash
PYTHONPATH=services/platform-app/tests uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -t services/platform-app/tests
uvx ruff check .
```

## Giới hạn

- Mô phỏng API chỉ ghi vào nguồn qua API; không upload log hoặc trigger Airflow/Superset.
- Mỗi thư mục dữ liệu có tối đa một run API đồng thời; không có queue phân tán (không Celery/Redis).
- Không sinh/nạp dataset từ giao diện. Generator CLI đã có `generate/verify/logs/reset`, xem [generator](../../generator/README.md).

## Kịch bản mô phỏng qua API

Mở **Mô phỏng API**, chọn **Kịch bản**, số lượt (1–1.000), số lượng mỗi đơn (1 SKU, 1–20 sản phẩm), thời gian nghỉ giữa các lượt và thông tin người nhận mô phỏng (COD/VietQR chỉ chọn được ở kịch bản Mua hàng). Bấm **Bắt đầu mô phỏng**; đây là thao tác ghi dữ liệu vào API đích. Mỗi kịch bản làm thay đổi một nhóm mart Gold sau lần chạy pipeline kế tiếp.

| Kịch bản | Các bước | Mart Gold |
|---|---|---|
| Mua hàng | đăng ký → giỏ → checkout (COD hoặc VietQR) | `mart_orders_daily`, `mart_sales_daily` |
| Mua và giao thành công | mua COD → admin `dispatch` → `deliver` → khách `complete` | `mart_delivery_daily`, `mart_fulfilment_backlog_daily` |
| Giao thất bại | mua COD → `dispatch` → `failed-delivery` 3 lần (lần thứ ba đơn thành `failed_delivery`) | `mart_delivery_daily` |
| Khách huỷ đơn | mua VietQR (đơn `paid`) → khách huỷ, API hoàn tiền | `mart_cancellations_daily` |
| Mua rồi đánh giá | giao thành công → review 3–5 sao | `mart_reviews_daily` |
| Đổi trả, hoàn tiền | giao thành công → yêu cầu trả → admin duyệt → nhận hàng → kiểm và hoàn tiền | `mart_returns_daily` |
| Mua có coupon | admin tạo coupon `SIM-<run>` giảm 10% một lần mỗi run → mọi checkout dùng mã | `mart_coupon_daily` |
| Bán tại quầy | admin tìm cửa hàng còn tồn cho một SKU ngẫu nhiên (thử tối đa 3 SKU) → bán POS tiền mặt | `mart_pos_hourly` |

Mỗi lượt chọn ngẫu nhiên một sản phẩm còn hàng trong tối đa 20 trang catalog. Mã nguồn: các bước HTTP dùng chung ở [steps.py](src/platform_app/ui/scenarios/steps.py), kịch bản ở [flows.py](src/platform_app/ui/scenarios/flows.py).

Điều kiện: API đã chạy, schema đã khởi tạo, catalog có sản phẩm active và đủ tồn kho; kịch bản có bước admin cần nhân viên giao đang hoạt động (giao hàng) và tồn kho cửa hàng (POS). Khi chạy cục bộ, khởi động API ở terminal riêng bằng `uv run --locked --env-file .env --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log`; cấu hình `API_DATABASE_URL` và `API_SECRET_KEY` như phần API. Compose chạy sẵn API và console trong cùng container.

- `SCENARIO_API_BASE_URL`: origin API, mặc định `http://127.0.0.1:8000`; không có credential, query hoặc path. Không chỉnh đích trên form. Nếu API chạy cổng khác trên host, sửa biến này; Compose dùng cổng nội bộ 8000.
- `SCENARIO_HTTP_TIMEOUT_SECONDS`: timeout HTTP mặc định 15 giây (1–120). Không tự retry request; không dùng proxy môi trường hoặc tự theo redirect.
- `SCENARIO_ADMIN_EMAIL`, `SCENARIO_ADMIN_PASSWORD`: tài khoản API role `admin` chỉ dùng cho bước admin của kịch bản. `platform-admin-init` tạo tài khoản nếu chưa có (giữ mật khẩu cũ); email phải khác `API_BOOTSTRAP_ADMIN_EMAIL`, nên tài khoản này không vào được Quản trị quyền. Thiếu hai biến thì kịch bản có bước admin bị từ chối khi bắt đầu.
- Cookie name lấy từ cấu hình `API_CSRF_COOKIE_NAME` của API; origin/domain/Secure phải cho phép cookie hoạt động. API từ xa cần cùng cấu hình cookie ở worker.
- Run JSON lưu tại `PLATFORM_APP_SCENARIOS_DIR` (mặc định `data/scenarios/<run_id>.json`); `platform-admin-init` tạo thư mục này với chủ sở hữu là `PLATFORM_APP_UID`.

Mỗi lượt có khách (mọi kịch bản trừ Bán tại quầy) dùng email riêng `scenario-<run_id>-<index>@example.com`, tạo tài khoản rồi đăng nhập bằng API. Mật khẩu ngẫu nhiên, cookie và token chỉ ở bộ nhớ; không ghi journal. Admin đăng nhập một lần mỗi run; đăng nhập lỗi thì run `failed` trước khi ghi gì. Mỗi lệnh ghi có idempotency key riêng: `sim-<run_id>-<index>` cho checkout, thêm hậu tố bước (`disp`, `dlvr`, `cmpl`, `fail1`–`fail3`, `cncl`, `ret`, `rinsp`) cho các bước sau. Một client/cookie jar riêng cho mỗi lượt khách. Khoảng nghỉ áp dụng giữa hai lượt hoàn tất, không phải giới hạn request/giây.

Sau checkout, runner lưu mã đơn rồi đọc lại đơn và giỏ; mỗi bước sau kiểm trạng thái trả về (`shipping`, `delivered`, `completed`, `cancelled`, `pending_review`, `approved`, `goods_received`…). Chỉ ghi `succeeded` khi mọi bước khớp. COD tạo đơn `confirmed`/payment `pending`; VietQR theo logic API hiện có tạo `paid`/`succeeded`, không xác minh thanh toán ngân hàng thực.

Mỗi thư mục chỉ một run API active. Nút **Yêu cầu dừng** chờ lượt hiện tại xong rồi dừng trước lượt sau; không rollback hoặc xóa khách/giỏ/đơn đã tạo. Đóng tab không dừng worker. Refresh không tạo run mới. Các trạng thái run: `queued`, `running`, `succeeded`, `partial`, `failed`, `stopped`, `unknown`.

HTTP 4xx là lượt thất bại (408 được coi chưa xác định); runner có thể chuyển sang lượt mới nhưng không retry lượt lỗi. Lỗi kết nối hoặc HTTP 5xx khi ghi, trạng thái sai sau một lệnh ghi hay worker mất đều có thể để lại thay đổi: đánh dấu `unknown`, dừng các lượt tiếp theo và giữ email/mã đơn/mã return/idempotency key để tra cứu. Không có chức năng resume hoặc tự gửi lại; bắt đầu lần nữa tạo run mới, không giải quyết kết quả cũ. Lỗi chỉ lưu endpoint/mã HTTP, không lưu response body hay credential.

Runner là kịch bản kiểm chứng nghiệp vụ, không phải sampler đã hiệu chỉnh: mỗi lượt là khách mới và một SKU. API vẫn ghi `data_origin=manual`/`generation_run_id=NULL`; HTTP middleware ghi `observed` cho request thực sự nhận, không chứng minh hành vi người dùng thật. Run journal liên kết các giao dịch mô phỏng bằng email và idempotency key; chưa bổ sung lineage mô phỏng vào schema/API. Không suy tỷ lệ mua lại, độ phổ biến sản phẩm hay conversion tự nhiên từ kịch bản này.

FastAPI ghi access log ra stdout. Chưa có nhập kho hoặc tự khôi phục mật khẩu tài khoản mô phỏng. [Access log](../../docs/SETUP.md#5-access-log)

## Bootstrap ứng dụng

`platform_app.provisioning.admin` chạy trong service one-off `platform-admin-init` trước API/exporter. Nó tạo schema nguồn nếu trống, tạo admin từ `API_BOOTSTRAP_ADMIN_*`, chuẩn bị storage secret/audit và khóa ký cho exporter. Khi schema đã đủ, bootstrap giữ dữ liệu/password; schema dở dang cần migration riêng. Startup không chạy seed catalog hoặc tạo account/role/policy workload mẫu. `provisioning.trino` vẫn chuẩn bị TLS/password cho Trino khi khởi động dịch vụ đó.

## Quản trị quyền trên Streamlit

Mở **Quản trị quyền** và đăng nhập bằng admin bootstrap cấu hình ở `API_BOOTSTRAP_ADMIN_EMAIL`. API kiểm tra tài khoản active, role `admin` và đúng danh tính bootstrap ở mỗi request quản trị; admin khác hoặc `system_admin` bị từ chối. Cookie/token nằm trong bộ nhớ từng session Streamlit, không cache chung hoặc ghi file; form xóa password sau submit. Đăng xuất xóa cookie và session quản trị của client này.

Streamlit gọi FastAPI tại `http://127.0.0.1:8000/api/v1/access` trong cùng container `platform-app`. Router nằm trong `access_admin/router.py`, dùng chung cơ chế auth và xử lý lỗi với API nghiệp vụ; validation quản trị được rút gọn để không phản hồi dữ liệu bí mật. Request ghi cần CSRF. Không có API quản trị riêng ở cổng 8010.

`platform-app` chạy bằng `PLATFORM_APP_UID`/`PLATFORM_APP_GID`, giữ credential admin Polaris và mount `access-admin-secrets`. FastAPI và Streamlit dùng chung môi trường container. `credential-exporter` mount các volume credential của consumer.

Phạm vi CRUD:

- **MinIO:** không quản trị trên Console. Service `minio-init` tạo bucket, policy `lakehouse-polaris` ([file](../../infrastructure/minio/policies/lakehouse-polaris.json)) và account lưu trữ của Polaris; workload nhận credential lưu trữ tạm do Polaris cấp.
- **Polaris:** tạo/xem/sửa thuộc tính/xóa principal, principal role, catalog role và catalog; gán/gỡ liên kết và các privilege được hỗ trợ ở cấp catalog/namespace/table. Danh tính/role hệ thống được bảo vệ. Credential của principal do UI tạo được lưu trong `access-admin-secrets` và có thể xem lại bằng thao tác được audit; không tự lấy lại secret của principal bên ngoài hoặc xoay secret.
- UI hiển thị **liên kết trực tiếp**, không tính đầy đủ quyền hiệu lực từ mọi role. Gỡ một grant chưa chứng minh toàn bộ quyền đã mất nếu còn đường kế thừa khác.

Quyền hiển thị là snapshot của lần tải. Dùng **Tải lại quyền** sau thao tác từ công cụ khác. Mỗi thao tác ghi đọc trạng thái hiện tại để kiểm tra, rồi ghi, dưới khóa audit; xóa yêu cầu nhập đúng tên và bị chặn khi tài nguyên còn được tham chiếu hoặc đang cấp cho workload. Thao tác lỗi không tự gửi lại. Khóa file chỉ serialize các thao tác dùng cùng audit path, không khóa công cụ quản trị bên ngoài.

### Cấp credential Polaris cho workload

Trong tab **Cấp cho workload**, chọn principal có credential nguồn đã lưu và cấp cho `batch-airflow`, `trino-reader` hoặc `trino-writer`.

```text
Streamlit ─► platform-app API ──(JWT ký)──► credential-exporter ─► volume workload/clients.env ─► Airflow / Trino
             đọc credential principal        ghi file theo UID/GID                                    │ đăng nhập
                                                                                                      ▼
                                       credential S3 tạm, chỉ cho bảng được mở ◄─ MinIO STS ◄─ Polaris (kiểm tra role)
```

1. API đọc client ID/secret của principal trong `access-admin-secrets` (lưu lúc tạo principal) và đối chiếu với Polaris, dưới khóa audit.
2. API gửi yêu cầu tới `credential-exporter:8011` với JWT ký bằng `exporter.key`; token chứa method, path và digest của payload nên yêu cầu bị sửa sẽ bị từ chối. Payload mang `revision` trạng thái exporter; trạng thái vừa đổi thì trả 409.
3. Exporter chạy root riêng, đọc `access-admin-secrets` ở chế độ read-only, từ chối đường dẫn qua symlink, rồi ghi `clients.env` (`POLARIS_<WORKLOAD>_CLIENT_ID`/`_CLIENT_SECRET`) với file `0600`, thư mục `0700`, chủ là UID/GID của consumer, kèm `fsync`. Phân công workload → principal và audit của exporter nằm trong `credential-export-state`. `platform-app` không mount volume của consumer.
4. Consumer đọc file khi khởi động, đăng nhập Polaris; khi mở một bảng, Polaris kiểm tra quyền theo role rồi xin MinIO STS credential S3 tạm chỉ cho bảng đó. Workload không giữ key MinIO.

UI cho xem trạng thái file/ownership và principal đã cấp. Thay file đã có cần xác nhận; gỡ cần nhập đúng workload. Principal còn được phân công bị chặn xóa. Consumer giữ credential trong bộ nhớ cần khởi động lại hoặc submit lại job để áp dụng. Gỡ file không tự thu hồi quyền/secret trên Polaris.

Cấp file cập nhật credential Polaris trên volume consumer. [Thứ tự khởi động](../../docs/SETUP.md#2-khởi-động)

### Audit và chạy ứng dụng

Audit append-only cục bộ tại `data/access-admin/audit.jsonl` (0600) ghi actor, thao tác, đối tượng, operation ID và kết quả (`started`, rồi `done`, `rejected` hoặc `failed`); không ghi password/token/admin secret. Không ghi được audit trước thao tác thì không gọi upstream. Khi thao tác `failed`, tải lại quyền để xem trạng thái thật trước khi làm tiếp. Audit không phải kho chống sửa/xóa hoặc giao dịch phân tán với Polaris.

Khi chạy API trên host, dùng `uv run --locked --env-file .env --package platform-app -- uvicorn platform_app.api.main:app --host 127.0.0.1 --port 8000 --no-access-log`, rồi chạy Streamlit riêng. `CONSOLE_ACCESS_ADMIN_URL` mặc định là `http://127.0.0.1:8000`; nếu `.env` cũ còn đặt cổng 8010 thì đổi sang cổng API chung. Host cần URL Polaris (và MinIO cho cấu hình catalog), root credential Polaris, PostgreSQL/JWT và thư mục secret/audit có quyền phù hợp. Cấp credential cho volume consumer qua exporter là luồng Compose; không tự hoạt động chỉ bằng cách chạy API trên host.

### API quản trị

Các path dưới đây có prefix `/api/v1/access` trên FastAPI cổng 8000:

| Method | Path | Chức năng |
|---|---|---|
| POST | `/login`, `/logout` | Đăng nhập/đăng xuất quản trị |
| GET | `/me` | Kiểm tra danh tính và role hiện tại |
| GET | `/polaris` | Đọc snapshot quyền Polaris |
| POST | `/polaris/resources` | Tạo tài nguyên |
| PATCH | `/polaris/resources` | Sửa hoặc xóa qua trường `operation` |
| POST | `/polaris/bindings` | Gán/gỡ một liên kết |
| POST | `/polaris/credentials/{name}` | Xem lại credential principal đã lưu; cần CSRF, được audit |
| GET | `/polaris/workload-credentials` | Xem trạng thái các consumer |
| POST | `/polaris/workload-credentials` | Cấp/thay/gỡ file credential workload |
| GET | `/audit` | Đọc tối đa 50 audit record gần nhất |

Payload được kiểm tra theo từng loại tài nguyên; dữ liệu thừa bị từ chối. Thao tác ghi cần CSRF; cấp credential workload còn cần revision trạng thái của exporter. API không nhận credential quản trị upstream hay command tùy ý từ form.
