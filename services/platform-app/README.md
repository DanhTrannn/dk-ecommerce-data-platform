# Platform App

Package Python `platform_app` tách `api/` (FastAPI) và `ui/` (Streamlit), cùng dùng một `pyproject.toml`.

Container Python slim chạy chung FastAPI (`API_PORT`, mặc định 8000) và Streamlit (`PLATFORM_APP_PORT`, mặc định 8501). Chuẩn bị thư mục `data/generator/jobs` với quyền ghi cho `PLATFORM_APP_UID`/`PLATFORM_APP_GID`. Khởi động bằng `bash /app/start.sh`; nếu một process thoát thì process còn lại cũng dừng. Healthcheck kiểm tra cả `/health/live` của FastAPI và `/_stcore/health` của Streamlit. Schema/ORM đã chuyển PostgreSQL và được kiểm tra biệt lập, xem [database guide](../../database/README.md). Startup không tự tạo schema hoặc seed; generator SQL, seed và nghiệm thu nghiệp vụ vẫn còn ở phần sau.

`API_DATABASE_URL` bắt buộc khi chạy API, dùng `postgresql+psycopg2`. Không còn fallback URL/mật khẩu MySQL trong code. Session dùng UTC và schema public; `/health/ready` hiện chỉ kiểm tra kết nối, chưa kiểm tra schema nghiệp vụ.

Streamlit console điều khiển generator 0.8: chọn preset, chỉnh cấu hình, tạo job sinh SQL/access log chạy nền, theo dõi theo công đoạn và tải kết quả. Console chỉ đọc/ghi file; không import MySQL, không chạy traffic, không upload log, không kích hoạt Airflow và không thay đổi sampler 2c.

Thiết kế và quyết định kỹ thuật: [docs/project/GENERATOR_CONSOLE.md](../../docs/project/GENERATOR_CONSOLE.md).

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
- Các trường thường dùng: scenario, seed, anchor time, số tháng lịch sử, customers, products, variants, orders, output mode, expected requests.
- Phân phối nâng cao sửa trong expander YAML. Chỉ khối `distributions` được thay; các trường khác lấy từ preset và form.
- Submit mới đọc form. Cấu hình cuối được ghi nguyên văn vào `config.yml` của job và là file được generator thực thi.
- Validation dùng chung `generator.config.config_from_dict` và `generator.validation.validate_scale`: timezone bắt buộc, scale hợp lệ, `variants >= products`, giới hạn tổ hợp size/color, phân phối/trọng số/tỷ lệ.
- `scenario_id` chỉ nhận chữ, số, `.`, `_`, `-` và tối đa 64 ký tự để tránh ghi ra ngoài thư mục job.
- `expected_requests = 0` tự động thành `max(1000, orders × 20)`.

## Job và trạng thái

Mỗi lần bấm Tạo job sinh một Job ID mới dạng `<UTC timestamp>-<8 hex>`. Chỉ một job `queued`/`running` trên một console; submit trùng bị chặn bằng lock và kiểm tra PID. Refresh hoặc chuyển màn hình không tạo job; đóng tab không dừng worker.

Job status: `queued`, `running`, `succeeded`, `partial`, `failed`, `interrupted`.

- `partial`: ít nhất một công đoạn được chọn thành công và một công đoạn thất bại. SQL thành công nhưng log lỗi vẫn giữ SQL.
- `interrupted`: console/worker restart nhưng PID hoặc `/proc/<pid>/cmdline` không còn xác minh được. Artifact đã có vẫn được giữ.

Công đoạn: `validate` (ghi manifest), `sql`, `logs`. SQL và log chạy độc lập sau khi validate thành công.

## Thư mục job

```text
data/generator/jobs/
├── .lock
├── .active.json
└── <job_id>/
    ├── config.yml
    ├── request.json
    ├── state.json
    ├── checksums.sha256
    ├── <scenario>-<identity>.manifest.json
    ├── logs/
    │   ├── worker.log
    │   ├── validate.log
    │   ├── sql.log
    │   └── logs.log
    └── outputs/
        ├── sql/<scenario>-<identity>.sql
        ├── access-logs/...
        └── access-logs.zip     # nếu tổng log <= ngưỡng
```

`state.json` là nguồn sự thật; Session State chỉ giữ màn hình và job đang chọn. State ghi atomic; lịch sử còn sau restart.

## Tệp và checksum

- `checksums.sha256` chứa SHA-256 của config, request, manifest, SQL, log, archive.
- UI hiển thị dung lượng/checksum; file lớn không được nạp để preview.
- Access log được tóm tắt theo thư mục (số file, tổng dung lượng, checksum cây). Zip chỉ được tạo khi tổng dung lượng không vượt `GENERATOR_CONSOLE_MAX_ARCHIVE_BYTES` (mặc định 200 MB).
- File quá lớn để tải qua Streamlit sẽ hiển thị đường dẫn trên host.

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `GENERATOR_CONSOLE_DATA_DIR` | `<repo>/data/generator/jobs` | Thư mục job |
| `GENERATOR_CONFIGS_DIR` | `<repo>/generator/configs` | Thư mục preset |
| `GENERATOR_PROFILE_PATH` | `<repo>/generator/profiles/uci-online-retail-uk-v1.json` | Profile tham chiếu |
| `GENERATOR_CONSOLE_MAX_ARCHIVE_BYTES` | `209715200` | Ngưỡng tạo zip access log |
| `GENERATOR_CONSOLE_REPO_ROOT` | suy ra từ package | Working directory cho worker |

## Kiểm tra

```bash
uv run --locked --package data-generator -- \
  python -m unittest discover -s generator/tests -v
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
uvx ruff check .
```

## Giới hạn

- Không nạp SQL vào MySQL, không chạy traffic, không upload log, không trigger Airflow/Superset.
- Không hỗ trợ nhiều job song song hoặc queue phân tán (không Celery/Redis).
- Không áp profile UCI vào sampler trong phiên bản này; profile chỉ để xem tham chiếu.
