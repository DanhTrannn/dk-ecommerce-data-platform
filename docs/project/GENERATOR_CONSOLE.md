# Generator Console — thiết kế đợt 3

Cập nhật **07/11/2026**. Streamlit Generator Console dùng generator 0.10 và có bước nạp SQL PostgreSQL riêng; phần thiết kế sinh file ban đầu được giữ bên dưới. Phạm vi và lý do chuyển đổi nằm trong [refactor_v2](refactor_v2.md); profile tham chiếu nằm trong [GENERATOR_REFERENCE_PROFILE](GENERATOR_REFERENCE_PROFILE.md).

## 1. Mục tiêu

Cho người dùng chọn preset, chỉnh các tham số thường dùng, kiểm tra cấu hình, tạo job sinh dữ liệu chạy nền, theo dõi job và tải kết quả SQL/access log. Console điều khiển generator và nạp SQL của job đã hoàn tất vào PostgreSQL bằng importer hiện có. Không tự nạp khi sinh file, không chạy traffic, upload log hoặc kích hoạt Airflow; sampler 2c giữ nguyên.

## 2. Ranh giới

**Có trong đợt này**

- Preset `generator/configs/*.yml`, chỉnh tham số thường dùng và sửa YAML phân phối nâng cao.
- Validate bằng đúng logic generator: thời gian có timezone, scale, quan hệ products/variants, phân phối/trọng số/tỷ lệ.
- Xuất SQL, access log hoặc cả hai; nạp SQL PostgreSQL bằng tác vụ riêng tại kết quả job.
- Một job nền tại một thời điểm trên một console; Job ID riêng cho từng lần chạy.
- Lưu trạng thái, log, cấu hình đóng băng và artifact dưới `data/generator/jobs/<job_id>/`.
- Theo dõi theo công đoạn; không hiển thị phần trăm giả.
- Lịch sử bền vững qua refresh/restart; job không xác minh được tiến trình bị đánh dấu gián đoạn.
- Xem profile tham chiếu UCI đã đo ở đợt 2b.

**Không có trong đợt này**

- Import MySQL, upload SQL tùy ý, chạy traffic API, upload log, kích hoạt Airflow hoặc Superset.
- Áp profile/reference vào sampler hoặc hiệu chỉnh sampler 2c.
- Celery/Redis/hàng đợi phân tán; nhiều job sinh file song song.
- Xóa `FINAL_REPORT.md` hoặc thay Next.js.

## 3. Cách tiếp cận

### 3.1. Dùng chung validation với generator

Thêm `generator/src/generator/validation.py` chứa `SIZES`, `COLORS` và `validate_scale(scale)`. `load_config()` và `config_from_dict()` validate scale; `export_sql()` và `export_logs()` cùng gọi hàm này. Console chỉ cần parse YAML rồi gọi `config_from_dict()`; không chép luật scale sang Streamlit.

### 3.2. CLI trả summary JSON

Thêm `--json` cho `run`, `export-sql`, `export-logs`. Worker đọc JSON cuối stdout để lấy đường dẫn manifest/SQL/log, dataset identity và số liệu từng công đoạn. Output người dùng của CLI giữ nguyên.

### 3.3. Job nền tách khỏi Streamlit

Console ghi cấu hình đã đóng băng và state ban đầu, sau đó spawn một process nền:

```text
python -m platform_app.ui.worker <job_dir>
```

Worker gọi CLI generator theo thứ tự:

1. `run` — validate cấu hình, ghi manifest.
2. `export-sql` — nếu chọn SQL/cả hai.
3. `export-logs` — nếu chọn log/cả hai.

SQL và log chạy độc lập: một công đoạn lỗi không xóa artifact của công đoạn kia. Đóng tab không dừng worker.

### 3.4. Trạng thái

Job status: `queued`, `running`, `succeeded`, `partial`, `failed`, `interrupted`.

- `partial`: ít nhất một công đoạn được chọn thành công và ít nhất một công đoạn thất bại.
- `interrupted`: console/worker restart nhưng PID không còn xác minh được, hoặc active job không còn tiến trình sống.

Công đoạn: `validate`, `sql`, `logs`; mỗi công đoạn có `pending`, `running`, `succeeded`, `failed`, `skipped`, `interrupted`.

## 4. Tổ chức code

```text
services/platform-app/
├── pyproject.toml
├── README.md
├── Dockerfile
├── src/platform_app/ui/
│   ├── app.py              # entrypoint Streamlit
│   ├── launcher.py         # console script gọi streamlit
│   ├── paths.py            # đường dẫn repo/data, biến môi trường
│   ├── config_builder.py   # preset -> cấu hình cuối, validate
│   ├── job_store.py        # thư mục job, state, lock, reconcile
│   ├── worker.py           # process nền, gọi CLI, cập nhật state
│   ├── import_store.py     # journal, khóa và chống nạp trùng
│   ├── import_worker.py    # kiểm tra snapshot SQL và gọi importer
│   ├── artifacts.py        # file, size, sha256, checksum, log archive
│   ├── reference_profile.py# đọc profile UCI
│   └── views/
│       ├── config.py
│       ├── jobs.py
│       ├── imports.py
│       └── profile.py
└── tests/
```

Generator vẫn là nguồn logic sinh dữ liệu. Console không import sampler và không sao chép thuật toán sinh.

## 5. Lưu trữ job

```text
data/generator/jobs/
├── .lock                    # flock cho thao tác tạo/reconcile job
├── .active.json             # job đang chạy (job_id, pid, created_at)
└── <job_id>/
    ├── config.yml           # cấu hình đã đóng băng
    ├── request.json         # preset, output mode, expected_requests
    ├── state.json           # nguồn sự thật cho UI
    ├── checksums.sha256
    ├── imports/<attempt_id>.json # journal nạp, không thuộc checksum generation
    ├── <scenario>-<identity>.manifest.json
    ├── logs/
    │   ├── worker.log
    │   ├── validate.log
    │   ├── sql.log
    │   └── logs.log
    └── outputs/
        ├── sql/<scenario>-<identity>.sql
        └── access-logs/landing/logs/...
```

- Job ID: `<UTC timestamp>-<8 hex>`, duy nhất và sắp xếp được.
- State ghi atomic qua file tạm + `os.replace`.
- Chỉ một job `queued`/`running` có PID sống trên một instance. Tạo job đồng thời bị chặn bằng `flock` và kiểm tra active.
- `state.json` là nguồn sự thật; Session State chỉ giữ tab đang xem, preset và job đang chọn.
- Khi mở console, `reconcile()` kiểm tra PID và `/proc/<pid>/cmdline`; job không xác minh được chuyển `interrupted`.
- Artifact lớn giữ trên đĩa. UI chỉ đọc log tail và file nhỏ; file lớn hiển thị đường dẫn/dung lượng/checksum. Access log có thể tải qua zip nếu tổng dung lượng dưới ngưỡng cấu hình.

## 6. Cấu hình form

- Preset chọn ngoài form; đổi preset nạp lại giá trị mặc định.
- Form gồm: scenario, seed, anchor time, history months, customers, products, variants, orders, output mode, expected requests.
- Phân phối nâng cao nằm trong expander dưới dạng YAML; submit mới parse.
- Cấu hình cuối = preset gốc + các trường form + `distributions` từ YAML. YAML cuối được ghi vào job và hiển thị/tải lại từ job.
- `scenario_id` bị giới hạn ký tự an toàn để tránh ghi ra ngoài thư mục job.
- `expected_requests = 0` nghĩa là tự động `max(1000, orders * 20)`.

## 7. Kiểm chứng

- Unit test generator: validation scale, `config_from_dict`, CLI `--json`.
- Unit test console: build cấu hình, từ chối cấu hình sai, job store/lock/reconcile, artifact/checksum, worker với command runner giả.
- Kiểm chứng tay: chạy preset small qua console, so sánh SQL với CLI cùng cấu hình; chạy cấu hình chỉ SQL, chỉ log, cả hai; kiểm tra refresh/restart và job gián đoạn.
- `ruff`, test generator, test console, Compose config; Next.js và `FINAL_REPORT.md` không bị sửa.

## 8. Quyết định mở rộng: nạp SQL PostgreSQL từ console

Nạp là tác vụ nền độc lập với sinh file, không thêm stage tự chạy vào pipeline generation. Chỉ job terminal succeeded/partial với SQL succeeded và artifact hợp lệ được chọn; kiểm tra checksum trên bản sao riêng và identity trước khi gọi `platform_app.api.db.import_dataset.import_dataset`. Giữ nguyên transaction, schema gate, đối soát và identity restart của importer; không tạo một cơ chế nạp SQL thứ hai.

`ui/import_store.py` quản lý khóa, chống khởi chạy trùng, nhận diện đích không chứa password và journal `job/imports/<attempt_id>.json`. `ui/import_worker.py` thực thi nạp ngoài tiến trình Streamlit; `ui/views/imports.py` hiển thị nút và receipt trong kết quả job. Dùng hai flock riêng: khóa ngắn cho journal và khóa execution cho toàn tác vụ; tối đa một import trên một thư mục jobs. Import không sửa generation state hoặc checksum, và artifact collector bỏ qua thư mục imports.

Trạng thái bền vững: queued/running/succeeded/failed/already_present/unknown. Thành công chỉ ghi sau khi importer trả về từ commit. Lỗi driver được rút gọn, không ghi raw SQL/credential. Nếu commit chưa rõ kết quả hoặc worker mất, không giả định rollback: ghi unknown và chặn tự nạp lại cùng dataset/đích. Database đã có generation run được ghi already_present, không nhận là một lần import mới thành công. Thay đổi đích sau khi queue được phát hiện trước khi ghi. Trạng thái failed có thể thử lại bằng thao tác rõ ràng; không auto retry.

Đích lấy từ API_DATABASE_URL hiện có, không nhận connection string qua form và không lưu credential vào state/command. Mật khẩu/user thay đổi không làm mất nhận diện cùng host/port/database; alias host vẫn có thể khác, nên chống trùng trong PostgreSQL của importer là lớp quyết định cuối. Không tự sửa schema/seed, xóa volume, chạy pipeline hoặc thay API nghiệp vụ. Xem [hướng dẫn sử dụng và giới hạn](../../services/platform-app/README.md#nạp-sql-vào-postgresql).
