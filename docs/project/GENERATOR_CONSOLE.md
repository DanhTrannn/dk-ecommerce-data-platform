# Generator Console — thiết kế đợt 3

Cập nhật **07/11/2026**: đã gỡ sinh access log, console chỉ xuất SQL. Tài liệu này chốt cách triển khai Streamlit Generator Console trên generator 0.8 hiện tại. Phạm vi và lý do chuyển đổi nằm trong [refactor_v2](refactor_v2.md).

## 1. Mục tiêu

Cho người dùng chọn preset, chỉnh các tham số thường dùng, kiểm tra cấu hình, tạo job sinh dữ liệu chạy nền, theo dõi job và tải kết quả SQL. Console chỉ điều khiển generator; không nạp MySQL, không chạy traffic, không upload log, không kích hoạt Airflow.

## 2. Ranh giới

**Có trong đợt này**

- Preset `generator/configs/*.yml`, chỉnh tham số thường dùng và sửa YAML phân phối nâng cao.
- Validate bằng đúng logic generator: thời gian có timezone, scale, quan hệ products/variants, phân phối/trọng số/tỷ lệ.
- Xuất SQL nghiệp vụ.
- Một job nền tại một thời điểm trên một console; Job ID riêng cho từng lần chạy.
- Lưu trạng thái, log, cấu hình đóng băng và artifact dưới `data/generator/jobs/<job_id>/`.
- Theo dõi theo công đoạn; không hiển thị phần trăm giả.
- Lịch sử bền vững qua refresh/restart; job không xác minh được tiến trình bị đánh dấu gián đoạn.

**Không có trong đợt này**

- Import SQL vào MySQL, chạy traffic API, upload log, kích hoạt Airflow, Superset.
- Hiệu chỉnh quy luật sinh dữ liệu trong console.
- Celery/Redis/hàng đợi phân tán; nhiều job song song.
- Xóa `FINAL_REPORT.md` hoặc thay Next.js.

## 3. Cách tiếp cận

### 3.1. Dùng chung validation với generator

Thêm `generator/src/generator/validation.py` chứa `SIZES`, `COLORS` và `validate_scale(scale)`. `load_config()` và `config_from_dict()` validate scale; `export_sql()` gọi hàm này. Console chỉ cần parse YAML rồi gọi `config_from_dict()`; không chép luật scale sang Streamlit.

### 3.2. CLI trả summary JSON

Thêm `--json` cho `run`, `export-sql`. Worker đọc JSON cuối stdout để lấy đường dẫn manifest/SQL, dataset identity và số liệu từng công đoạn. Output người dùng của CLI giữ nguyên.

### 3.3. Job nền tách khỏi Streamlit

Console ghi cấu hình đã đóng băng và state ban đầu, sau đó spawn một process nền:

```text
python -m platform_app.ui.worker <job_dir>
```

Worker gọi CLI generator theo thứ tự:

1. `run` — validate cấu hình, ghi manifest.
2. `export-sql` — sinh SQL sau khi validate thành công.

Validate lỗi thì bỏ qua SQL; SQL lỗi thì job thất bại, giữ artifact đã có. Đóng tab không dừng worker.

### 3.4. Trạng thái

Job status: `queued`, `running`, `succeeded`, `failed`, `interrupted`.

- `interrupted`: console/worker restart nhưng PID không còn xác minh được, hoặc active job không còn tiến trình sống.

Công đoạn: `validate`, `sql`; mỗi công đoạn có `pending`, `running`, `succeeded`, `failed`, `skipped`, `interrupted`.

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
│   ├── artifacts.py        # file, size, sha256, checksum
│   └── views/
│       ├── config.py
│       ├── jobs.py
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
    ├── request.json         # preset và thông tin job
    ├── state.json           # nguồn sự thật cho UI
    ├── checksums.sha256
    ├── <scenario>-<identity>.manifest.json
    ├── logs/
    │   ├── worker.log
    │   ├── validate.log
    │   └── sql.log
    └── outputs/
        └── sql/<scenario>-<identity>.sql
```

- Job ID: `<UTC timestamp>-<8 hex>`, duy nhất và sắp xếp được.
- State ghi atomic qua file tạm + `os.replace`.
- Chỉ một job `queued`/`running` có PID sống trên một instance. Tạo job đồng thời bị chặn bằng `flock` và kiểm tra active.
- `state.json` là nguồn sự thật; Session State chỉ giữ tab đang xem, preset và job đang chọn.
- Khi mở console, `reconcile()` kiểm tra PID và `/proc/<pid>/cmdline`; job không xác minh được chuyển `interrupted`.
- Artifact lớn giữ trên đĩa. UI chỉ đọc log tail và file nhỏ; file lớn hiển thị đường dẫn/dung lượng/checksum.

## 6. Cấu hình form

- Preset chọn ngoài form; đổi preset nạp lại giá trị mặc định.
- Form gồm: scenario, seed, anchor time, history months, customers, products, variants, orders.
- Phân phối nâng cao nằm trong expander dưới dạng YAML; submit mới parse.
- Cấu hình cuối = preset gốc + các trường form + `distributions` từ YAML. YAML cuối được ghi vào job và hiển thị/tải lại từ job.
- `scenario_id` bị giới hạn ký tự an toàn để tránh ghi ra ngoài thư mục job.

## 7. Kiểm chứng

- Unit test generator: validation scale, `config_from_dict`, CLI `--json`.
- Unit test console: build cấu hình, từ chối cấu hình sai, job store/lock/reconcile, artifact/checksum, worker với command runner giả.
- Kiểm chứng tay: chạy preset small qua console, so sánh SQL với CLI cùng cấu hình; kiểm tra refresh/restart và job gián đoạn.
- `ruff`, test generator, test console, Compose config; Next.js và `FINAL_REPORT.md` không bị sửa.
