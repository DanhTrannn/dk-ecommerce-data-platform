# Superset sales PoC

Superset 6.1.0 đọc Trino, dùng PostgreSQL hiện có để lưu metadata trong database/user riêng. Profile `bi` thêm một web container; không cần Redis, Celery hoặc frontend tự viết. Next.js vẫn giữ trong source. PoC chỉ dành cho admin, chưa thay toàn bộ BI và phân quyền theo cửa hàng.

## Hợp đồng dữ liệu

Mỗi dashboard gắn với **một publication OLTP schema v3, mode batch**. Hai virtual dataset dùng `FOR VERSION AS OF <snapshot_id>` theo cùng manifest:

| Dataset | Grain | Dùng cho |
|---|---|---|
| `mart_order_daily` | Ngày đặt hàng × kênh × điểm bán | Tổng đơn, giá trị đặt hàng, doanh thu thuần, lợi nhuận gộp; xu hướng, kênh, điểm bán |
| `mart_product_sales_daily` | Ngày đặt hàng × sản phẩm | Top sản phẩm theo doanh thu thuần |

Không JOIN hai grain để tính KPI. Công thức lấy trực tiếp từ mart: `SUM(total_orders)`, `SUM(order_total_vnd)`, `SUM(net_revenue_vnd)`, `SUM(gross_profit_vnd)`. Đơn vị tiền là VND; ngày lọc là `order_date` hiện có, không đổi sang ngày thanh toán. Checkout COD chưa thu không tự tạo doanh thu được ghi nhận. Không có target/delta hoặc định nghĩa KPI mới. Metric kỹ thuật `count = COUNT(*)` chỉ hỗ trợ native filter, không dùng làm số đơn.

Lệnh sync đọc publication mới nhất, kiểm tra hai snapshot qua Trino trước khi ghi metadata. Manifest/run ID quyết định định danh assets: chạy lại không tạo trùng; publication khác tạo **dashboard và dataset mới**, giữ bản cũ. Snapshot NULL được đọc bằng một quan hệ rỗng, không dùng dữ liệu chưa công bố hoặc tự thay bằng 0. Không đọc PostgreSQL nghiệp vụ để tính KPI và không có fallback về bảng Iceberg hiện tại.

Dashboard có bốn KPI và bốn biểu đồ, mốc công bố UTC/run ID, cùng bộ lọc ngày áp dụng cả hai dataset. Không bật cross-filter hoặc bộ lọc cửa hàng/kênh chung vì mart sản phẩm chưa chứa các chiều đó. Dashboard không tự chuyển publication khi refresh trang; cần sync sau batch và mở đường dẫn mới.

## Khởi tạo và chạy

Trước hết cần source/schema/dataset, provisioning MinIO/Polaris, Trino và ít nhất một batch publication thành công theo [SETUP](SETUP.md). Health check của các dịch vụ không thay thế điều kiện này.

Điền các giá trị sau trong `.env` (ba secret độc lập, không để trống):

```dotenv
SUPERSET_SECRET_KEY=<at-least-32-random-characters>
SUPERSET_DB_PASSWORD=<dedicated-metadata-password>
SUPERSET_ADMIN_PASSWORD=<initial-admin-password>
SUPERSET_ADMIN_USER=admin
SUPERSET_ADMIN_EMAIL=bi-admin@example.com
```

Có thể sinh giá trị ngẫu nhiên bằng `openssl rand -hex 32`. Không commit `.env`. Secret/key có ý nghĩa với metadata đã lưu; không đổi tùy ý trên volume đang dùng.

```bash
# Build runtime và driver đã khóa phiên bản/hash.
docker compose --profile core --profile bi build superset

# Áp dụng env/mount mới cho PostgreSQL đang giữ volume.
# Chạy khi các job đang rảnh; bước này có thể tạo lại container PostgreSQL.
docker compose --profile core up -d postgres

# Tạo database/user metadata riêng trên cả volume mới và volume đã có.
make superset-db
make superset-init

docker compose --profile core --profile bi up -d superset
make superset-sync
```

Mở `http://127.0.0.1:8088`, đăng nhập bằng `SUPERSET_ADMIN_USER`/`SUPERSET_ADMIN_PASSWORD`. Lệnh sync in `SUPERSET_RESULT` với đường dẫn dashboard; hoặc mở danh sách Dashboards. Port/bind address có thể đổi bằng `SUPERSET_PORT`/`SUPERSET_BIND_ADDRESS`.

`superset-init` và `superset-sync` là lệnh chủ động, scale 0. Init chạy migration metadata/khởi tạo quyền Superset và tạo admin nếu chưa có; chạy lại không reset mật khẩu. `superset-db` giữ credential role đã tồn tại; đổi `.env` không tự rotate role. Không tự khởi tạo source/schema, nạp dataset hoặc chạy DAG.

Sau mỗi batch thành công, chạy `make superset-sync` để tạo dashboard của publication mới. Muốn dừng riêng BI:

```bash
docker compose --profile core --profile bi stop superset
```

Metadata nằm trong volume PostgreSQL hiện có; dừng/tạo lại web container không xóa dashboard. Superset không giữ secret MinIO/Polaris hoặc account database nguồn. Trino dùng catalog đang đặt `iceberg.security=READ_ONLY`; username `superset_reader` là nhãn client, không phải cơ chế xác thực riêng cho từng người dùng.

## Giới hạn và vận hành

- Đây là PoC local HTTP, login Superset độc lập; chưa kế thừa RBAC/SSO của FastAPI, chưa có RLS theo cửa hàng hoặc tài khoản người xem hạn chế. Không bật public dashboards, SQL Lab cho connection này, file upload, Jinja hoặc warehouse DML. Admin vẫn là người dùng tin cậy và có thể sửa assets qua UI.
- Mỗi URL giữ một phiên dữ liệu. Sync không overwrite dashboard đã có; không tự nâng dashboard cũ lên publication mới. Chưa có bộ điều hướng “latest” hoặc sync do Airflow tự kích hoạt.
- Lịch sử chỉ đọc được khi Iceberg còn giữ snapshot/data file tương ứng. Maintenance hiện có **không biết** các dashboard Superset; phải giữ retention phù hợp với khoảng thời gian muốn tra cứu, hoặc tạo/mở dashboard publication mới trước khi bỏ snapshot cũ. Không có bảo đảm lưu dashboard lịch sử vô thời hạn; snapshot hết hạn gây lỗi, không đọc ngầm bảng hiện tại.
- Cache dữ liệu tắt cho PoC; chưa nghiệm thu tải đồng thời, hiệu năng production, HTTPS/reverse proxy hoặc backup/restore metadata. Cấu hình cookie/proxy có biến riêng nhưng không tự bật HTTPS.

Quyết định giữ assets theo publication giúp các biểu đồ cùng dashboard dùng cùng manifest và giữ bản đã đọc khi một batch sau thất bại hoặc đang ghi dở. Đổi lại, PoC cần thao tác sync/mở dashboard mới; cơ chế cập nhật tự động và chính sách vòng đời assets/snapshot là phần sau.

## Kiểm tra

Offline, không cần Superset hoặc mạng:

```bash
uv run --package batch-pipeline python -m unittest discover \
  -s infrastructure/superset/tests -p test_sales.py -v
```

Nghiệm thu opt-in trên stack biệt lập, cần các image của repo đã build/cache:

```bash
RUN_SUPERSET_STACK_TESTS=1 \
PYTHONPATH=infrastructure/superset/tests:pipelines/src:pipelines/tests \
uv run --package batch-pipeline python -m unittest discover \
  -s infrastructure/superset/tests -p test_superset_stack.py -v
```

Fixture dùng credentials ngẫu nhiên, network nội bộ, volume riêng, không mở host port và không bật Kafka/Airflow. Nó chạy các batch builder/quality gate thực qua Spark client, không kiểm tra lại scheduler 109 task. Kiểm tra query tám chart, KPI so với SQL, khoảng ngày rỗng, chặn ghi Trino, không tạo assets khi chưa có publication, giữ snapshot khi mart bị sửa chưa publish, hai COD qua API, publication mới và persistence sau restart. `BI_TEST_IMAGE` chọn image Superset tương đương.

Kiểm tra trình duyệt là tùy chọn: đặt `BI_BROWSER_PYTHON` tới Python đã cài Playwright, và `BI_CHROMIUM_PATH` nếu dùng Chromium đã có. Test không tự cài dependency/tải browser. Screenshot và report được lưu dưới `data/pipeline-runs/<project>/` (ignored), tài nguyên của lượt thử được dọn khi hoàn tất hoặc lỗi thông thường.

## Nguồn công nghệ

- [Superset 6.1.0 release](https://github.com/apache/superset/releases/tag/6.1.0).
- [Superset database drivers](https://superset.apache.org/docs/6.0.0/configuration/databases/); driver được cài trong image, khóa qua `requirements.in`, `constraints.txt` và `requirements.lock`.
- [Trino Iceberg time travel](https://trino.io/docs/current/connector/iceberg.html#time-travel).
- Mã nguồn Superset được kiểm tra tại tag 6.1.0 cho model dataset/chart/dashboard và query context; không dùng API/CLI import phỏng đoán.


## Kết quả nghiệm thu — 09/11/2026

Lượt chạy sạch tự động hoàn tất trong **516,8 giây** trên stack biệt lập, không sửa metadata thủ công. Hai lượt batch đều đạt đối soát nguồn–Silver **30/30** và Gold **55/55**. KPI qua query engine Superset khớp SQL Trino:

| Chỉ số | Publication đầu | Sau hai COD checkout |
|---|---:|---:|
| Tổng đơn | 30 | 32 |
| Giá trị đặt hàng (VND) | 11.565.410 | 12.063.410 |
| Doanh thu thuần (VND) | 8.618.410 | 8.618.410 |
| Lợi nhuận gộp (VND) | 5.347.230 | 5.347.230 |

Lượt sau đọc lại source bằng các production builder; đây không phải kiểm tra scheduler hoặc interval extraction của Airflow. Hai COD chưa thu không tăng doanh thu/lợi nhuận ghi nhận. Sửa mart chưa publish không làm thay số trên dashboard cũ; sync lặp giữ nguyên assets. Publication mới tạo bộ assets riêng, bản cũ giữ nguyên. Restart web vẫn còn hai dashboard, 16 chart và bốn dataset.

Query cả tám chart với khoảng ngày rỗng không tạo số 0 giả; ghi Trino bị chặn. Request anonymous bị chặn; đăng nhập có CSRF thật đọc được API và dashboard. Chromium mở cả hai dashboard, nhận **18 phản hồi chart/native filter**, không có lỗi query hoặc JavaScript. Sáu offline contract test, Ruff toàn repo, kiểm tra formatter, shell, sáu tổ hợp Compose, Make dry-run, workspace lock và dependency check trong image đều đạt. Image có 164 package tương thích; dependencies Superset tách khỏi workspace ứng dụng.

Bằng chứng local (ignored): `data/pipeline-runs/tlcn-stack-9b0e7fd0a7/report.json`, `data/pipeline-runs/superset-stack-check.log`, cùng `sales-dashboard.png` và `sales-dashboard-latest.png` trong thư mục report. Tài nguyên của lượt thử đã dọn; FINAL_REPORT và dữ liệu deployment hiện có giữ nguyên. Chưa nghiệm thu production, cron, RBAC theo cửa hàng, toàn bộ 18 mart hoặc bảo đảm lưu snapshot vô thời hạn.
