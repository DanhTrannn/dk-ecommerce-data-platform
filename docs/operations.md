# Vận hành, logging và xử lý sự cố

Tài liệu này mô tả kiểm tra/phục hồi sau [setup.md](setup.md). Định nghĩa dữ liệu ở [data.md](data.md); không dùng số lần healthcheck hoặc checkpoint interval làm bằng chứng dữ liệu đúng/SLA.

## 1. Kiểm tra theo từng lớp

| Lớp | Kiểm tra | Không chứng minh được |
|---|---|---|
| Container | `make ps`, Compose health | Schema, quyền bucket/catalog, pipeline nghiệp vụ hoặc dữ liệu mới |
| API | `/health/live`, `/health/ready` | Ready chỉ kiểm tra DB; không xác nhận Kafka/Trino/Gold |
| Airflow | DAG/task state, run/interval, gate và publication | Task green riêng lẻ không đủ để chứng nhận cả bộ mart |
| Flink | Job access-log RUNNING, checkpoint, Landing có event mới | Có job không chứng minh đã bắt đủ request |
| Lakehouse | Row count theo grain/run/snapshot, DQ/parity, manifest | Bronze count khác current source count; không cộng lịch sử làm tổng đơn |
| BI | Publication/run/time đúng, KPI đối chiếu SQL, trạng thái cache | Refresh trang không tự làm snapshot mới hoặc đổi thời điểm quan sát |

```bash
make ps
docker compose logs --tail=100 platform-app postgres
docker compose logs --tail=100 airflow-scheduler airflow-dag-processor airflow-api-server
docker compose --profile streaming logs --tail=100 \
  flink-jobmanager flink-taskmanager
docker compose --profile bi logs --tail=100 superset
curl -fsS http://127.0.0.1:8000/health/ready
```

Các curl cần service tương ứng đã bật; dùng port override nếu khác mặc định. Không in toàn bộ `docker compose config` với `.env` thật hoặc dump env/credential khi chia sẻ log. Dùng `config --quiet` để kiểm tra cấu hình.

## 2. Log và journal cần giữ

| Nơi | Nội dung |
|---|---|
| Container platform-app | Structured API log, request ID, lỗi nghiệp vụ/producer |
| `data/events/dlq_access_logs.jsonl` | Event chưa xác nhận/offline/overload; chưa tự replay |
| `data/scenarios/` | Scenario/run/attempt, idempotency key, order number và kết quả hậu kiểm |
| `data/access-admin/audit.jsonl` | Binding management journal/readback và operation ID; không phải tamper-proof audit store |
| `airflow-logs` | Task logs của các DAG batch/log |
| `lakehouse.system.data_quality_results` | Chẩn đoán kiểm tra chất lượng; kết quả gắn dataset/run |
| `system.analytics_publications` / `logs_analytics_publications` | Snapshot manifest độc lập theo từng nguồn |
| `data/pipeline-runs/` | Report/log/screenshot local của opt-in acceptance; ignored, không phải dataset production |

Không lưu password/cookie/token vào scenario journal. Request `X-Request-ID` giúp nối API log → Kafka payload → Landing/Silver/fact. `generation_run_id`, Airflow `run_id` và operation ID quản trị có namespace riêng.

## 3. Kiểm tra bằng SQL Trino

Ví dụ chạy qua Trino SQL client hoặc client đang có của dự án:

```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.gold;
SELECT run_id, published_at, schema_version, source_mode, snapshots_json
FROM lakehouse.system.analytics_publications ORDER BY published_at DESC LIMIT 1;
SELECT run_id, published_at, observed_at, snapshots_json
FROM lakehouse.system.logs_analytics_publications ORDER BY published_at DESC LIMIT 1;
```

Để so KPI, lấy snapshot ID trong manifest rồi dùng cú pháp sau, **thay `<snapshot_id>` bằng số thực**, không chạy nguyên placeholder:

```sql
SELECT sum(total_orders), sum(order_total_vnd), sum(net_revenue_vnd), sum(gross_profit_vnd)
FROM lakehouse.gold.mart_order_daily FOR VERSION AS OF <snapshot_id>;
```

Không lấy current mart chưa publish để kết luận dashboard sai. Với log, đối chiếu theo snapshot/cutoff publication log, không gộp thời điểm OLTP vào cùng một nhãn freshness. Đối soát source–Silver cần nguồn không đổi trong cửa sổ đo.

## 4. Troubleshooting

| Triệu chứng | Kiểm tra và xử lý đúng phạm vi |
|---|---|
| Compose báo thiếu biến | So `.env.example`, profile và biến bắt buộc; không thêm secret mặc định để làm config qua |
| PostgreSQL healthy nhưng API thiếu bảng | Schema bootstrap/seed là bước riêng; không reset volume. Kiểm tra đang kết nối đúng DB/user |
| Đổi password env nhưng không login | Init PostgreSQL/seed/metadata không tự đổi password đã lưu; xác nhận credential retained trước khi rotate có kiểm soát |
| Batch đọc PostgreSQL lỗi auth/permission | Reader password/env đúng và script `make source-reader` đã chạy sau đủ schema; không đưa credential vào URL kết nối |
| Job batch không thấy credential | Kiểm tra provisioning, consumer file đúng volume/UID/GID; không thay bằng MinIO root hoặc chmod mở rộng toàn bộ |
| Silver lỗi salt | Cấp `SILVER_PSEUDONYMIZE_SALT` ổn định; không bỏ pseudonymization để job qua |
| Thiếu staging/interval | Chạy init nếu chưa có nền; manual interval phải có UTC start/end; interval rỗng hợp lệ không xóa lịch sử Bronze/Silver |
| Giá trị UUID không nối log–product | Xem 16-byte canonical và encoding UTF-8 cũ; resolver hỗ trợ cả hai. Muốn sửa toàn bộ current Silver phải init có kiểm soát |
| Gold gate thất bại | Đọc failed check/key/cost/refund/lineage; sửa nguồn hoặc transform, chạy lại đúng writer/run. Không publish bỏ qua gate |
| Dashboard nghiệp vụ không đổi sau UPDATE | Chờ/chạy batch phù hợp và publication mới. Superset còn cần sync/mở URL mới |
| API có request nhưng Landing trống | `KAFKA_ENABLED`, topic chung, producer/DLQ, job access RUNNING và checkpoint. Seed dữ liệu không tạo access log |
| Kafka phục hồi nhưng log cũ thiếu | Producer phục hồi chỉ gửi event mới; DLQ cũ cần thao tác replay riêng |
| Trino unavailable/permission denied | Kiểm tra Polaris/MinIO reader binding và credential; read-only write denial là đúng thiết kế |
| HTTP 503/cache degraded | Xem publication log/Trino; cache last-good có giới hạn tuổi, không coi là dữ liệu vừa được cập nhật |
| Superset sync không tạo assets | Metadata init/admin, publication schema 3/mode batch và hai snapshot phải đọc được; giữ nguyên dashboard cũ khi lỗi |
| Bộ lọc Superset báo metric count | Dùng image hiện tại; dataset có helper `count=COUNT(*)`, KPI đơn dùng `SUM(total_orders)`. Không sửa KPI thành đếm dòng mart |
| Quản trị binding trả 409 | Reload snapshot/revision; nếu kết quả ghi chưa rõ thì xem journal/readback trước khi thử lại |

Các lỗi kiểu dữ liệu/constraint không giải quyết bằng tắt FK, bỏ quality gate, xóa lịch sử hoặc bật warehouse write cho Trino.

## 5. Phục hồi và replay

### Runner mô phỏng API

Scenario chạy bằng worker độc lập. Mất process có thể chuyển unknown. Worker mất hoặc write timeout không tự replay checkout; dùng key/mã đơn đã ghi để xác minh. Nút dừng scenario ngăn lượt tiếp theo, không hủy transaction đang chạy.

### Batch và publication

Giữ staging/Bronze/Silver và failed task logs để retry. Init và interval DAG không có khóa chung; tạm dừng writer còn lại khi phục hồi. Một lần ghi Gold dở có thể làm current physical tables thay đổi, nhưng reader vẫn dùng manifest cũ. Sau khi sửa lỗi, chạy quality gate/publish đúng logical run; manifest dùng MERGE chống trùng logical run, không chỉnh snapshot ID bằng tay để “làm mới” dashboard.

### Flink/Kafka

- Giữ Kafka data/group offsets và volume checkpoints.
- TaskManager restart phục hồi bằng checkpoint của JobManager đang còn; việc này khác với submit mới từ committed Kafka offsets.
- Submit mới không tự truyền đường dẫn restore/savepoint. Giữ group/offset và retention.
- Trước đổi graph, dừng đúng job writer với savepoint hoàn tất, giữ path và xác nhận không còn writer cũ.
- `KAFKA_LOG_DIRS=/var/lib/kafka/data` tương ứng volume `kafka-data`. Cài đặt cũ dùng `/tmp/kafka-logs` cần sao lưu và copy đầy đủ metadata/partition vào volume trong lúc broker đã dừng; đổi env không tự chuyển file.

Checkpoint mặc định 10 giây là cấu hình, không phải cam kết latency hoặc exactly-once toàn hệ thống. Raw Landing có thể có event lặp khi chủ động replay; đọc theo key/grain đúng. Chưa nghiệm thu JobManager HA hoặc tùy ý reset offsets.

### DLQ và raw log

Producer chờ broker ack ở task nền có deadline 10 giây, tối đa 256 pending; retry kết nối ban đầu sau mỗi lần thất bại có khoảng nghỉ 5 giây. Gửi timeout có thể đã tới broker, nên không khẳng định DLQ chỉ chứa event chưa được ghi. Replay cần giữ **nguyên request ID và payload**, phân biệt bản sao với payload xung đột. Không có command tự replay DLQ trong repo; không bịa lệnh upload JSONL để kỳ vọng mart tự cập nhật.

Raw malformed event được giữ để điều tra, không đưa vào Silver/fact hợp lệ. Sau sửa lỗi giữa các commit, job log tìm raw hash/ID còn thiếu từ các layer persist. Không chạy standalone log writer chồng với `lakehouse_logs_marts`.

## 6. Bảo trì và lưu trữ

Hai DAG maintenance gọi các tác vụ compaction/manifests, expire snapshots, orphan cleanup và quarantine retention theo cấu hình. Lịch OLTP 03:00 cùng giờ batch không tự đảm bảo không chạy chồng; cần điều phối vận hành. Maintenance streaming hai giờ/lần tách khỏi hourly log mart.

[Table profiles](../pipelines/src/lakehouse/utils/maintenance.py) là danh sách tường minh, **không tự bao phủ mọi bảng/mart mới**. CLI hỗ trợ `--tables`/`--profile`, action, retain snapshots và tuổi cleanup; xem [entrypoint](../pipelines/src/jobs/maintenance/iceberg_table_maintenance.py). Không chạy expire/orphan tùy tiện trong khi writer còn ghi hoặc report vẫn cần snapshot cũ.

Superset/PublishedReader cần data file và snapshot manifest còn tồn tại. Maintenance hiện không biết dashboard nào cần snapshot nào; giữ retention theo cửa sổ tra cứu, không bảo đảm URL lịch sử tồn tại vĩnh viễn. Snapshot hết hạn phải báo lỗi, không thay bằng current data. Sync publication mới tạo URL mới, không sửa report cũ.

| Tài nguyên | Nội dung cần bảo toàn |
|---|---|
| `postgres-data` | Nguồn nghiệp vụ, Airflow/Polaris và metadata Superset khi đã tạo |
| `minio-data` | Object/data/metadata Iceberg |
| `polaris-provision-state`, consumer credential volumes | Secret và ownership để chạy lại/khôi phục provisioning |
| `kafka-data`, `flink-checkpoints` | Broker offsets, log transport và checkpoint state |
| `./data` | Scenario journals, DLQ và audit binding |
| `.env`/secret store | Credential, PII salt và Superset encryption secret phù hợp với dữ liệu đã lưu |

`make down` giữ named volumes, không phải backup. Chưa nghiệm thu backup/restore đồng bộ nhiều hệ thống hoặc disaster recovery; cần kế hoạch riêng nếu dùng ngoài local PoC. Không commit backup/raw data/credential vào Git.

## 7. Kiểm tra chất lượng và CI

Các lệnh static/offline thường dùng:

```bash
uv lock --check
uvx ruff check .
python3 -m compileall -q airflow/dags/
docker compose --env-file .env.example --profile '*' config --quiet
# Test chỉ giữ local: mọi thư mục tests/ bị gitignore, trừ dbt data tests trong pipelines/dbt/tests/
PYTHONPATH=pipelines/src:pipelines/tests uv run --locked --package batch-pipeline -- \
  python -m unittest discover -s pipelines/tests -v
PYTHONPATH=services/platform-app/tests uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
uv run --locked --package batch-pipeline -- \
  python -m unittest discover -s infrastructure/superset/tests -p test_sales.py -v
```

Opt-in integration dùng fixture riêng, không phải chạy trên dữ liệu hiện có. Hướng dẫn component còn ở [pipelines README](../pipelines/README.md), [platform README](../services/platform-app/README.md) và test source. Ví dụ:

```bash
RUN_ACCESS_LOG_STACK_TESTS=1 PYTHONPATH=pipelines/src:pipelines/tests \
  uv run --locked --package batch-pipeline -- \
  python -m unittest discover -s pipelines/tests -p test_logs_stack.py -v
RUN_SUPERSET_STACK_TESTS=1 PYTHONPATH=infrastructure/superset/tests:pipelines/src:pipelines/tests \
  uv run --locked --package batch-pipeline -- \
  python -m unittest discover -s infrastructure/superset/tests -p test_superset_stack.py -v
```

Cache/build trước các image test tương ứng; không gọi opt-in và kỳ vọng không có Docker/chi phí xử lý. Browser Superset tùy chọn qua `BI_BROWSER_PYTHON` và `BI_CHROMIUM_PATH`, test không tự tải browser. Fixture dọn tài nguyên sau hoàn tất/lỗi thông thường; bị kill đột ngột có thể để lại stack tạm, cần xác định project prefix trước khi dọn và không tác động deployment chính.

CI hiện có hai job: repository validation (sáu tổ hợp Compose gồm BI, compile DAG, Ruff) và storefront (npm ci/lint/typecheck/build). Docs-only/Markdown-only thay đổi có thể bị skip theo paths-ignore. Unit/integration nêu trên là kiểm tra riêng, không tuyên bố CI tự chạy tất cả.

## 8. Thứ tự công việc tiếp theo

Tiếp theo: viết generator OLTP và access log mới; quyết định có cần đồng bộ hard delete bằng batch không. Sau đó mở rộng Superset ngoài hai mart doanh thu, bổ sung role/viewer/store và quy tắc lọc đúng grain; chốt chọn publication mới/lifecycle assets và retention snapshots. Hoàn thiện runner các nghiệp vụ còn thiếu khi cần tạo dữ liệu tương ứng. Sau khi BI/source flow có thay thế và kiểm chứng đủ mới dọn Next.js/CI/dependency liên quan. Không đưa chức năng mới vào tài liệu hiện trạng như thể đã triển khai.
