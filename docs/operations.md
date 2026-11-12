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
| `data/generator/scenarios/` | Scenario/run/attempt, idempotency key, order number và kết quả hậu kiểm |
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
| Batch JDBC lỗi auth/permission | Reader password/env đúng và script `make source-reader` đã chạy sau đủ schema; credentials không được nhét vào JDBC URL |
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

Không chuyển history MySQL/unclassified vào cùng Bronze PostgreSQL bằng cách đổi `_source_system`; guard chủ động từ chối để tránh overwrite history. Dùng catalog/warehouse riêng hoặc migration được review. Không tự xóa bảng legacy khi nâng schema.

### Flink/Kafka

- Giữ Kafka data/group offsets và volume checkpoints.
- TaskManager restart phục hồi bằng checkpoint của JobManager đang còn; việc này khác với submit mới từ committed Kafka offsets.
- Submit mới không tự truyền đường dẫn restore/savepoint. Giữ group/offset và retention; không giả định savepoint MySQL hoặc graph cũ tương thích graph PostgreSQL/Landing mới.
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

## 8. Bằng chứng nghiệm thu đã ghi nhận

Đây là kết quả của fixture biệt lập tại thời điểm đo, **không phải live status, production benchmark hoặc cam kết SLA**. Report local bị ignore nên clone mới không tự có file bằng chứng.

| Đường dữ liệu | Kết quả đã ghi nhận | Report local trong `data/pipeline-runs/` |
|---|---|---|
| Generator full preset/batch (generator 0.10, đã gỡ) | 3.000 → 3.002 đơn sau hai COD; hai DAG 109/109 task; source–Silver 30/30, Gold 55/55; COD chưa thu không tăng net revenue | `tlcn-stack-25aa75d70a/report.json` |
| CDC PostgreSQL (đã gỡ) | 594 snapshot event khớp 27 nguồn, C/U/D/tombstone/transaction, history legacy, TaskManager/Connect restart; không tạo Silver/Gold | `tlcn-stack-085770b323/report.json` |
| Log thực | 21 request, hai checkout/hai cart additions resolve, ba mart; retry/dedup, invalid retention, checkpoint recovery và last-good publication | `tlcn-stack-0ba785099b/report.json` |
| Superset PoC | Hai publication/dashboard, 8 chart/phiên; Chromium 18 phản hồi chart/filter; số đơn 30 → 32, placed value 11.565.410 → 12.063.410 VND, net 8.618.410 và profit 5.347.230 giữ nguyên | `tlcn-stack-9b0e7fd0a7/report.json` |

Superset acceptance chạy production batch builders hai lượt bằng Spark client, không kiểm tra lại Airflow interval/scheduler; log/CDC được nghiệm thu độc lập. Source/API runner, filesystem và quyền đã có kiểm tra theo component, nhưng không suy rộng thành mọi scenario hoặc triển khai public.

## 9. Thứ tự công việc tiếp theo

Engine batch đã chuyển hẳn sang Polars/PyIceberg/dbt và CDC đã gỡ. Tiếp theo: viết generator OLTP và access log mới; quyết định có cần đồng bộ hard delete bằng batch không. Sau đó mở rộng Superset ngoài hai mart doanh thu, bổ sung role/viewer/store và quy tắc lọc đúng grain; chốt chọn publication mới/lifecycle assets và retention snapshots. Hoàn thiện runner các nghiệp vụ còn thiếu khi cần tạo dữ liệu tương ứng. Sau khi BI/source flow có thay thế và kiểm chứng đủ mới dọn Next.js/CI/dependency liên quan. Không đưa chức năng mới vào tài liệu hiện trạng như thể đã triển khai.

## 10. Kiểm chứng thay Spark bằng Polars/PyIceberg (lịch sử)

Các mục 10–11 ghi lại quá trình chuyển engine trước khi cutover; lệnh và CLI trong đó (ví dụ `run_polars_gold`, `run_polars_marts`) không còn dùng.

Luồng thử chỉ ghi namespace `polars_pilot_*`; không chạy trên Gold hoặc publication hiện tại. Lệnh offline và opt-in stack nằm trong [pipeline guide](../pipelines/README.md#polars--pyiceberg-migration-pilot). Stack dùng volume/network riêng, secret ngẫu nhiên và không publish port. Bằng chứng lưu trong `data/pipeline-runs/<project>/report.json`; file log tổng là `data/pipeline-runs/polars-stack-check.log`.

Giữ nguyên account/policy đang có. Staging thử nằm dưới `landing/polars-pilot/<namespace>/<run>/`, phù hợp prefix được cấp cho workload; không mở rộng quyền bucket để làm thử nghiệm qua. Chỉ đọc credential file bằng parser key/value, không source shell trong Python. Pilot dùng tối đa 10.000 dòng/trang; không suy rộng thành bảo đảm bounded memory cho mọi thao tác merge/scan.

| Chức năng | Phương án và giới hạn trong bước đầu |
|---|---|
| Reader PostgreSQL | Kết nối riêng, read-only/UTC, reader credentials hiện có; không đọc `customer_credentials` |
| Catalog/object storage | REST Polaris + PyArrow FileIO, credential file và key workload; chưa đổi tên account |
| Commit dữ liệu | Đã kiểm chứng append/upsert/delete/overwrite và time travel trong fixture với Spark/Trino |
| Streaming interop | Đã đọc lại ba log API thực từ Landing do Flink ghi trong fixture; job Flink giữ nguyên |
| Snapshot expiration | PyIceberg 0.12.0 có API; chưa đưa vào lịch production hoặc nghiệm thu việc dọn file vật lý |
| Compaction, rewrite manifests, orphan cleanup | Chưa có thay thế được nghiệm thu; không tuyên bố maintenance đã chuyển hoàn tất |
| Publication/Superset | Chưa đổi engine phục vụ hoặc contract; pilot không publish |

Các khả năng API được đối chiếu từ [PyIceberg API](https://py.iceberg.apache.org/api/) và runtime đã pin, không coi tài liệu latest là bằng chứng nghiệm thu của hệ thống. Trước khi bỏ Spark phải bổ sung đủ schema/PII/quarantine, Gold/log/quality/publication, maintenance, Airflow cutover và chạy nghiệm thu toàn luồng. Chưa có số đo RAM/thời gian đủ để kết luận mức cải thiện hiệu năng.

### Nghiệm thu pilot ngày 10/11/2026

Run sạch `tlcn-stack-3c7af4837d` đạt trong **167,890 giây** trên fixture nhỏ; đây là thời gian toàn bộ kiểm chứng có khởi động/dọn service, không phải benchmark xử lý dữ liệu. Report local: `data/pipeline-runs/tlcn-stack-3c7af4837d/report.json`.

- 15 category qua 8 trang ở page size 2; replay vẫn 15 Bronze/15 Silver. Một cập nhật làm Bronze tăng 16 phiên bản, Silver giữ 15 dòng.
- Interval có offset `+07:00` lấy đúng hai dòng tại lower/inside, loại upper; interval rỗng giữ nguyên snapshot và dữ liệu.
- Trino đọc đúng UUID 16 bytes và snapshot trước cập nhật.
- PyIceberg đọc được một positional delete file do Spark tạo; append/upsert/delete/overwrite trên bảng partition, Decimal, timestamp microsecond/UTC, null, boolean và UTF-8 được kiểm chứng. Spark và Trino đọc đúng kết quả Python; schema/field IDs giữ nguyên trong ca thử.
- Reader nguồn bị từ chối UPDATE và đọc `customer_credentials`. Thu hồi binding Polaris làm pilot thất bại; cấp lại thì chạy được, không thay policy để bỏ qua lỗi.
- Ba request API thực được Kafka/Flink ghi Landing và PyIceberg đọc lại. Không tạo OLTP/log publication.
- Runtime khóa Polars 2.1.0, PyIceberg 0.12.0, PyArrow 18.1.0, pyiceberg-core 0.10.1; image Python không có Java/PySpark. Namespace, network và volumes kiểm thử đã được dọn.

Bộ kiểm tra pipeline offline đạt 25 test, bỏ qua 5 integration opt-in; trong đó có 7 test mới cho pilot. Generator đạt 33 test; platform suite đạt 71 test và bỏ qua 49 test có điều kiện. Ruff, format cho các file Python thay đổi, workspace lock và dependency compatibility đều đạt. Chưa nghiệm thu đủ 27 nguồn, PII/quarantine production, Gold/log rollup, publication, scheduler hoặc maintenance trên engine mới.

## 11. Native Bronze/Silver cho 27 nguồn

Bước mở rộng có CLI `jobs.oltp.run_polars_batch` và nhóm module `lakehouse/oltp/polars/`. Tất cả bảng từ catalog nguồn được hỗ trợ ở Bronze/Silver, gồm schema typed cho bảng/interval rỗng, validation nhiều lỗi, PII và quarantine. Các namespace vẫn tách biệt bằng prefix `polars_batch_*`; Spark tiếp tục chạy Gold/log/publication, maintenance và DAG hiện tại.

Lệnh kiểm chứng:

```bash
PYTHONPATH=pipelines/src uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_batch.py -v
RUN_POLARS_BATCH_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  uv run --locked --all-packages --extra polars -- \
  python -m unittest discover -s pipelines/tests -p test_polars_batch_stack.py -v
```

Rebuild image pilot trước integration. Fixture so sánh fingerprint toàn bộ cột nghiệp vụ của 27 Bronze, 27 Silver và quarantine giữa Spark/native. Metadata thời điểm ingest/run không tham gia fingerprint. Dữ liệu rỗng của generator được bổ sung bằng service nhập hàng/chuyển kho trên nguồn riêng, để cả 27 bảng có dữ liệu; thêm JSON có Unicode để kiểm tra giữ nguyên văn bản nguồn. Hai đơn COD được tạo qua API thật để đối chiếu interval.

Quy trình thử dòng lỗi dùng cùng một dòng đã sắp thứ tự và materialize trước khi đưa cho hai engine. Không so hai kết quả `LIMIT 1` không có thứ tự vì có thể lấy hai phiên bản/bản ghi khác nhau. Diagnostics chỉ xuất tên cột, kiểu/độ dài và hash, không trả nội dung bản ghi.

Quarantine giữ cùng tập mã lỗi và chuẩn JSON của engine hiện tại: token hóa theo pattern cột, bỏ trường NULL, timestamp UTC ở milliseconds, dedup theo source/record/mã lỗi; không coi đây là xử lý PII cho mọi nội dung free-text. Field ID của partition được bind sau khi catalog cấp schema, không suy đoán ID từ vị trí cột Arrow. Guard từ chối Bronze thiếu thông tin nguồn ngay cả khi bảng chưa có snapshot.

Chưa đưa vào production: schema/type evolution mới cần review, adapter không phải bộ dịch Spark SQL tổng quát; chưa có transaction snapshot nguồn xuyên 27 bảng khi source còn ghi; chưa đo RAM của upsert/file scan ở tải lớn. Tiếp theo cần chuyển Gold/quality/publication, log rollup, hard-delete/maintenance và scheduler trước khi bỏ Spark. Không tự nới quyền MinIO/Polaris hoặc dùng root để vượt lỗi.

### Nghiệm thu 27 nguồn ngày 10/11/2026

Run sạch `tlcn-stack-7e0f251bbd` đạt trong **381,475 giây**, gồm khởi động/dọn service và cả hai engine tham chiếu; không dùng thời gian này để so tốc độ Spark với Polars. Report local: `data/pipeline-runs/tlcn-stack-7e0f251bbd/report.json`.

- Cả 27 bảng nguồn có dòng dữ liệu, tổng 600 dòng ở lần nạp đầu. Service nhập hàng tạo receipt/item/ledger; service chuyển kho tạo store inventory và ledger tương ứng.
- Fingerprint toàn bộ cột nghiệp vụ của 27 Bronze và 27 Silver khớp Spark sau init, replay, interval rỗng và incremental. JSON Unicode nguồn được giữ nguyên.
- Hai đơn COD tạo qua API làm Silver orders tăng 30 → 32; snapshot cũ vẫn có 30 đơn. Backfill từ staging ban đầu không ghi đè state mới hoặc xóa hai đơn mới.
- 27 ca dòng lỗi, mỗi bảng một ca, có cùng mã lỗi và nội dung JSON quarantine với Spark ở mức so sánh JSON semantic. Replay không tăng số record quarantine; các cột PII trong payload được token hóa theo hợp đồng hiện có.
- Bronze bị gắn nhãn legacy làm run bị từ chối; phục hồi classification trong fixture cho kết quả dữ liệu không đổi. Không tạo publication hoặc chạy Gold/log/maintenance/scheduler native.
- Pipeline offline đạt 34 test, bỏ qua 6 integration opt-in; có 9 test mới cho bước 27 nguồn. Ruff, format, lock/dependency checks, liên kết local và byte của `FINAL_REPORT.md` được kiểm tra. Network/volume/container riêng đã dọn; dữ liệu triển khai hiện có không thay đổi.

Phạm vi nghiệm thu là fixture nhỏ và các quy tắc hiện tại. Arrow casts là strict; giá trị ngoài miền biểu diễn của schema Silver cần được chốt chính sách riêng trước cutover. Chưa nghiệm thu throughput, tải lớn hoặc toàn bộ pipeline production bằng engine native.

### Gold native: chạy, audit và xử lý gate

CLI `jobs.oltp.run_polars_gold` xây đủ 27 bảng, ghi audit 55 kiểm tra ở `<prefix>_system.data_quality_results` rồi mới cập nhật `<prefix>_system.analytics_publications`. Giữ nguyên `--as-of` khi retry cùng lần quan sát. Khi gate thất bại, xem các audit `status='FAILED'` theo `run_id`; sửa nguyên nhân hoặc rebuild rồi audit lại. Không bỏ qua gate bằng cách tự sửa manifest.

`--verify-only` kiểm tra snapshot hiện tại và lưu audit, không tạo/cập nhật publication. Cùng `run_id`/`source_mode` chỉ có một publication logic; audit vẫn append từng lần kiểm tra. Manifest cũ ghim snapshot đã audit, nên rebuild thất bại không đổi kết quả đọc manifest đó. Không expire snapshot đang còn được publication sử dụng.

```bash
PYTHONPATH=pipelines/src .venv/bin/python -m unittest discover -s pipelines/tests -v
RUN_POLARS_GOLD_STACK_TESTS=1 PYTHONPATH=pipelines/src \
  .venv/bin/python -m unittest discover -s pipelines/tests -p test_polars_gold_stack.py -v
```

Rebuild image Python theo hướng dẫn setup trước integration. Fixture riêng dùng PostgreSQL/MinIO/Polaris/Trino, Spark tham chiếu và API nguồn; không cần Kafka/Flink/Airflow cho ca Gold này. Fingerprint bỏ metadata ingest/run, chuẩn hóa DATE và so DOUBLE ở sáu chữ số thập phân; tiền VND nguyên phải khớp chính xác. Kiểm tra lịch sử tồn kho có fixture ngày trước được chèn chủ động, không tuyên bố tái dựng lịch sử thực từ một snapshot nguồn.

Phần native vẫn dùng DataFrame/bảng đọc vào bộ nhớ; chưa nghiệm thu dữ liệu lớn, giới hạn RAM hoặc latency production. Access-log rollups, maintenance, hard delete, Airflow và reader BI chưa chuyển engine.


### Nghiệm thu Gold native ngày 10/11/2026

Run riêng `tlcn-stack-dc4b12de35` đạt trong **361,334 giây**, gồm khởi động/dọn stack và hai engine; không dùng số này để kết luận engine nào nhanh hơn. Report local (gitignored): `data/pipeline-runs/tlcn-stack-dc4b12de35/report.json`.

- Cả 27 bảng Gold khớp fingerprint nghiệp vụ Spark ở lượt đầu và lượt cập nhật; mỗi engine đạt 55/55 kiểm tra ở cả hai lượt.
- Hai đơn COD qua API thật tăng số đơn 30 → 32; giá trị đặt hàng 11.565.410 → 12.063.410 VND; doanh thu đã ghi nhận giữ 8.618.410 VND. Trino đọc theo manifest mới và vẫn đọc được kết quả cũ qua manifest trước đó.
- Sửa sai tổng tiền mart trong fixture khiến gate chặn publication và lưu audit lỗi. Rebuild phục hồi cùng `run_id` giữ một publication logic; `--verify-only` đạt không tạo publication. Sau lượt cập nhật có hai publication logic.
- Snapshot tồn kho giữ hai ngày (56 dòng) trong daily, còn current chỉ giữ ngày quan sát (28 dòng). Ngày trước là fixture chủ động để kiểm tra retention.
- Bộ pipeline offline có 50 test: 43 đạt, 7 integration opt-in được bỏ qua; ca Gold stack được chạy riêng và đạt. Image Python không cần Java/PySpark. Các tài nguyên stack riêng đã được dọn.

Gold native vẫn là luồng migration riêng, chưa thay production Airflow/Spark hoặc Superset. Chưa có nghiệm thu tải lớn/hiệu năng và chưa chuyển log, hard delete hay maintenance.


### Chạy và khắc phục lỗi mart log native

Chạy `jobs.logs.run_polars_marts` sau khi fact Spark và publication catalog có sẵn. Đích là namespace migration, reader BI hiện tại vẫn dùng serving. Xem audit ở `<prefix>_system.logs_quality_results`, lọc `run_id`/`status='FAILED'`. Nếu `event_identity` lỗi, kiểm tra nguồn fact và gate upstream; không dedup tùy tiện để vượt lỗi. Nếu identity mapping nhân bản, kiểm tra slug/UUID catalog đã công bố. Nếu readback/conservation lỗi, sửa nguồn/logic hoặc rebuild rồi verify lại; không tự cập nhật manifest.

Retry cùng `run_id`/`source_mode=logs_rollup` giữ một publication logic. `--verify-only` lưu audit mà không publish. Manifest cũ vẫn đọc snapshot cũ sau rebuild hoặc lỗi gate; không expire các snapshot mà publication còn dùng. `--as-of` là cutoff event, không phải watermark ingest: sự kiện đến muộn đã có trong fact vẫn tham gia full rebuild nếu thời gian event trong phạm vi. Event tương lai bị loại khỏi mart ở cutoff đang chọn.

Bộ kiểm thử offline có các ca UUID cũ/mới, unresolved keys, duplicate mapping, request/visitor semantics, percentile, NULL/empty và corruption. Ca stack opt-in dùng handoff DLQ kiểm thử; không coi đây là nghiệm thu Kafka/Flink hoặc full native access-log ingestion. Chưa đo giới hạn RAM, latency hay equivalence percentile trên dữ liệu lớn.

### Nghiệm thu mart log native ngày 11/11/2026

Run sạch `tlcn-stack-53b840e713` đạt trong **194,555 giây**, gồm lifecycle và Spark tham chiếu; không dùng thời gian này để so hiệu năng engine. Report local, gitignored: `data/pipeline-runs/tlcn-stack-53b840e713/report.json`.

- 21 envelope từ API thật (gồm hai checkout qua runner) đi qua handoff DLQ kiểm thử và các phép biến đổi medallion Spark. Replay raw không nhân bản fact; một envelope hỏng nằm ở Bronze. Không tuyên bố nghiệm thu Kafka/Flink hay tự replay DLQ trong ca này.
- Cả ba mart native khớp fingerprint nghiệp vụ Spark ở 21 event; khi thêm một event đến muộn vào fact, cả ba tiếp tục khớp ở 22 event. Native audit đủ 15 kiểm tra trước publication.
- Sai tổng request khiến verify gate lỗi; publication cũ vẫn đọc được qua Trino. Rebuild phục hồi cùng run chỉ giữ một publication logic; verify-only không tạo publication. Duplicate event ID ở fact bị chặn trước khi ghi mart mới.
- Sửa tên sản phẩm trong Silver mà chưa publish không thay kết quả; event tương lai bị loại ở cutoff cố định. Publication OLTP giữ nguyên suốt ca kiểm chứng.
- Cutoff trước mọi event xóa toàn bộ mart current về rỗng và publish `source_event_at=NULL`; các snapshot cũ vẫn đọc được. Kết thúc có bốn publication log logic riêng.
- Pipeline offline: **55 test đạt, 8 integration opt-in được bỏ qua**; ca log stack chạy riêng và đạt. 12 test log mới cũng skip đúng khi thiếu extra Polars. Ruff/format, lock/dependency, liên kết local và hash source trong image đạt; `FINAL_REPORT.md` giữ nguyên. Tài nguyên stack riêng đã được dọn.

Phạm vi còn lại: chuyển log ingestion từ Landing tới fact, rồi hard delete/maintenance và scheduler/serving. Cần chốt tương thích percentile với Spark approximate trên tập dữ liệu lớn trước cutover; ca nhỏ này không chứng minh equivalence tổng quát hoặc giới hạn RAM.
