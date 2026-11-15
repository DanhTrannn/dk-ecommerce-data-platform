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
| `data/access-admin/audit.jsonl` | Access CRUD/binding/workload assignment journal/readback và operation ID; không phải tamper-proof audit store |
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
| PostgreSQL healthy nhưng API thiếu bảng | Kiểm tra kết quả `platform-admin-init`, đúng DB/user và schema có đầy đủ; bootstrap không migrate schema dở dang. Seed catalog vẫn là bước riêng; không reset volume |
| Đổi password env nhưng không login | Init PostgreSQL/seed/metadata không tự đổi password đã lưu; xác nhận credential retained trước khi rotate có kiểm soát |
| Batch đọc PostgreSQL lỗi auth/permission | Reader password/env đúng và script `make source-reader` đã chạy sau đủ schema; không đưa credential vào URL kết nối |
| Job batch không thấy credential | Kiểm tra tab Cấp cho workload, trạng thái exporter và consumer file đúng volume/UID/GID; không thay bằng MinIO root hoặc chmod mở rộng toàn bộ |
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

Giữ staging/Bronze/Silver và failed task logs để retry. Job ingest/Gold/log/maintenance dùng advisory lock chung theo namespace; writer đồng thời bị từ chối. Lock không bao cả DAG nhiều task, nên vẫn cần tạm dừng DAG còn lại khi phục hồi để tránh xen kẽ. Một lần ghi Gold dở có thể làm current physical tables thay đổi, nhưng reader vẫn dùng manifest cũ. Sau khi sửa lỗi, chạy quality gate/publish đúng logical run; manifest dùng PyIceberg upsert theo `run_id`/`source_mode` chống trùng logical run, không chỉnh snapshot ID bằng tay để “làm mới” dashboard.

### Flink/Kafka

- Giữ Kafka data/group offsets và volume checkpoints.
- TaskManager restart phục hồi bằng checkpoint của JobManager đang còn; việc này khác với submit mới từ committed Kafka offsets.
- Submit mới không tự truyền đường dẫn restore/savepoint. Giữ group/offset và retention.
- Trước đổi graph, dừng đúng job writer với savepoint hoàn tất, giữ path và xác nhận không còn writer cũ.
- `KAFKA_LOG_DIRS=/var/lib/kafka/data` tương ứng volume `kafka-data`. Cài đặt cũ dùng `/tmp/kafka-logs` cần sao lưu và copy đầy đủ metadata/partition vào volume trong lúc broker đã dừng; đổi env không tự chuyển file.

Job log đặt checkpoint 10 giây trong Python runtime (ghi đè interval 30 giây ở file cấu hình Flink); đây là cấu hình, không phải cam kết latency hoặc exactly-once toàn hệ thống. Raw Landing có thể có event lặp khi chủ động replay; đọc theo key/grain đúng. Chưa nghiệm thu JobManager HA hoặc tùy ý reset offsets.

### DLQ và raw log

`KAFKA_ENABLED=false` bỏ qua gửi Kafka và không ghi DLQ; structured access log vẫn được middleware ghi. Khi bật producer, event offline/overload/lỗi gửi mới được đưa vào DLQ. Biến này được đọc từ môi trường tiến trình lúc import, không qua API Settings đọc `.env`.

Producer chờ broker ack ở task nền có deadline 10 giây, tối đa 256 pending; retry kết nối ban đầu sau mỗi lần thất bại có khoảng nghỉ 5 giây. Gửi timeout có thể đã tới broker, nên không khẳng định DLQ chỉ chứa event chưa được ghi. Replay cần giữ **nguyên request ID và payload**, phân biệt bản sao với payload xung đột. Không có command tự replay DLQ trong repo; không bịa lệnh upload JSONL để kỳ vọng mart tự cập nhật.

Raw malformed event được giữ để điều tra, không đưa vào Silver/fact hợp lệ. Sau sửa lỗi giữa các commit, job log tìm raw hash/ID còn thiếu từ các layer persist. Không chạy standalone log writer chồng với `lakehouse_logs_marts`.

## 6. Bảo trì và lưu trữ

DAG OLTP maintenance chạy **04:00**, sau lịch batch 03:00 nhưng không đảm bảo batch dài đã kết thúc. DAG streaming maintenance chạy hai giờ/lần. Các job maintenance giữ advisory lock theo namespace như writer batch/log; vẫn cần điều phối với Flink đang append vì Flink không dùng khóa PostgreSQL này.

[Table profiles](../pipelines/src/lakehouse/utils/maintenance.py) lấy OLTP từ `SOURCE_TABLES` và Gold `TABLES`, còn streaming liệt kê Landing/Bronze/Silver/fact/ba mart. `--tables` vẫn bị giới hạn vào whitelist này. Compaction overwrite dữ liệu hiện tại trong transaction; expire giữ snapshot mới nhất theo retention, snapshot refs và các snapshot xuất hiện trong **mọi dòng publication hiện có**, gồm snapshot đầu vào log.

Orphan mặc định chỉ **báo số candidate**, giới hạn trong thư mục `data/` của bảng thuộc bucket cấu hình. Xóa vật lý cần đồng thời `--delete-orphans --writers-stopped`; các DAG định kỳ không truyền hai cờ này. CLI còn có cleanup quarantine theo tuổi; xem [entrypoint](../pipelines/src/jobs/maintenance/iceberg_table_maintenance.py) cho tham số thực tế.

Publication-aware retention chưa phải quản lý vòng đời dashboard: giữ mọi manifest có thể giữ snapshot cũ lâu dài và làm dung lượng tăng. Không tự xóa manifest/snapshot để giảm dung lượng nếu chưa đối chiếu report còn dùng. Nếu snapshot mất do thao tác ngoài luồng, reader phải báo lỗi thay vì chuyển sang current data. Sync publication mới tạo URL mới, không sửa dashboard cũ.

| Tài nguyên | Nội dung cần bảo toàn |
|---|---|
| `postgres-data` | Nguồn nghiệp vụ, Airflow/Polaris và metadata Superset khi đã tạo |
| `minio-data` | Object/data/metadata Iceberg |
| `access-admin-secrets` | Credential nguồn của principal và khóa ký yêu cầu exporter |
| `credential-export-state` | Manifest phân công workload và audit exporter |
| Consumer credential volumes | File `clients.env` cùng ownership của Airflow/Flink/Trino; khôi phục đồng bộ với principal và manifest |
| `kafka-data`, `flink-checkpoints` | Broker offsets, log transport và checkpoint state |
| `./data` | Scenario journals, DLQ và audit quản trị quyền |
| `.env`/secret store | Credential, PII salt và Superset encryption secret phù hợp với dữ liệu đã lưu |

`make down` giữ named volumes, không phải backup. Chưa nghiệm thu backup/restore đồng bộ nhiều hệ thống hoặc disaster recovery; cần kế hoạch riêng nếu dùng ngoài local PoC. Không commit backup/raw data/credential vào Git.

## 7. Kiểm tra chất lượng và CI

Các lệnh static/offline chạy từ root repo:

```bash
uv lock --check
uvx ruff check .
python3 -m compileall -q airflow/dags/
docker compose --env-file .env.example --profile '*' config --quiet
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Unit/integration test Python chỉ giữ local và không được clone từ Git; dbt data tests ở `pipelines/dbt/tests/` là ngoại lệ được version control. Nếu đã có thư mục test local, chạy:

```bash
uv run --locked --package platform-app -- \
  python -m unittest discover -s services/platform-app/tests -v
PYTHONPATH=pipelines/src uv run --locked --package batch-pipeline -- \
  python -m unittest discover -s pipelines/tests -v
```

Không coi `Ran 0 tests` là bằng chứng có test coverage. Checkout mới không kèm bộ fixture pipeline/Superset hay các lệnh `RUN_ACCESS_LOG_STACK_TESTS`/`RUN_SUPERSET_STACK_TESTS` của thiết kế cũ. Các suite opt-in chỉ được chạy khi đã đọc test source, có đủ fixture và xác nhận tài nguyên cô lập; không trỏ vào dữ liệu hiện tại để thay thế fixture thiếu.

Kiểm chứng local ghi nhận ngày **15/11/2026** cho phần gộp API: 86 test platform-app (5 opt-in bỏ qua), build image và bốn test tích hợp riêng đạt trên stack quyền cô lập; stack test đã dọn. Phạm vi gồm CRUD MinIO/Polaris, quyền MinIO thực, cấp/gỡ credential và Streamlit. Đây không phải lần nghiệm thu toàn bộ generator → Lakehouse → BI. Số đo generator trong [generator README](../generator/README.md) là các lần đo được ghi lại trước đó, không phải job đang chạy hoặc benchmark lặp lại ở mỗi lần sửa tài liệu.

CI có hai job theo [workflow](../.github/workflows/ci.yml):

- Repository validation: **bốn** tổ hợp profile `core`, `core,streaming`, `core,bi`, `core,streaming,bi`; compile DAG và Ruff.
- Storefront: `npm ci`, lint, typecheck và production build của Next.js cũ.

CI không chạy unit/integration Python, không dựng và nghiệm thu cả stack dữ liệu. Docs/Markdown-only có thể bị bỏ qua theo `paths-ignore`; các kết quả local không thay thế trạng thái CI thực tế.

## 8. Phần cần hoàn thiện

- Loader log tổng hợp Parquet vào Lakehouse và mapping typed cho `session.id`/`ecommerce.order_number`; giữ tách provenance synthetic/observed.
- Bộ kiểm chứng có thể tái lập generator → batch/log → publication → Superset trên stack cô lập, gồm khối lượng và đối soát dữ liệu.
- Chính sách đồng bộ hard delete nguồn; mở rộng BI ngoài hai mart doanh thu, viewer/store và vòng đời publication/assets/snapshot.
- Runner cho nghiệp vụ ngoài mua online, replay DLQ và kịch bản phục hồi đã được kiểm chứng.
- Dọn Next.js/CI/dependency sau khi xác định đủ chức năng thay thế. Không mô tả hạng mục kế hoạch như chức năng đã nghiệm thu.
