# Vận hành và xử lý sự cố

Cài đặt ở [SETUP.md](SETUP.md); chi tiết pipeline ở [airflow/README.md](../airflow/README.md).

## 1. Kiểm tra nhanh

| Thành phần | Kiểm tra |
|---|---|
| Container | `make ps`; trạng thái health trong Compose |
| API | `curl -fsS http://127.0.0.1:8000/health/ready` (chỉ kiểm tra kết nối DB) |
| Airflow | Lần chạy mới nhất của `lakehouse_oltp_init`/`_incremental`/`_sync` xanh; log task có dòng `STAGING`, `RECONCILED`, `BRONZE`, `SCHEMA`, `SYNC`, `PURGE` |
| Lakehouse | Truy vấn Trino ở mục 3 |

```bash
docker compose logs --tail=100 platform-app postgres
docker compose logs --tail=100 airflow-scheduler airflow-dag-processor
docker compose logs --tail=100 trino polaris
```

Không in `docker compose config` với `.env` thật hay dump biến môi trường khi chia sẻ log.

## 2. Log và dữ liệu cần giữ

| Nơi | Nội dung |
|---|---|
| Volume `airflow-logs` | Log task Airflow, gồm log dbt của từng model |
| Container `platform-app` | Log API có request ID |
| `data/scenarios/` | Journal kịch bản mua hàng của runner |
| `data/access-admin/audit.jsonl` | Journal thao tác quản trị quyền |
| `data/events/dlq_access_logs.jsonl` | Event Kafka gửi chưa thành công; chưa có lệnh replay |

## 3. Kiểm tra bằng Trino

```sql
SHOW TABLES FROM lakehouse.gold;
SELECT max(_ingested_at), count(*) FROM lakehouse.bronze.orders;
SELECT sum(orders), sum(order_value_vnd) FROM lakehouse.gold.mart_orders_daily;
SELECT sum(net_revenue_vnd), sum(gross_profit_vnd) FROM lakehouse.gold.mart_sales_daily;
```

## 4. Xử lý sự cố

| Task đỏ hoặc triệu chứng | Nguyên nhân thường gặp | Xử lý |
|---|---|---|
| `create_schema`, `staging.<bảng>` lỗi kết nối nguồn | Connection `oltp_postgres` sai, chưa chạy `make source-reader`, mật khẩu reader trống | Sửa connection trong Airflow UI, **Clear** task |
| `schema.<bảng>` lỗi `SchemaChanged ... unlisted` | Nguồn có cột mới chưa duyệt | Nạp: thêm vào `columns` trong `sources.py` **và** model Silver, cùng một commit. Không nạp: thêm vào `excluded`. Rồi **Clear** task; sync tự thêm cột và nạp lại bảng |
| Log `schema.<bảng>` có `silver_missing=[...]` | Bronze có cột mà Silver chưa có | Thêm cột vào model Silver, rồi chạy `lakehouse_oltp_init` để các dòng cũ có giá trị |
| `staging.<bảng>` hoặc `schema.<bảng>` lỗi `SchemaChanged ... dropped` | Cột đang nạp đã bị bỏ ở nguồn | Bỏ cột khỏi `sources.py`, model Silver và các model Gold dùng nó, rồi **Clear** task. Bronze giữ cột cũ làm lịch sử |
| `SchemaChanged ... incompatible` | Cột đổi loại (số ↔ chữ…) | Sửa model Silver theo kiểu mới. Iceberg không đổi được loại cột, nên phải xoá bảng Bronze đó rồi chạy init (cần duyệt; mất lịch sử xoá mềm của bảng) |
| `staging.<bảng>` lỗi giá trị vượt kiểu, nhắc `lakehouse_oltp_sync` | Nguồn đã nới kiểu, sync chưa chạy | Trigger tay `lakehouse_oltp_sync`, rồi **Clear** task incremental |
| `staging.<bảng>` báo thiếu data interval | Trigger tay DAG incremental | Để lịch chạy, hoặc dùng `airflow backfill create` |
| Cần nạp lại các ngày đã qua | | Tạm dừng `lakehouse_oltp_sync`, backfill incremental, rồi bật lại và trigger tay sync một lần ([airflow/README.md](../airflow/README.md#run)) |
| Dòng đã xoá ở nguồn vẫn sống ở Gold sau purge | Purge chạy khi lần sync trước chưa xong | Chạy `lakehouse_oltp_init` |
| `reconcile.<bảng>` lỗi `ReconciliationFailed` | Bản trích xuất lệch nguồn tại mốc chạy (số dòng, tổng tiền, sổ kho); Bronze chưa đổi | Xem log, **Clear** `staging.<bảng>` kèm downstream |
| `silver.<bảng>` lỗi test `matches_bronze` | Silver và Bronze lệch khoá | Chạy `lakehouse_oltp_init` để dựng lại Silver |
| `gold.<model>` lỗi | Lỗi SQL dbt hoặc Trino | Chạy tay theo [airflow/README.md](../airflow/README.md#run), **Clear** task kèm downstream |
| Lỗi credential Polaris/MinIO | Workload chưa được cấp credential, hoặc consumer chưa restart sau khi đổi | Kiểm tra tab **Cấp cho workload** và exporter; restart consumer |
| Trino từ chối quyền ghi | Đúng thiết kế với catalog `lakehouse`; dbt ghi qua `lakehouse_etl` | Không cấp quyền ghi cho reader |
| Compose báo thiếu biến | `.env` thiếu biến mới | So với `.env.example` |
| API thiếu bảng | `platform-admin-init` không áp được schema | Xem log init; bootstrap không migrate schema dở dang |

Mọi task đều chạy lại được và cho cùng kết quả: staging xoá trắng rồi ghi lại, Bronze upsert theo khoá, Silver và Gold merge theo khoá từ mốc của chúng, mốc của mart chỉ tiến khi mart đã ghi xong. Nếu một lần chạy dừng giữa chừng, lần chạy sau (hoặc **Clear**) bắt kịp mọi thay đổi.

## 5. Bảo trì Iceberg

Nhóm `maintenance` chạy cuối mỗi lần chạy DAG:

| Task | Việc |
|---|---|
| `compact` | Gộp file nhỏ của Bronze khi có từ 10 file nhỏ hoặc có file xoá |
| `optimize` | dbt `ALTER TABLE … EXECUTE optimize` cho Silver, Gold và `gold._mart_state` |
| `expire` | Xoá snapshot ngoài 20 snapshot mới nhất, ref và các snapshot dưới 3 ngày |
| `orphan` | Chỉ đếm file không còn snapshot nào tham chiếu; không xoá |

## 6. Sao lưu

| Volume | Nội dung |
|---|---|
| `postgres-data` | Nguồn nghiệp vụ, metadata Airflow, Polaris, Superset |
| `minio-data` | Dữ liệu và metadata Iceberg |
| `access-admin-secrets`, `credential-export-state` | Credential principal, khoá ký exporter, phân công workload |
| Volume credential của consumer | `clients.env` của Airflow, Flink, Trino |
| `./data` | Journal kịch bản, DLQ, audit quản trị quyền |
| `.env` | Credential và secret khớp với dữ liệu đã lưu |

`make down` giữ named volumes nhưng không phải bản sao lưu. Backup/restore đồng bộ nhiều hệ thống chưa được kiểm chứng.

## 7. Kiểm tra mã và CI

```bash
uv lock --check
uvx ruff check .
make test                                                    # mọi bộ test; test pipeline chạy trong container Airflow
docker compose --env-file .env.example --profile '*' config --quiet
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Mọi bộ test đều nằm trong git và chạy không cần stack thật: `airflow/tests`, `services/platform-app/tests`, `generator/tests`, `infrastructure/superset/tests`, cùng dbt data test trong `dbt/tests`.

CI ([workflow](../.github/workflows/ci.yml)) kiểm tra bốn tổ hợp profile Compose, compile DAG và chạy Ruff; CI không chạy test.
