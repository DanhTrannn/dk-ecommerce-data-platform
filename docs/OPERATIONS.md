# Vận hành và xử lý sự cố

Cài đặt ở [SETUP.md](SETUP.md); chi tiết pipeline ở [airflow/README.md](../airflow/README.md).

## 1. Kiểm tra nhanh

| Thành phần | Kiểm tra |
|---|---|
| Container | `make ps`; trạng thái health trong Compose |
| API | `curl -fsS http://127.0.0.1:8000/health/ready` (chỉ kiểm tra kết nối DB) |
| Airflow | Lần chạy mới nhất của `lakehouse_oltp_init`/`lakehouse_oltp_incremental` xanh; log task có dòng `STAGING`, `RECONCILED`, `BRONZE` |
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
| `staging.<bảng>` báo thiếu cột nguồn | Nguồn có cột mới | Chạy `lakehouse_oltp_init` |
| `staging.<bảng>` báo thiếu data interval | Trigger tay DAG incremental | Để lịch chạy, hoặc dùng `airflow backfill create` |
| `reconcile.<bảng>` lỗi `ReconciliationFailed` | Bản trích xuất lệch nguồn tại mốc chạy (số dòng, tổng tiền, sổ kho); Bronze chưa đổi | Xem log, **Clear** `staging.<bảng>` kèm downstream |
| `silver.<bảng>` lỗi test `matches_bronze` | Silver và Bronze lệch khoá | Chạy `lakehouse_oltp_init` để dựng lại Silver |
| `gold.<model>` lỗi | Lỗi SQL dbt hoặc Trino | Chạy tay theo [airflow/README.md](../airflow/README.md#run), **Clear** task kèm downstream |
| Lỗi credential Polaris/MinIO | Workload chưa được cấp credential, hoặc consumer chưa restart sau khi đổi | Kiểm tra tab **Cấp cho workload** và exporter; restart consumer |
| Trino từ chối quyền ghi | Đúng thiết kế với catalog `lakehouse`; dbt ghi qua `lakehouse_etl` | Không cấp quyền ghi cho reader |
| Compose báo thiếu biến | `.env` thiếu biến mới | So với `.env.example` |
| API thiếu bảng | `platform-admin-init` không áp được schema | Xem log init; bootstrap không migrate schema dở dang |

Mọi task đều chạy lại được: staging xoá trắng rồi ghi lại, Bronze upsert theo khoá, Silver merge theo khoá, Gold dựng lại toàn bộ. Nếu một lần chạy dừng giữa chừng, Gold có thể lệch Silver tới khi lần chạy sau hoàn tất.

## 5. Bảo trì Iceberg

Nhóm `maintenance` chạy cuối mỗi lần chạy DAG:

| Task | Việc |
|---|---|
| `compact` | Gộp file nhỏ của staging, Bronze và Gold; Silver được dbt `optimize` sau mỗi lần merge |
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
make test                                                    # test pipeline trong container Airflow
docker compose --env-file .env.example --profile '*' config --quiet
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

Test pipeline (`airflow/tests`) và dbt data test (`dbt/tests`) nằm trong git. Test của `platform-app`, generator và Superset chỉ giữ ở máy.

CI ([workflow](../.github/workflows/ci.yml)) kiểm tra bốn tổ hợp profile Compose, compile DAG và chạy Ruff; CI không chạy test.
