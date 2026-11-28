# Vận hành và xử lý sự cố

Cài đặt ở [SETUP.md](SETUP.md); chi tiết pipeline ở [airflow/README.md](../airflow/README.md).

## 1. Kiểm tra nhanh

| Thành phần | Kiểm tra |
|---|---|
| Container | `make ps`; trạng thái health trong Compose |
| API | `curl -fsS http://127.0.0.1:8000/health/ready` (chỉ kiểm tra kết nối DB) |
| Airflow | Lần chạy mới nhất của `lakehouse_oltp_init`/`_full_daily`/`_incremental_daily`/`_sync` xanh; log task có dòng `STAGING`, `RECONCILED`, `BRONZE`, `SCHEMA`, `SYNC`, `PURGE` |
| Lakehouse | Truy vấn Trino ở mục 3 |
| Superset | `curl -fsS http://127.0.0.1:8088/health`, sau đó mở một dashboard và kiểm tra dữ liệu của biểu đồ |

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

## 3. Kiểm tra bằng Trino

```sql
SHOW TABLES FROM lakehouse.gold;
SELECT max(_ingested_at), count(*) FROM lakehouse.bronze.orders;
SELECT min(order_date), max(order_date), sum(orders), sum(order_value_vnd)
FROM lakehouse.gold.mart_orders_daily;
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
| `staging.<bảng>` báo thiếu data interval | Trigger tay DAG incremental | Để lịch chạy |
| Cần nạp lại các ngày đã qua | DAG incremental không backfill | Chạy `lakehouse_oltp_init` |
| `wait_full_daily` chờ lâu hoặc đỏ | Run `lakehouse_oltp_full_daily` cùng ngày chưa xong, lỗi hoặc DAG đang tạm dừng | Bật hoặc sửa run full daily cùng ngày; sensor tự chạy tiếp, hoặc **Clear** `wait_full_daily` |
| Dòng đã xoá ở nguồn vẫn sống ở Gold sau purge | Purge chạy khi lần sync trước chưa xong | Chạy `lakehouse_oltp_init` |
| `reconcile.<bảng>` lỗi `ReconciliationFailed` | Bản trích xuất lệch nguồn tại mốc chạy (số dòng, tổng tiền, sổ kho); Bronze chưa đổi | Xem log, **Clear** `staging.<bảng>` kèm downstream |
| `silver.<bảng>` lỗi test `matches_bronze` | Silver và Bronze lệch khoá | Chạy `lakehouse_oltp_init` để dựng lại Silver |
| `gold.<model>` lỗi | Lỗi SQL dbt hoặc Trino | Chạy tay theo [airflow/README.md](../airflow/README.md#run), **Clear** task kèm downstream |
| `maintenance.<task>` báo `Invalid credentials` hoặc `cannot become user` | `TRINO_MAINTAINER_PASSWORD` thiếu trong `.env`, hoặc `trino-auth-init` chạy bằng image cũ | Thêm biến vào `.env`, build lại `platform-app`, chạy lại `trino-auth-init` rồi restart Trino |
| Airflow báo `The CSRF session token is missing` khi đăng nhập | Cookie phiên thiếu, hết hạn hoặc còn từ trước khi tách cookie hai ứng dụng | Mở lại trang đăng nhập Airflow từ đúng địa chỉ host. Nếu lỗi còn, xóa cookie của host hoặc thử cửa sổ riêng tư. Superset dùng cookie `superset_session` |
| Superset chạy nhưng danh sách dashboard trống | Metadata đã khởi tạo, các tài sản BI chưa được đồng bộ | Sau khi DAG init thành công, chạy `make superset-sync` và mở URL từ `SUPERSET_RESULT.dashboards` |
| Dashboard báo `No data` | Nguồn chưa có đơn, mart rỗng hoặc bộ lọc nằm ngoài lịch sử | Đối chiếu phạm vi ngày và tổng đơn bằng SQL ở mục 3. Chọn **No filter** để xem toàn kỳ |
| Superset báo `TLS/SSL is required for authentication` khi đọc Trino | Kết nối BI cũ thiếu HTTPS trong `connect_args` | Chạy `make superset-sync` để cập nhật kết nối, rồi tải lại dashboard. Kiểm tra `trino-ca` được mount vào Superset |
| Generator báo thiếu `GENERATOR_DATABASE_URL` | `.env` cũ chưa có cấu hình kết nối cho generator | Bổ sung dòng tương ứng từ [`.env.example`](../.env.example) và dùng các giá trị PostgreSQL hiện có |
| Lỗi credential Polaris | Workload chưa được cấp credential, hoặc consumer chưa restart sau khi đổi | Kiểm tra tab **Cấp cho workload** và exporter; restart consumer |
| S3 `403` khi đọc/ghi bảng | Account MinIO của Polaris thiếu hoặc lệch secret với `.env` | Xem `docker compose logs minio-init`, chạy lại `docker compose up minio-init`, rồi restart Polaris |
| Trino từ chối quyền ghi | Đúng thiết kế với catalog `lakehouse`; dbt ghi qua `lakehouse_etl` | Không cấp quyền ghi cho reader |
| Compose báo thiếu biến | `.env` thiếu biến mới | So với `.env.example` |
| API thiếu bảng | `platform-admin-init` không áp được schema | Xem log init; bootstrap không migrate schema dở dang |

Staging được ghi lại. Bronze bảng danh mục được ghi đè, bảng giao dịch upsert theo khóa. Silver giao dịch và fact merge từ watermark của chúng; dimension và mart dựng lại mỗi lần chạy. Dùng **Clear** cho task đã lỗi và downstream cần chạy lại. Nếu một cửa sổ incremental bị bỏ sót, hoặc một cập nhật commit muộn ngoài cửa sổ, chạy lại init. [Đồng bộ nguồn](SYNC_DESIGN.md)

## 5. Bảo trì Iceberg

Nhóm `maintenance` chạy cuối các DAG init, full daily, incremental và sync; full daily chỉ bảo trì bảng danh mục, incremental và sync chỉ bảo trì bảng giao dịch, init bảo trì tất cả. Mọi task chạy `ALTER TABLE … EXECUTE` qua Trino bằng tài khoản `lakehouse_maintainer` (chỉ có `SELECT`, không ghi dòng hay xoá bảng được).

| Task | Việc |
|---|---|
| `optimize` | `EXECUTE optimize`: gộp file nhỏ của Bronze, Silver, Gold (staging bị ghi lại mỗi lần nạp nên bỏ qua) |
| `expire` | `EXECUTE expire_snapshots(retention_threshold => '7d')` cho mọi tầng |
| `orphan` | `EXECUTE remove_orphan_files(retention_threshold => '7d')`: **xoá** file không snapshot nào còn tham chiếu và cũ hơn 7 ngày |

7 ngày là mức tối thiểu mặc định của Trino; file và snapshot trẻ hơn không bị đụng tới, nên reader hay lần ghi đang chạy vẫn an toàn.

## 6. Sao lưu

| Volume | Nội dung |
|---|---|
| `postgres-data` | Nguồn nghiệp vụ, metadata Airflow, Polaris, Superset |
| `minio-data` | Dữ liệu và metadata Iceberg |
| `access-admin-secrets`, `credential-export-state` | Credential principal, khoá ký exporter, phân công workload |
| Volume credential của consumer | `clients.env` của Airflow và Trino |
| `./data` | Journal kịch bản và audit quản trị quyền |
| `.env` | Credential và secret khớp với dữ liệu đã lưu |

`make down` giữ named volumes nhưng không phải bản sao lưu. Backup/restore đồng bộ nhiều hệ thống chưa được kiểm chứng.

## 7. Kiểm tra mã và CI

```bash
uv lock --check
uvx ruff check .
make test                                                    # mọi bộ test; test pipeline chạy trong container Airflow
docker compose --env-file .env.example config --quiet
uv run --locked --package platform-app -- \
  python -m platform_app.api.db.schema --output database/schema.sql --check
```

`make test` dùng container `airflow-scheduler` đang chạy cho test Python của pipeline. Các bộ platform-app, generator và Superset chạy trong Python workspace trên host. Data test trong `dbt/tests` chạy qua task dbt của Airflow hoặc lệnh dbt riêng với Trino. Các test được thêm để kiểm chứng cục bộ tuân theo quy ước của repo.

CI kiểm tra cấu hình Compose, compile DAG và chạy Ruff. [Workflow](../.github/workflows/ci.yml)
