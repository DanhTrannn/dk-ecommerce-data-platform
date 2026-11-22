# **THIẾT KẾ VÀ TRIỂN KHAI DATA LAKEHOUSE PHỤC VỤ XỬ LÝ VÀ PHÂN TÍCH DỮ LIỆU CHO WEBSITE THƯƠNG MẠI ĐIỆN TỬ THỜI TRANG**

---

# **CHƯƠNG 1. TỔNG QUAN ĐỀ TÀI**

## **1.1. Tính cấp thiết của đề tài**

Trong bối cảnh chuyển đổi số và sự phát triển của thương mại điện tử (TMĐT), các doanh nghiệp bán lẻ thời trang phải quản lý và khai thác một khối dữ liệu lớn, phát sinh liên tục. Dữ liệu này đa dạng về cấu trúc: dữ liệu giao dịch có cấu trúc chặt chẽ (đơn hàng, thanh toán, tồn kho theo biến thể màu sắc và kích cỡ) và dữ liệu nhật ký hành vi người dùng bán cấu trúc (xem sản phẩm, tìm kiếm, thêm vào giỏ, đặt hàng).

Khai thác hiệu quả nguồn dữ liệu hỗn hợp này là thách thức với các kiến trúc truyền thống. Kho dữ liệu (Data Warehouse) bảo đảm tính nhất quán giao dịch (ACID) và tối ưu cho truy vấn phân tích (OLAP), nhưng chi phí lưu trữ cao khi mở rộng với dữ liệu bán cấu trúc và kém linh hoạt khi tiếp nhận dữ liệu streaming. Ngược lại, Hồ dữ liệu (Data Lake) lưu dữ liệu thô với chi phí thấp trên lưu trữ đối tượng (Object Storage), nhưng thiếu cơ chế kiểm soát giao dịch, dễ trở thành "vũng lầy dữ liệu" (Data Swamp), làm giảm chất lượng dữ liệu và hiệu năng truy vấn.

Kiến trúc **Data Lakehouse** kết hợp chi phí thấp và khả năng mở rộng của Data Lake với năng lực quản trị, độ tin cậy và giao dịch ACID của Data Warehouse. Nghiên cứu, thiết kế và triển khai một Data Lakehouse cho bài toán bán lẻ thời trang đa kênh (bán trực tuyến và bán tại cửa hàng POS) có ý nghĩa thực tiễn: hợp nhất dữ liệu, quản lý tồn kho chính xác hơn, phân tích hành vi mua sắm và hỗ trợ các cấp quản lý ra quyết định dựa trên số liệu đối soát được với nguồn.

## **1.2. Mục tiêu nghiên cứu**

Đề tài hướng tới hai nhóm mục tiêu:

1. **Mục tiêu tổng quát**:
   * Nghiên cứu, thiết kế và xây dựng một hệ thống Data Lakehouse đầu cuối (End-to-End) cho lĩnh vực TMĐT thời trang, gồm thu thập, lưu trữ phân tầng, xử lý, quản trị quyền truy cập và trực quan hóa các chỉ số kinh doanh.

2. **Mục tiêu cụ thể**:
   * Xây dựng hệ thống giao dịch nguồn: API nghiệp vụ FastAPI cho bán hàng trực tuyến, bán hàng tại quầy (POS), kho, giao vận và đổi trả, dùng chung một cơ sở dữ liệu PostgreSQL 16 gồm 28 bảng.
   * Thiết kế và triển khai Data Lakehouse phân tầng Medallion (Staging – Bronze – Silver – Gold) trên định dạng bảng mở Apache Iceberg, lưu trữ MinIO S3 và catalog Apache Polaris (Iceberg REST Catalog).
   * Xây dựng các đường ống dữ liệu tự động bằng Apache Airflow: nạp toàn bộ (init), nạp gia tăng hằng ngày (incremental), đồng bộ thay đổi nguồn mà nạp gia tăng không thấy (dòng bị xoá, thay đổi cột) và xoá cứng chủ động; mọi bước chạy lại an toàn (idempotent).
   * Xây dựng mô hình dữ liệu phân tích Star Schema bằng dbt trên Trino, gồm 7 bảng Dimension, 6 bảng Fact và 14 Data Mart, kèm đối soát với nguồn và kiểm tra chất lượng dữ liệu.
   * Tự quản trị quyền truy cập Lakehouse: mỗi workload một danh tính Polaris, credential lưu trữ tạm cấp theo từng bảng, Trino tách quyền đọc/ghi; trực quan hóa bằng Apache Superset.

## **1.3. Nhiệm vụ nghiên cứu**

* **Nhiệm vụ 1: Khảo sát nghiệp vụ và cơ sở lý thuyết**: Tìm hiểu quy trình bán hàng thời trang đa kênh, vòng đời đơn hàng, hạch toán doanh thu – giá vốn, quản lý tồn kho đa điểm và hệ thống KPI. Nghiên cứu các công nghệ: Apache Iceberg, MinIO, Apache Polaris, PyIceberg, dbt, Apache Airflow, Trino, Apache Kafka, Apache Flink và Apache Superset.
* **Nhiệm vụ 2: Xây dựng hệ thống giao dịch nguồn**: Thiết kế cơ sở dữ liệu PostgreSQL 28 bảng sinh từ ORM SQLAlchemy, phát triển API FastAPI với xác thực, phân quyền và middleware ghi nhật ký truy cập có che dữ liệu cá nhân; xây dựng Console Streamlit mô phỏng mua hàng qua API.
* **Nhiệm vụ 3: Thiết kế và triển khai Data Lakehouse**: Thiết lập lưu trữ đối tượng, catalog Polaris, các namespace phân tầng và mô hình phân quyền theo workload.
* **Nhiệm vụ 4: Xây dựng đường ống dữ liệu và kiểm soát chất lượng**: Trích xuất theo bảng bằng PyIceberg trong Airflow; dựng Silver và Gold bằng dbt; đối soát với nguồn; đồng bộ dòng bị xoá và thay đổi cột; bảo đảm mọi tầng chạy lại an toàn.
* **Nhiệm vụ 5: Sinh dữ liệu mô phỏng, trực quan hóa và kiểm thử**: Xây dựng bộ sinh dữ liệu `datagen` có căn cứ thống kê từ dữ liệu công khai, dashboard Superset và kiểm thử pipeline trên dữ liệu mô phỏng.

## **1.4. Đối tượng và phạm vi nghiên cứu**

* **Đối tượng nghiên cứu**:
  * Kiến trúc Data Lakehouse, định dạng bảng mở Apache Iceberg và catalog chuẩn mở Apache Polaris.
  * Quy trình thu thập, xử lý và tích hợp hai loại dữ liệu: dữ liệu giao dịch có cấu trúc (OLTP) và dữ liệu sự kiện nhật ký bán cấu trúc (Access Log).
  * Mô hình hóa đa chiều Kimball Star Schema và Data Mart phục vụ khai thác kinh doanh.
  * Các KPI kinh doanh, kho vận, giao vận và phễu tương tác trong TMĐT.

* **Phạm vi nghiên cứu**:
  * *Phạm vi nghiệp vụ*: chu trình bán lẻ thời trang gồm danh mục sản phẩm theo biến thể (màu, size), giỏ hàng, đặt hàng trực tuyến, bán tại quầy POS, tồn kho đa vị trí, giao hàng COD, đổi trả – hoàn tiền và theo dõi tương tác người dùng.
  * *Phạm vi kỹ thuật*: triển khai trên một máy chủ bằng Docker Compose với các profile `core`, `streaming`, `bi`. Pipeline OLTP và quản trị quyền đã hoàn thiện; luồng access log vào Lakehouse đã có thiết kế, chưa xây. Phân cụm chịu lỗi cao (High Availability), tự động co giãn và học máy là hướng mở rộng.

## **1.5. Tổng quan các công trình liên quan**

Các triển khai ban đầu dựa trên Data Warehouse tập trung (Inmon hay Kimball): toàn vẹn dữ liệu cao, hỗ trợ ACID và truy vấn phân tích được tối ưu, nhưng chi phí lưu trữ trên hệ thống chuyên dụng đắt đỏ, khó theo kịp khối lượng dữ liệu nhật ký clickstream [1], [7].

Hadoop và phong trào Data Lake trên lưu trữ đối tượng (S3, HDFS) giải quyết bài toán chi phí lưu trữ quy mô lớn. Tuy nhiên Data Lake thiếu quản lý giao dịch, dẫn đến rủi ro đọc dữ liệu dở dang khi ghi đồng thời, khó cập nhật/xoá dữ liệu (đặc biệt với yêu cầu quyền riêng tư như GDPR) và hiệu năng truy vấn báo cáo thấp.

Data Lakehouse ra đời cùng các định dạng bảng mở Apache Iceberg, Delta Lake và Apache Hudi. Apache Iceberg nổi bật với quản lý bảng theo snapshot, time-travel, tiến hóa lược đồ (schema evolution) an toàn và tách biệt lưu trữ với công cụ tính toán [9]. Các triển khai gần đây thường tổ chức dữ liệu theo kiến trúc Medallion (Bronze – Silver – Gold) [7], [11].

Phần lớn công trình dừng ở lý thuyết, dùng tập dữ liệu tĩnh (TPC-DS, TPC-H) hoặc triển khai rời rạc từng thành phần. Đề tài hiện thực một giải pháp liền mạch: từ ứng dụng nguồn phát sinh giao dịch, bộ sinh dữ liệu có căn cứ, pipeline tự động chạy lại an toàn, đồng bộ thay đổi nguồn, đến quản trị quyền tự vận hành và trực quan hóa.

## **1.6. Kết quả dự kiến đạt được**

1. **Sản phẩm phần mềm ứng dụng**:
   * API nghiệp vụ FastAPI cho mua hàng trực tuyến (giỏ hàng, checkout VietQR/COD), POS, kho, nhập hàng, giao vận, đổi trả và đánh giá.
   * Console Streamlit (Platform Console) gồm màn hình mô phỏng mua hàng qua API thật và màn hình quản trị quyền Lakehouse.
   * Middleware ghi nhật ký truy cập theo hợp đồng `ecommerce.access` v1.0.0, che dữ liệu nhạy cảm, có thể gửi vào Kafka.

2. **Sản phẩm hạ tầng và nền tảng dữ liệu**:
   * Data Lakehouse bốn namespace (`staging`, `bronze`, `silver`, `gold`) trên MinIO S3 và Apache Iceberg, quản lý bởi Polaris.
   * Bốn DAG Airflow: `lakehouse_oltp_init`, `lakehouse_oltp_incremental`, `lakehouse_oltp_sync`, `lakehouse_oltp_purge`; mỗi bảng và mỗi model dbt là một task riêng.
   * Quản trị quyền tự vận hành: principal Polaris riêng cho Airflow, Flink, Trino đọc và Trino ghi; credential lưu trữ tạm cấp theo bảng.

3. **Sản phẩm phân tích và báo cáo (BI)**:
   * Mô hình Star Schema gồm 7 dimension, 6 fact và 14 mart, cùng 2 seed (`delivery_sla`, `calendar_events`).
   * Dashboard doanh thu trên Apache Superset đọc trực tiếp các bảng Gold.

4. **Sản phẩm tài liệu kỹ thuật**:
   * Báo cáo đồ án, tài liệu kiến trúc, hợp đồng dữ liệu, cài đặt, vận hành và thiết kế trong thư mục `docs/`.

---

# **CHƯƠNG 2. CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ ÁP DỤNG**

## **2.1. Tổng quan về thương mại điện tử và bán hàng đa kênh**

### **2.1.1. Mô hình thương mại điện tử và bán hàng đa kênh**

Thương mại điện tử là phương thức kinh doanh trong đó giao dịch mua bán hàng hóa và dịch vụ được tiến hành qua mạng máy tính và phương tiện điện tử [1]. Trong mô hình B2C (Business-to-Consumer), doanh nghiệp tương tác trực tiếp với người tiêu dùng qua web hoặc ứng dụng di động.

Khi mở rộng sang bán hàng đa kênh (Omnichannel), doanh nghiệp kết hợp website với chuỗi cửa hàng vật lý. Thách thức lớn nhất là dữ liệu phân mảnh giữa các kênh, dẫn đến sai lệch tồn kho (overselling) và khó hợp nhất doanh thu. Đề tài tích hợp kênh trực tuyến và POS trên cùng một Backend và một cơ sở dữ liệu giao dịch, nên sản phẩm, giá, khách hàng và tồn kho luôn có một nguồn sự thật duy nhất.

### **2.1.2. Quy trình quản lý bán hàng, đơn hàng và tồn kho**

Bán hàng thời trang đòi hỏi quản lý theo biến thể (SKU) màu sắc và kích cỡ. Dù khách đặt online hay mua tại quầy, hệ thống đều thực hiện cùng các bước: xác thực giá bán, áp dụng mã giảm giá hợp lệ và kiểm tra tồn kho khả dụng.

Vòng đời đơn hàng tách ba luồng trạng thái liên kết với nhau:
1. **Trạng thái đơn hàng** (chờ xử lý, đã xác nhận, đang xử lý, hoàn thành, đã hủy).
2. **Trạng thái thanh toán** (chưa thanh toán, đã thanh toán, hoàn tiền một phần, hoàn tiền toàn bộ).
3. **Trạng thái giao nhận** (chờ lấy hàng, đang giao, giao thành công, giao thất bại).

Cách tách này giúp xử lý đúng các trường hợp COD, hủy đơn và đổi trả mà không ghi nhận sai thời điểm doanh thu. Với tồn kho, mọi biến động (nhập, xuất bán online, bán tại quầy, hoàn trả, điều chỉnh) được ghi vào sổ cái `inventory_transactions`; số dư tồn bằng tồn đầu cộng tổng sổ cái theo biến thể và vị trí.

### **2.1.3. Vai trò của phân tích dữ liệu trong hoạt động kinh doanh**

Dữ liệu giao dịch phản ánh kết quả kinh doanh: doanh thu thuần, số đơn, tỷ lệ hủy, lợi nhuận gộp và tồn kho. Tuy nhiên, nó chỉ cho biết *kết quả đã xảy ra*, không giải thích *hành vi dẫn đến kết quả đó*.

Dữ liệu nhật ký truy cập bổ sung góc nhìn hành vi: lưu lượng theo giờ, sản phẩm được xem nhiều, điểm rơi trong phễu giỏ hàng – thanh toán. Hệ thống OLTP được tối ưu cho giao dịch ngắn; chạy truy vấn tổng hợp nặng trên đó gây tranh chấp tài nguyên và ảnh hưởng người mua. Vì vậy dữ liệu được đồng bộ sang Lakehouse để phân tích mà không làm chậm hệ thống vận hành.

### **2.1.4. Hệ thống chỉ số KPI trong thương mại điện tử**

| Nhóm KPI | Chỉ số cốt lõi | Ý nghĩa nghiệp vụ |
|---|---|---|
| **Kinh doanh (Sales)** | Số đơn, giá trị đặt hàng, doanh thu hàng hóa, hoàn tiền, doanh thu thuần, giá vốn, lợi nhuận gộp | Sức khỏe tài chính sau giảm giá và hoàn tiền; doanh thu ghi nhận vào ngày muộn hơn giữa hoàn tất và thanh toán đủ |
| **Kho vận (Inventory)** | Tồn theo vị trí, giá trị tồn, số biến thể còn/hết hàng, bán trung bình 28 ngày | Cảnh báo thiếu hàng hoặc ứ đọng vốn tại từng điểm bán |
| **Vận hành (Operations)** | Tỷ lệ giao đúng hạn theo SLA thành phố, thời gian giao, đơn tồn đọng theo tuổi, tiền COD, hủy và đổi trả | Hiệu quả giao vận và rủi ro dòng tiền thu hộ |
| **Khách hàng và Marketing** | Hiệu quả coupon, đánh giá theo giao đúng hạn/trễ, cohort mua lại, giá trị khách hàng, phễu xem → giỏ → checkout → đặt đơn | Hiệu quả khuyến mại, chất lượng dịch vụ và giữ chân khách |
| **Kỹ thuật** | Lưu lượng request, tỷ lệ lỗi HTTP, độ trễ theo route | Độ ổn định của hệ thống nguồn |

Định nghĩa chi tiết từng chỉ số nằm trong [DATA.md](DATA.md).

## **2.2. Kiến trúc và công nghệ phát triển hệ thống thương mại điện tử**

### **2.2.1. Kiến trúc Web Client–Server và REST API**

Hệ thống nguồn theo kiến trúc Client–Server, tách giao diện và nghiệp vụ qua RESTful API [3]. Backend dùng **FastAPI** (Python 3.11): kiểm tra dữ liệu bằng Pydantic, sinh tài liệu OpenAPI và định nghĩa phụ thuộc (xác thực, quyền, phiên giao dịch) bằng Dependency Injection.

Giao diện là **Streamlit** [2] (Platform Console), chạy cùng container với FastAPI. Console gọi API thật qua HTTP như mọi client, gồm hai màn hình: mô phỏng mua hàng online và quản trị quyền Lakehouse. Giao diện storefront web trước đây (Next.js) đã được gỡ để tập trung vào nền tảng dữ liệu.

### **2.2.2. Cơ sở dữ liệu giao dịch OLTP**

Cơ sở dữ liệu giao dịch là **PostgreSQL 16** [4]: giao dịch ACID, khóa mức dòng và đọc nhất quán qua MVCC (Multi-Version Concurrency Control). Thời gian lưu kiểu `TIMESTAMPTZ` (UTC), UUID lưu kiểu UUID gốc, trạng thái được ràng buộc bằng CHECK, khóa ngoại và UNIQUE.

Tầng ứng dụng dùng **SQLAlchemy 2.0 ORM** [5]; lược đồ `database/schema.sql` được sinh từ ORM và kiểm tra khớp trong CI. Để tránh bán vượt tồn khi nhiều người mua cùng lúc, checkout chạy trong một giao dịch READ COMMITTED, khóa dòng tồn kho bằng `SELECT ... FOR UPDATE`, và tự thử lại có giới hạn khi gặp deadlock, lỗi tuần tự hóa hoặc khóa không lấy được. Lỗi kết nối không được thử lại tự động vì kết quả commit khi đó không xác định.

### **2.2.3. Xác thực, phân quyền và bảo mật hệ thống**

Mật khẩu được băm bằng **Argon2id**, thuật toán thắng Password Hashing Competition, chống tấn công vét cạn bằng GPU. Thông tin đăng nhập nằm trong bảng riêng `customer_credentials`, không bao giờ được nạp vào Lakehouse.

Xác thực dùng JSON Web Token (JWT) trong **cookie HttpOnly**, chống đánh cắp token qua XSS; mọi yêu cầu thay đổi trạng thái cần CSRF token [6]. Quyền truy cập theo mô hình RBAC kết hợp phạm vi cửa hàng: tài khoản có một trong các vai trò `customer`, `admin`, `system_admin`, `store_manager`, `sales_manager`, `marketing_manager`, `inventory_manager`, `operations_manager`, `city_planner`, và route kiểm tra vai trò cùng cửa hàng được phân công.

## **2.3. Kiến trúc và công nghệ lưu trữ Data Lakehouse**

### **2.3.1. Tổng quan về kiến trúc Data Lakehouse**

Data Lakehouse hợp nhất ưu điểm của Data Warehouse và Data Lake trên một nền tảng [7]. Một tầng siêu dữ liệu (metadata layer) đặt trên lưu trữ đối tượng chi phí thấp cung cấp giao dịch ACID theo bảng, quản lý phiên bản snapshot (time-travel), tiến hóa lược đồ không cần viết lại dữ liệu, và cho phép nhiều engine (batch, streaming, SQL) cùng đọc ghi một bảng.

### **2.3.2. Mô hình kiến trúc dữ liệu phân tầng Medallion**

```text
[PostgreSQL] ──► [Staging] ──► [Bronze] ──► [Silver] ──► [Gold]
                 bản trích     dòng nguồn    kiểu chuẩn    Star Schema
                 xuất mới nhất hiện tại      hóa           và Data Mart
```

* **Staging**: bản trích xuất của lần chạy gần nhất, kèm `_ingested_at`. Mỗi lần nạp xoá trắng rồi ghi lại, nên chạy lại cho cùng kết quả.
* **Bronze**: dòng nguồn giữ nguyên kiểu dữ liệu của PostgreSQL, là phiên bản hiện tại theo khóa chính, kèm cờ xoá mềm `_is_deleted`, `_deleted_at`.
* **Silver**: cùng tập dòng với Bronze, kiểu dữ liệu chuẩn hóa (UUID 16 byte, timestamp có múi giờ), merge theo khóa chính.
* **Gold**: mô hình hình sao gồm dimension, fact và mart tổng hợp sẵn theo câu hỏi nghiệp vụ.

Dữ liệu cá nhân không bị băm; thay vào đó chỉ các cột được duyệt trong danh sách cho phép mới được nạp (mục 3.5.5).

### **2.3.3. Lưu trữ Object Storage và định dạng bảng mở Apache Iceberg**

Lưu trữ đối tượng dùng **MinIO**, tương thích Amazon S3 API [8], hỗ trợ chính sách IAM và dịch vụ STS cấp credential tạm.

Định dạng bảng là **Apache Iceberg** [9]. Khác với bảng Hive dựa trên thư mục, Iceberg quản lý bảng bằng cây siêu dữ liệu (metadata file, manifest list, manifest file). Mỗi lần ghi tạo một snapshot mới nguyên tử (atomic commit); người đọc luôn thấy một snapshot hoàn chỉnh (snapshot isolation), nên truy vấn không bị ảnh hưởng bởi tiến trình đang ghi. Iceberg cho thêm cột và nới kiểu (int → long, float → double, tăng độ chính xác decimal) mà không viết lại dữ liệu.

**Apache Polaris** là catalog trung tâm theo Iceberg REST Catalog Specification [10], cho phép PyIceberg, Trino và Flink dùng chung bảng. Polaris quản lý quyền theo vai trò (RBAC) và **cấp credential lưu trữ tạm (credential vending)**: khi một engine mở bảng, Polaris kiểm tra quyền rồi xin MinIO STS một credential chỉ dùng được cho thư mục của bảng đó, nên không engine nào giữ khóa MinIO.

### **2.3.4. Mô hình dữ liệu phân tích Kimball**

Tầng Gold theo phương pháp mô hình hóa đa chiều của Ralph Kimball [11]:
* **Bảng chiều (Dimension)**: thuộc tính mô tả ngữ cảnh (ngày, thành phố, cửa hàng, sản phẩm, biến thể, khách hàng, nhân viên) để cắt lát và gom nhóm.
* **Bảng sự kiện (Fact)**: độ đo tại một mức chi tiết (grain) xác định, ví dụ một dòng đơn hàng, một vận đơn hay tồn kho của một biến thể tại một vị trí trong một ngày.
* **Data Mart**: bảng tổng hợp sẵn từ fact và dimension theo câu hỏi của từng nhóm người xem, giúp dashboard không phải JOIN bảng lớn khi truy vấn.

## **2.4. Công nghệ thu thập và xử lý dữ liệu lớn**

### **2.4.1. Thu thập và xử lý dữ liệu theo lô với PyIceberg và dbt**

Ở quy mô của đề tài (đến hàng chục triệu đơn trên một máy), một engine phân tán như Apache Spark là thừa. Pipeline dùng hai công cụ nhẹ hơn:

* **PyIceberg** [12]: thư viện Python đọc ghi bảng Iceberg qua Polaris. Pipeline đọc PostgreSQL bằng con trỏ phía server (server-side cursor), gom thành lô Apache Arrow và ghi vào staging và Bronze; Bronze dùng `upsert` theo khóa chính và `overwrite` cho lần nạp toàn bộ, mỗi thao tác là một commit Iceberg.
* **dbt** [13] trên **Trino**: mỗi model Silver và Gold là một câu SQL; dbt dựng bảng theo thứ tự phụ thuộc `ref()`, dùng chiến lược incremental `merge` theo khóa và chạy data test sau khi dựng.

### **2.4.2. Đồng bộ thay đổi nguồn và nhật ký truy cập với Kafka và Flink**

Change Data Capture (CDC) theo nhật ký giao dịch (ví dụ Debezium đọc WAL của PostgreSQL) cho thấy mọi thay đổi gần thời gian thực, nhưng cần thêm dịch vụ, cấu hình `wal_level = logical` và thay đổi cả luồng nạp lô. Đề tài **không dùng CDC**. Thay vào đó, các thay đổi mà nạp gia tăng theo `updated_at` không thấy được xử lý bằng một DAG đồng bộ riêng (mục 3.5.2): dòng bị xoá được phát hiện bằng đối chiếu khóa chính, thay đổi cột bằng so lược đồ. Cách này chỉ đọc nguồn và không cần dịch vụ thêm.

Với nhật ký truy cập, kiến trúc streaming gồm:
* **Apache Kafka** [14]: hàng đợi phân tán (chế độ KRaft), nhận sự kiện truy cập từ middleware API qua topic `KAFKA_ACCESS_LOG_TOPIC`.
* **Apache Flink** [15]: xử lý luồng có trạng thái, đọc từ Kafka và ghi vào bảng Iceberg theo chu kỳ checkpoint.

Hạ tầng Kafka và Flink có trong profile `streaming`; job Flink nạp log vào Lakehouse đang được xây lại theo thiết kế ở mục 4.4.3.

### **2.4.3. Điều phối luồng dữ liệu tự động với Apache Airflow**

**Apache Airflow 3** [16] điều phối pipeline dưới dạng DAG viết bằng Python. Đề tài dùng các khái niệm:
* **Data interval** (`data_interval_start`, `data_interval_end`): mỗi lần chạy theo lịch xử lý đúng một khoảng thời gian, nên chạy lại hay backfill một ngày cho cùng kết quả.
* **TaskGroup**: mỗi tầng là một nhóm, mỗi bảng hoặc model dbt là một task; lỗi chỉ ra đúng bảng, và **Clear** chạy lại đúng task đó.
* **Asset**: task cuối của DAG incremental phát Asset `lakehouse_oltp_loaded`, kích hoạt DAG đồng bộ.
* **Pool và retry**: các task dùng chung pool `lakehouse_oltp` 4 slot, tự thử lại hai lần.

### **2.4.4. Kiểm soát chất lượng dữ liệu và quản lý lỗi**

Mô hình chất lượng dữ liệu ISO/IEC 25012 [17] gồm các đặc tính như đầy đủ, chính xác, nhất quán và kịp thời. Đề tài áp dụng nguyên tắc: **kiểm tra chặn đặt ở nơi có thể làm sai số liệu, không kiểm tra lại những gì nguồn đã bảo đảm**.

* PostgreSQL đã bảo đảm khóa chính, trường bắt buộc, khóa ngoại và CHECK, nên pipeline không kiểm tra lại.
* **Đối soát với nguồn** (lần nạp toàn bộ): so số dòng, tổng tiền (`orders`, `payments`, `refunds`) và số dư sổ kho của bản trích xuất với PostgreSQL tại cùng một mốc, trước khi Bronze thay đổi.
* **Silver khớp Bronze**: test `matches_bronze` chặn khi tập khóa của hai tầng lệch nhau.
* **Kiểm tra sau Gold** (chỉ cảnh báo, không chặn): phân bổ giảm giá bảo toàn tổng, đơn Gold khớp Silver, các mart doanh thu khớp nhau, tồn kho khớp sổ cái.
* **Thay đổi lược đồ nguồn** được phân loại; thay đổi có thể làm sai số liệu (bỏ cột, đổi loại) dừng pipeline với thông báo cách xử lý.

Kết quả kiểm tra nằm trong log của task Airflow; task trượt kiểm tra chặn sẽ lỗi và tầng sau giữ nguyên bản cũ.

## **2.5. Công nghệ phân tích và trực quan hóa dữ liệu**

### **2.5.1. Công cụ truy vấn dữ liệu phân tán Apache Trino**

**Trino** [18] là engine SQL phân tán cho truy vấn tương tác trên hồ dữ liệu. Trino đọc bảng Iceberg qua Polaris, dùng siêu dữ liệu Iceberg để cắt tỉa phân vùng (partition pruning) và bỏ qua tệp theo thống kê min/max, giảm lượng dữ liệu đọc từ MinIO.

Mỗi truy vấn Trino đọc một snapshot Iceberg hoàn chỉnh, nên dashboard không thấy dữ liệu ghi dở. Trino có hai catalog trỏ cùng warehouse: `lakehouse` chỉ đọc cho phân tích và `lakehouse_etl` cho dbt ghi `silver`/`gold`.

### **2.5.2. Trực quan hóa dữ liệu và xây dựng Dashboard với Apache Superset**

**Apache Superset** [20] là nền tảng BI mã nguồn mở, kết nối Trino qua SQLAlchemy, định nghĩa dataset trên bảng và dựng chart, dashboard. Thiết kế dashboard theo nguyên tắc của Stephen Few [19]: chỉ số chính ở đầu, xu hướng theo thời gian, rồi phân rã theo chiều.

Dashboard được khai báo bằng mã (dataset, chart, bố cục) và đồng bộ vào Superset bằng lệnh `make superset-sync`, nên dựng lại được giống hệt trên môi trường mới.

---

# **CHƯƠNG 3. PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG**

## **3.1. Phân tích yêu cầu hệ thống**

### **3.1.1. Bối cảnh và phát biểu bài toán**

Doanh nghiệp thời trang D&K bán lẻ trên website và tại các cửa hàng. Hệ thống giao dịch xử lý duyệt sản phẩm, đặt hàng, thanh toán VietQR, giao COD và đổi trả; máy chủ web ghi nhận lượt tương tác của người dùng.

Ban giám đốc và các bộ phận thiếu một nơi phân tích tập trung: tổng hợp báo cáo thủ công từ cơ sở dữ liệu giao dịch tốn thời gian, dễ sai và làm chậm hệ thống bán hàng; dữ liệu nhật ký bị bỏ phí. Bài toán là thiết kế một nền tảng dữ liệu vừa không ảnh hưởng hệ thống vận hành, vừa tự động thu thập, xử lý, đối soát và phân quyền truy cập dữ liệu phân tích.

### **3.1.2. Tác nhân và phạm vi hệ thống**

| Tác nhân | Trách nhiệm và nhu cầu khai thác |
|---|---|
| **Khách hàng (Customer)** | Duyệt sản phẩm theo màu/size, quản lý giỏ hàng, đặt hàng, thanh toán VietQR/COD, theo dõi đơn, đánh giá và yêu cầu đổi trả. |
| **Nhân viên cửa hàng và kho** | Bán hàng tại quầy qua API POS, tra cứu tồn kho, nhập hàng và kiểm định hàng hoàn. |
| **Nhân viên giao nhận** | Hồ sơ trong `delivery_staff`; các lần giao và tiền COD được ghi nhận trên vận đơn. |
| **Cấp quản lý** | Xem báo cáo doanh thu, lợi nhuận, kho vận, giao vận, khách hàng trên Superset. |
| **Kỹ sư dữ liệu và quản trị nền tảng** | Vận hành DAG Airflow, xử lý thay đổi nguồn, quản trị quyền Polaris và cấp credential cho workload qua Console. |

![Sơ đồ bối cảnh hệ thống](../assets/report/context.svg)

### **3.1.3. Yêu cầu chức năng**

* **Nhóm chức năng ứng dụng giao dịch**:
  * Quản lý danh mục, sản phẩm và biến thể SKU (màu, size, giá niêm yết, giá vốn).
  * Giỏ hàng và checkout bảo đảm tồn kho nhất quán bằng khóa dòng.
  * Bán hàng tại quầy POS, áp dụng mã giảm giá.
  * Nhập hàng và sổ cái biến động kho.
  * Vận đơn, các lần giao, thu hộ COD và quy trình đổi trả – hoàn tiền nhiều bước.
  * Ghi nhật ký truy cập tự động trên mọi route API.
* **Nhóm chức năng nền tảng dữ liệu và phân tích**:
  * Nạp toàn bộ và nạp gia tăng hằng ngày 19 bảng nguồn vào Lakehouse, đối soát với nguồn ở lần nạp toàn bộ.
  * Phát hiện dòng bị xoá ở nguồn và giữ dấu vết (xoá mềm) qua mọi tầng; cho phép xoá cứng chủ động.
  * Phát hiện thay đổi cột ở nguồn: tự áp dụng thay đổi an toàn, dừng với thay đổi phá vỡ, không nạp cột chưa được duyệt.
  * Dựng 7 dimension, 6 fact và 14 mart phục vụ KPI.
  * Quản trị quyền Lakehouse: principal, role, quyền theo catalog/namespace/bảng và cấp credential cho workload.
  * Dashboard trực quan hóa trên Superset.

### **3.1.4. Yêu cầu phi chức năng**

* **Tính toàn vẹn và nhất quán**: giao dịch nguồn tuân thủ ACID. Mỗi truy vấn phân tích đọc một snapshot Iceberg hoàn chỉnh. Lần nạp toàn bộ phải khớp nguồn về số dòng, tổng tiền và số dư sổ kho tại cùng mốc thời gian.
* **Chạy lại an toàn (idempotency)**: retry, clear, chạy hỏng rồi chạy tiếp, hay backfill đều cho cùng bảng ở mọi tầng; mốc tiến độ chỉ tiến khi dữ liệu đã ghi xong.
* **Khả năng mở rộng**: tách tính toán (PyIceberg, Trino, Flink) và lưu trữ (MinIO), mở rộng độc lập; pipeline chạy được từ preset 100 nghìn tới 50 triệu đơn.
* **Bảo mật**: dữ liệu cá nhân chỉ vào Lakehouse khi cột được duyệt; workload không giữ khóa lưu trữ; Trino tách quyền đọc và ghi; API xác thực JWT, chống CSRF và phân quyền RBAC.

## **3.2. Phân tích chức năng và quy trình nghiệp vụ**

### **3.2.1. Sơ đồ Use Case hệ thống**

Hệ thống gồm ba phân hệ:
1. **Phân hệ mua hàng**: khách hàng duyệt sản phẩm, giỏ hàng, checkout, theo dõi đơn, đánh giá và đổi trả qua API.
2. **Phân hệ quản trị và POS**: bán tại quầy, quản lý sản phẩm, đơn, kho, nhập hàng, giao vận và hoàn tiền qua API có phân quyền.
3. **Phân hệ dữ liệu và BI**: kỹ sư dữ liệu vận hành pipeline và quản trị quyền; cấp quản lý xem dashboard.

![Sơ đồ Use Case mua hàng](../assets/report/uc-shopping.svg)

![Sơ đồ Use Case Quản trị](../assets/report/uc-admin.svg)

![Sơ đồ Use Case BI và Dữ liệu](../assets/report/uc-data-bi.svg)

### **3.2.2. Sơ đồ hoạt động (Activity Diagram) các nghiệp vụ cốt lõi**

* **Quy trình đặt hàng và thanh toán (Checkout)**: khách gửi checkout → backend kiểm tra giỏ, áp dụng coupon, khóa và kiểm tra tồn kho → tạo đơn, dòng đơn (chụp giá bán và giá vốn), bản ghi thanh toán và lịch sử trạng thái → trừ tồn kho và ghi sổ cái → commit một lần. COD bắt đầu ở trạng thái đã xác nhận/chờ thu; doanh thu chỉ ghi nhận khi hoàn tất và thu đủ tiền.

![Sơ đồ hoạt động Checkout](../assets/report/activity-checkout-commit.svg)

* **Quy trình bán hàng tại quầy (POS)**: nhân viên chọn cửa hàng → chọn biến thể → hệ thống kiểm tra tồn tại cửa hàng → áp dụng giảm giá → thanh toán → trừ tồn cửa hàng và hoàn tất đơn POS ngay.

![Sơ đồ hoạt động POS](../assets/report/activity-pos.svg)

* **Quy trình đổi trả và hoàn tiền (Returns)**: khách tạo yêu cầu kèm lý do → quản trị viên duyệt → kho nhận và kiểm định hàng hoàn → hàng đạt chuẩn được nhập lại kho → tạo hoàn tiền gắn với yêu cầu trả hàng, hoặc đổi sản phẩm khác.

![Sơ đồ hoạt động Đổi trả](../assets/report/activity-returns.svg)

### **3.2.3. Sơ đồ tuần tự (Sequence Diagram) các luồng xử lý**

* **Luồng giao dịch Checkout**: client (Console mô phỏng) → FastAPI → PostgreSQL trong một giao dịch; middleware ghi sự kiện truy cập và, khi bật Kafka, gửi sự kiện vào topic log.
* **Luồng truy vấn báo cáo BI**: người dùng mở dashboard Superset → Superset gửi SQL tới Trino bằng tài khoản `analytics_reader` → Trino lấy metadata bảng từ Polaris, nhận credential lưu trữ tạm cho đúng bảng → đọc tệp Parquet trên MinIO → trả kết quả cho chart.

![Sơ đồ tuần tự Checkout](../assets/report/seq-checkout.svg)

![Sơ đồ tuần tự Dashboard Serving](../assets/report/seq-dashboard.svg)

## **3.3. Thiết kế kiến trúc tổng thể**

### **3.3.1. Kiến trúc tổng thể toàn hệ thống**

Hệ thống tách tầng giao dịch trực tuyến (OLTP) và tầng phân tích (OLAP):

```text
Console / datagen ──► FastAPI ──► PostgreSQL ──► staging ──► [reconcile] ──► bronze ──► silver ──► gold ──► Trino ──► Superset
                                                 PyIceberg    chỉ init       PyIceberg   dbt merge   dbt
                         └─► Kafka (access log; job nạp Lakehouse đang xây lại)
```

![Sơ đồ kiến trúc tổng thể toàn hệ thống](../assets/report/architecture-overview.svg)

### **3.3.2. Kiến trúc thành phần ứng dụng thương mại điện tử**

Container `platform-app` chạy FastAPI (cổng 8000) và Streamlit (cổng 8501). API tổ chức theo module trong `services/platform-app/src/platform_app/api/modules/`:
* `auth`: đăng ký, đăng nhập, cookie JWT, CSRF và kiểm tra quyền.
* `catalog`, `location`, `store`: danh mục, sản phẩm, biến thể, thành phố và cửa hàng.
* `cart`, `wishlist`, `checkout`, `coupons`: giỏ hàng, yêu thích, đặt hàng và mã giảm giá.
* `orders`: vòng đời đơn hàng và lịch sử trạng thái.
* `pos`: bán hàng tại quầy.
* `inbound`: nhập hàng và sổ cái kho.
* `logistics`: vận đơn, lần giao và COD.
* `returns`, `reviews`: đổi trả – hoàn tiền và đánh giá.
* `admin`, `analytics`: tác vụ quản trị và số liệu vận hành.
* Middleware nhật ký truy cập (`api/core/access_logging.py`): ghi mọi request theo hợp đồng `ecommerce.access`, che dữ liệu nhạy cảm, gửi Kafka khi `KAFKA_ENABLED=true`.

Ngoài API nghiệp vụ, router `/api/v1/access` phục vụ quản trị quyền Lakehouse, và service riêng `credential-exporter` ghi file credential vào volume của từng workload.

![Sơ đồ kiến trúc Web](../assets/report/architecture-web.svg)

### **3.3.3. Kiến trúc nền tảng Data Lakehouse và phục vụ phân tích**

Lakehouse dùng một bucket MinIO (`MINIO_LAKEHOUSE_BUCKET`, mặc định `lakehouse`) và một catalog Polaris `lakehouse`, chia bốn namespace:
* `staging`: bản trích xuất của lần chạy gần nhất cho mỗi bảng nguồn.
* `bronze`: dòng nguồn hiện tại theo khóa chính, kiểu dữ liệu nguồn, kèm cờ xoá mềm.
* `silver`: cùng dòng với Bronze, kiểu chuẩn hóa.
* `gold`: 7 dimension, 6 fact, 14 mart, 2 seed và bảng trạng thái `_mart_state` giữ mốc tiến độ của mart.

Tầng phục vụ là Trino đọc trực tiếp bảng Gold hiện tại qua Polaris; Superset kết nối Trino. MinIO chỉ có một tài khoản lưu trữ, của Polaris, do service `minio-init` tạo; các workload nhận credential tạm theo bảng.

![Sơ đồ kiến trúc Lakehouse](../assets/report/lakehouse.svg)

### **3.3.4. Mô hình tương tác giữa các luồng dữ liệu**

| Luồng dữ liệu | Cơ chế thực thi | Tần suất | Đầu vào | Đầu ra |
|---|---|---|---|---|
| **1. Nạp toàn bộ (init)** | Tạo namespace và bảng → staging → đối soát → Bronze (thay cả bảng) → Silver → Gold → bảo trì | Chạy tay | Mọi dòng của 19 bảng tạo trước mốc chạy | Lakehouse dựng lại hoàn chỉnh |
| **2. Nạp gia tăng (incremental)** | Staging theo data interval → Bronze upsert → Silver merge → Gold merge/tính lại ngày → bảo trì | 03:00 hằng ngày (Asia/Ho_Chi_Minh), catchup và backfill | Dòng có `updated_at`/`created_at` trong khoảng | Gold cập nhật; phát Asset `lakehouse_oltp_loaded` |
| **3. Đồng bộ nguồn (sync)** | Kiểm tra lược đồ → phát hiện xoá/khôi phục → Bronze → Silver → Gold → bảo trì | Sau mỗi lần incremental thành công | Danh sách cột và toàn bộ khóa chính nguồn | Cột đã duyệt được thêm; dòng bị xoá được đánh dấu |
| **4. Xoá cứng (purge)** | Xoá dòng `_is_deleted` ở Gold → Silver → Bronze | Chạy tay | Dòng đã xoá mềm | Lakehouse không còn dòng đã xoá |
| **5. Access log** | Middleware → Kafka → Flink → `bronze.web_events` → Silver → mart phễu | Thiết kế, chưa xây | Sự kiện HTTP và file log lịch sử | `mart_web_funnel_daily` |

![Sơ đồ luồng Batch](../assets/report/flow-batch.svg)

![Sơ đồ luồng Access Log](../assets/report/flow-access-log.svg)

## **3.4. Thiết kế cơ sở dữ liệu giao dịch (OLTP)**

### **3.4.1. Phân tích các cụm thực thể nghiệp vụ**

Cơ sở dữ liệu PostgreSQL chuẩn hóa mức 3NF, gồm các cụm thực thể:
1. **Định danh và địa điểm**: `customers`, `customer_credentials`, `cities`, `stores`, `delivery_staff`.
2. **Sản phẩm**: `categories`, `products`, `product_variants`, `product_reviews`.
3. **Khám phá và mua**: `carts`, `cart_items`, `wishlist_items`.
4. **Khuyến mại**: `coupons`, `coupon_redemptions`.
5. **Đơn hàng và thanh toán**: `orders`, `order_items`, `order_status_history`, `payments`.
6. **Tồn kho và nhập hàng**: `inventory`, `store_inventory`, `inbound_receipts`, `inbound_receipt_items`, `inventory_transactions`.
7. **Giao vận và hậu mãi**: `shipments`, `shipment_attempts`, `return_requests`, `return_items`, `refunds`.

### **3.4.2. Thiết kế sơ đồ quan hệ thực thể (ERD)**

ERD thể hiện liên kết giữa các cụm: khách hàng – đơn hàng – dòng đơn – biến thể; đơn hàng – thanh toán – hoàn tiền; đơn hàng – vận đơn – lần giao; biến thể – tồn kho theo vị trí – sổ cái kho.

![Sơ đồ ERD Bán hàng](../assets/report/erd-conceptual-sales.svg)

![Sơ đồ ERD Tồn kho](../assets/report/erd-conceptual-stock.svg)

### **3.4.3. Thiết kế lược đồ cơ sở dữ liệu vật lý PostgreSQL (28 bảng)**

Lược đồ vật lý `database/schema.sql` được sinh từ ORM, gồm **28 bảng** với khóa ngoại, CHECK, UNIQUE và index. Cột "Nạp vào Lakehouse" cho biết bảng nằm trong 19 bảng của `SOURCE_TABLES` và cột cursor dùng cho nạp gia tăng.

| STT | Tên bảng | Khóa chính | Nạp vào Lakehouse (cursor) | Mục đích lưu trữ |
|---|---|---|---|---|
| 1 | `customers` | `customer_id` | `updated_at` | Người dùng, khách hàng và vai trò |
| 2 | `customer_credentials` | `customer_id` | Không nạp | Mật khẩu băm Argon2id, chỉ dùng cho xác thực |
| 3 | `categories` | `category_id` | `updated_at` | Cây danh mục |
| 4 | `products` | `product_id` | `updated_at` | Sản phẩm |
| 5 | `product_variants` | `variant_id` | `updated_at` | Biến thể SKU: màu, size, giá niêm yết, giá vốn |
| 6 | `carts` | `cart_id` | Không nạp | Giỏ hàng |
| 7 | `cart_items` | `cart_item_id` | Không nạp | Dòng giỏ hàng |
| 8 | `wishlist_items` | `wishlist_item_id` | Không nạp | Sản phẩm yêu thích |
| 9 | `orders` | `order_id` | `updated_at` | Đơn hàng online và POS |
| 10 | `order_items` | `order_item_id` | `created_at` | Dòng đơn; chụp giá bán và giá vốn lúc mua |
| 11 | `order_status_history` | `order_status_history_id` | `created_at` | Lịch sử trạng thái đơn |
| 12 | `inventory` | `variant_id` | Chỉ để đối soát | Tồn tổng theo biến thể |
| 13 | `inventory_transactions` | `transaction_id` | `created_at` | Sổ cái biến động kho |
| 14 | `inbound_receipts` | `receipt_id` | Không nạp | Phiếu nhập hàng |
| 15 | `inbound_receipt_items` | `item_id` | `created_at` | Dòng phiếu nhập |
| 16 | `coupons` | `coupon_id` | `updated_at` | Mã giảm giá |
| 17 | `coupon_redemptions` | `coupon_redemption_id` | Không nạp | Lượt dùng mã giảm giá |
| 18 | `product_reviews` | `review_id` | `updated_at` | Đánh giá sản phẩm |
| 19 | `cities` | `city_id` | `updated_at` | Tỉnh/thành phố |
| 20 | `stores` | `store_id` | `updated_at` | Cửa hàng |
| 21 | `store_inventory` | `store_inventory_id` | Chỉ để đối soát | Tồn theo cửa hàng và biến thể |
| 22 | `delivery_staff` | `staff_id` | `updated_at` | Nhân viên giao hàng |
| 23 | `shipments` | `shipment_id` | `updated_at` | Vận đơn và tiền COD |
| 24 | `shipment_attempts` | `attempt_id` | Không nạp | Các lần giao |
| 25 | `payments` | `payment_id` | `updated_at` | Thanh toán (VietQR, COD, thẻ, tiền mặt) |
| 26 | `return_requests` | `return_id` | `updated_at` | Yêu cầu đổi trả |
| 27 | `return_items` | `return_item_id` | `updated_at` | Dòng hàng đổi trả |
| 28 | `refunds` | `refund_id` | `updated_at` | Hoàn tiền |

Các bảng không nạp không được bảng Gold nào dùng; `customer_credentials` không được cấp quyền đọc cho pipeline.

### **3.4.4. Ràng buộc toàn vẹn và bảo đảm tính nhất quán giao dịch**

* **Toàn vẹn tham chiếu**: khóa ngoại ngăn xoá bản ghi cha khi đã có giao dịch con.
* **Bảo toàn giá lịch sử**: `order_items` lưu đơn giá bán, giảm giá và giá vốn tại thời điểm đặt; đổi giá trong `product_variants` sau đó không ảnh hưởng đơn cũ.
* **Idempotency key**: các thao tác nhạy cảm (tạo đơn, thanh toán, hoàn tiền) nhận `idempotency_key`, nên bấm đúp hay mạng gửi lại không tạo thao tác thứ hai.
* **Thời gian cập nhật**: trigger đặt `updated_at` khi dòng đổi, làm cơ sở cho nạp gia tăng.

## **3.5. Thiết kế nền tảng Data Lakehouse (OLAP)**

### **3.5.1. Xác định và phân loại nguồn dữ liệu**

1. **Nguồn giao dịch có cấu trúc (PostgreSQL)**: 19 bảng được khai báo tường minh trong `airflow/lakehouse/oltp/sources.py`. Bảng thay đổi trạng thái lọc theo `updated_at`; bảng chỉ ghi thêm lọc theo `created_at`. Mỗi bảng có danh sách cột được nạp (`columns`) và danh sách cột cố ý bỏ (`excluded`, gồm dữ liệu cá nhân như `receiver_phone`, `shipping_address_text`).
2. **Nguồn nhật ký truy cập bán cấu trúc**: sự kiện HTTP theo hợp đồng `ecommerce.access` (API phát v1.0.0; bộ sinh dữ liệu phát v1.1.0 có thêm `session.id`, `ecommerce.order_number`).

### **3.5.2. Thiết kế quy trình trích xuất Batch, đồng bộ nguồn và Access Log**

* **Trích xuất theo lô**:
  * Lần nạp toàn bộ đọc mọi dòng tạo trước mốc bắt đầu lần chạy, nên 19 bảng dừng ở cùng một thời điểm; lần nạp gia tăng đọc các dòng có cursor trong `[data_interval_start, data_interval_end)`.
  * Mỗi bảng đọc trong một giao dịch repeatable-read bằng con trỏ phía server (10.000 dòng mỗi lần fetch), ghi vào staging mỗi lô 200.000 dòng.
  * Staging được xoá trắng trước khi ghi, nên chạy lại cùng khoảng luôn cho cùng kết quả mà không cần lưu con trỏ.
* **Đồng bộ nguồn** (DAG `lakehouse_oltp_sync`, chạy sau mỗi lần incremental):
  1. **Thay đổi cột** (nhóm `schema`, chạy trước). So nguồn với danh sách cho phép và Bronze:
     * cột mới chưa duyệt: sync báo lỗi chờ người duyệt; incremental vẫn chạy;
     * cột đã duyệt: tự thêm vào staging và Bronze, bật cờ `lakehouse.reload-pending`, rồi nạp lại cả bảng trong một commit;
     * kiểu được nới (int → long…): tự nới;
     * cột bị bỏ hoặc đổi loại: mọi DAG dừng kèm thông báo cách xử lý.
  2. **Dòng bị xoá** (nhóm `detect`, chạy sau). So toàn bộ khóa chính nguồn với khóa còn sống ở Bronze:
     * khóa mất ở nguồn: xoá mềm (`_is_deleted`, `_deleted_at`) qua Bronze, Silver và Gold; mart không tính;
     * khóa có lại ở nguồn: dòng sống lại.
![Sơ đồ luồng đồng bộ nguồn](../assets/report/flow-sync.svg)

* **Xoá cứng chủ động**: DAG chạy tay `lakehouse_oltp_purge` xoá hẳn các dòng đã xoá mềm, theo thứ tự Gold → Silver → Bronze.
* **Access Log**: thiết kế ở mục 4.4.3.

### **3.5.3. Thiết kế các tầng dữ liệu Medallion**

![Sơ đồ phân tầng Medallion](../assets/report/medallion.svg)

* **Staging**: bản trích xuất của lần chạy, kèm `_ingested_at`.
* **Bronze**: lần nạp toàn bộ thay cả bảng trong một commit, giữ các dòng đã xoá mềm và đánh dấu xoá các dòng nguồn không còn; lần nạp gia tăng upsert theo khóa chính và không để dòng có cursor cũ hơn ghi đè bản mới.
* **Silver**: dbt merge các dòng Bronze mới hơn `_ingested_at` lớn nhất của Silver, chuẩn hóa kiểu; test `matches_bronze` chặn khi hai tầng lệch khóa.
* **Gold**: dimension và fact merge các dòng có đầu vào thay đổi; mart xoá rồi dựng lại các ngày bị ảnh hưởng kể từ mốc trong `gold._mart_state`.

### **3.5.4. Thiết kế mô hình dữ liệu Dimensional Model (Star Schema) và Data Marts**

* **7 bảng Dimension**:
  * `dim_date`: lịch, ngày lễ và ngày sale (seed `calendar_events`).
  * `dim_city`: thành phố, vùng giao và SLA giao hàng (seed `delivery_sla`).
  * `dim_store`: cửa hàng.
  * `dim_product`: sản phẩm và danh mục.
  * `dim_variant`: biến thể SKU.
  * `dim_customer`: khách hàng.
  * `dim_staff`: nhân viên giao hàng và nhân viên bán.

![Sơ đồ Star Schema Bán hàng](../assets/report/gold-sales.svg)

* **6 bảng Fact**:
  * `fact_order`: một dòng mỗi đơn.
  * `fact_order_item`: một dòng mỗi dòng đơn; giảm giá đơn được phân bổ theo tỷ lệ tiền dòng, làm tròn xuống tích lũy nên tổng phân bổ bằng đúng giảm giá đơn.
  * `fact_shipment`: một dòng mỗi vận đơn.
  * `fact_return`: một dòng mỗi dòng hàng trả.
  * `fact_inventory_daily`: ngày × vị trí × biến thể, dựng từ sổ cái kho.
  * `fact_review`: một dòng mỗi đánh giá.

  Dimension và fact mang cờ `_is_deleted`; mỗi fact giữ `_previous_dates` (các ngày trước lần merge) để mart tính lại cả ngày mà dòng vừa rời đi.

![Sơ đồ Star Schema Vận hành](../assets/report/gold-operations.svg)

* **14 Data Mart theo nhóm người xem**:
  1. *Kinh doanh*: `mart_orders_daily`, `mart_sales_daily`, `mart_product_sales_daily`, `mart_cancellations_daily`.
  2. *Cửa hàng*: `mart_pos_hourly`.
  3. *Giao vận và đổi trả*: `mart_delivery_daily`, `mart_fulfilment_backlog_daily`, `mart_returns_daily`.
  4. *Tồn kho*: `mart_inventory_current`, `mart_inventory_health_daily`.
  5. *Khách hàng và marketing*: `mart_coupon_daily`, `mart_reviews_daily`, `mart_customer_value`, `mart_customer_cohort_monthly`.

  Mart chỉ đọc dòng còn sống qua macro `live()`. Grain của từng mart nằm trong [DATA.md](DATA.md).

![Sơ đồ Data Marts OLTP](../assets/report/gold-marts.svg)

### **3.5.5. Thiết kế quy trình kiểm soát chất lượng dữ liệu và bảo mật thông tin**

* **Chất lượng dữ liệu**:
  * Đối soát với nguồn khi nạp toàn bộ (số dòng, tổng tiền `orders`/`payments`/`refunds`, số dư sổ kho so với `inventory`/`store_inventory`), chặn trước khi Bronze đổi.
  * Test `matches_bronze` chặn khi Silver lệch khóa với Bronze.
  * Data test sau Gold (cảnh báo): khóa duy nhất, phân bổ giảm giá bảo toàn, đơn khớp Silver, các mart doanh thu khớp nhau, tồn khớp sổ cái, grain của tồn kho ngày.
  * Phân loại thay đổi lược đồ nguồn (mục 3.5.2).
* **Bảo mật và phân quyền hạ tầng**:
  * Pipeline đọc PostgreSQL bằng role chỉ `SELECT`, phiên read-only.
  * Chỉ cột trong danh sách cho phép được nạp; cột cá nhân mới không thể lọt vào trước khi được duyệt.
  * Mỗi workload một principal Polaris: `batch_airflow`, `trino_writer`, `flink_landing` (vai trò ghi) và `trino_reader` (vai trò đọc). Quyền gán theo chuỗi principal → principal role → catalog role → quyền trên catalog, namespace hoặc bảng.
  * Polaris cấp credential MinIO tạm cho đúng bảng được mở; MinIO chỉ có một tài khoản lưu trữ, của Polaris.
  * Trino xác thực bằng mật khẩu qua HTTPS: `analytics_reader` chỉ đọc catalog `lakehouse`, `dbt_writer` chỉ ghi schema `silver` và `gold` của `lakehouse_etl`.

![Sơ đồ bảo mật RBAC](../assets/report/security-engines.svg)

## **3.6. Thiết kế giao diện người dùng và hệ thống Dashboard**

### **3.6.1. Thiết kế giao diện Console mô phỏng mua hàng**

Màn hình **Mô phỏng API** của Streamlit chạy kịch bản mua hàng online qua API thật, thay cho thao tác tay trên storefront:
* Người dùng chọn số lượt mua và cấu hình kịch bản; worker chạy nền thực hiện các bước như một khách thật: đăng nhập, xem sản phẩm, thêm giỏ, lấy báo giá, checkout VietQR hoặc COD.
* Mỗi lượt được ghi vào journal; lượt chỉ được ghi thành công khi đọc lại đơn và giỏ thấy khớp (mã đơn, tổng tiền, SKU, số lượng, trạng thái thanh toán, giỏ rỗng).
* Lượt không xác định được kết quả (lỗi mạng sau khi gửi) được đánh dấu riêng và không tự gửi lại.

![Sơ đồ giao diện Console](../assets/report/ui-console.svg)

### **3.6.2. Thiết kế giao diện quản trị quyền Lakehouse và nghiệp vụ Admin/POS**

* Nghiệp vụ Admin và POS (quản lý sản phẩm, đơn, kho, nhập hàng, bán tại quầy) được cung cấp qua API có phân quyền theo vai trò và cửa hàng.
* Màn hình **Quản trị quyền** của Streamlit chỉ dành cho admin bootstrap, gồm các tab: Danh sách, Tạo mới, Sửa, Xóa, Gán/Gỡ quyền, Credential và Cấp cho workload. Mọi thao tác ghi cần CSRF và được ghi audit trước và sau.

### **3.6.3. Thiết kế hệ thống Dashboard BI trên Superset**

![Sơ đồ Dashboard BI](../assets/report/ui-dashboard-sales.svg)

Dashboard doanh thu đọc ba mart `mart_orders_daily`, `mart_sales_daily`, `mart_product_sales_daily`:
1. **Chỉ số chính**: số đơn đặt, giá trị đặt hàng, doanh thu thuần, lợi nhuận gộp.
2. **Xu hướng**: doanh thu thuần và lợi nhuận gộp theo ngày ghi nhận.
3. **Phân rã**: doanh thu thuần theo kênh (online/POS) và theo điểm bán.
4. **Sản phẩm**: top 10 sản phẩm theo doanh thu sau giảm giá.

Các mart còn lại đã sẵn sàng; dashboard cho kho, giao vận, khách hàng và phân quyền theo cửa hàng sẽ được bổ sung sau khi các pipeline ổn định.

---

# **CHƯƠNG 4. TRIỂN KHAI VÀ ĐÁNH GIÁ THỰC NGHIỆM**

## **4.1. Môi trường và kiến trúc triển khai**

### **4.1.1. Cấu trúc dự án Monorepo và môi trường phát triển**

```text
dk-ecommerce-data-platform/
├── services/platform-app/   # FastAPI, Streamlit Console, quản trị quyền, credential exporter
├── database/                # schema.sql sinh từ ORM, seed danh mục
├── generator/               # CLI datagen, notebook EDA, preset OLTP và log
├── airflow/
│   ├── dags/                # lakehouse_oltp_init, _incremental, _sync, _purge
│   ├── tasks/               # callable của PythonOperator, task dbt
│   ├── lakehouse/           # thư viện: nguồn, staging, Bronze, đồng bộ, đối soát, tạo bảng
│   └── tests/               # unittest chạy trong container Airflow
├── dbt/                     # models/silver, models/oltp (Gold), macros, seeds, tests
├── streaming/sql/           # Flink SQL cũ của luồng log
├── infrastructure/          # Dockerfile và cấu hình Airflow, MinIO, PostgreSQL, Trino, Superset, Flink
└── docs/                    # kiến trúc, dữ liệu, cài đặt, vận hành, thiết kế
```

Môi trường Python 3.11 dùng chung `uv.lock` cho ba gói (`platform-app`, `data-generator`, pipeline trong `airflow/`). Mã được kiểm tra bằng `ruff`; `make test` chạy mọi bộ test (pipeline, platform-app, generator, Superset) mà không cần stack thật.

### **4.1.2. Đóng gói và triển khai dịch vụ bằng Docker Compose**

| Profile | Dịch vụ chính | Mục đích |
|---|---|---|
| `core` | `postgres`, `minio`, `minio-init`, `polaris`, `polaris-bootstrap`, `platform-app`, `platform-admin-init`, `credential-exporter`, Airflow (`airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`), `trino`, `trino-auth-init` | Nguồn giao dịch, Lakehouse, pipeline và quản trị quyền |
| `streaming` | `kafka`, `kafka-init`, `flink-jobmanager`, `flink-taskmanager`, `flink-submit` | Hạ tầng luồng access log |
| `bi` | `superset`, `superset-init`, `superset-sync` | Dashboard |

`make up` chỉ bật Console, API và các dependency quản trị; `make core` bật thêm Airflow và Trino sau khi quyền workload đã được cấp. Pipeline chạy bằng Python của chính image Airflow (`PythonOperator`), dbt chạy qua `BashOperator`; không có cụm tính toán thường trực.

### **4.1.3. Cấu hình mạng, kết nối và phân bổ tài nguyên**

22 dịch vụ kết nối qua mạng nội bộ `lakehouse-network`; các cổng quản trị chỉ mở trên `127.0.0.1`. Biến nhạy cảm (mật khẩu, secret) nằm trong `.env` tạo từ `.env.example`; không có secret hard-code trong mã.

Dữ liệu PostgreSQL, MinIO, Kafka và log Airflow nằm trên Docker named volume. Credential Polaris của từng workload nằm trên volume riêng (`polaris-batch-airflow-credentials`, `polaris-trino-reader-credentials`…), do `credential-exporter` ghi; `platform-app` không mount các volume này.

## **4.2. Xây dựng Backend và cơ sở dữ liệu giao dịch**

### **4.2.1. Khởi tạo cơ sở dữ liệu và quản lý lược đồ**

Service một lần `platform-admin-init` áp lược đồ `database/schema.sql` lên database trống và tạo tài khoản admin bootstrap. Lược đồ được sinh từ ORM và kiểm tra khớp bằng lệnh `python -m platform_app.api.db.schema --check`. Thời gian lưu `TIMESTAMPTZ` theo UTC; trigger cập nhật `updated_at` khi dữ liệu đổi.

Dữ liệu danh mục gốc nạp bằng `make seed`; tồn đầu ghi thành giao dịch `adjustment` trong sổ cái, nên số dư tồn luôn cộng ra được từ sổ cái.

### **4.2.2. Xây dựng dịch vụ xác thực, phân quyền và bảo mật API**

* Băm mật khẩu bằng Argon2id.
* Phiên đăng nhập bằng JWT trong cookie HttpOnly; yêu cầu ghi cần CSRF token.
* Quyền kiểm tra qua Dependency Injection của FastAPI theo vai trò và cửa hàng của từng route; riêng quản trị quyền Lakehouse chỉ chấp nhận đúng tài khoản admin bootstrap.

### **4.2.3. Triển khai các module quản lý sản phẩm, giỏ hàng và đơn hàng**

* Danh mục và biến thể SKU, nạp kèm quan hệ để tránh truy vấn N+1.
* Giỏ hàng và danh sách yêu thích theo khách hàng.
* Checkout chạy trong một giao dịch: khóa dòng tồn kho (`FOR UPDATE`), kiểm tra coupon, tạo đơn, dòng đơn (chụp giá bán và giá vốn), thanh toán và lịch sử trạng thái, trừ tồn và ghi sổ cái; tự thử lại có giới hạn khi deadlock hoặc lỗi tuần tự hóa.

### **4.2.4. Triển khai module quản lý kho hàng, nhập kho và POS**

* Nhập hàng tạo phiếu nhập và dòng phiếu, ghi biến động vào sổ cái `inventory_transactions`.
* POS bán tại quầy, kiểm tra tồn tại đúng cửa hàng và hoàn tất đơn ngay.
* Tồn được theo dõi theo biến thể và vị trí (kho tổng và từng cửa hàng).

### **4.2.5. Triển khai module thanh toán, vận chuyển và đổi trả hàng**

* Thanh toán VietQR, COD, thẻ và tiền mặt tại quầy; COD chỉ ghi nhận đã thu khi giao thành công.
* Vận đơn theo dõi các lần giao (`shipment_attempts`) và tiền thu hộ COD; giao thất bại có biến động kho tương ứng.
* Đổi trả theo các bước: yêu cầu → duyệt → nhận và kiểm định hàng hoàn → nhập lại kho → hoàn tiền gắn với yêu cầu.

### **4.2.6. Triển khai Middleware ghi nhận Access Log phi cấu trúc**

Middleware trong `api/core/access_logging.py` ghi mọi request (trừ health check):
* Theo hợp đồng `ecommerce.access` v1.0.0: `request.id` 32 ký tự hex do server tạo, thời gian UTC, method, route template (không dùng đường dẫn thô), mã trạng thái, thời gian xử lý, actor và hành động thương mại (ví dụ `product_detail`, `cart_item_set`, `checkout_submit`) kèm sản phẩm nếu route có.
* Không ghi request body, mật khẩu, header Authorization, cookie hay thông tin thanh toán; từ khóa tìm kiếm chứa dữ liệu cá nhân được che.
* Ghi structured log; gửi thêm vào Kafka khi `KAFKA_ENABLED=true`, không chặn request của người dùng.

## **4.3. Xây dựng giao diện người dùng**

### **4.3.1. Giao diện Console mô phỏng mua hàng (Streamlit)**

Màn hình **Mô phỏng API** (`ui/views/scenarios.py`, `ui/scenarios/`) chạy kịch bản mua online qua API thật bằng worker nền: tạo phiên khách, xem sản phẩm, thêm giỏ, báo giá, checkout. Journal ghi từng lượt (`succeeded`, `failed`, `unknown`) và hậu điều kiện kiểm tra sau checkout, nên dữ liệu sinh qua Console đi đúng đường của khách thật, khác với dữ liệu lịch sử do `datagen` ghi thẳng.

### **4.3.2. Giao diện quản trị quyền Lakehouse (Streamlit)**

Màn hình **Quản trị quyền** (`ui/views/access*.py`) gọi router `/api/v1/access`:
* Tạo, sửa thuộc tính, xoá catalog, principal, principal role và catalog role Polaris; xoá bị chặn khi còn liên kết hoặc principal đang cấp cho workload.
* Gán/gỡ liên kết principal → principal role → catalog role → quyền ở cấp catalog, namespace hoặc bảng.
* Cấp credential cho workload: API đọc credential principal đã lưu, gửi yêu cầu ký JWT (kèm digest payload) tới `credential-exporter`; exporter ghi `clients.env` quyền `0600` với đúng chủ là UID/GID của workload.
* Mọi thao tác ghi được ghi audit (`started`, rồi `done`, `rejected` hoặc `failed`) trong `data/access-admin/audit.jsonl`.

## **4.4. Xây dựng hệ thống thu thập dữ liệu**

### **4.4.1. Thu thập dữ liệu giao dịch PostgreSQL theo lô bằng PyIceberg và Airflow**

Thư viện `airflow/lakehouse/oltp/` và callable `airflow/tasks/oltp.py`:
* Kết nối PostgreSQL qua Airflow connection `oltp_postgres` (`PostgresHook`), phiên read-only.
* Mỗi bảng một task `staging.<bảng>`: một giao dịch repeatable-read, con trỏ phía server đọc 10.000 dòng mỗi lần, ghi vào staging mỗi lô 200.000 dòng kèm `_ingested_at`.
* Nạp gia tăng chỉ đọc các cột vừa được duyệt vừa có ở Bronze, và dừng với thông báo rõ khi nguồn bỏ cột hoặc đổi loại cột.
* Lần nạp toàn bộ có nhóm `reconcile.<bảng>` đối soát staging với nguồn tại cùng mốc trước khi Bronze đổi.

### **4.4.2. Đồng bộ dòng bị xoá và thay đổi cột ở nguồn**

DAG `lakehouse_oltp_sync` thay vai trò mà CDC thường đảm nhận, chỉ bằng cách đọc nguồn:
* `schema.<bảng>`: thêm cột đã duyệt (cùng cờ `lakehouse.reload-pending` trong một commit Iceberg), nới kiểu, báo lỗi cột chưa duyệt. Khi bảng có tệp dữ liệu cũ thiếu cột, Bronze được ghi lại một lần, vì PyIceberg 0.12 không upsert được vào bảng như vậy.
* `detect.<bảng>`: đọc toàn bộ khóa chính nguồn và khóa sống ở Bronze, đánh dấu xoá mềm khóa đã mất, đưa dòng khôi phục vào staging; khi cờ nạp lại bật, đưa mọi dòng của bảng vào staging.
* `bronze.<bảng>`: khi cờ bật, thay cả bảng Bronze trong một commit (giữ dòng đã xoá mềm) rồi mới xoá cờ; nếu không, upsert như incremental. Silver và Gold nhận thay đổi qua `_ingested_at` như mọi dòng khác.

Khi backfill incremental, DAG sync được tạm dừng và trigger tay một lần sau khi backfill xong, vì `max_active_runs` không giới hạn giữa hai DAG.

### **4.4.3. Thu thập và xử lý dòng dữ liệu Access Log bằng Apache Flink**

Luồng log cũ (Flink ghi `landing.access_logs`, mart log tính hai lần) đã được gỡ. Thiết kế mới đã duyệt ([ACCESS_LOG_DESIGN.md](ACCESS_LOG_DESIGN.md)), chưa xây:
* Hợp đồng log v1.1.0 thêm `session.id` và `ecommerce.order_number`; API sẽ cấp cookie phiên và gắn mã đơn vào sự kiện checkout.
* `bronze.web_events` chỉ lưu dữ liệu thô (`raw_payload`, `event_at`, `_ingested_at`, `_source`), nạp bằng hai đường: streaming Kafka → Flink và nạp file Parquet lịch sử từ `datagen logs`.
* `silver.web_events` tách trường, kiểm hợp đồng, bỏ trùng theo `request.id`.
* `mart_web_funnel_daily` đếm số phiên ở mỗi bước (xem sản phẩm → thêm giỏ → checkout → đặt đơn khớp `fact_order`), nối hành vi web với đơn hàng thật.

## **4.5. Xây dựng nền tảng Data Lakehouse**

### **4.5.1. Triển khai lớp lưu trữ MinIO S3, Iceberg và Polaris REST Catalog**

* Service `minio-init` (dùng `mc`) tạo bucket `lakehouse`, policy `lakehouse-polaris` và tài khoản lưu trữ của Polaris; chạy lại không đổi gì.
* Polaris quản lý catalog `lakehouse` với đường dẫn `s3://lakehouse/warehouse`, cấu hình MinIO STS để cấp credential tạm theo bảng.
* Principal, role và quyền được tạo trên Console; credential được cấp cho Airflow, Trino (đọc, ghi) và Flink qua exporter.

### **4.5.2. Xử lý và chuyển đổi dữ liệu tại tầng Bronze và Silver**

* **Bronze** (`lakehouse/oltp/bronze.py`): lần nạp toàn bộ thay cả bảng trong một commit, giữ dấu xoá cũ và đánh dấu dòng nguồn không còn; lần nạp gia tăng upsert theo khóa chính, bỏ dòng có cursor cũ hơn bản hiện có. Mỗi dòng mới hoặc khôi phục có `_is_deleted = false`.
* **Silver** (`dbt/models/silver`): 19 model incremental merge các dòng Bronze mới hơn mốc `_ingested_at` của Silver, chuẩn hóa UUID và timestamp, chuyển nguyên cờ xoá mềm; test `matches_bronze` chặn khi lệch khóa.

### **4.5.3. Xây dựng tầng Gold và các Data Mart phục vụ phân tích**

Gold (`dbt/models/oltp`) áp dụng các quy tắc hạch toán:
* Doanh thu hàng hóa = dòng hàng trừ giảm giá phân bổ, ghi nhận vào ngày muộn hơn giữa `completed_at` và `paid_at`; không gồm phí vận chuyển.
* Hoàn tiền thành công của đơn đã ghi nhận giảm doanh thu vào ngày hoàn; hoàn tiền của đơn chưa từng ghi nhận (trả tiền rồi hủy) không trừ doanh thu.
* Giá vốn lấy từ giá vốn chụp trên dòng đơn; hàng trả đạt kiểm định được trừ lại giá vốn vào ngày hoàn tiền, nên đơn trả toàn bộ cho lãi 0.
* Giao đúng hạn khi ngày giao (giờ Việt Nam) không quá ngày đặt cộng SLA của thành phố nhận (TP.HCM 4 ngày; Bình Dương, Cần Thơ, Hà Nội 6; Đà Nẵng, Hải Phòng 8).
* Tồn kho cộng dồn từ sổ cái theo vị trí tới từng ngày; giá trị tồn = tồn × giá vốn hiện tại.

Dimension và fact là model incremental merge; mart xoá và dựng lại các ngày bị ảnh hưởng. Mốc của mart nằm trong `gold._mart_state` (chỉ ghi thêm), được post-hook đẩy lên chỉ sau khi mart đã ghi xong, nên một lần chạy hỏng sẽ được lần sau bắt kịp. `DBT_AS_OF` (mốc bắt đầu lần chạy) là ngày hiện tại của Gold.

### **4.5.4. Điều phối toàn diện Pipeline bằng Apache Airflow**

| Tên DAG | Lịch thực thi | Cấu trúc |
|---|---|---|
| `lakehouse_oltp_init` | Chạy tay | `create_schema` → `staging` → `reconcile` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_incremental` | 03:00 hằng ngày (Asia/Ho_Chi_Minh), catchup, backfill | `staging` → `bronze` → `silver` → `gold` → `maintenance`; task cuối phát Asset `lakehouse_oltp_loaded` |
| `lakehouse_oltp_sync` | Sau mỗi sự kiện `lakehouse_oltp_loaded` | `schema` → `detect` → `bronze` → `silver` → `gold` → `maintenance` |
| `lakehouse_oltp_purge` | Chạy tay | `silver_gold` → `bronze` |

Mỗi nhóm có một task cho mỗi bảng hoặc model dbt; thứ tự các task Gold được suy ra từ lời gọi `ref()`/`live()` trong model. Nhóm `maintenance`: `compact` gộp tệp nhỏ của Bronze, `optimize` (dbt) viết lại tệp nhỏ của Silver và Gold, `expire` xoá snapshot ngoài 20 snapshot mới nhất và ngoài 3 ngày gần nhất, `orphan` đếm tệp mồ côi mà không xoá.

### **4.5.5. Kiểm soát chất lượng dữ liệu và khả năng chạy lại (idempotency)**

Kiểm tra chặn gồm đối soát với nguồn ở lần nạp toàn bộ và test `matches_bronze`; trượt thì task lỗi và tầng sau giữ bản cũ. Data test sau Gold chạy ở mức cảnh báo.

Mọi tầng được thiết kế chạy lại an toàn:
* Staging xoá trắng rồi ghi lại; Bronze upsert theo khóa; Silver và Gold merge theo khóa từ mốc của chúng.
* Mốc của mart chỉ tiến khi mart đã ghi xong.
* Đồng bộ nguồn: chạy lại `detect` cho cùng hai tập khóa; `_deleted_at` giữ giá trị lần phát hiện đầu; cờ nạp lại chỉ bị xoá sau khi Bronze đã nạp đủ.

Do đó retry, Clear, chạy tiếp sau một lần hỏng, hay backfill đều cho cùng bảng (kết quả kiểm chứng ở mục 4.8.1).

## **4.6. Xây dựng hệ thống Business Intelligence và phục vụ phân tích**

### **4.6.1. Kết nối Apache Trino và phân quyền truy vấn phân tích**

* Trino chạy HTTPS, xác thực bằng mật khẩu; SQL access rules giới hạn `analytics_reader` chỉ đọc catalog `lakehouse` và `dbt_writer` chỉ ghi schema `silver`, `gold` của catalog `lakehouse_etl`.
* Hai catalog Trino dùng hai principal Polaris khác nhau (`trino_reader`, `trino_writer`), nên quyền đọc và ghi được kiểm soát ở cả Trino lẫn Polaris.
* Mỗi truy vấn đọc snapshot Iceberg hiện tại hoàn chỉnh; không cần bảng công bố riêng để tránh đọc dữ liệu ghi dở.

### **4.6.2. Trực quan hóa dữ liệu với Apache Superset**

Dashboard doanh thu (mục 3.6.3) được khai báo trong `infrastructure/superset/` (dataset, chart, bố cục) và đồng bộ bằng `make superset-sync`. Superset đọc trực tiếp bảng Gold hiện tại, nên dữ liệu mới hiện ngay sau mỗi lần pipeline chạy mà không cần đồng bộ lại.

### **4.6.3. Kiểm soát phân quyền truy cập dữ liệu phân tích**

* Superset kết nối Trino bằng tài khoản chỉ đọc `analytics_reader`; Superset có đăng nhập riêng.
* Trino không ghi được tầng phân tích qua catalog đọc; dbt là đường ghi duy nhất vào Silver và Gold.
* Khi Polaris thu hồi một quyền, credential lưu trữ cấp sau đó không còn mở được bảng tương ứng.
* Phân quyền dashboard theo cửa hàng (quản lý cửa hàng chỉ thấy cửa hàng của mình) là việc tiếp theo.

## **4.7. Xây dựng dữ liệu mô phỏng phục vụ trình diễn và kiểm thử**

### **4.7.1. Hướng tiếp cận**

Dữ liệu mô phỏng theo nguyên tắc **quy luật trước, bộ sinh sau**: quy luật hành vi được rút ra từ dữ liệu công khai, ghi thành cấu hình YAML có nhãn nguồn, rồi bộ sinh đọc cấu hình đó. Mỗi khối tham số mang một trong ba nhãn: `measured` (đo trực tiếp trong notebook), `transfer` (đo ở nguồn nước ngoài, áp cho shop thời trang Việt Nam như giả định chuyển nguồn) hoặc `assumption` (tham số kịch bản).

Mỗi quy luật gồm: dữ liệu cho thấy gì, quy luật một câu với ít tham số nhất, và kiểm tra bằng cách so phân phối mô hình với thực tế (sai lệch tính bằng điểm phần trăm). Chỉ quy luật lặp lại giữa các nguồn hoặc dự báo đúng trên phần dữ liệu chưa dùng để ước lượng mới được chuyển sang shop Việt Nam.

CLI `datagen` hiện thực các quy luật:
* `datagen generate`: nạp 12 tháng lịch sử OLTP vào PostgreSQL trong một giao dịch, dựng lại index sau khi nạp (`make generate PRESET=1m`).
* `datagen verify`: đối chiếu dữ liệu đã nạp với quy luật của preset (sổ kho, R01–R07, tỷ lệ kịch bản, POS, địa điểm).
* `datagen logs`: sinh log truy cập mùa sale khớp với đơn online đã nạp, ghi Parquet theo ngày.

### **4.7.2. Quy luật mua hàng (OLTP)**

Phân tích nằm trong [purchase_behavior_eda.ipynb](../generator/oltp/notebooks/purchase_behavior_eda.ipynb), dùng H&M kênh online năm 2019 và Olist năm 2017.

| Quy luật | Kết quả thực nghiệm | Tham số |
| :--- | :--- | :--- |
| R01. Số đơn mỗi khách | Log-series lệch tối đa khoảng 1 điểm %, trong khi giả định "mọi khách cùng tốc độ" (Poisson) lệch 31 điểm %. α ước lượng từ 6 tháng dự báo đúng tỷ lệ khách mua một lần cho 3 và 12 tháng (lệch 1–3 điểm %). | θ = T/(α+T), α = 0,1141 năm |
| R02. Khoảng cách mua lại | Khách mua ít lần có các đơn gần nhau hơn mức rải đều cả năm; một hệ số c bù được cho mọi k, sai số trung bình dưới 2%. | Mũ, trung bình T(1 − c/k)/(k+1), c = 0,518 |
| R03. Độ bán chạy | Log(số bán) của sản phẩm bán đủ 12 tháng có dạng chuông; tỷ trọng top 10%/20% sản phẩm lệch 2–3,5 điểm %. Giá không liên quan độ bán chạy (Spearman 0,01). | LogNormal(0; 1,35) × hệ số nhóm hàng |
| R04. Giỏ hàng | Số sản phẩm mỗi đơn theo phân phối hình học, lệch tối đa 1,6 điểm %; kích thước giỏ độc lập với khách. | p = 0,296; bảng biến thể/số lượng |
| R05. Lịch | Black Friday gấp 5,55 (H&M) và 7,24 (Olist) lần ngày thường, ba ngày sau gấp khoảng 2,6 lần; hai nguồn không có hình dạng chung theo thứ trong tuần. | Ngày sale ×6,39; sau sale ×2,6; giờ theo Olist |
| R06. Giao trễ và đánh giá | Giao đúng hẹn 62% chấm 5 sao; trễ trên 8 ngày 68% chấm 1 sao. | Bảng số sao theo mức trễ; tỷ lệ trễ 5,5% |
| R07. Thời gian giao | Hãng lấy hàng cùng ngày nếu shop giao trước 10h30, sau đó lấy ngày kế; số ngày giao đều trong khoảng của vùng theo cam kết công bố của GHTK Express. | Nội thành TP.HCM 1–2; Bình Dương, Cần Thơ 2–4; Hà Nội 3–4; Đà Nẵng, Hải Phòng 4–6 ngày |

Bốn preset `100k`, `1m`, `10m`, `50m` dùng chung các quy luật trên, chỉ khác quy mô: 26.000, 260.000, 2.600.000 và 13.000.000 khách có mua, tương ứng khoảng 0,1, 1, 10 và 50 triệu đơn online trong 12 tháng. Tổng số đơn là kết quả của R01, không phải đầu vào. `100k` dùng để chạy thử pipeline nhanh. Giá, tỷ lệ hủy/COD/trả hàng, coupon và Tết là giả định; notebook đối chiếu chúng với số liệu Việt Nam công khai nhưng không dùng các số đó để hiệu chỉnh.

### **4.7.3. Quy luật duyệt web (access log)**

Phân tích nằm trong [browsing_behavior_eda.ipynb](../generator/logs/notebooks/browsing_behavior_eda.ipynb), dùng eCommerce Behavior tháng 10/2019 (cohort 2–31/10, khoảng 9 triệu phiên) cho L1–L3 và tháng 11/2019 cho L4. Tham số lấy theo nhóm phiên `apparel`, gần với shop thời trang nhất.

| Quy luật | Kết quả thực nghiệm | Tham số |
| :--- | :--- | :--- |
| L1. Lưu lượng | Tỷ lệ phiên không mua trên phiên có mua ổn định theo ngày (13,6 với toàn bộ dữ liệu, 41,4 với `apparel`); giá trị phụ thuộc định nghĩa phiên `apparel` (31–52). | r = 41,39 |
| L2. Lượt xem mỗi phiên | Phiên không mua theo log-series (lệch 1,3 điểm %); phiên có mua theo phân phối hình học (lệch 6,4 điểm %). | θ = 0,9239; p = 0,1044 |
| L3. Nhịp request | Khoảng cách giữa hai sự kiện theo log-normal, lệch 4,4 điểm %. | Trung vị 36,2 giây; σ = 0,938 |
| L4. Chuyển đổi ngày sale | Ngày đông nhất tháng 11 (17/11/2019) có r = 16,2 so với trung vị 45,9 của ngày thường: khách ngày sale vào để mua, tỷ lệ chuyển đổi cao gấp khoảng 2,8 lần. Chỉ có một đợt sale để đo. | r × 0,35 ngày sự kiện |

Phễu giỏ hàng được đặt làm giả định vì nguồn ghi thiếu sự kiện giỏ và phép hiệu chỉnh không ổn định giữa các khối ngày. Hành trình nhiều phiên không được mô hình hóa. Lệnh `datagen logs` dùng các quy luật này sinh log mùa sale 01/11–15/12/2025 theo hợp đồng v1.1.0: mỗi đơn online một phiên kết thúc bằng `checkout_submit` mang `order_number` (ở `1m`: khoảng 45 triệu request).

## **4.8. Đánh giá kết quả, giới hạn hệ thống và hướng phát triển**

### **4.8.1. Đánh giá kết quả đạt được của đề tài**

* **Hệ thống nguồn**: API FastAPI đầy đủ nghiệp vụ mua online, POS, kho, giao vận, đổi trả trên PostgreSQL 28 bảng; Console Streamlit mô phỏng mua hàng qua API thật.
* **Lakehouse**: Medallion bốn tầng trên Iceberg, MinIO, Polaris; 19 bảng nguồn; Gold 7 dimension, 6 fact, 14 mart, 2 seed bằng dbt trên Trino.
* **Pipeline chạy lại an toàn**, kiểm chứng trên preset `100k` bằng dấu vân tay của từng bảng (số dòng và checksum các cột nghiệp vụ):
  * chạy lại init cho cùng kết quả;
  * Clear rồi chạy lại một lần incremental cho cùng kết quả;
  * một lần chạy hỏng (Trino dừng) rồi lần sau bằng dựng lại toàn bộ;
  * backfill ba ngày bằng dựng lại toàn bộ;
  * incremental cộng sync bằng init trên toàn bộ 67 bảng.
* **Đồng bộ nguồn**, kiểm chứng trên `100k`:
  * dòng bị xoá được đánh dấu qua mọi tầng; dòng khôi phục sống lại;
  * purge để lại 0 dòng đã đánh dấu, mart không đổi, chạy purge lần hai không làm gì;
  * cột chưa duyệt: incremental vẫn xanh, sync báo lỗi rõ, Lakehouse không đổi;
  * cột đã duyệt: nạp lại đủ 28.610 dòng khách hàng; kiểu `integer` → `bigint` được nới; cột bị bỏ làm incremental dừng rõ, sửa theo thông báo thì chạy tiếp.
* **Quản trị quyền tự vận hành**: principal Polaris riêng cho từng workload, credential lưu trữ tạm theo bảng, Trino tách đọc/ghi, Console quản trị có audit.
* **Kiểm thử tự động**: `make test` chạy 102 test pipeline, 41 test platform-app, 143 test generator và 6 test Superset, không cần stack thật.

### **4.8.2. Các giới hạn hiện tại của hệ thống**

* Luồng access log vào Lakehouse (`bronze.web_events`, `silver.web_events`, `mart_web_funnel_daily`) mới có thiết kế; API còn phát hợp đồng v1.0.0.
* Superset mới có dashboard doanh thu; chưa phân quyền dashboard theo cửa hàng.
* Đồng bộ nguồn dựa trên đối chiếu khóa định kỳ: `_deleted_at` là lúc phát hiện chứ không phải lúc xoá thật; lần sửa dòng mà nạp gia tăng bỏ sót (giao dịch commit sau khi cửa sổ đã đóng) chưa được bắt.
* Duyệt một cột mới làm nạp lại cả bảng; một cột chưa duyệt ở một bảng làm hoãn đồng bộ xoá của mọi bảng cho tới khi được duyệt.
* Dữ liệu sinh dùng quy luật đo trên dữ liệu nước ngoài và các giả định có ghi rõ; chưa khẳng định đại diện cho thị trường Việt Nam.
* Hệ thống chạy trên một máy bằng Docker Compose; chưa có phân cụm, HA, sao lưu/khôi phục đồng bộ nhiều hệ thống.

### **4.8.3. Hướng nghiên cứu và phát triển trong tương lai**

* Xây luồng access log theo thiết kế đã duyệt: hợp đồng v1.1.0 trên API, Flink ghi `bronze.web_events`, nạp file log lịch sử, Silver và mart phễu nối phiên web với đơn hàng; kèm kịch bản "traffic liên tục" trên Console để trình diễn streaming.
* Hoàn thiện dashboard cho kho, giao vận, khách hàng và phân quyền theo cửa hàng.
* CDC theo WAL (Debezium) để bắt thay đổi và xoá gần thời gian thực khi quy mô hoặc yêu cầu độ trễ tăng.
* Triển khai trên Kubernetes với HA cho Trino, Polaris và Flink.
* Ứng dụng học máy trên Lakehouse: gợi ý sản phẩm, dự báo nhu cầu tồn kho theo mùa, phát hiện gian lận thanh toán.

---

# **TÀI LIỆU THAM KHẢO**

[1] OECD, *OECD Handbook on Compiling Digital Supply and Use Tables*, OECD Publishing, Paris, 2023. DOI: https://doi.org/10.1787/11a0db02-en.  
[2] Streamlit, *Streamlit Documentation*, Snowflake Inc., 2024. https://docs.streamlit.io.  
[3] FastAPI, *FastAPI Framework Documentation*, Tiangolo, 2024. https://fastapi.tiangolo.com.  
[4] PostgreSQL Global Development Group, *PostgreSQL 16 Documentation*, 2024. https://www.postgresql.org/docs/16/.  
[5] SQLAlchemy, *SQLAlchemy 2.0 Documentation*, SQLAlchemy Authors, 2024. https://docs.sqlalchemy.org/en/20/.  
[6] OWASP, *API Security Top 10 2023*, Open Web Application Security Project, 2023. https://owasp.org/API-Security/.  
[7] M. Armbrust, A. Ghodsi, R. Xin, và M. Zaharia, "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," trong *Proceedings of the 11th Conference on Innovative Data Systems Research (CIDR)*, 2021.  
[8] MinIO, *MinIO High Performance Object Storage Documentation*, MinIO, Inc., 2024. https://min.io/docs/minio/linux/index.html.  
[9] Apache Iceberg, *Apache Iceberg Table Format Documentation*, Apache Software Foundation, 2024. https://iceberg.apache.org/docs/latest/.  
[10] Apache Polaris, *Apache Polaris Documentation*, Apache Software Foundation, 2025. https://polaris.apache.org/.  
[11] R. Kimball và M. Ross, *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*, 3rd ed., Wiley, Indianapolis, 2013.  
[12] Apache Iceberg, *PyIceberg Documentation*, Apache Software Foundation, 2025. https://py.iceberg.apache.org/.  
[13] dbt Labs, *dbt Documentation*, dbt Labs, Inc., 2025. https://docs.getdbt.com/.  
[14] Apache Kafka, *Apache Kafka Documentation*, Apache Software Foundation, 2024. https://kafka.apache.org/documentation/.  
[15] Apache Flink, *Apache Flink Documentation: Stateful Stream Processing*, Apache Software Foundation, 2024. https://nightlies.apache.org/flink/flink-docs-stable/.  
[16] Apache Airflow, *Apache Airflow Documentation*, Apache Software Foundation, 2025. https://airflow.apache.org/docs/apache-airflow/stable/.  
[17] ISO/IEC, *ISO/IEC 25012:2008 - Software engineering -- Software product Quality Requirements and Evaluation (SQuaRE) -- Data quality model*, International Organization for Standardization, Geneva, 2008.  
[18] Trino, *Trino Documentation: Iceberg Connector*, Trino Software Foundation, 2024. https://trino.io/docs/current/connector/iceberg.html.  
[19] S. Few, *Information Dashboard Design: Displaying Data for At-a-Glance Monitoring*, 2nd ed., Analytics Press, Burlingame, 2013.  
[20] Apache Superset, *Apache Superset Documentation*, Apache Software Foundation, 2025. https://superset.apache.org/docs/intro.
