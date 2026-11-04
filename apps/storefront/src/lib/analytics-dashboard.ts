import type { IconName } from "@/components/ui/Icon";
import type { RoleMetricsResponse } from "@/lib/api";

export type AnalyticsRole = RoleMetricsResponse["role"];

export const ANALYTICS_ROLES: { id: AnalyticsRole; label: string; icon: IconName; purpose: string; questions: string[]; scope: string }[] = [
  { id: "executive", label: "Tổng quan", icon: "dashboard", purpose: "Theo dõi kết quả tài chính và các khoản cần đối chiếu.", questions: ["Doanh thu và lợi nhuận đang ở mức nào?", "Xu hướng các ngày gần đây thay đổi ra sao?", "Khoản COD và giao thất bại nào cần kiểm tra?"], scope: "KPI: toàn bộ dữ liệu nguồn hiện có; xu hướng: kỳ riêng bên dưới." },
  { id: "sales", label: "Kinh doanh", icon: "bar-chart", purpose: "So sánh đóng góp của điểm bán, sản phẩm và danh mục.", questions: ["Điểm bán/kênh nào đóng góp nhiều doanh thu?", "Sản phẩm và danh mục nào dẫn đầu?", "Doanh thu theo ngày biến động thế nào?"], scope: "Phân nhóm: toàn bộ dữ liệu nguồn hiện có; xu hướng: kỳ riêng bên dưới." },
  { id: "store", label: "Cửa hàng", icon: "store", purpose: "Theo dõi kết quả trong ngày và SKU cần bổ sung tại cửa hàng.", questions: ["Hôm nay cửa hàng ghi nhận bao nhiêu doanh thu và đơn?", "Có bao nhiêu dòng tồn kho thấp?", "SKU nào đã hết để kiểm tra bổ sung?"], scope: "Doanh thu/đơn: hôm nay, Asia/Ho_Chi_Minh. Tồn kho: trạng thái hiện tại." },
  { id: "inventory", label: "Tồn kho", icon: "box", purpose: "Kiểm tra lượng tồn, giá trị tồn và rủi ro thiếu hàng.", questions: ["Hàng đang nằm tại kho hay cửa hàng?", "SKU nào đã hết?", "SKU nào tại kho còn dưới ngưỡng tồn thấp?"], scope: "Tồn kho: quan sát đã công bố. Lịch sử: từng ngày trong 30 ngày gần nhất." },
  { id: "operations", label: "Vận hành", icon: "truck", purpose: "Ưu tiên đơn chờ xử lý, giao vận có vấn đề và đổi trả.", questions: ["Có bao nhiêu đơn chờ xuất?", "Đơn nào chờ từ 2 giờ tại mốc đo?", "Khu vực COD nào cần kiểm tra với số mẫu cụ thể?"], scope: "Đơn chờ tại mốc đo mart; giao vận/đổi trả: toàn bộ lần công bố." },
  { id: "marketing", label: "Marketing", icon: "ticket", purpose: "Đọc lượng tương tác theo bước và rà soát voucher, phản hồi.", questions: ["Các bước tương tác ghi nhận bao nhiêu sự kiện?", "Tỷ lệ mua/xem theo sự kiện là bao nhiêu?", "Voucher hay sản phẩm nào cần kiểm tra?"], scope: "Log: 30 ngày UTC theo mốc rollup. Voucher/đánh giá: 15/45 phút theo mốc mart nghiệp vụ riêng." },
  { id: "system", label: "Chất lượng dữ liệu", icon: "shield", purpose: "Đối soát nguồn với Lakehouse và theo dõi request, lỗi, độ trễ HTTP.", questions: ["Gold đã được nạp cách đây bao lâu?", "Chỉ số đối soát nào đang lệch?", "Route nào ghi nhận nhiều lỗi hoặc độ trễ cao?"], scope: "Đối soát: mart đơn hàng so với MySQL. HTTP: kỳ UTC riêng từ publication log theo giờ." },
];

export const CANONICAL_ROLES: Record<string, AnalyticsRole> = {
  admin: "executive", sales_manager: "sales", store_manager: "store", inventory_manager: "inventory",
  operations_manager: "operations", marketing_manager: "marketing", system_admin: "system",
};

export function isAnalyticsRole(value: string | null): value is AnalyticsRole {
  return ANALYTICS_ROLES.some(role => role.id === value);
}

export function sourceName(source?: string) {
  return source === "lakehouse" ? "Lakehouse · Trino / Iceberg" : source === "oltp" ? "MySQL · vận hành" : source === "oltp_fallback" ? "MySQL · nguồn thay thế" : "Chưa xác định nguồn";
}

export interface MetricDefinition {
  name: string;
  calculation: string;
  unit: string;
  grain: string;
  scope: string;
  caveat: string;
}

const definition = (name: string, calculation: string, unit: string, grain: string, scope: string, caveat: string): MetricDefinition => ({ name, calculation, unit, grain, scope, caveat });

export function metricDefinitions(metrics: RoleMetricsResponse): MetricDefinition[] {
  switch (metrics.role) {
    case "executive": return [
      definition("Doanh thu thuần", "SUM(mart_order_daily.net_revenue_vnd)", "VND", "Ngày × kênh × cửa hàng", "Toàn bộ lần công bố", "Ghi nhận delivered/completed/returned; trừ giảm giá và hoàn tiền hàng hóa thành công, không gồm vận chuyển."),
      definition("Lợi nhuận gộp", "SUM(mart_order_daily.gross_profit_vnd)", "VND", "Ngày × kênh × cửa hàng", "Toàn bộ lần công bố", "Doanh thu thuần trừ giá vốn ghi nhận; chưa trừ chi phí vận hành, thuế và quảng cáo."),
      definition("Tổng đơn", "SUM(mart_order_daily.total_orders)", "Đơn", "Ngày × kênh × cửa hàng", "Toàn bộ lần công bố", "Bao gồm mọi trạng thái; mỗi đơn thuộc đúng một nhóm, không cộng số đơn qua danh mục."),
      definition("AOV", "SUM(net_revenue_vnd) / SUM(recognized_orders)", "VND/đơn", "mart_order_daily", "Cùng phạm vi KPI", "Mẫu số gồm đơn delivered/completed/returned. Không có mẫu số thì trả null và hiển thị —."),
      definition("Tổng giá trị đơn", "SUM(mart_order_daily.order_total_vnd)", "VND", "Ngày × kênh × cửa hàng", "Toàn bộ lần công bố", "Gồm mọi trạng thái và phí vận chuyển; không phải doanh thu. Trường API giữ tên gmv_vnd."),
      definition("Tỷ lệ giao thất bại", "SUM(boom_orders) / SUM(total_orders) × 100", "%", "mart_order_daily", "Cùng phạm vi KPI", "Đơn failed_delivery; không đồng nghĩa tất cả đều là COD. Mẫu số 0 → —."),
      definition("Yêu cầu trả hàng / đơn", "SUM(return_requests_count) / SUM(total_orders) × 100", "%", "mart_order_daily", "Cùng phạm vi KPI", "Bỏ yêu cầu cancelled. Một đơn có thể có nhiều yêu cầu; không phải tỷ lệ đơn trả distinct."),
      definition("COD đang giao", "SUM(mart_order_daily.cod_in_transit_vnd)", "VND", "Shipment COD assigned/picked_up/in_transit", "Lần công bố OLAP", "Tổng giá trị đơn gắn shipment đang giao; không phải tiền thực tế đã thu hộ."),
    ];
    case "sales": return [
      definition("Đóng góp điểm bán", "SUM(mart_order_daily.net_revenue_vnd) theo cửa hàng/kênh", "VND", "Ngày × kênh × cửa hàng", "Toàn bộ lần công bố", "Số đơn trong bảng đếm đơn có doanh thu thuần dương; bộ lọc kỳ chỉ áp dụng xu hướng."),
      definition("Sản phẩm dẫn đầu", "SUM(mart_product_sales_daily.net_revenue_vnd), SUM(units_sold)", "VND / đơn vị", "Ngày × sản phẩm; top 10 theo doanh thu", "Toàn bộ lần công bố", "Số lượng đã trừ hàng trả hoàn tất và kiểm định đạt; tiền cùng quy tắc giảm giá/hoàn tiền như tổng quan."),
      definition("Danh mục", "SUM(mart_sales_daily.net_revenue_vnd) theo danh mục", "VND / %", "Ngày × kênh × cửa hàng × danh mục", "Toàn bộ lần công bố", "Tỷ trọng tính lại từ tổng tiền; không có mẫu số dương thì để trống. Không cộng số đơn qua danh mục."),
    ];
    case "store": return [
      definition("Doanh thu hôm nay", "SUM(mart_order_daily.net_revenue_vnd) theo store_key và ngày Việt Nam", "VND", "Ngày × kênh × cửa hàng", "Ngày đặt hàng hôm nay", "Chỉ ghi nhận delivered/completed/returned, trừ hoàn tiền hàng hóa; không gồm vận chuyển."),
      definition("Đơn hôm nay", "SUM(mart_order_daily.total_orders)", "Đơn", "Cửa hàng được phân quyền", "Hôm nay", "Gồm mọi trạng thái. Không có dòng trong ngày và publication còn ở ngày cũ thì trả null."),
      definition("Tồn thấp / hết hàng", "mart_store_overview.low_stock_count / stockout_count", "SKU tại cửa hàng", "SKU × địa điểm", "Quan sát đã công bố", "Tồn thấp: on_hand ≤ 5, gồm hết hàng. Danh sách hết hàng chỉ lấy on_hand = 0 và tối đa 10 dòng."),
    ];
    case "inventory": return [
      definition("Giá trị tồn", "mart_inventory_summary.total_inventory_value_vnd", "VND", "Tổng quan tồn hiện tại", "Quan sát đã công bố", "Tổng on_hand × giá vốn hiện có; không cộng snapshot nhiều ngày."),
      definition("Lượng tồn", "warehouse_stock_units / store_stock_units từ mart_inventory_summary", "Đơn vị", "Nhóm địa điểm", "Quan sát đã công bố", "Không có dòng quan sát cho nhóm thì null; dòng có số lượng 0 vẫn là số 0 hợp lệ."),
      definition("SKU có điểm hết hàng", "COUNT(DISTINCT variant_id) có on_hand = 0", "SKU", "SKU duy nhất trên mọi địa điểm", "mart_inventory_summary", "Một SKU hết ở hai địa điểm chỉ đếm một lần; cảnh báo chi tiết chỉ liệt kê kho trung tâm."),
      definition("Phiếu nhập", "COUNT(receipt_id), tổng hợp tại mart_inventory_summary", "Phiếu", "Nguồn nhập hàng đã công bố", "Toàn bộ dữ liệu đã nạp", "Số phiếu ghi nhận, không phải số phiếu đã nhận đủ hàng."),
      definition("Lịch sử lượng tồn", "SUM(mart_inventory_health.total_on_hand_units) theo ngày và nhóm địa điểm", "Đơn vị", "Ngày quan sát", "30 ngày gần nhất", "Tách khỏi số tồn hiện tại; không tự điền ngày thiếu snapshot."),
    ];
    case "operations": return [
      definition("Chờ xuất", "mart_operations_summary.pending_orders, trạng thái paid/confirmed", "Đơn", "Một dòng tổng quan", "operations_observed_at", "Thời gian chờ đóng băng tại mốc đo; không tự tăng khi refresh giao diện."),
      definition("Giao vận cần kiểm tra", "Shipment có attempt_count > 1 OR status = failed", "Shipment", "mart_operations_summary", "Toàn bộ lần công bố", "Không phải vi phạm SLA; một shipment chỉ đếm một lần dù thỏa hai điều kiện."),
      definition("Shipment thất bại", "Shipment status = failed", "Shipment", "mart_operations_summary", "Toàn bộ lần công bố", "Không phải số đơn, không giới hạn COD."),
      definition("Đúng hạn", "SUM(on_time_count) / SUM(on_time_eligible_count) × 100", "%", "mart_logistics_performance", "Nhóm nhân sự/địa bàn", "Chỉ có mẫu số khi biết ngày giao thực tế và dự kiến. Nguồn hiện chưa có deadline nên tỷ lệ null, không mặc định 100%."),
      definition("Thời gian vận chuyển", "SUM(duration_minutes_sum) / SUM(duration_sample_count)", "Phút", "mart_logistics_performance", "Mẫu đã kết thúc có đủ timestamp", "Không lấy trung bình của các trung bình theo ngày; không dùng thời gian của shipment còn đang giao."),
      definition("COD theo vùng", "SUM(failed_cod_shipments) / SUM(total_cod_shipments) × 100", "%", "mart_cod_region_daily", "Toàn bộ shipment COD đã công bố", "Nhóm vùng địa chỉ đơn khác địa bàn nhân sự; không có mẫu số thì null."),
      definition("Yêu cầu trả hàng", "COUNT DISTINCT return_id không cancelled, tại operations summary", "Yêu cầu", "Header yêu cầu", "Toàn bộ lần công bố", "Không cộng số yêu cầu theo SKU; một yêu cầu có thể có nhiều dòng/SKU."),
      definition("Tiền đề nghị / tiền đã hoàn", "SUM line refund ở product returns / SUM refunds.amount_vnd status succeeded ở summary", "VND", "Hai nguồn và grain riêng", "Toàn bộ lần công bố", "Khoản đề nghị không chứng minh tiền đã trả. Refund thành công có thể gồm vận chuyển hoặc đơn hủy, chưa được quy về từng SKU."),
    ];
    case "marketing": return [
      definition("Phễu tương tác", "SUM bộ đếm mart_marketing_funnel_daily trong 30 ngày UTC tại mốc rollup", "Request", "Ngày", "30 ngày theo nguồn log", "Chỉ request thành công. Xem/tìm, đặt lượng giỏ, quote/submit và submit thành công; không phải người/session/đơn duy nhất."),
      definition("Checkout / đầu phễu", "SUM orders_completed / SUM product_views × 100", "%", "Tổng trong kỳ", "Cùng kỳ log", "Tính lại từ tổng bộ đếm; không trung bình tỷ lệ ngày, mẫu số 0 trả null."),
      definition("Quan tâm sản phẩm", "mart_daily_product_demand: lượt xem, set/add giỏ và yêu thích theo khóa đã ánh xạ", "Request", "Ngày × sản phẩm/khóa chưa ánh xạ", "Cùng kỳ log", "Ánh xạ slug/UUID/variant qua danh mục pin snapshot khi build; API không JOIN Fact/Silver. Không cộng actor distinct qua ngày."),
      definition("Sử dụng voucher", "mart_coupon_usage.used_count / usage_limit; recent_redemptions_count / 15", "Lượt, %", "Coupon", "Mốc mart nghiệp vụ / 15 phút", "Không giới hạn trả null cho hạn mức/tỷ lệ. Lượt gần đây chỉ còn redeemed; không phải chi phí quảng cáo."),
      definition("Đánh giá cần xem lại", "mart_product_review_summary.recent_negative_count", "Đánh giá", "Sản phẩm", "45 phút tại mốc mart nghiệp vụ", "Chỉ approved, rating 1–2; không bao gồm tương lai. Hai nguồn có publication và cache riêng."),
    ];
    case "system": return [
      definition("Tuổi publication", "Thời điểm API − analytics_publications.published_at", "Phút", "Snapshot đã công bố", "Hiện tại", "Tuổi publication không chứng minh pipeline lỗi; batch có lịch riêng, CDC phục vụ audit."),
      definition("Chênh lệch tuyệt đối", "ABS(MySQL hiện tại − SUM mart_order_daily)", "Đơn hoặc VND", "Hai tổng độc lập", "Nguồn tại lần đo / mart publication", "MySQL count và tổng tiền đọc trong cùng một câu SQL. Dữ liệu nguồn có thể tiến trước batch."),
      definition("Chênh lệch tương đối", "Chênh tuyệt đối / MySQL × 100", "%", "Một chỉ số", "Tại lần đo", "Mẫu số MySQL bằng 0 trả null; chênh tuyệt đối vẫn xác định mismatch. Cache không phải lần đối soát mới."),
      definition("Kết quả đối soát", "matched/mismatch khi đủ hai phía; unavailable hoặc cached khi chưa có lần đo mới", "Trạng thái", "Phản hồi API", "Tại lần đo", "Không suy ra toàn bộ hệ thống healthy từ một publication hoặc hai tổng khớp."),
      definition("Request / lỗi HTTP", "SUM total_requests, client_error_4xx_count, server_error_5xx_count từ mart_hourly_route_metrics", "Request", "Giờ UTC × route × method", "Kỳ HTTP riêng, 1/7/30 ngày UTC", "Publication log độc lập đối soát đơn. Thiếu giờ để trống; request không phải người hay giao dịch duy nhất."),
      definition("Tỷ lệ lỗi HTTP", "100 × (SUM 4xx + SUM 5xx) / SUM request", "%", "Toàn kỳ HTTP", "Cùng kỳ HTTP", "Tính từ tổng bộ đếm, không trung bình tỷ lệ nhóm; mẫu số 0 trả null."),
      definition("Độ trễ HTTP", "Mean nhóm có trọng số request; p50/p95/p99 giữ từng nhóm gốc", "ms", "Mean kỳ / percentile từng giờ × route × method", "Cùng kỳ HTTP", "Mean nhóm đã làm tròn; không gộp percentile thành toàn kỳ. Bảng route/percentile chỉ trả top 20, tổng KPI không bị giới hạn này."),
    ];
  }
}
