"use client";

import { useState, type ReactNode } from "react";
import { formatMetricVnd, formatVnd, type RoleMetricsResponse } from "@/lib/api";
import { Chart, DetailTable, EmptyState, MetricCard, Panel, PublicationStatus, ViewSwitch, number, percent } from "./AnalyticsUI";
import { barOption, reconciliationOption } from "./chart-options";

const grid = "grid gap-4 sm:grid-cols-2 xl:grid-cols-4";
const pair = "grid gap-4 xl:grid-cols-2";
const money = (value: number) => formatMetricVnd(value);

function Bars({ title, rows, unit, mart, format = number, color = "#315b4f", emptyTitle, children }: {
  title: string; rows: { name: string; value: number | null }[]; unit: string; mart: string; format?: (value: number) => string; color?: string; emptyTitle?: string; children?: ReactNode;
}) {
  const values = rows.filter((row): row is { name: string; value: number } => row.value != null && Number.isFinite(row.value));
  return <div className="min-w-0" data-mart={mart}><Panel title={title} subtitle={unit}>
    {values.length ? <Chart height={Math.max(200, Math.min(360, values.length * 32 + 56))} label={`${title}, ${unit}`} option={barOption(values, title, format, color, unit.startsWith("%") ? "%" : unit.split(" · ")[0], unit === "VND" ? formatVnd : format)} /> : <EmptyState title={emptyTitle} />}
    {children}
  </Panel></div>;
}

export function RoleDashboards({ metrics, trend }: { metrics: RoleMetricsResponse; trend?: ReactNode }) {
  const [section, setSection] = useState("main");
  switch (metrics.role) {
    case "executive": return <>
      <div className={grid} data-mart="mart_order_daily">
        <MetricCard label="Doanh thu thuần" value={formatMetricVnd(metrics.net_revenue_vnd)} exactValue={formatVnd(metrics.net_revenue_vnd)} emphasis scope="VND · Toàn kỳ đã nạp" detail="Hàng hóa sau giảm giá và hoàn tiền, không gồm vận chuyển." />
        <MetricCard label="Lợi nhuận gộp" value={formatMetricVnd(metrics.gross_profit_vnd)} exactValue={formatVnd(metrics.gross_profit_vnd)} scope={`Biên ${percent(metrics.gross_margin_percent)}`} warning={metrics.gross_profit_vnd != null && metrics.gross_profit_vnd < 0} detail="Doanh thu thuần trừ giá vốn; chưa trừ chi phí vận hành." />
        <MetricCard label="Tổng đơn" value={number(metrics.total_orders)} scope="Mọi trạng thái · Toàn kỳ" />
        <MetricCard label="Giá trị đơn trung bình" value={formatMetricVnd(metrics.aov_vnd)} exactValue={formatVnd(metrics.aov_vnd)} scope="VND / đơn ghi nhận doanh thu" />
      </div>
      {trend}
      <div className={pair}>
        <Bars title="Tỷ lệ giao thất bại và yêu cầu trả" unit="% trên tổng đơn" mart="mart_order_daily" color="#a94728" rows={[{ name: "Giao thất bại", value: metrics.boom_rate_percent }, { name: "Yêu cầu trả / đơn", value: metrics.return_rate_percent }]} />
        <Panel title="Giá vốn và COD đang giao" subtitle="VND · Toàn kỳ đã nạp"><dl className="grid gap-6 py-6 sm:grid-cols-2">
          <div><dt className="text-sm text-muted">Giá vốn</dt><dd className="mt-3 text-xl font-semibold" title={formatVnd(metrics.cogs_vnd)}>{formatMetricVnd(metrics.cogs_vnd)}</dd></div>
          <div><dt className="text-sm text-muted">COD đang giao</dt><dd className="mt-3 text-xl font-semibold" title={formatVnd(metrics.cash_in_transit?.total_cod_amount_vnd)}>{formatMetricVnd(metrics.cash_in_transit?.total_cod_amount_vnd)}</dd></div>
        </dl></Panel>
      </div>
    </>;
    case "sales": {
      const stores = [...metrics.store_contributions].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      const products = [...metrics.top_selling_products].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      const categories = [...metrics.category_shares].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      return <>
        <div className="grid gap-4 sm:grid-cols-2" data-mart="mart_order_daily">
          <MetricCard label="Doanh thu thuần" value={stores.length ? formatMetricVnd(stores.reduce((sum, row) => sum + row.revenue_vnd, 0)) : "—"} exactValue={stores.length ? formatVnd(stores.reduce((sum, row) => sum + row.revenue_vnd, 0)) : undefined} emphasis scope="VND · Toàn kỳ đã nạp" />
          <MetricCard label="Đơn có doanh thu dương" value={stores.length ? number(stores.reduce((sum, row) => sum + row.order_count, 0)) : "—"} scope="Đơn · Toàn kỳ đã nạp" />
        </div>
        <ViewSwitch value={section} onChange={setSection} label="Phân tích kinh doanh" items={[{ value: "main", label: "Điểm bán & xu hướng" }, { value: "products", label: "Sản phẩm & danh mục" }]} />
        {section === "main" ? <>{trend}<Bars title="Doanh thu theo điểm bán · Top 10" unit="VND" mart="mart_order_daily" format={money} rows={stores.slice(0, 10).map(row => ({ name: row.store_name, value: row.revenue_vnd }))}>
          <DetailTable rows={stores} caption="Điểm bán · Toàn kỳ" filename="diem-ban" rowKey={row => `${row.store_id}-${row.store_name}`} columns={[{ key: "store", label: "Điểm bán", value: row => row.store_name }, { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, numeric: true }, { key: "orders", label: "Đơn doanh thu dương", value: row => row.order_count, numeric: true }]} />
        </Bars></> : <div className={pair}>
          <Bars title="Sản phẩm theo doanh thu · Top 10" unit="VND" mart="mart_product_sales_daily" format={money} rows={products.map(row => ({ name: row.product_name, value: row.revenue_vnd }))}><DetailTable rows={products} caption="Doanh thu sản phẩm · Top 10" filename="san-pham" rowKey={row => row.product_id} columns={[{ key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "units", label: "Lượng bán thuần", value: row => row.units_sold, numeric: true }, { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, numeric: true }]} /></Bars>
          <Bars title="Doanh thu theo danh mục · Top 10" unit="VND" mart="mart_sales_daily" format={money} rows={categories.slice(0, 10).map(row => ({ name: row.category_name, value: row.revenue_vnd }))}><DetailTable rows={categories} caption="Doanh thu danh mục" filename="danh-muc" rowKey={row => row.category_id} columns={[{ key: "category", label: "Danh mục", value: row => row.category_name }, { key: "money", label: "Doanh thu (VND)", value: row => row.revenue_vnd, numeric: true }, { key: "share", label: "Tỷ trọng (%)", value: row => row.share_percent, numeric: true }]} /></Bars>
        </div>}
      </>;
    }
    case "store": return <>
      <div className={grid} data-mart="mart_store_overview mart_order_daily">
        <MetricCard label="Doanh thu hôm nay" value={formatMetricVnd(metrics.store_revenue_today_vnd)} exactValue={formatVnd(metrics.store_revenue_today_vnd)} emphasis scope="VND · Ngày đặt hàng UTC+7" />
        <MetricCard label="Đơn hôm nay" value={number(metrics.store_orders_count)} scope="Mọi trạng thái · UTC+7" />
        <MetricCard label="Dòng tồn thấp" value={number(metrics.low_stock_at_store_count)} scope="SKU × cửa hàng · Tồn ≤ 5" />
        <MetricCard label="SKU hết hàng" value={number(metrics.stockout_at_store_count)} scope="Tại cửa hàng · Tồn = 0" />
      </div>
      <PublicationStatus data={metrics} observedAt={metrics.inventory_observed_at} label="Tồn kho" />
      <Bars title="SKU hết hàng tại cửa hàng · Tối đa 10" emptyTitle={metrics.stockout_at_store_count === 0 ? "Không có SKU hết hàng" : undefined} unit="Đơn vị tồn" mart="mart_inventory_current" rows={(metrics.store_stockouts ?? []).map(row => ({ name: row.sku, value: row.on_hand }))} color="#a63f3b">
        <DetailTable caption="SKU hết hàng" filename="het-hang-cua-hang" rows={metrics.store_stockouts ?? []} rowKey={row => row.variant_id} columns={[{ key: "sku", label: "SKU", value: row => row.sku }, { key: "name", label: "Sản phẩm", value: row => row.product_name }, { key: "stock", label: "Tồn", value: row => row.on_hand, numeric: true }]} />
      </Bars>
    </>;
    case "inventory": {
      const history = [...metrics.inventory_history].sort((a, b) => a.date.localeCompare(b.date));
      const dates: string[] = [];
      if (history.length) for (let t = Date.parse(history[0].date); t <= Date.parse(history[history.length - 1].date); t += 86_400_000) dates.push(new Date(t).toISOString().slice(0, 10));
      const byDate = new Map(history.map(row => [row.date, row]));
      return <>
        <div className={grid} data-mart="mart_inventory_summary">
          <MetricCard label="Giá trị tồn" value={formatMetricVnd(metrics.total_inventory_value_vnd)} exactValue={formatVnd(metrics.total_inventory_value_vnd)} emphasis scope="VND · Số dư hiện tại" />
          <MetricCard label="Tồn kho trung tâm" value={number(metrics.warehouse_stock_units)} scope="Đơn vị hàng" />
          <MetricCard label="Tồn tại cửa hàng" value={number(metrics.store_stock_units)} scope="Đơn vị hàng" />
          <MetricCard label="SKU có điểm hết hàng" value={number(metrics.stockout_count)} scope="SKU duy nhất · Tồn = 0" />
        </div>
        <PublicationStatus data={metrics} observedAt={metrics.inventory_observed_at} label="Tồn kho" />
        <div data-mart="mart_inventory_health"><Panel title="Tồn kho qua các ngày quan sát" subtitle="Đơn vị hàng · 30 ngày gần nhất">
          {history.length ? <Chart label="Tồn kho và cửa hàng theo ngày, đơn vị hàng" option={{ color: ["#315b4f", "#8b5b16"], tooltip: { trigger: "axis", renderMode: "richText", confine: true }, legend: { top: 0 }, grid: { left: 8, right: 16, top: 40, bottom: 30, containLabel: true }, xAxis: { type: "category", data: dates, axisLabel: { formatter: (value: string) => value.slice(5) } }, yAxis: { type: "value", name: "Đơn vị hàng", splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } }, series: [{ name: "Kho", type: "line", connectNulls: false, data: dates.map(date => byDate.get(date)?.warehouse_stock_units ?? null) }, { name: "Cửa hàng", type: "line", lineStyle: { type: "dashed" }, connectNulls: false, data: dates.map(date => byDate.get(date)?.store_stock_units ?? null) }] }} /> : <EmptyState />}
          <DetailTable rows={history} caption="Lịch sử tồn kho" filename="lich-su-ton" rowKey={row => row.date} columns={[{ key: "date", label: "Ngày", value: row => row.date }, { key: "warehouse", label: "Kho (đơn vị)", value: row => row.warehouse_stock_units, numeric: true }, { key: "store", label: "Cửa hàng (đơn vị)", value: row => row.store_stock_units, numeric: true }, { key: "value", label: "Giá trị (VND)", value: row => row.total_inventory_value_vnd, numeric: true }]} />
        </Panel></div>
        <div className={pair}>
          <Bars title="SKU tồn thấp tại kho · Tối đa 10" emptyTitle={metrics.warehouse_stock_units != null ? "Không có SKU tồn thấp" : undefined} unit="Đơn vị tồn · 1–5" mart="mart_inventory_current" rows={metrics.low_stock_alerts.map(row => ({ name: row.sku, value: row.on_hand }))} color="#8b5b16" />
          <Panel title="Hết hàng và nhập kho"><div data-mart="mart_inventory_summary"><MetricCard label="Phiếu nhập ghi nhận" value={number(metrics.inbound_batches_count)} scope="Toàn kỳ đã nạp" /></div><div data-mart="mart_inventory_current"><DetailTable rows={metrics.critical_stockout_alerts ?? []} caption="SKU hết tại kho · Tối đa 10" filename="het-hang-kho" rowKey={row => row.variant_id} columns={[{ key: "sku", label: "SKU", value: row => row.sku }, { key: "name", label: "Sản phẩm", value: row => row.product_name }, { key: "stock", label: "Tồn", value: row => row.on_hand, numeric: true }]} /></div></Panel>
        </div>
      </>;
    }
    case "operations": return <>
      <PublicationStatus data={metrics} observedAt={metrics.operations_observed_at} label="Vận hành" />
      <div className={grid} data-mart="mart_operations_summary">
        <MetricCard label="Đơn chờ xuất" value={number(metrics.pending_fulfillment_count)} emphasis scope="Đơn paid / confirmed" />
        <MetricCard label="Chờ từ 2 giờ" value={number(metrics.fulfillment?.over_two_hours_orders)} scope="Đơn · Tại mốc đo" />
        <MetricCard label="Shipment thất bại" value={number(metrics.failed_shipments_count)} scope="Toàn kỳ đã nạp" />
        <MetricCard label="Shipment cần kiểm tra" value={number(metrics.problematic_shipments_count)} scope="Nhiều lần giao hoặc failed" />
      </div>
      <ViewSwitch value={section} onChange={setSection} label="Phân tích vận hành" items={[{ value: "main", label: "Giao vận" }, { value: "returns", label: "Đổi trả" }]} />
      {section === "main" ? <div className={pair}>
        <Bars title="Shipment thất bại theo nhân sự / địa bàn · Top 10" unit="Shipment" mart="mart_logistics_performance" color="#a63f3b" rows={[...metrics.logistics].sort((a, b) => b.failed_count - a.failed_count).slice(0, 10).map(row => ({ name: `${row.staff_code} · ${row.city_name}`, value: row.failed_count }))}>
          <DetailTable rows={metrics.logistics} caption="Hiệu suất giao vận" filename="giao-van" rowKey={row => `${row.staff_id}-${row.city_id}`} columns={[{ key: "staff", label: "Nhân sự", value: row => row.staff_code }, { key: "city", label: "Địa bàn", value: row => row.city_name }, { key: "total", label: "Shipment", value: row => row.total_shipments, numeric: true }, { key: "delivered", label: "Đã giao", value: row => row.delivered_count, numeric: true }, { key: "failed", label: "Thất bại", value: row => row.failed_count, numeric: true }, { key: "duration", label: "Vận chuyển TB (phút)", value: row => row.average_transit_minutes, numeric: true }, { key: "rate", label: "Đúng hạn (%)", value: row => row.on_time_rate_percent, numeric: true }, { key: "cod", label: "COD đã thu (VND)", value: row => row.cod_collected_vnd, numeric: true }]} />
        </Bars>
        <Bars title="Tỷ lệ COD thất bại theo vùng" unit="% shipment COD" mart="mart_cod_region_daily" color="#a94728" rows={[...metrics.cod_regions].sort((a, b) => (b.failure_rate_percent ?? -1) - (a.failure_rate_percent ?? -1)).map(row => ({ name: row.region, value: row.failure_rate_percent }))}>
          <DetailTable rows={metrics.cod_regions} caption="COD theo vùng · Số mẫu" filename="cod-vung" rowKey={row => row.region} columns={[{ key: "region", label: "Vùng", value: row => row.region }, { key: "total", label: "Shipment COD", value: row => row.total_cod_shipments, numeric: true }, { key: "failed", label: "Thất bại", value: row => row.failed_cod_shipments, numeric: true }, { key: "rate", label: "Tỷ lệ (%)", value: row => row.failure_rate_percent, numeric: true }]} />
        </Bars>
      </div> : <><div className="grid gap-4 sm:grid-cols-2" data-mart="mart_operations_summary"><MetricCard label="Yêu cầu đổi trả" value={number(metrics.return_requests_count)} scope="Yêu cầu duy nhất · Không cancelled" /><MetricCard label="Tiền hoàn thành công" value={formatMetricVnd(metrics.successful_refund_amount_vnd)} exactValue={formatVnd(metrics.successful_refund_amount_vnd)} scope={`${number(metrics.successful_refunds_count)} refund · VND`} /></div>
        <Bars title="Lượng đề nghị trả theo SKU · Top 10" unit="Đơn vị hàng" mart="mart_product_returns" rows={metrics.product_returns.map(row => ({ name: row.sku, value: row.requested_units }))} color="#a94728"><DetailTable rows={metrics.product_returns} caption="Đổi trả theo SKU" filename="doi-tra" rowKey={row => row.variant_id} columns={[{ key: "sku", label: "SKU", value: row => row.sku }, { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "requests", label: "Yêu cầu có SKU", value: row => row.request_count, numeric: true }, { key: "units", label: "Lượng đề nghị", value: row => row.requested_units, numeric: true }, { key: "accepted", label: "Kiểm định đạt", value: row => row.accepted_units, numeric: true }, { key: "refund", label: "Tiền đề nghị (VND)", value: row => row.requested_refund_vnd, numeric: true }]} /></Bars>
      </>}
    </>;
    case "marketing": return <>
      <ViewSwitch value={section} onChange={setSection} label="Phân tích Marketing" items={[{ value: "main", label: "Tương tác" }, { value: "commerce", label: "Voucher & đánh giá" }]} />
      {section === "main" ? <>
        <PublicationStatus data={metrics.log_status} observedAt={metrics.log_status.observed_at} label="Log" />
        <p className="text-xs text-muted">{metrics.funnel_window_start ?? "—"} → {metrics.funnel_window_end ?? "—"} · UTC · Request thành công</p>
        <div className="grid gap-4 sm:grid-cols-3" data-mart="mart_marketing_funnel_daily">
          <MetricCard label="Xem / tìm sản phẩm" value={number(metrics.view_events_count)} emphasis scope="Lượt request · 30 ngày UTC" />
          <MetricCard label="Gửi checkout thành công" value={number(metrics.checkout_success_events_count)} scope="Lượt request · Cùng kỳ" />
          <MetricCard label="Checkout / đầu phễu" value={percent(metrics.checkout_to_view_rate_percent)} scope="Tỷ lệ request · Cùng kỳ" detail="Không phải tỷ lệ chuyển đổi người dùng duy nhất." />
        </div>
        <div className={pair}>
          <Bars title="Tương tác theo bước" unit="Request thành công" mart="mart_marketing_funnel_daily" rows={metrics.funnel_steps.map(row => ({ name: row.step_name, value: row.count }))} />
          <Bars title="Sản phẩm được xem · Top 10" unit="Lượt xem chi tiết" mart="mart_daily_product_demand" rows={metrics.product_demand.map(row => ({ name: row.product_name ?? row.product_key, value: row.detail_views }))}>
            <DetailTable rows={metrics.product_demand} caption="Tương tác sản phẩm · Top 10" filename="tuong-tac" rowKey={row => row.product_key} columns={[{ key: "name", label: "Sản phẩm / khóa", value: row => row.product_name ?? row.product_key }, { key: "views", label: "Xem", value: row => row.detail_views, numeric: true }, { key: "cart", label: "Đặt lượng giỏ", value: row => row.cart_sets, numeric: true }, { key: "wish", label: "Yêu thích", value: row => row.wishlist_adds, numeric: true }, { key: "rate", label: "Giỏ / xem (%)", value: row => row.cart_to_view_rate_percent, numeric: true }]} />
          </Bars>
        </div>
      </> : <>
        <PublicationStatus data={metrics.commerce_status} observedAt={metrics.commerce_status.observed_at} label="Nghiệp vụ" />
        <div className="grid gap-4 sm:grid-cols-2" data-mart="mart_marketing_summary">
          <MetricCard label="Voucher đã ghi nhận" value={number(metrics.coupon_source_rows)} scope="Bản ghi · Toàn kỳ đã nạp" />
          <MetricCard label="Đánh giá đã ghi nhận" value={number(metrics.review_source_rows)} scope="Bản ghi · Toàn kỳ đã nạp" />
        </div>
        <div className={pair}>
          <Bars title="Lượt dùng voucher · Top 10" emptyTitle={metrics.commerce_status.available && (metrics.coupon_source_rows ?? 0) > 0 ? "Không có voucher khả dụng" : undefined} unit="Lượt · Voucher khả dụng tại mốc đo" mart="mart_coupon_usage" rows={metrics.coupons.map(row => ({ name: row.coupon_code, value: row.used_count }))}>
            <DetailTable rows={metrics.coupons} caption="Voucher khả dụng · Top 10" filename="voucher" rowKey={row => row.coupon_id} columns={[{ key: "code", label: "Mã", value: row => row.coupon_code }, { key: "used", label: "Lượt dùng", value: row => row.used_count, numeric: true }, { key: "limit", label: "Hạn mức", value: row => row.usage_limit, render: row => row.usage_limit == null ? "Không giới hạn" : number(row.usage_limit), numeric: true }, { key: "rate", label: "Dùng (%)", value: row => row.usage_percent, numeric: true }, { key: "recent", label: "Lượt / 15 phút", value: row => row.recent_redemptions_count, numeric: true }]} />
          </Bars>
          <Bars title="Đánh giá 1–2 sao đã duyệt · Top 5" emptyTitle={metrics.commerce_status.available && (metrics.review_source_rows ?? 0) > 0 ? "Không có đánh giá 1–2 sao trong kỳ" : undefined} unit="Đánh giá · 45 phút tại mốc đo" mart="mart_product_review_summary" color="#a94728" rows={metrics.negative_reviews.map(row => ({ name: row.product_name, value: row.negative_count }))} />
        </div>
      </>}
    </>;
    case "system": {
      const fresh = !metrics.served_from_cache && ["matched", "mismatch"].includes(metrics.reconciliation_status);
      const status = { matched: "Khớp", mismatch: "Có chênh lệch", unavailable: "Thiếu dữ liệu", cached: "Bản lưu" }[metrics.reconciliation_status] ?? "—";
      return <>
        <div className="grid gap-4 sm:grid-cols-2" data-mart="mart_order_daily"><MetricCard label="Kết quả đối soát" value={status} emphasis scope="MySQL tại lần đo / mart đã công bố" /><MetricCard label="Tuổi publication" value={metrics.publication_age_minutes == null ? "—" : `${number(metrics.publication_age_minutes)} phút`} scope={`Nguồn ${metrics.source_mode ?? "—"}`} detail="Không phải phép đo sức khỏe pipeline hoặc SLA." /></div>
        <PublicationStatus data={metrics} observedAt={metrics.source_observed_at} label="Đối soát · MySQL quan sát" />
        <div className={pair}>{metrics.reconciliation_variance.map(row => {
          const monetary = row.metric_name.endsWith("_vnd");
          return <Panel key={row.metric_name} title={monetary ? "Tổng giá trị đơn" : "Tổng đơn"} subtitle={`Chênh tuyệt đối ${number(row.absolute_difference)} ${monetary ? "VND" : "đơn"} · ${percent(row.variance_percent)}`}>
            {fresh && row.oltp_value != null && row.lakehouse_value != null ? <Chart height={240} label={`MySQL / mart · ${monetary ? "VND" : "Đơn"}`} option={reconciliationOption(row, monetary ? "VND" : "Đơn", monetary ? formatMetricVnd : number, monetary ? formatVnd : number)} /> : <EmptyState title="Chưa có đối soát mới" />}
            <dl className="mt-3 flex flex-wrap justify-between gap-3 text-xs"><div><dt className="text-muted">MySQL{metrics.served_from_cache ? " · Bản lưu" : ""}</dt><dd className="font-semibold">{monetary ? formatVnd(row.oltp_value) : number(row.oltp_value)}</dd></div><div><dt className="text-muted">Mart{metrics.served_from_cache ? " · Bản lưu" : ""}</dt><dd className="font-semibold">{monetary ? formatVnd(row.lakehouse_value) : number(row.lakehouse_value)}</dd></div></dl>
          </Panel>;
        })}</div>
      </>;
    }
  }
}
