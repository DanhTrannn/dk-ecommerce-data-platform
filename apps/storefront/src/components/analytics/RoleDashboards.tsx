"use client";

import type { ReactNode } from "react";
import { formatMetricVnd, formatVnd, type RoleMetricsResponse, type ReconciliationVariance } from "@/lib/api";
import { Chart, DetailTable, EmptyState, MetricCard, NextSteps, Panel, number, percent, ratio } from "./AnalyticsUI";
import { barOption, reconciliationOption } from "./chart-options";

export function RoleDashboards({ metrics, userRole, trend }: { metrics: RoleMetricsResponse; userRole: string; trend?: ReactNode }) {
  const kpiGrid = "grid gap-4 sm:grid-cols-2 xl:grid-cols-4";
  const allTime = "Toàn bộ nguồn hiện có";
  const current = "Lần công bố OLAP gần nhất";

  switch (metrics.role) {
    case "executive": {
      const financials = [
        { name: "Tổng giá trị đơn", value: metrics.gmv_vnd, note: "Tổng giá trị đơn, gồm mọi trạng thái và phí vận chuyển" },
        { name: "Giá vốn", value: metrics.cogs_vnd, note: "Theo quy tắc nguồn; xem định nghĩa chỉ số" },
        { name: "COD đang giao", value: metrics.cash_in_transit?.total_cod_amount_vnd ?? null, note: "Theo lần công bố OLAP; chưa phải tiền đã thu hộ" },
      ];
      return <>
        <div className={kpiGrid}>
          <MetricCard detail="Hàng đã giao, sau giảm giá và hoàn tiền thành công; không gồm phí vận chuyển." emphasis label="Doanh thu thuần" scope={allTime} value={formatMetricVnd(metrics.net_revenue_vnd)} />
          <MetricCard detail={`Biên lợi nhuận: ${metrics.net_revenue_vnd != null && metrics.net_revenue_vnd > 0 ? percent(metrics.gross_margin_percent) : "chưa có mẫu số dương"}. Chưa trừ đủ chi phí vận hành.`} label="Lợi nhuận gộp" scope={allTime} value={formatMetricVnd(metrics.gross_profit_vnd)} warning={metrics.gross_profit_vnd != null && metrics.gross_profit_vnd < 0} />
          <MetricCard detail="Đơn duy nhất, gồm mọi trạng thái." label="Tổng đơn" scope={allTime} value={number(metrics.total_orders)} />
          <MetricCard detail="Doanh thu thuần chia số đơn đã giao, gồm đơn có trả hàng." label="Giá trị đơn trung bình" scope={allTime} value={formatMetricVnd(metrics.aov_vnd)} />
        </div>
        {trend}
        <div className="grid gap-5 xl:grid-cols-2">
          <Panel title="Những tỷ lệ nào cần đối chiếu?" subtitle="Hai chỉ số độc lập, có thể chồng lặp. Không suy ngược ra số đơn giao thành công.">
            {metrics.boom_rate_percent != null && metrics.return_rate_percent != null ? <Chart height={210} label="Tỷ lệ giao thất bại và yêu cầu trả hàng trên tổng đơn, đơn vị phần trăm, cùng mẫu số tổng đơn." option={barOption([{ name: "Giao thất bại", value: metrics.boom_rate_percent }, { name: "Yêu cầu trả hàng / đơn", value: metrics.return_rate_percent }], "Tỷ lệ", value => `${number(value)}%`, "#a94728")} /> : <EmptyState detail="Chưa có tổng đơn để tính tỷ lệ. Không tự thay dữ liệu thiếu bằng 0%." />}
            <p className="mt-3 text-xs leading-5 text-muted">Đếm yêu cầu trả hàng không bị hủy; một đơn có thể có nhiều yêu cầu. Giao thất bại không được mặc định là COD.</p>
          </Panel>
          <Panel title="Các khoản bổ sung" subtitle="Đơn vị VND. Các khoản lấy từ cùng lần công bố Data Mart với KPI tổng quan.">
            <dl className="divide-y divide-line">{financials.map(item => <div className="py-4 first:pt-0" key={item.name}><div className="flex flex-wrap justify-between gap-2"><dt className="text-sm font-medium text-muted">{item.name}</dt><dd className="font-semibold tabular-nums">{item.value == null ? "—" : formatVnd(item.value)}</dd></div><p className="mt-1 text-xs leading-5 text-muted">{item.note}</p></div>)}</dl>
          </Panel>
        </div>
        <NextSteps userRole={userRole} items={[
          { title: "Đối chiếu trước khi kết luận tăng trưởng", detail: "KPI tổng quan và xu hướng dùng cùng quy tắc ghi nhận, với phạm vi thời gian riêng. Chưa có kỳ trước đồng nhất để tính delta." },
          { title: "Kiểm tra đơn giao thất bại", detail: metrics.total_orders != null && metrics.total_orders > 0 ? `API ghi nhận tỷ lệ ${percent(metrics.boom_rate_percent)}. Xem đơn cụ thể để xác định tình trạng và nguyên nhân.` : "Chưa có mẫu số; kiểm tra nguồn dữ liệu trước.", href: "/admin/orders?status=failed_delivery", linkLabel: "Xem đơn" },
          { title: "Đối soát COD đang giao", detail: "Kiểm tra shipment và khoản tiền theo đơn, không coi toàn bộ giá trị đang giao là tiền thực tế bên vận chuyển đã thu.", href: "/admin/logistics", linkLabel: "Xem giao vận" },
        ]} />
      </>;
    }
    case "sales": {
      const stores = [...metrics.store_contributions].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      const products = [...metrics.top_selling_products].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      const categories = [...metrics.category_shares].sort((a, b) => b.revenue_vnd - a.revenue_vnd);
      const total = stores.reduce((sum, row) => sum + row.revenue_vnd, 0);
      const online = stores.filter(row => row.store_id == null).reduce((sum, row) => sum + row.revenue_vnd, 0);
      return <>
        <div className={kpiGrid}>
          <MetricCard detail="Tổng revenue_vnd của các điểm bán/kênh API trả về." emphasis label="Doanh thu theo điểm bán" scope={allTime} value={stores.length ? formatMetricVnd(total) : "—"} />
          <MetricCard detail="Số nhóm điểm bán và kênh online trong phản hồi." label="Nhóm được ghi nhận" scope={allTime} value={stores.length ? number(stores.length) : "—"} />
          <MetricCard detail="Doanh thu nhóm không có store_id / tổng đóng góp. Không có tổng dương thì để trống." label="Tỷ trọng online" scope={allTime} value={percent(ratio(online, total))} />
          <MetricCard detail="API trả tối đa 10 sản phẩm, không đại diện toàn bộ danh mục." label="Sản phẩm trong top" scope={allTime} value={products.length ? number(products.length) : "—"} />
        </div>
        {trend}
        <Panel title="Điểm bán và kênh nào đóng góp nhiều nhất?" subtitle="Top 10 nhóm theo doanh thu, đơn vị VND. Bảng bên dưới giữ đầy đủ nhóm từ API.">
          {stores.length ? <Chart label="So sánh đóng góp doanh thu theo điểm bán và kênh, VND." option={barOption(stores.slice(0, 10).map(row => ({ name: row.store_name, value: row.revenue_vnd })), "Doanh thu", formatMetricVnd)} /> : <EmptyState />}
          <DetailTable caption="Đóng góp theo điểm bán và kênh" filename="dong-gop-diem-ban" rowKey={row => `${row.store_id}-${row.store_name}`} rows={stores} columns={[
            { key: "store", label: "Điểm bán / kênh", value: row => row.store_name },
            { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, render: row => formatVnd(row.revenue_vnd), numeric: true },
            { key: "orders", label: "Đơn có doanh thu thuần dương", value: row => row.order_count, numeric: true },
          ]} />
        </Panel>
        <div className="grid gap-5 xl:grid-cols-2">
          <Panel title="Sản phẩm nào dẫn đầu về doanh thu?" subtitle="Top 10 theo doanh thu thuần, cùng quy tắc ghi nhận và hoàn tiền với KPI tổng quan.">
            {products.length ? <Chart label="Top sản phẩm theo doanh thu, VND." option={barOption(products.map(row => ({ name: row.product_name, value: row.revenue_vnd })), "Doanh thu", formatMetricVnd)} /> : <EmptyState />}
            <details className="mt-4"><summary className="cursor-pointer text-sm font-semibold">Xem sản phẩm và số lượng bán</summary><div className="mt-4"><DetailTable caption="Top sản phẩm" filename="top-san-pham" rowKey={row => row.product_id} rows={products} columns={[
              { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "units", label: "Đơn vị bán", value: row => row.units_sold, numeric: true }, { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, numeric: true },
            ]} /></div></details>
          </Panel>
          <Panel title="Danh mục nào tạo doanh thu?" subtitle="Top 10 theo doanh thu. Dùng thanh ngang để so sánh rõ khi có nhiều danh mục.">
            {categories.length ? <Chart label="Doanh thu theo danh mục, VND." option={barOption(categories.slice(0, 10).map(row => ({ name: row.category_name || "Chưa xác định", value: row.revenue_vnd })), "Doanh thu", formatMetricVnd, "#a94728")} /> : <EmptyState />}
            <details className="mt-4"><summary className="cursor-pointer text-sm font-semibold">Xem doanh thu và tỷ trọng</summary><div className="mt-4"><DetailTable caption="Doanh thu danh mục" filename="doanh-thu-danh-muc" rowKey={row => `${row.category_id}-${row.category_name}`} rows={categories} columns={[
              { key: "category", label: "Danh mục", value: row => row.category_name }, { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, numeric: true }, { key: "share", label: "Tỷ trọng (%)", value: row => row.share_percent, numeric: true },
            ]} /></div></details>
          </Panel>
        </div>
        <NextSteps userRole={userRole} items={[
          { title: stores[0] ? `Kiểm tra đóng góp của ${stores[0].store_name}` : "Kiểm tra dữ liệu điểm bán", detail: stores[0] ? `Nhóm đứng đầu ghi nhận ${formatVnd(stores[0].revenue_vnd)} trong phạm vi nguồn. Chưa có kỳ trước để kết luận tăng trưởng.` : "Nguồn chưa trả về nhóm; kiểm tra lần nạp dữ liệu gần nhất." },
          { title: "Đối chiếu sản phẩm với đơn gốc", detail: "Doanh thu sản phẩm, danh mục và điểm bán dùng cùng quy tắc ghi nhận. Top 10 chỉ là một phần danh sách sản phẩm.", href: "/admin/orders", linkLabel: "Xem đơn" },
        ]} />
      </>;
    }
    case "store": return <>
      <div className={kpiGrid}>
        <MetricCard detail="Doanh thu hàng hóa đã ghi nhận, sau giảm giá/hoàn tiền; theo ngày đặt hàng Việt Nam." emphasis label="Doanh thu hôm nay" scope="Từ 00:00 · UTC+7" value={formatMetricVnd(metrics.store_revenue_today_vnd)} />
        <MetricCard detail="Mọi trạng thái đơn tại cửa hàng; cùng ngày đặt hàng, không chỉ đơn đã ghi nhận doanh thu." label="Đơn hôm nay" scope="Từ 00:00 · UTC+7" value={number(metrics.store_orders_count)} />
        <MetricCard detail="Số dòng SKU có on_hand ≤ 5, bao gồm dòng hết hàng." label="Dòng tồn thấp" scope={current} value={number(metrics.low_stock_at_store_count)} warning={metrics.low_stock_at_store_count != null && metrics.low_stock_at_store_count > 0} />
        <MetricCard detail="Tổng SKU có on_hand = 0 tại cửa hàng; danh sách bên dưới trả tối đa 10 dòng." label="SKU hết hàng" scope={current} value={number(metrics.stockout_at_store_count)} />
      </div>
      <Panel title={`SKU nào cần kiểm tra tại ${metrics.store_name ?? "cửa hàng được phân quyền"}?`} subtitle="Danh sách có on_hand = 0 theo lần công bố, tối đa 10 SKU; không lấy số dòng danh sách thay tổng số hết hàng.">
        <DetailTable caption="SKU hết hàng tại cửa hàng" filename={`het-hang-cua-hang-${metrics.store_id ?? "assigned"}`} rows={metrics.store_stockouts ?? []} rowKey={row => row.variant_id} columns={[
          { key: "sku", label: "SKU", value: row => row.sku }, { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "stock", label: "Tồn hiện tại (đơn vị)", value: row => row.on_hand, numeric: true },
        ]} />
      </Panel>
      {metrics.target_achievement_percent != null && metrics.run_rate && metrics.run_rate.daily_target_vnd > 0 && <Panel title="Tiến độ so với chỉ tiêu đã cung cấp" subtitle="Chỉ hiển thị khi API cung cấp chỉ tiêu thực tế."><p className="text-2xl font-semibold">{percent(metrics.target_achievement_percent)}</p><p className="mt-2 text-sm text-muted">Chỉ tiêu: {formatVnd(metrics.run_rate.daily_target_vnd)}</p></Panel>}
      <NextSteps userRole={userRole} items={[
        { title: "Kiểm tra bổ sung hàng tại quầy", detail: `Có ${number(metrics.low_stock_at_store_count)} dòng tồn thấp. Danh sách hết hàng và tồn thấp có thể chồng lặp.`, href: userRole === "store_manager" ? "/store" : "/admin/products", linkLabel: "Mở cửa hàng / hàng hóa" },
        { title: "Đánh giá kết quả ngày khi đủ dữ liệu", detail: "Chưa có chỉ tiêu hoặc kỳ trước thì không kết luận cửa hàng vượt/chậm tiến độ. Không có chuỗi thời gian thì không dựng xu hướng." },
      ]} />
    </>;
    case "inventory": {
      const historyByDate = new Map(metrics.inventory_history.map(row => [row.date, row]));
      const historyDates: string[] = [];
      if (metrics.inventory_history.length) {
        const first = metrics.inventory_history[0].date;
        const last = metrics.inventory_history[metrics.inventory_history.length - 1].date;
        for (let time = Date.parse(`${first}T00:00:00Z`); time <= Date.parse(`${last}T00:00:00Z`); time += 86_400_000) {
          historyDates.push(new Date(time).toISOString().slice(0, 10));
        }
      }
      return <>
      <p className="text-xs text-muted">Quan sát tồn kho: {metrics.inventory_observed_at ? new Date(metrics.inventory_observed_at).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", hour12: false }) : "Chưa có quan sát"} · UTC+7</p>
      <div className={kpiGrid}>
        <MetricCard detail="Lượng tồn nhân giá vốn, không phải giá bán." emphasis label="Giá trị tồn kho" scope={current} value={formatMetricVnd(metrics.total_inventory_value_vnd)} />
        <MetricCard detail="Tổng on_hand tại kho trung tâm." label="Lượng tại kho" scope={current} value={number(metrics.warehouse_stock_units)} />
        <MetricCard detail="Tổng on_hand tại các cửa hàng." label="Lượng tại cửa hàng" scope={current} value={number(metrics.store_stock_units)} />
        <MetricCard detail="SKU duy nhất có ít nhất một địa điểm hết hàng, không phải số dòng tồn." label="SKU có điểm hết hàng" scope={current} value={number(metrics.stockout_count)} warning={metrics.stockout_count != null && metrics.stockout_count > 0} />
      </div>
      <Panel title="Hàng đang nằm ở đâu?" subtitle="Cùng đơn vị số lượng hàng. Đây là cơ cấu lượng tồn, không phải tỷ trọng SKU.">
        {metrics.warehouse_stock_units != null || metrics.store_stock_units != null ? <Chart height={200} label="So sánh tổng lượng tồn tại kho và cửa hàng, đơn vị hàng." option={barOption([{ name: "Kho trung tâm", value: metrics.warehouse_stock_units }, { name: "Cửa hàng", value: metrics.store_stock_units }].filter((row): row is { name: string; value: number } => row.value != null), "Lượng tồn")} /> : <EmptyState detail="Chưa có quan sát lượng tồn. Dấu — là thiếu dữ liệu; số 0 chỉ hiển thị khi đã có dòng tồn hợp lệ." />}
        <p className="mt-3 text-sm text-muted">Phiếu nhập được ghi nhận: <strong className="text-ink">{number(metrics.inbound_batches_count)}</strong> · phạm vi dữ liệu nguồn hiện có.</p>
      </Panel>
      <Panel title="SKU nào tại kho đã hết hàng?" subtitle="Kho trung tâm, on_hand = 0; tối đa 10 dòng. KPI phía trên đếm SKU duy nhất trên cả kho và cửa hàng.">
        <DetailTable caption="Cảnh báo hết hàng" filename="canh-bao-het-hang" rows={metrics.critical_stockout_alerts ?? []} rowKey={row => row.variant_id} columns={[
          { key: "sku", label: "SKU", value: row => row.sku }, { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "stock", label: "Tồn (đơn vị)", value: row => row.on_hand, numeric: true },
        ]} />
      </Panel>
      <Panel title="SKU nào tại kho còn ít hàng?" subtitle="Kho trung tâm, 0 < on_hand ≤ 5; tối đa 10 dòng. Danh sách này không trùng nhóm đã hết hàng.">
        <DetailTable caption="Tồn thấp tại kho" filename="ton-thap-tai-kho" rows={metrics.low_stock_alerts} rowKey={row => row.variant_id} columns={[
          { key: "sku", label: "SKU", value: row => row.sku }, { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "stock", label: "Tồn (đơn vị)", value: row => row.on_hand, numeric: true },
        ]} />
      </Panel>
      <Panel title="Lượng tồn thay đổi giữa các ngày quan sát thế nào?" subtitle="30 ngày gần nhất từ mart lịch sử; mỗi điểm là một ngày riêng, không cộng các ngày thành tồn hiện tại.">
        {metrics.inventory_history.length ? <Chart label="Lượng tồn kho và cửa hàng theo ngày quan sát, cùng đơn vị hàng." option={{
          tooltip: { trigger: "axis", renderMode: "richText", confine: true },
          legend: { top: 0, data: ["Kho", "Cửa hàng"] }, grid: { left: 12, right: 12, bottom: 38, top: 48, containLabel: true },
          xAxis: { type: "category", data: historyDates },
          yAxis: { type: "value", name: "Đơn vị hàng" },
          series: [
            { name: "Kho", type: "line", connectNulls: false, data: historyDates.map(date => historyByDate.get(date)?.warehouse_stock_units ?? null) },
            { name: "Cửa hàng", type: "line", connectNulls: false, data: historyDates.map(date => historyByDate.get(date)?.store_stock_units ?? null) },
          ],
        }} /> : <EmptyState detail="Chưa có snapshot tồn kho trong kỳ. Không tự nội suy hoặc điền số 0." />}
      </Panel>
      <NextSteps userRole={userRole} items={[
        { title: "Đối chiếu tồn trước khi điều chuyển", detail: "Kiểm tra địa điểm và tồn thực tế của SKU trong danh sách; dashboard không tự tạo lệnh điều chuyển.", href: "/admin/products", linkLabel: "Xem hàng hóa" },
        { title: "Kiểm tra phiếu nhập", detail: "Xem các phiếu và số lượng nhập liên quan trước khi quyết định bổ sung.", href: "/admin/inbound", linkLabel: "Xem phiếu nhập" },
      ]} />
    </>;
    }
    case "operations": {
      const regions = [...metrics.cod_regions].sort((a, b) => (b.failure_rate_percent ?? -1) - (a.failure_rate_percent ?? -1));
      const fulfillment = metrics.fulfillment;
      return <>
        <p className="text-xs text-muted">Mốc đo vận hành: {metrics.operations_observed_at ? new Date(metrics.operations_observed_at).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", hour12: false }) : "Chưa có mốc đo"} · UTC+7. Thời gian chờ giữ theo mốc này, không tự tăng khi tải lại trang.</p>
        <div className={kpiGrid}>
          <MetricCard detail="Đơn paid/confirmed theo lần quan sát; chưa phải toàn bộ đơn đang giao." emphasis label="Đơn chờ xuất" scope={current} value={number(metrics.pending_fulfillment_count)} />
          <MetricCard detail="Shipment có attempt_count > 1 hoặc failed; không phải đo vi phạm SLA." label="Giao vận cần kiểm tra" scope={allTime} value={number(metrics.problematic_shipments_count)} warning={metrics.problematic_shipments_count != null && metrics.problematic_shipments_count > 0} />
          <MetricCard detail="Số shipment failed, không phải số đơn hoặc riêng COD." label="Shipment thất bại" scope={allTime} value={number(metrics.failed_shipments_count)} warning={metrics.failed_shipments_count != null && metrics.failed_shipments_count > 0} />
          <MetricCard detail="Yêu cầu duy nhất không cancelled; không đếm theo dòng hàng hoặc cộng qua SKU." label="Yêu cầu đổi trả" scope={allTime} value={number(metrics.return_requests_count)} />
        </div>
        <Panel title="Có đơn chờ quá 2 giờ tại mốc đo không?" subtitle="Ngưỡng rà soát theo thời điểm công bố mart, không phải SLA đã cam kết.">
          {fulfillment ? <div className="grid gap-5 sm:grid-cols-3">
            <div><p className="text-sm text-muted">Đơn paid/confirmed</p><p className="mt-2 text-2xl font-semibold">{number(fulfillment.pending_orders)}</p></div>
            <div><p className="text-sm text-muted">Chờ từ 2 giờ</p><p className={`mt-2 text-2xl font-semibold ${fulfillment.over_two_hours_orders > 0 ? "text-danger" : ""}`}>{number(fulfillment.over_two_hours_orders)}</p></div>
            <div><p className="text-sm text-muted">Thời gian chờ trung bình</p><p className="mt-2 text-2xl font-semibold">{fulfillment.average_waiting_hours == null ? "—" : `${number(fulfillment.average_waiting_hours)} giờ`}</p></div>
          </div> : <EmptyState detail="Chưa có dữ liệu đơn tại mốc đo; không suy ra số đơn chờ bằng 0." />}
        </Panel>
        <Panel title="Giao vận theo nhân sự và địa bàn được phân công" subtitle="Cùng bộ dữ liệu đã công bố. Thiếu thời gian giao dự kiến thì tỷ lệ đúng hạn để trống; thời gian vận chuyển tính theo số mẫu hợp lệ.">
          <DetailTable caption="Hiệu suất giao vận" filename="hieu-suat-giao-van" rows={metrics.logistics} rowKey={row => `${row.staff_id}-${row.city_id}`} columns={[
            { key: "staff", label: "Mã nhân sự", value: row => row.staff_code }, { key: "city", label: "Địa bàn phân công", value: row => row.city_name },
            { key: "total", label: "Shipment", value: row => row.total_shipments, numeric: true }, { key: "delivered", label: "Đã giao", value: row => row.delivered_count, numeric: true },
            { key: "failed", label: "Thất bại", value: row => row.failed_count, numeric: true }, { key: "eligible", label: "Mẫu đo đúng hạn", value: row => row.on_time_eligible_count, numeric: true },
            { key: "ontime", label: "Đúng hạn (%)", value: row => row.on_time_rate_percent, numeric: true }, { key: "duration", label: "Vận chuyển TB (phút)", value: row => row.average_transit_minutes, numeric: true },
            { key: "cod", label: "COD ghi nhận đã thu (VND)", value: row => row.cod_collected_vnd, numeric: true },
          ]} />
        </Panel>
        <Panel title="Vùng COD nào cần kiểm tra?" subtitle="Chỉ shipment của đơn COD; nhóm vùng từ địa chỉ đơn, khác địa bàn phân công nhân sự. Đọc tỷ lệ cùng số mẫu.">
          {regions.length ? <Chart label="Tỷ lệ shipment COD thất bại theo vùng, phần trăm." option={barOption(regions.filter(row => row.failure_rate_percent != null).slice(0, 10).map(row => ({ name: row.region, value: row.failure_rate_percent! })), "COD thất bại", value => `${number(value)}%`, "#a94728")} /> : <EmptyState detail="Chưa có nhóm shipment COD được công bố." />}
          <DetailTable caption="COD theo vùng và số mẫu" filename="cod-theo-vung" rows={regions} rowKey={row => row.region} columns={[
            { key: "region", label: "Vùng", value: row => row.region }, { key: "failed", label: "COD thất bại", value: row => row.failed_cod_shipments, numeric: true },
            { key: "total", label: "Shipment COD", value: row => row.total_cod_shipments, numeric: true }, { key: "rate", label: "Tỷ lệ (%)", value: row => row.failure_rate_percent, numeric: true },
          ]} />
        </Panel>
        <Panel title="SKU nào có yêu cầu đổi trả?" subtitle="Top 10 theo lượng đề nghị trả, bỏ yêu cầu cancelled. Một yêu cầu có nhiều SKU xuất hiện ở nhiều dòng; không cộng cột số yêu cầu thành tổng toàn hệ thống.">
          <DetailTable caption="Yêu cầu đổi trả theo SKU" filename="doi-tra-theo-sku" rows={metrics.product_returns} rowKey={row => row.variant_id} columns={[
            { key: "sku", label: "SKU", value: row => row.sku }, { key: "product", label: "Sản phẩm", value: row => row.product_name },
            { key: "requests", label: "Yêu cầu có SKU", value: row => row.request_count, numeric: true }, { key: "lines", label: "Dòng hàng", value: row => row.line_count, numeric: true },
            { key: "requested", label: "Lượng đề nghị trả", value: row => row.requested_units, numeric: true }, { key: "accepted", label: "Hoàn tất, kiểm định đạt", value: row => row.accepted_units, numeric: true },
            { key: "money", label: "Tiền đề nghị hoàn (VND)", value: row => row.requested_refund_vnd, numeric: true },
          ]} />
        </Panel>
        <Panel title="Tiền hoàn thành công được ghi nhận bao nhiêu?" subtitle="Tổng refund succeeded, có thể gồm vận chuyển hoặc đơn hủy. Không phân bổ khoản này xuống SKU khi chưa có liên kết phù hợp.">
          <div className="grid gap-4 sm:grid-cols-2"><div><p className="text-sm text-muted">Số refund thành công</p><p className="mt-2 text-xl font-semibold">{number(metrics.successful_refunds_count)}</p></div><div><p className="text-sm text-muted">Tổng tiền đã ghi nhận hoàn</p><p className="mt-2 text-xl font-semibold">{formatVnd(metrics.successful_refund_amount_vnd)}</p></div></div>
        </Panel>
        <NextSteps userRole={userRole} items={[
          { title: "Rà đơn chờ tại mốc đo", detail: "Kiểm tra thời điểm quan sát trước khi dùng số đơn chờ. Refresh giao diện không tự tạo snapshot mới.", href: "/admin/orders", linkLabel: "Xem đơn" },
          { title: "Đối chiếu giao vận và đổi trả", detail: "Phân biệt shipment, yêu cầu, dòng hàng và refund; không cộng các nhóm này thành tổng đơn.", href: "/admin/logistics", linkLabel: "Xem giao vận" },
        ]} />
      </>;
    }
    case "marketing": {
      const window = metrics.funnel_window_start && metrics.funnel_window_end ? `${metrics.funnel_window_start} → ${metrics.funnel_window_end} · UTC` : "Chưa có kỳ log đã công bố";
      const stamp = (value: string | null) => value ? new Date(value).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", hour12: false }) : "—";
      return <>
        <Panel title="Nguồn dữ liệu Marketing" subtitle="Log và nghiệp vụ có publication riêng. Tải lại giao diện không tự chạy rollup hoặc làm cửa sổ đo tiến lên.">
          <div className="grid gap-4 sm:grid-cols-2">{[
            { label: "Log tương tác · rollup mỗi giờ", state: metrics.log_status },
            { label: "Voucher / đánh giá · publication OLTP", state: metrics.commerce_status },
          ].map(({ label, state }) => <div className="min-w-0 text-xs leading-6 text-muted" key={label}>
            <p className="text-sm font-semibold text-ink">{label}</p>
            <p>{!state.available ? "Chưa đọc được dữ liệu đã công bố" : state.served_from_cache ? `Đang dùng bản lưu · ${number(state.cache_age_seconds)} giây` : "Đã đọc mart công bố"}</p>
            <p>Mốc đo: {stamp(state.observed_at)} · UTC+7</p><p>Công bố: {stamp(state.data_updated_at)} · UTC+7</p>
          </div>)}</div>
        </Panel>
        <div className="grid gap-4 sm:grid-cols-3">
          <MetricCard detail="Yêu cầu xem/tìm sản phẩm thành công; không phải số người duy nhất." emphasis label="Sự kiện đầu phễu" scope={window} value={number(metrics.view_events_count)} />
          <MetricCard detail="Số request checkout_submit thành công; không cộng thêm xác nhận hoặc hoàn tất đơn." label="Checkout gửi thành công" scope={window} value={number(metrics.checkout_success_events_count)} />
          <MetricCard detail="Tổng sự kiện checkout thành công / tổng sự kiện đầu phễu. Không có mẫu số thì để trống." label="Checkout / đầu phễu" scope={window} value={percent(metrics.checkout_to_view_rate_percent)} />
        </div>
        <Panel title="Mỗi bước tương tác ghi nhận bao nhiêu sự kiện?" subtitle="Chỉ request thành công. Đặt lượng giỏ không đồng nghĩa thêm một sản phẩm mới; các bước không tạo thành nhóm người hoặc session liên kết.">
          {metrics.funnel_steps.length ? <Chart label="Số sự kiện tương tác theo bước nghiệp vụ." option={barOption(metrics.funnel_steps.map(row => ({ name: row.step_name, value: row.count })), "Sự kiện")} /> : <EmptyState detail="Chưa có quan sát log trong kỳ đã công bố; không suy ra các bước bằng 0." />}
          <DetailTable caption="Số lượng theo bước tương tác" filename="cac-buoc-tuong-tac" rows={metrics.funnel_steps} rowKey={row => row.step_name} columns={[
            { key: "step", label: "Bước", value: row => row.step_name }, { key: "count", label: "Lượt request", value: row => row.count, numeric: true },
            { key: "ratio", label: "So với đầu phễu (%)", value: row => row.conversion_rate_percent, numeric: true },
          ]} />
        </Panel>
        <Panel title="Sản phẩm nào được quan tâm?" subtitle="Top 10 theo lượt xem chi tiết trong cùng kỳ log. Khóa slug/UUID được đối chiếu danh mục khi build; khóa chưa ánh xạ vẫn được giữ. Không cộng actor distinct theo ngày thành số người trong kỳ.">
          <DetailTable caption="Tương tác theo sản phẩm" filename="nhu-cau-san-pham" rows={metrics.product_demand} rowKey={row => row.product_key} columns={[
            { key: "product", label: "Sản phẩm / khóa chưa ánh xạ", value: row => row.product_name ?? row.product_key },
            { key: "views", label: "Xem chi tiết", value: row => row.detail_views, numeric: true },
            { key: "cart", label: "Đặt lượng giỏ", value: row => row.cart_sets, numeric: true },
            { key: "wish", label: "Thêm yêu thích", value: row => row.wishlist_adds, numeric: true },
            { key: "rate", label: "Giỏ / xem (%)", value: row => row.cart_to_view_rate_percent, numeric: true },
          ]} />
        </Panel>
        <Panel title="Voucher được sử dụng thế nào tại mốc đo?" subtitle="Tối đa 10 mã active, chưa lưu trữ và trong thời hạn tại mốc đo. Lượt gần đây còn trạng thái redeemed, loại lượt đã released; cửa sổ 15 phút giữ theo mart.">
          {!metrics.commerce_status.available || metrics.coupon_source_rows === 0 ? <EmptyState detail="Chưa có quan sát voucher đã công bố." /> : <DetailTable caption="Sử dụng voucher" filename="su-dung-voucher" rows={metrics.coupons} rowKey={row => row.coupon_id} columns={[
            { key: "code", label: "Mã", value: row => row.coupon_code }, { key: "used", label: "Lượt dùng", value: row => row.used_count, numeric: true },
            { key: "limit", label: "Hạn mức", value: row => row.usage_limit, render: row => row.usage_limit == null ? "Không giới hạn" : number(row.usage_limit), numeric: true },
            { key: "percent", label: "Sử dụng (%)", value: row => row.usage_percent, numeric: true },
            { key: "recent", label: "Lượt / 15 phút", value: row => row.recent_redemptions_count, numeric: true },
            { key: "pace", label: "Lượt / phút", value: row => row.recent_redemptions_per_minute, numeric: true },
          ]} />}
        </Panel>
        <Panel title="Sản phẩm nào có đánh giá cần xem lại?" subtitle="Tối đa 5 sản phẩm có đánh giá đã duyệt 1–2 sao trong 45 phút tại mốc đo nghiệp vụ; không suy luận nguyên nhân từ số lượng.">
          {!metrics.commerce_status.available || metrics.review_source_rows === 0 ? <EmptyState detail="Chưa có quan sát đánh giá đã công bố." /> : <DetailTable caption="Đánh giá tiêu cực đã duyệt" filename="danh-gia-can-kiem-tra" rows={metrics.negative_reviews} rowKey={row => row.product_id} columns={[
            { key: "product", label: "Sản phẩm", value: row => row.product_name }, { key: "count", label: "Đánh giá 1–2 sao", value: row => row.negative_count, numeric: true },
          ]} />}
        </Panel>
        <NextSteps userRole={userRole} items={[
          { title: "Rà điều kiện sử dụng voucher", detail: "Đối chiếu hạn mức và mốc đo trước khi điều chỉnh mã.", href: "/admin/coupons", linkLabel: "Xem voucher" },
          { title: "Đọc phản hồi trước khi kết luận", detail: "Xem nội dung đánh giá và giao dịch liên quan.", href: "/admin/reviews", linkLabel: "Xem đánh giá" },
        ]} />
      </>;
    }
    case "system": {
      const comparable = metrics.data_source === "lakehouse" && !metrics.served_from_cache && metrics.reconciliation_variance.length > 0;
      const variances = metrics.reconciliation_variance;
      const maximum = comparable ? Math.max(...variances.map(row => row.variance_percent)) : null;
      const status = { healthy: "Trong ngưỡng UI", degraded: "Cần kiểm tra", unhealthy: "Bất thường" }[metrics.pipeline_status] ?? "Chưa xác định";
      const label = (row: ReconciliationVariance) => row.metric_name === "total_orders" ? "Tổng đơn" : (row.metric_name === "gross_revenue_vnd" || row.metric_name === "order_total_vnd") ? "Tổng giá trị đơn (VND)" : row.metric_name;
      return <>
        <div className="grid gap-4 sm:grid-cols-3">
          <MetricCard detail="Theo heartbeat CDC, độ trễ áp dụng và variance của API; không phải kiểm tra trực tiếp từng job." emphasis label="Tình trạng dữ liệu phân tích" scope="Phản hồi hiện tại" value={status} warning={metrics.pipeline_status !== "healthy"} />
          <MetricCard detail={`CDC: ${metrics.cdc_apply_status ?? "chưa có heartbeat"}; ${metrics.cdc_pending_events} sự kiện đang chờ. Không có thay đổi mới thì không tạo lần công bố mới.`} label="Tuổi lần công bố" scope="Từ lần công bố OLAP gần nhất" value={metrics.data_freshness_minutes == null ? "Chưa xác định" : `${number(metrics.data_freshness_minutes)} phút`} warning={metrics.cdc_apply_status === "failed" || (metrics.cdc_apply_status !== "not_applicable" && !metrics.cdc_heartbeat_at)} />
          <MetricCard detail={comparable ? "Trong các chỉ số đối soát API trả về. Không so đơn và VND trên cùng trục giá trị." : "Không có kết quả đối soát mới. Phản hồi đang lấy từ cache OLAP hoặc thiếu nguồn."} label="Sai lệch lớn nhất" scope="MySQL hiện tại ↔ Gold" value={percent(maximum)} warning={maximum != null && maximum > 0} />
        </div>
        <Panel title="Đối soát dữ liệu OLTP và Lakehouse" subtitle="Tổng đơn và giá trị tiền có thang đo riêng; mỗi biểu đồ so sánh cùng một chỉ số giữa MySQL và Gold.">
          {comparable ? <>
            <p className={`mb-4 rounded-xl border p-3 text-sm leading-6 ${maximum === 0 ? "border-success/25 bg-success/5 text-success" : "border-warning/25 bg-warning/5 text-warning"}`}>{maximum === 0 ? "Các chỉ số được trả về đang có sai lệch 0%. Kết quả này không chứng minh toàn bộ bảng hoặc pipeline đều chính xác." : "Có chênh lệch trong các chỉ số được trả về. Kiểm tra kỳ dữ liệu, lần chạy batch và bản ghi gốc trước khi xác định nguyên nhân."}</p>
            <div className="mb-5 grid gap-4 xl:grid-cols-2">
              {variances.map(row => {
                const monetary = row.metric_name.endsWith("_vnd");
                const unit = monetary ? "VND" : row.metric_name === "total_orders" ? "Đơn" : "Giá trị";
                return <section className="min-w-0 rounded-xl border border-line p-3 sm:p-4" key={row.metric_name}>
                  <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
                    <h3 className="text-sm font-semibold text-ink">{label(row)}</h3>
                    <p className={`text-xs tabular-nums ${row.variance_percent === 0 ? "text-success" : "text-warning"}`}>Sai lệch: {percent(row.variance_percent)}</p>
                  </div>
                  <Chart height={240} label={`${label(row)}: so sánh MySQL và Gold, đơn vị ${unit}.`} option={reconciliationOption(row, unit, monetary ? formatMetricVnd : number, monetary ? formatVnd : number)} />
                </section>;
              })}
            </div>
          </> : <EmptyState title="Chưa đủ dữ liệu để đối soát" detail="Chưa có cặp số liệu OLAP mới để so sánh. Khi API phục vụ cache, phản hồi không đại diện cho lần đối soát hiện tại." />}
          <DetailTable caption="Đối soát MySQL và Gold" filename="doi-soat-nguon-gold" rows={variances} rowKey={row => row.metric_name} columns={[
            { key: "metric", label: "Chỉ số / đơn vị", value: label },
            { key: "oltp", label: "MySQL", value: row => row.oltp_value, numeric: true },
            { key: "gold", label: "Gold", value: row => comparable ? row.lakehouse_value : null, numeric: true },
            { key: "variance", label: "Sai lệch (%)", value: row => comparable ? row.variance_percent : null, numeric: true },
          ]} />
        </Panel>
        <NextSteps userRole={userRole} items={[
          { title: "Kiểm tra publication OLAP gần nhất", detail: "Kiểm tra spark-cdc, heartbeat, số sự kiện đang chờ và run_id của publication. DAG hằng ngày đối soát dữ liệu; làm mới trang chỉ tải lại kết quả truy vấn." },
          { title: "Đọc kết quả quality gate", detail: "Tra lakehouse.system.data_quality_results theo run_id. Ngưỡng đối soát pipeline mặc định 0,05% khác ngưỡng cảnh báo UI; kiểm tra bảng và snapshot liên quan." },
          { title: "Đọc độ trễ áp dụng CDC", detail: "Đối chiếu event tại Bronze với publication và snapshot Gold. Checkpoint Flink 10 giây cùng poll 30 giây không tạo cam kết latency đầu-cuối." },
        ]} />
      </>;
    }
  }
}
