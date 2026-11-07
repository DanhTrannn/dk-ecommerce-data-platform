"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";

import { AUTO_REFRESH_INTERVAL_MS } from "@/lib/dashboard-refresh";
import { Chart, EmptyState, MetricCard, Panel, PublicationStatus, number } from "@/components/analytics/AnalyticsUI";
import { barOption } from "@/components/analytics/chart-options";
import {
  ApiError,
  getAdminOverview,
  type AdminOverview,
  formatMetricVnd,
  formatVnd,
} from "@/lib/api";

const orderStages = [
  { key: "pending_payment_orders", label: "Chờ thanh toán" }, { key: "paid_orders", label: "Chờ xác nhận" },
  { key: "confirmed_orders", label: "Đã xác nhận" }, { key: "shipping_orders", label: "Đang giao" },
  { key: "delivered_orders", label: "Đã giao" }, { key: "completed_orders", label: "Hoàn tất" },
  { key: "boom_orders_count", label: "Giao thất bại" }, { key: "return_orders_count", label: "Đã trả hàng" },
  { key: "cancelled_orders", label: "Đã hủy" },
] as const;

export default function AdminOverviewPage() {
  const [data, setData] = useState<AdminOverview | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const inFlight = useRef(false);
  const loadData = useCallback(() => {
    if (inFlight.current) return;
    inFlight.current = true;
    setRefreshing(true);
    getAdminOverview()
      .then((overviewData) => {
        setData(overviewData);
        setError(null);
      })
      .catch((requestError) => {
        setError(requestError instanceof ApiError ? requestError.message : "Không tải được dữ liệu");
      })
      .finally(() => {
        setLoading(false);
        setRefreshing(false);
        inFlight.current = false;
      });
  }, []);

  useEffect(() => {
    loadData();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") loadData();
    }, AUTO_REFRESH_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [loadData]);

  if (loading && !data) {
    return (
      <div className="space-y-6">
        <div className="h-14 animate-pulse rounded-2xl bg-sand/60" />
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {[0, 1, 2, 3].map((item) => (
            <div className="h-24 animate-pulse rounded-2xl bg-sand/60" key={item} />
          ))}
        </div>
        <div className="grid gap-6 xl:grid-cols-[1.25fr_0.75fr]">
          <div className="h-72 animate-pulse rounded-2xl bg-sand/50" />
          <div className="h-72 animate-pulse rounded-2xl bg-sand/50" />
        </div>
      </div>
    );
  }

  if (error && !data) {
    return (
      <section className="feedback-error">
        <h1 className="font-semibold text-lg">Không tải được dữ liệu</h1>
        <p className="mt-1 text-sm">{error}</p>
        <button
          className="button-secondary mt-4 text-xs font-semibold"
          onClick={loadData}
          type="button"
        >
          Thử lại
        </button>
      </section>
    );
  }

  if (!data) return null;

  const states = orderStages.map(stage => ({ name: stage.label, value: data[stage.key] })).filter((row): row is typeof row & { value: number } => row.value != null);
  const finance = [{ name: "Doanh thu thuần", value: data.net_revenue_vnd }, { name: "Giá vốn", value: data.cogs_vnd }, { name: "Lợi nhuận gộp", value: data.gross_profit_vnd }].filter((row): row is typeof row & { value: number } => row.value != null);
  return <section className="space-y-4">
    <header className="flex flex-wrap items-center justify-between gap-3"><div><h1 className="text-2xl font-semibold">Tổng quan quản trị</h1><p className="mt-1 text-xs text-muted">Toàn kỳ đã nạp</p></div><div className="flex gap-2"><button className="button-secondary min-h-10 px-4 text-sm" disabled={refreshing} onClick={loadData} type="button">{refreshing ? "Đang tải…" : "Làm mới"}</button><Link className="button-secondary min-h-10 px-4 text-sm" href="/admin/analytics">Phân tích</Link></div></header>
    <PublicationStatus data={data} observedAt={data.admin_observed_at} />
    {error && <p className="feedback-error" role="alert">Chưa tải được số liệu mới; đang giữ kết quả trước đó.</p>}
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" data-mart="mart_admin_overview mart_order_daily">
      <MetricCard label="Doanh thu thuần" value={formatMetricVnd(data.net_revenue_vnd)} exactValue={formatVnd(data.net_revenue_vnd)} scope="VND" emphasis />
      <MetricCard label="Lợi nhuận gộp" value={formatMetricVnd(data.gross_profit_vnd)} exactValue={formatVnd(data.gross_profit_vnd)} scope="VND · Chưa trừ chi phí vận hành" warning={data.gross_profit_vnd != null && data.gross_profit_vnd < 0} />
      <MetricCard label="Tổng đơn" value={number(data.total_orders)} scope="Mọi trạng thái" />
      <MetricCard label="Biến thể tồn thấp" value={number(data.low_stock_variants)} scope="Kho · Tồn ≤ 5" />
    </div>
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="min-w-0" data-mart="mart_admin_overview"><Panel title="Đơn hàng theo trạng thái" subtitle="Đơn · Trạng thái hiện tại">
        {states.length ? <Chart height={340} label="Số đơn theo chín trạng thái" option={barOption(states, "Đơn", number, "#315b4f", "Đơn")} /> : <EmptyState />}
      </Panel></div>
      <div className="min-w-0" data-mart="mart_order_daily"><Panel title="Doanh thu, giá vốn và lợi nhuận" subtitle="VND · Toàn kỳ đã nạp">
        {finance.length ? <Chart height={340} label="Doanh thu thuần, giá vốn, lợi nhuận gộp VND" option={barOption(finance, "Giá trị", formatMetricVnd, "#315b4f", "VND", formatVnd)} /> : <EmptyState />}
      </Panel></div>
    </div>
    <details className="rounded-xl border border-line bg-surface p-4"><summary className="cursor-pointer text-sm font-medium">Chỉ số danh mục và khách hàng</summary><dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-5">{[
      ["Sản phẩm active", data.active_products], ["Biến thể active", data.active_variants], ["Voucher khả dụng", data.active_coupons], ["Đánh giá", data.total_reviews], ["Tài khoản khách hàng", data.customers],
    ].map(([label, value]) => <div key={label}><dt className="text-xs text-muted">{label}</dt><dd className="mt-1 text-lg font-semibold">{number(value as number | null)}</dd></div>)}</dl></details>
  </section>;
}
