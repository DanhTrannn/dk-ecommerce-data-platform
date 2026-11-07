"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { AUTO_REFRESH_INTERVAL_MS } from "@/lib/dashboard-refresh";
import { Chart, MetricCard, Panel, PublicationStatus, number } from "@/components/analytics/AnalyticsUI";
import { barOption } from "@/components/analytics/chart-options";
import { apiFetch } from "@/lib/api-client";
import { formatMetricVnd, formatVnd } from "@/lib/api";

import type { AnalyticsDataStatus } from "@/lib/commerce";

interface DashboardData extends AnalyticsDataStatus {
  store_name: string;
  today_orders: number | null;
  today_revenue_vnd: number | null;
  total_orders: number;
  completed_orders: number;
  total_revenue_vnd: number;
  low_stock_items: number | null;
  active_staff: number;
  inventory_observed_at: string | null;
}

export default function StoreDashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const inFlight = useRef(false);
  const loadData = useCallback(() => {
    if (inFlight.current) return;
    inFlight.current = true;
    setRefreshing(true);
    apiFetch<DashboardData>("/api/v1/admin/store/dashboard")
      .then((res) => {
        setData(res);
        setError(null);
      })
      .catch((err) => setError(err instanceof Error ? err.message : "Không thể tải dữ liệu."))
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

  if (loading) {
    return (
      <div>
        <div className="h-8 w-48 animate-pulse rounded bg-sand/60" />
        <div className="mt-6 grid gap-4 sm:grid-cols-2 2xl:grid-cols-4">
          {[1, 2, 3, 4].map((n) => (
            <div key={n} className="h-32 animate-pulse rounded-2xl bg-sand/60" />
          ))}
        </div>
      </div>
    );
  }

  if (error && !data) {
    return (
      <section className="feedback-error">
        <h1 className="font-semibold">Không tải được dữ liệu cửa hàng</h1>
        <p className="mt-1">{error}</p><button className="button-secondary mt-3" disabled={refreshing} onClick={loadData} type="button">Thử lại</button>
      </section>
    );
  }

  if (!data) {
    return <div className="admin-panel text-muted">Không thể tải dữ liệu.</div>;
  }

  return <section className="space-y-4">
    <header className="flex flex-wrap items-center justify-between gap-3"><div><h1 className="text-2xl font-semibold">{data.store_name}</h1><p className="mt-1 text-xs text-muted">Hôm nay · Ngày đặt hàng UTC+7</p></div><button className="button-secondary min-h-10 px-4 text-sm" disabled={refreshing} onClick={loadData} type="button">{refreshing ? "Đang tải…" : "Làm mới"}</button></header>
    <PublicationStatus data={data} observedAt={data.inventory_observed_at} />
    {error && <p className="feedback-error" role="alert">Chưa tải được dữ liệu mới; đang giữ kết quả trước đó.</p>}
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" data-mart="mart_order_daily mart_store_overview">
      <MetricCard label="Doanh thu hôm nay" value={formatMetricVnd(data.today_revenue_vnd)} exactValue={formatVnd(data.today_revenue_vnd)} scope="VND · UTC+7" emphasis />
      <MetricCard label="Đơn hôm nay" value={number(data.today_orders)} scope="Mọi trạng thái · UTC+7" />
      <MetricCard label="Dòng tồn thấp" value={number(data.low_stock_items)} scope="Tại cửa hàng · Tồn ≤ 5" />
      <MetricCard label="Nhân sự active" value={number(data.active_staff)} scope="Tại mốc đo" />
    </div>
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="min-w-0" data-mart="mart_store_overview"><Panel title="Đơn hoàn tất và tổng đơn" subtitle="Đơn · Toàn kỳ đã nạp"><Chart height={250} label="Số đơn hoàn tất và tổng đơn tại cửa hàng" option={barOption([{ name: "Hoàn tất", value: data.completed_orders }, { name: "Tổng đơn", value: data.total_orders }], "Đơn", number, "#315b4f", "Đơn")} /></Panel></div>
      <Panel title="Doanh thu đã ghi nhận" subtitle="VND · Toàn kỳ đã nạp"><div className="py-8" data-mart="mart_store_overview"><MetricCard label="Doanh thu thuần" value={formatMetricVnd(data.total_revenue_vnd)} exactValue={formatVnd(data.total_revenue_vnd)} detail="Hàng hóa sau giảm giá và hoàn tiền; không gồm vận chuyển." /></div></Panel>
    </div>
  </section>;
}
