"use client";

import { useEffect, useRef, useState } from "react";
import { getAdminSalesTrend, formatMetricVnd, formatVnd, type SalesTrendResponse } from "@/lib/api";
import { Chart, DetailTable, EmptyState, Panel, PublicationStatus, type Column } from "./AnalyticsUI";
import type { DailySalesTrendPoint } from "@/lib/api";

const columns: Column<DailySalesTrendPoint>[] = [
  { key: "date", label: "Ngày", value: row => row.date },
  { key: "revenue", label: "Doanh thu (VND)", value: row => row.revenue_vnd, render: row => formatVnd(row.revenue_vnd), numeric: true },
  { key: "cost", label: "Giá vốn (VND)", value: row => row.cogs_vnd, render: row => formatVnd(row.cogs_vnd), numeric: true },
  { key: "profit", label: "Lợi nhuận gộp (VND)", value: row => row.profit_vnd, render: row => formatVnd(row.profit_vnd), numeric: true },
  { key: "orders", label: "Tổng đơn", value: row => row.orders_count, numeric: true },
];

export function SalesTrendPanel({ refreshVersion }: { refreshVersion: number }) {
  const [days, setDays] = useState(30);
  const [data, setData] = useState<{ days: number; response: SalesTrendResponse; loadedAt: Date } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retry, setRetry] = useState(0);
  const requests = useRef(new Map<number, Promise<SalesTrendResponse>>());

  useEffect(() => {
    let current = true;
    setLoading(true);
    setError(null);
    // Scope the result to the requested window so older responses cannot replace it.
    let promise = requests.current.get(days);
    if (!promise) {
      promise = getAdminSalesTrend(days);
      requests.current.set(days, promise);
      const pending = promise;
      void pending.finally(() => { if (requests.current.get(days) === pending) requests.current.delete(days); }).catch(() => {});
    }
    promise.then(response => {
      if (current) setData({ days, response, loadedAt: new Date() });
    }).catch((cause: unknown) => {
      if (current) setError(cause instanceof Error ? cause.message : "Không tải được xu hướng.");
    }).finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [days, refreshVersion, retry]);

  const response = data?.days === days ? data.response : null;
  const points = response ? [...response.points].sort((a, b) => a.date.localeCompare(b.date)) : [];
  const pointsByDate = new Map(points.map(point => [point.date, point]));
  const dates: string[] = [];
  if (points.length) {
    const last = Date.parse(`${points[points.length - 1].date}T00:00:00Z`);
    for (let timestamp = Date.parse(`${points[0].date}T00:00:00Z`); timestamp <= last; timestamp += 86_400_000) {
      dates.push(new Date(timestamp).toISOString().slice(0, 10));
    }
  }

  return <div data-mart="mart_order_daily"><Panel id="analytics-trend" title="Doanh thu, giá vốn và lợi nhuận" subtitle={`VND · ${days} ngày gần nhất · Ngày đặt hàng UTC+7`}
    action={<fieldset className="flex rounded-xl border border-line bg-paper p-1"><legend className="sr-only">Kỳ của xu hướng</legend>{[7, 30, 90].map(value => <button aria-pressed={days === value} className={`min-h-9 rounded-lg px-3 text-xs font-semibold ${days === value ? "bg-ink text-paper" : "text-muted hover:bg-sand"}`} key={value} onClick={() => setDays(value)} type="button">{value} ngày</button>)}</fieldset>}>
    {loading && !response && <div aria-label="Đang tải xu hướng" className="h-72 animate-pulse rounded-xl bg-sand/50" role="status" />}
    {error && <div className="feedback-error" role="alert"><p>{error}{response ? " · Đang giữ kết quả trước đó." : ""}</p><button className="mt-2 underline" onClick={() => setRetry(value => value + 1)} type="button">Thử tải lại xu hướng</button></div>}
    {response && <>
      <PublicationStatus data={response} />
      {!points.length ? <EmptyState detail="Chưa có điểm dữ liệu trong kỳ được chọn. Không nội suy thành doanh thu bằng 0." /> : <>
        <Chart label="Xu hướng theo ngày, cùng đơn vị VND. Giá trị chi tiết nằm trong bảng bên dưới." height={280} option={{
          color: ["#315b4f", "#8b5b16", "#152722"],
          tooltip: { trigger: "axis", renderMode: "richText", confine: true, valueFormatter: value => value == null || value === "-" ? "Chưa có giá trị" : formatVnd(Number(value)) },
          legend: { top: 0, textStyle: { color: "#5d6965", fontSize: 12 }, itemWidth: 16 },
          grid: { left: 4, right: 16, top: 48, bottom: 38, containLabel: true },
          xAxis: { type: "category", boundaryGap: false, data: dates, axisLabel: { color: "#5d6965", formatter: (value: string) => value.slice(5) }, axisLine: { lineStyle: { color: "#dcd8cf" } } },
          yAxis: { type: "value", name: "VND", axisLabel: { color: "#5d6965", formatter: formatMetricVnd }, splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } },
          series: [
            { name: "Doanh thu", type: "line", showSymbol: points.length < 15, data: dates.map(date => pointsByDate.get(date)?.revenue_vnd ?? null), lineStyle: { width: 2.5 } },
            { name: "Giá vốn", type: "line", showSymbol: false, data: dates.map(date => pointsByDate.get(date)?.cogs_vnd ?? null), lineStyle: { width: 2, type: "dashed" } },
            { name: "Lợi nhuận gộp", type: "line", showSymbol: false, data: dates.map(date => pointsByDate.get(date)?.profit_vnd ?? null), lineStyle: { width: 2, type: "dotted" } },
          ],
        }} />
        <DetailTable caption="Xu hướng theo ngày" columns={columns} filename={`xu-huong-${days}-ngay`} rowKey={row => row.date} rows={points} />
      </>}
    </>}
  </Panel></div>;
}
