"use client";

import { useEffect, useRef, useState } from "react";
import { getAdminHttpMetrics, type HttpMetricsResponse, type HttpRouteMetric, type HttpLatencyGroup } from "@/lib/api";
import { Chart, DetailTable, EmptyState, MetricCard, Panel, PublicationStatus, number, percent, type Column } from "./AnalyticsUI";

const stamp = (value: string) => new Date(value).toLocaleString("vi-VN", { timeZone: "UTC", hour12: false });
const hour = (row: { metric_date: string; metric_hour: number }) => `${row.metric_date}T${String(row.metric_hour).padStart(2, "0")}:00:00Z`;
const routeColumns: Column<HttpRouteMetric>[] = [
  { key: "route", label: "Route", value: row => row.http_route },
  { key: "method", label: "Method", value: row => row.http_request_method },
  { key: "requests", label: "Request", value: row => row.total_requests, numeric: true },
  { key: "success", label: "2xx/3xx", value: row => row.success_count, numeric: true },
  { key: "client", label: "4xx", value: row => row.client_error_count, numeric: true },
  { key: "server", label: "5xx", value: row => row.server_error_count, numeric: true },
  { key: "rate", label: "Lỗi (%)", value: row => row.error_rate_percent, render: row => percent(row.error_rate_percent), numeric: true },
  { key: "duration", label: "Trung bình (ms)", value: row => row.avg_duration_ms, numeric: true },
];
const latencyColumns: Column<HttpLatencyGroup>[] = [
  { key: "hour", label: "Giờ (UTC)", value: row => `${row.metric_date} ${String(row.metric_hour).padStart(2, "0")}:00` },
  { key: "route", label: "Route", value: row => row.http_route },
  { key: "method", label: "Method", value: row => row.http_request_method },
  { key: "requests", label: "Request", value: row => row.total_requests, numeric: true },
  { key: "p50", label: "p50 (ms)", value: row => row.p50_duration_ms, numeric: true },
  { key: "p95", label: "p95 (ms)", value: row => row.p95_duration_ms, numeric: true },
  { key: "p99", label: "p99 (ms)", value: row => row.p99_duration_ms, numeric: true },
];

export function HttpMetricsPanel({ refreshVersion }: { refreshVersion: number }) {
  const [days, setDays] = useState(7);
  const [data, setData] = useState<{ days: number; response: HttpMetricsResponse } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retry, setRetry] = useState(0);
  const requests = useRef(new Map<number, Promise<HttpMetricsResponse>>());

  useEffect(() => {
    let current = true;
    setLoading(true);
    setError(null);
    let promise = requests.current.get(days);
    if (!promise) {
      promise = getAdminHttpMetrics(days);
      requests.current.set(days, promise);
      const pending = promise;
      void pending.finally(() => { if (requests.current.get(days) === pending) requests.current.delete(days); }).catch(() => {});
    }
    promise.then(response => {
      if (current) setData({ days, response });
    }).catch((cause: unknown) => {
      if (current) setError(cause instanceof Error ? cause.message : "Không tải được báo cáo HTTP.");
    }).finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [days, refreshVersion, retry]);

  const response = data?.days === days ? data.response : null;
  const points = response ? [...response.hours].sort((a, b) => hour(a).localeCompare(hour(b))) : [];
  const byHour = new Map(points.map(point => [hour(point), point]));
  const hours: string[] = [];
  if (response && points.length) {
    const end = Date.parse(response.observed_at);
    for (let time = Date.parse(response.window_start); time <= end; time += 3_600_000) hours.push(new Date(time).toISOString().replace(".000Z", "Z"));
  }
  const key = (row: { http_route: string | null; http_request_method: string | null }) => JSON.stringify([row.http_route, row.http_request_method]);

  return <div data-mart="mart_hourly_route_metrics"><Panel id="analytics-http" title="HTTP Metrics" subtitle={`${days} ngày UTC · Request đã ghi nhận`}
    action={<fieldset className="flex rounded-xl border border-line bg-paper p-1"><legend className="sr-only">Kỳ báo cáo HTTP</legend>{[1, 7, 30].map(value => <button aria-pressed={days === value} className={`min-h-9 rounded-lg px-3 text-xs font-semibold ${days === value ? "bg-ink text-paper" : "text-muted hover:bg-sand"}`} key={value} onClick={() => setDays(value)} type="button">{value} ngày UTC</button>)}</fieldset>}>
    {loading && !response && <div aria-label="Đang tải HTTP Metrics" className="h-64 animate-pulse rounded-xl bg-sand/50" role="status" />}
    {error && <div className="feedback-error" role="alert"><p>Không tải được HTTP Metrics: {error}{response ? " · Đang giữ kết quả trước đó." : ""}</p><button className="mt-2 min-h-8 underline" onClick={() => setRetry(value => value + 1)} type="button">Thử lại HTTP Metrics</button></div>}
    {response && <div className="space-y-5">
      <PublicationStatus data={response} observedAt={response.observed_at} label="HTTP" />
      <p className="text-xs text-muted">{stamp(response.window_start)} → {stamp(response.observed_at)} · UTC</p>
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Tổng request HTTP" value={number(response.totals.total_requests)} detail="Sự kiện request đã ghi nhận; không phải người dùng duy nhất hay giao dịch." scope="Request · Kỳ đã chọn" />
        <MetricCard label="Lỗi phía client · 4xx" value={number(response.totals.client_error_count)} detail="Số phản hồi 4xx trong log." scope="Kỳ đã chọn" />
        <MetricCard label="Tỷ lệ lỗi HTTP" value={percent(response.totals.error_rate_percent)} detail={`(4xx + 5xx) / request. Lỗi 5xx: ${number(response.totals.server_error_count)}.`} scope={`5xx: ${number(response.totals.server_error_count)}`} />
        <MetricCard label="Độ trễ trung bình" value={response.totals.avg_duration_ms == null ? "—" : `${number(response.totals.avg_duration_ms)} ms`} detail="Trung bình nhóm mart có trọng số request; giá trị nhóm đã làm tròn." scope="Kỳ đã chọn" />
      </div>
      {!points.length ? <EmptyState detail="Chưa có quan sát HTTP trong kỳ. Không tự điền số request hoặc độ trễ bằng 0." /> : <>
        <div className="grid gap-5 xl:grid-cols-2">{[false, true].map(errors => <div className="min-w-0" key={String(errors)}>
          <h3 className="mb-3 text-sm font-semibold">{errors ? "Phản hồi lỗi theo giờ" : "Request theo giờ"}</h3>
          <Chart height={270} label={errors ? "Số phản hồi 4xx và 5xx theo giờ UTC" : "Số request theo giờ UTC"} option={{
            color: errors ? ["#8b5b16", "#a94728"] : ["#315b4f"],
            tooltip: { trigger: "axis", renderMode: "richText", confine: true },
            legend: { top: 0, textStyle: { color: "#5d6965", fontSize: 11 } },
            grid: { left: 4, right: 16, top: 40, bottom: 44, containLabel: true },
            xAxis: { type: "category", boundaryGap: false, data: hours.map(value => `${value.slice(5, 10)} ${value.slice(11, 16)}`), axisLabel: { color: "#5d6965", hideOverlap: true }, axisLine: { lineStyle: { color: "#dcd8cf" } } },
            yAxis: { type: "value", name: "Request", minInterval: 1, splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } },
            series: (errors ? [{ name: "4xx", field: "client_error_count" as const }, { name: "5xx", field: "server_error_count" as const }] : [{ name: "Request", field: "total_requests" as const }]).map(item => ({ name: item.name, type: "line", lineStyle: { type: item.field === "server_error_count" ? "dashed" : "solid" }, showSymbol: points.length < 25, connectNulls: false, data: hours.map(value => byHour.get(value)?.[item.field] ?? null) })),
          }} />
        </div>)}</div>
        <div>
          <h3 className="text-sm font-semibold">Route theo số lỗi · Top 20</h3>
          <DetailTable caption="HTTP theo route và method" filename={`http-routes-${days}-ngay`} rows={response.routes} columns={routeColumns} rowKey={key} />
        </div>
        <div><h3 className="text-sm font-semibold">p95 theo giờ × route × method · Top 20</h3>
          <DetailTable caption="Percentile HTTP từng nhóm giờ" filename={`http-percentiles-${days}-ngay`} rows={response.latency_groups} columns={latencyColumns} rowKey={row => JSON.stringify([row.metric_date, row.metric_hour, row.http_route, row.http_request_method])} />
        </div>
      </>}
    </div>}
  </Panel></div>;
}
