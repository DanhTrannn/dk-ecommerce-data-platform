"use client";

import { useEffect, useRef, useState } from "react";
import { getAdminHttpRealtime, type RealtimeHttpResponse, type RealtimeHttpRoute } from "@/lib/api";
import { Chart, DetailTable, EmptyState, MetricCard, Panel, number, percent, type Column } from "./AnalyticsUI";
import { barOption } from "./chart-options";

const REFRESH_MS = 10_000;
const stamp = (value: string | null) => value ? new Date(value).toLocaleString("vi-VN", { timeZone: "UTC", hour12: false }) : "—";
const columns: Column<RealtimeHttpRoute>[] = [
  { key: "route", label: "Route", value: row => row.http_route },
  { key: "method", label: "Method", value: row => row.http_request_method },
  { key: "requests", label: "Request", value: row => row.total_requests, numeric: true },
  { key: "client", label: "4xx", value: row => row.client_error_count, numeric: true },
  { key: "server", label: "5xx", value: row => row.server_error_count, numeric: true },
  { key: "rate", label: "Lỗi (%)", value: row => row.error_rate_percent, render: row => percent(row.error_rate_percent), numeric: true },
  { key: "mean", label: "Trung bình (ms)", value: row => row.avg_duration_ms, numeric: true },
  { key: "p95", label: "p95 xấp xỉ (ms)", value: row => row.p95_duration_ms, numeric: true },
];

export function HttpRealtimePanel({ refreshVersion }: { refreshVersion: number }) {
  const [minutes, setMinutes] = useState(15);
  const [refresh, setRefresh] = useState(0);
  const [data, setData] = useState<{ minutes: number; response: RealtimeHttpResponse } | null>(null);
  const [failure, setFailure] = useState<{ minutes: number; message: string } | null>(null);
  const [loading, setLoading] = useState(false);
  const requests = useRef(new Map<number, Promise<RealtimeHttpResponse>>());

  useEffect(() => {
    let current = true;
    if (document.visibilityState !== "visible") return;
    setLoading(true);
    let promise = requests.current.get(minutes);
    if (!promise) {
      promise = getAdminHttpRealtime(minutes);
      requests.current.set(minutes, promise);
      const pending = promise;
      void pending.finally(() => { if (requests.current.get(minutes) === pending) requests.current.delete(minutes); }).catch(() => {});
    }
    promise.then(response => {
      if (!current) return;
      if (response.minutes !== minutes || response.data_source !== "landing") throw new Error("Phản hồi không khớp cửa sổ Landing được yêu cầu.");
      setData({ minutes, response });
      setFailure(null);
    }).catch((cause: unknown) => {
      if (current) setFailure({ minutes, message: cause instanceof Error ? cause.message : "Không đọc được Landing." });
    }).finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [minutes, refresh, refreshVersion]);

  useEffect(() => {
    const update = () => {
      if (document.visibilityState === "visible" && !requests.current.has(minutes)) setRefresh(value => value + 1);
    };
    const timer = window.setInterval(update, REFRESH_MS);
    document.addEventListener("visibilitychange", update);
    return () => { window.clearInterval(timer); document.removeEventListener("visibilitychange", update); };
  }, [minutes]);

  const response = data?.minutes === minutes ? data.response : null;
  const error = failure?.minutes === minutes ? failure.message : null;
  const byMinute = new Map(response?.series.map(point => [Date.parse(point.minute), point]));
  const times: number[] = [];
  if (response?.series.length) {
    const end = Date.parse(response.observed_at);
    for (let time = Math.floor(Date.parse(response.window_start) / 60_000) * 60_000; time < end; time += 60_000) times.push(time);
  }
  const quality = response?.quality;

  return <Panel id="analytics-http-realtime" title="HTTP Realtime" subtitle="Landing · Số liệu sơ bộ · Làm mới 10 giây"
    action={<fieldset className="flex rounded-xl border border-line bg-paper p-1"><legend className="sr-only">Cửa sổ HTTP realtime</legend>{[5, 15, 60].map(value => <button aria-pressed={minutes === value} className={`min-h-9 rounded-lg px-3 text-xs font-semibold ${minutes === value ? "bg-ink text-paper" : "text-muted hover:bg-sand"}`} key={value} onClick={() => setMinutes(value)} type="button">{value} phút</button>)}</fieldset>}>
    {loading && !response && <div aria-label="Đang tải HTTP realtime" className="h-64 animate-pulse rounded-xl bg-sand/50" role="status" />}
    {error && <div className="feedback-error" role="alert">{error}{response ? " Đang giữ kết quả và mốc đo trước đó." : ""} <button className="min-h-9 underline" onClick={() => setRefresh(value => value + 1)} type="button">Thử lại realtime</button></div>}
    {response && <div aria-busy={loading} className="space-y-5" data-snapshot-id={response.snapshot_id ?? undefined} data-observed-at={response.observed_at}>
      <p className="text-xs text-muted" role="status">{stamp(response.window_start)} → {stamp(response.observed_at)} · UTC<br />Nhận log gần nhất trong phạm vi đọc: {stamp(response.latest_ingest_at)}</p>
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Request HTTP" value={number(response.totals.total_requests)} scope={`${minutes} phút · Theo event time`} detail="Event ID duy nhất sau loại xung đột trong phạm vi đọc; không phải số người dùng hay đơn hàng." />
        <MetricCard label="Phản hồi lỗi · 4xx / 5xx" value={`${number(response.totals.client_error_count)} / ${number(response.totals.server_error_count)}`} scope="Request" />
        <MetricCard label="Tỷ lệ lỗi HTTP" value={percent(response.totals.error_rate_percent)} scope="(4xx + 5xx) / request" />
        <MetricCard label="p95 độ trễ · Xấp xỉ" value={response.totals.p95_duration_ms == null ? "—" : `${number(response.totals.p95_duration_ms)} ms`} scope={`Trung bình: ${number(response.totals.avg_duration_ms)} ms`} detail="Tính trực tiếp từ request duy nhất trong cửa sổ; không lấy trung bình percentile từng phút." />
      </div>
      {!response.series.length ? <EmptyState detail="Chưa có request hợp lệ trong cửa sổ đã chọn. Không suy ra lưu lượng bằng 0 hoặc pipeline đã dừng." /> : <>
        <div className="grid gap-5 xl:grid-cols-2">{[false, true].map(errors => <div className="min-w-0" key={String(errors)}>
          <h3 className="mb-3 text-sm font-semibold">{errors ? "Phản hồi lỗi theo phút" : "Request theo phút"}</h3>
          <Chart height={260} label={errors ? "Phản hồi 4xx và 5xx mỗi phút UTC" : "Số request mỗi phút UTC"} option={{
            color: errors ? ["#8b5b16", "#a94728"] : ["#315b4f"],
            tooltip: { trigger: "axis", renderMode: "richText", confine: true },
            legend: { top: 0, textStyle: { color: "#5d6965", fontSize: 11 } },
            grid: { left: 4, right: 16, top: 40, bottom: 44, containLabel: true },
            xAxis: { type: "category", boundaryGap: false, data: times.map(time => new Date(time).toISOString().slice(11, 16)), axisLabel: { color: "#5d6965", hideOverlap: true }, axisLine: { lineStyle: { color: "#dcd8cf" } } },
            yAxis: { type: "value", name: "Request", min: 0, minInterval: 1, splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } },
            series: (errors ? [{ name: "4xx", field: "client_error_count" as const }, { name: "5xx", field: "server_error_count" as const }] : [{ name: "Request", field: "total_requests" as const }]).map(item => ({ name: item.name, type: "line", lineStyle: { type: item.field === "server_error_count" ? "dashed" : "solid" }, showSymbol: times.length < 25, connectNulls: false, data: times.map(time => byMinute.get(time)?.[item.field] ?? null) })),
          }} />
        </div>)}</div>
        <div><h3 className="mb-3 text-sm font-semibold">Route theo số lỗi · Top 10</h3>
          <Chart height={300} label="Số lỗi HTTP theo route và method" option={barOption(response.routes.slice(0, 10).map(row => ({ name: `${row.http_request_method} ${row.http_route}`, value: (row.client_error_count ?? 0) + (row.server_error_count ?? 0) })), "Lỗi 4xx + 5xx", number, "#a94728", "Request")} />
          <DetailTable caption="HTTP realtime theo route · Top 20" filename={`http-realtime-${minutes}-phut`} rows={response.routes} columns={columns} rowKey={row => JSON.stringify([row.http_route, row.http_request_method])} />
        </div>
      </>}
      {quality && <details className="border-t border-line pt-3 text-xs text-muted"><summary className="cursor-pointer font-medium">Phạm vi và chất lượng Landing{quality.invalid_rows || quality.conflicting_event_ids ? " · Có bản ghi bị loại" : ""}</summary>
        <dl className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3">{[
          ["Dòng trong phạm vi đọc", quality.scanned_rows], ["Dòng không hợp lệ", quality.invalid_rows], ["Dòng phát lại đã gộp", quality.replayed_rows],
          ["ID xung đột đã loại", quality.conflicting_event_ids], ["ID đến muộn ngoài cửa sổ", quality.late_event_ids], ["ID có giờ tương lai", quality.future_event_ids],
        ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd className="mt-1 font-semibold text-ink">{number(Number(value))}</dd></div>)}</dl>
        <p className="mt-3">Cửa sổ event time [bắt đầu, mốc đo); chỉ log đã nhận tới mốc đo. Phút đầu/cuối có thể chưa đủ 60 giây. Khoảng trống giữ nguyên; log đến muộn có thể sửa số phút trước trong cửa sổ còn mở.</p>
        <p className="mt-2">Snapshot: {response.snapshot_id ?? "Chưa có"} · Current từ: {stamp(response.snapshot_current_at)} · Truy vấn xong: {stamp(response.queried_at)} UTC. Chống lặp/xung đột chỉ trong phạm vi đọc; chưa qua đầy đủ kiểm soát Silver. Chu kỳ 10 giây không phải cam kết độ trễ dữ liệu.</p>
      </details>}
    </div>}
  </Panel>;
}
