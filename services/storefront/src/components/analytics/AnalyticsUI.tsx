"use client";

import { useMemo, useState, type ReactNode } from "react";

import { EChart, type EChartsOption } from "@/components/ui/EChart";
import { Icon } from "@/components/ui/Icon";
import { exportToCsv } from "@/lib/csv-export";
import type { AnalyticsDataStatus } from "@/lib/commerce";

export const number = (value: number | null | undefined) =>
  value != null && Number.isFinite(value) ? new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 2 }).format(value) : "—";
export const percent = (value: number | null | undefined) => value != null && Number.isFinite(value) ? `${number(value)}%` : "—";
export const ratio = (value: number, total: number) => total > 0 ? value / total * 100 : null;

export function MetricCard({ label, value, detail, scope, exactValue, emphasis = false, warning = false }: {
  label: string; value: string; detail?: string; scope?: string; exactValue?: string; emphasis?: boolean; warning?: boolean;
}) {
  return <article className={`min-w-0 rounded-xl border p-4 ${emphasis ? "border-moss bg-moss text-white" : "border-line bg-surface text-ink"}`} tabIndex={0} title={[exactValue ?? value, scope, detail].filter(Boolean).join(" · ")}>
    <p className={`text-sm font-medium ${emphasis ? "text-white" : "text-muted"}`}>{label}</p>
    <p className={`mt-2 break-words text-[clamp(1.5rem,2.2vw,2rem)] font-semibold leading-tight tracking-tight tabular-nums ${warning ? emphasis ? "text-amber-200" : "text-danger" : ""}`}>{value}</p>
    {scope && <p className={`mt-2 text-xs ${emphasis ? "text-white" : "text-muted"}`}>{scope}</p>}
    {exactValue && <span className="sr-only">{exactValue}</span>}{detail && <span className="sr-only">{detail}</span>}
  </article>;
}

export function PublicationStatus({ data, observedAt, label = "Công bố" }: { data: AnalyticsDataStatus; observedAt?: string | null; label?: string }) {
  const stamp = (value: string) => new Date(value).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
  return <p className={`text-xs leading-5 ${data.is_degraded ? "text-warning" : "text-muted"}`} data-run-id={data.source_run_id ?? undefined} role="status" title={data.degraded_reason ?? undefined}>
    {label}: {data.data_updated_at ? stamp(data.data_updated_at) : "Chưa có dữ liệu"}{observedAt && ` · Đo ${stamp(observedAt)}`} · UTC+7
    {data.served_from_cache ? ` · Bản lưu ${number(data.cache_age_seconds)} giây` : data.is_degraded ? " · Dữ liệu chưa đầy đủ" : ""}
  </p>;
}

export function ViewSwitch({ value, onChange, items, label }: { value: string; onChange: (value: string) => void; items: { value: string; label: string }[]; label: string }) {
  return <div aria-label={label} className="flex flex-wrap gap-1 border-b border-line" role="group">{items.map(item => <button aria-pressed={value === item.value} className={`min-h-11 border-b-2 px-3 py-2 text-sm font-medium ${value === item.value ? "border-moss text-moss" : "border-transparent text-muted hover:text-ink"}`} key={item.value} onClick={() => onChange(item.value)} type="button">{item.label}</button>)}</div>;
}

export function Panel({ title, subtitle, children, action, id }: {
  title: string; subtitle?: string; children: ReactNode; action?: ReactNode; id?: string;
}) {
  return (
    <section className="min-w-0 rounded-xl border border-line bg-surface p-4 sm:p-5" id={id}>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-base font-semibold tracking-tight text-ink">{title}</h2>
          {subtitle && <p className="mt-1 max-w-3xl text-xs leading-5 text-muted">{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

export function EmptyState({ title = "Chưa có dữ liệu", detail = "Không có bản ghi trong phạm vi hiện tại." }: { title?: string; detail?: string }) {
  return <div className="flex min-h-28 flex-col items-center justify-center rounded-xl border border-dashed border-line bg-paper/50 px-5 py-8 text-center" role="status">
    <p className="text-sm font-semibold text-ink">{title}</p>
    <p className="mt-1 max-w-md text-sm leading-6 text-muted">{detail}</p>
  </div>;
}

export function Chart({ option, label, height = 300 }: { option: EChartsOption; label: string; height?: number }) {
  return <EChart className="overflow-hidden" height={height} label={label} option={{ animation: false, aria: { enabled: true }, ...option }} />;
}

export interface Column<T> {
  key: string;
  label: string;
  value: (row: T) => string | number | null | undefined;
  render?: (row: T) => ReactNode;
  numeric?: boolean;
}

export function DetailTable<T>({ rows, columns, filename, caption, rowKey }: {
  rows: T[]; columns: Column<T>[]; filename: string; caption: string; rowKey: (row: T) => string | number;
}) {
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<{ key: string; descending: boolean } | null>(null);
  const filtered = useMemo(() => {
    const term = query.trim().toLocaleLowerCase("vi-VN");
    const result = rows.filter(row => columns.some(col => String(col.value(row) ?? "").toLocaleLowerCase("vi-VN").includes(term)));
    const column = columns.find(col => col.key === sort?.key);
    if (column && sort) result.sort((a, b) => {
      const av = column.value(a), bv = column.value(b);
      if (av == null) return bv == null ? 0 : 1;
      if (bv == null) return -1;
      const comparison = typeof av === "number" && typeof bv === "number" ? av - bv : String(av).localeCompare(String(bv), "vi");
      return sort.descending ? -comparison : comparison;
    });
    return result;
  }, [rows, columns, query, sort]);

  function download() {
    // Product names and other source strings are untrusted spreadsheet input.
    const safeCell = (value: string | number | null | undefined) => typeof value === "string" && /^[\s]*[=+@-]/.test(value) ? `'${value}` : value;
    exportToCsv(filename, columns.map(col => col.label), filtered.map(row => columns.map(col => safeCell(col.value(row)))));
  }

  return <details className="mt-4 border-t border-line pt-3"><summary className="cursor-pointer text-xs font-medium text-muted">{caption} · {number(rows.length)} dòng</summary><div className="mt-3">
    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
      <label className="flex min-h-10 w-full items-center gap-2 rounded-xl border border-line bg-paper/50 px-3 sm:max-w-xs">
        <Icon className="shrink-0 text-muted" name="search" size={16} />
        <input aria-label={`Tìm trong ${caption}`} className="min-w-0 flex-1 bg-transparent py-2 text-sm outline-none placeholder:text-muted" onChange={event => setQuery(event.target.value)} placeholder="Tìm trong bảng…" type="search" value={query} />
      </label>
      <button className="button-secondary min-h-10 px-4 py-2 text-xs" disabled={!filtered.length} onClick={download} type="button">Xuất CSV · {number(filtered.length)} dòng</button>
    </div>
    {!filtered.length ? <EmptyState title={query ? "Không có kết quả phù hợp" : "Chưa có bản ghi"} detail={query ? "Thử từ khóa khác hoặc xóa nội dung tìm kiếm." : "API chưa trả về bản ghi chi tiết cho phạm vi này."} /> :
      <div className="overflow-x-auto rounded-xl border border-line">
        <table className="w-full text-sm">
          <caption className="sr-only">{caption}. Tìm kiếm và sắp xếp chỉ áp dụng cho bảng này.</caption>
          <thead className="bg-paper text-muted"><tr>{columns.map(col => <th aria-sort={sort?.key === col.key ? (sort.descending ? "descending" : "ascending") : "none"} className={`px-4 py-3 text-xs font-semibold ${col.numeric ? "text-right" : "text-left"}`} key={col.key} scope="col">
            <button className="inline-flex min-h-7 items-center gap-1 whitespace-nowrap" onClick={() => setSort({ key: col.key, descending: sort?.key === col.key ? !sort.descending : !!col.numeric })} type="button">{col.label}<span aria-hidden="true">{sort?.key === col.key ? (sort.descending ? "↓" : "↑") : "↕"}</span></button>
          </th>)}</tr></thead>
          <tbody className="divide-y divide-line">{filtered.map(row => <tr className="hover:bg-paper/60" key={rowKey(row)}>{columns.map(col => <td className={`px-4 py-3 ${col.numeric ? "whitespace-nowrap text-right tabular-nums" : "min-w-36"}`} key={col.key}>{col.render ? col.render(row) : (typeof col.value(row) === "number" ? number(col.value(row) as number) : col.value(row) ?? "—")}</td>)}</tr>)}</tbody>
        </table>
      </div>}
  </div></details>;
}
