"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useEffect, useRef, useState } from "react";

import { EmptyState, PublicationStatus, ViewSwitch } from "@/components/analytics/AnalyticsUI";
import { RoleDashboards } from "@/components/analytics/RoleDashboards";
import { HttpMetricsPanel } from "@/components/analytics/HttpMetricsPanel";
import { SalesTrendPanel } from "@/components/analytics/SalesTrendPanel";
import { getAdminAnalyticsStores, getAdminRoleMetrics, type RoleMetricsResponse, type StoreItemResponse } from "@/lib/api";
import { ANALYTICS_ROLES, CANONICAL_ROLES, isAnalyticsRole } from "@/lib/analytics-dashboard";
import { useAuth } from "@/lib/auth";
import { AUTO_REFRESH_INTERVAL_MS, shouldAutoRefreshRole } from "@/lib/dashboard-refresh";

function LoadingDashboard() {
  return <div aria-label="Đang tải báo cáo" className="space-y-5" role="status">
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">{[0, 1, 2, 3].map(index => <div className="h-40 animate-pulse rounded-2xl bg-sand/60" key={index} />)}</div>
    <div className="h-72 animate-pulse rounded-2xl bg-sand/40" />
  </div>;
}

function AnalyticsHubContent() {
  const { customer, loading: authLoading } = useAuth();
  const searchParams = useSearchParams();
  const canonicalRole = CANONICAL_ROLES[customer?.role ?? ""];
  const requestedRole = searchParams.get("role");
  const activeRole = isAnalyticsRole(requestedRole) ? requestedRole : canonicalRole ?? "executive";
  const role = ANALYTICS_ROLES.find(item => item.id === activeRole)!;
  const isAdmin = customer?.role === "admin";
  const unauthorized = !isAdmin && activeRole !== canonicalRole;
  const [selectedStoreId, setSelectedStoreId] = useState<number | undefined>();
  const [stores, setStores] = useState<{ customerId: string; items: StoreItemResponse[] } | null>(null);
  const [storeError, setStoreError] = useState<string | null>(null);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [systemView, setSystemView] = useState("reconciliation");
  const [snapshot, setSnapshot] = useState<{ key: string; metrics: RoleMetricsResponse } | null>(null);
  const [failure, setFailure] = useState<{ key: string; message: string } | null>(null);
  const [pending, setPending] = useState(false);
  const inFlight = useRef(false);
  const customerId = customer?.public_id;
  const storeParam = activeRole === "store" && isAdmin ? selectedStoreId : undefined;
  const requestKey = `${customerId}:${activeRole}:${storeParam ?? "default"}`;
  const metrics = snapshot?.key === requestKey ? snapshot.metrics : null;
  const error = failure?.key === requestKey ? failure.message : null;
  const loading = !metrics && !error;
  const autoRefresh = shouldAutoRefreshRole(activeRole);
  const availableStores = stores && stores.customerId === customerId ? stores.items : [];

  useEffect(() => {
    if (!customerId || activeRole !== "store" || unauthorized) return;
    let current = true;
    setStoreError(null);
    getAdminAnalyticsStores().then(items => {
      if (current) setStores({ customerId, items });
    }).catch((cause: unknown) => {
      if (current) setStoreError(cause instanceof Error ? cause.message : "Không tải được danh sách cửa hàng.");
    });
    return () => { current = false; };
  }, [customerId, activeRole, unauthorized, refreshVersion]);

  useEffect(() => {
    if (authLoading || !customerId || unauthorized || (activeRole === "system" && systemView === "http")) return;
    let current = true;
    inFlight.current = true;
    setPending(true);
    setFailure(null);
    getAdminRoleMetrics(activeRole, storeParam).then(response => {
      if (!current) return;
      if (response.role !== activeRole) throw new Error("Phản hồi không khớp vai trò được yêu cầu. Vui lòng tải lại.");
      setSnapshot({ key: requestKey, metrics: response });
    }).catch((cause: unknown) => {
      if (current) setFailure({ key: requestKey, message: cause instanceof Error ? cause.message : "Không thể tải báo cáo." });
    }).finally(() => {
      if (current) { inFlight.current = false; setPending(false); }
    });
    return () => { current = false; inFlight.current = false; };
  }, [authLoading, customerId, activeRole, unauthorized, storeParam, requestKey, refreshVersion, systemView]);

  useEffect(() => {
    if (!customerId || unauthorized || !autoRefresh) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible" && !inFlight.current) setRefreshVersion(value => value + 1);
    }, AUTO_REFRESH_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [customerId, unauthorized, autoRefresh, activeRole]);

  if (authLoading || !customer) return <LoadingDashboard />;

  const visibleRoles = isAdmin ? ANALYTICS_ROLES : ANALYTICS_ROLES.filter(item => item.id === canonicalRole);
  const effectiveStoreId = selectedStoreId ?? (metrics?.role === "store" ? metrics.store_id ?? undefined : undefined);
  const displayedStoreMissing = effectiveStoreId != null && !availableStores.some(store => store.store_id === effectiveStoreId);
  const showHttp = activeRole === "system" && systemView === "http";
  const scope = { executive: "Toàn kỳ đã nạp", sales: "Toàn kỳ đã nạp · Xu hướng có kỳ riêng", store: "Hôm nay · UTC+7", inventory: "Số dư hiện tại · Lịch sử 30 ngày", operations: "Theo mốc đo đã công bố", marketing: "Kỳ riêng theo nguồn", system: "Theo lần quan sát đã công bố" }[activeRole];

  return <section className="space-y-4">
    <header className="flex flex-wrap items-center justify-between gap-3">
      <div><h1 className="text-2xl font-semibold tracking-tight text-ink">{role.label}</h1><p className="mt-1 text-xs text-muted">{scope}</p></div>
      {!unauthorized && <button className="button-secondary min-h-10 px-4 text-sm" disabled={pending && !showHttp} onClick={() => setRefreshVersion(value => value + 1)} type="button">{pending && !showHttp ? "Đang tải…" : "Làm mới"}</button>}
    </header>
    <nav aria-label="Góc nhìn báo cáo" className="flex flex-wrap gap-1 border-b border-line">
      {visibleRoles.map(item => <Link aria-current={activeRole === item.id ? "page" : undefined} className={`inline-flex min-h-11 items-center border-b-2 px-3 py-2 text-sm font-medium ${activeRole === item.id ? "border-moss text-moss" : "border-transparent text-muted hover:text-ink"}`} href={`/admin/analytics?role=${item.id}`} key={item.id}>{item.label}</Link>)}
    </nav>
    {unauthorized ? <div className="feedback-error" role="alert">Bạn không có quyền xem góc nhìn này. <Link className="underline" href={`/admin/analytics?role=${canonicalRole}`}>Mở dashboard của bạn</Link></div> : <>
      {activeRole === "store" && <div className="max-w-sm">
        {isAdmin ? <><label className="field-label" htmlFor="analytics-store">Cửa hàng</label><select className="admin-input" disabled={pending} id="analytics-store" onChange={event => setSelectedStoreId(event.target.value ? Number(event.target.value) : undefined)} value={effectiveStoreId ?? ""}>
          <option value="">Cửa hàng mặc định</option>
          {displayedStoreMissing && <option value={effectiveStoreId}>{metrics?.role === "store" ? metrics.store_name ?? `Cửa hàng #${effectiveStoreId}` : `Cửa hàng #${effectiveStoreId}`}</option>}
          {availableStores.map(store => <option key={store.store_id} value={store.store_id}>{store.name}</option>)}
        </select>{selectedStoreId != null && <button className="min-h-9 text-xs font-medium text-moss underline" onClick={() => setSelectedStoreId(undefined)} type="button">Về mặc định</button>}</> : <p className="text-sm font-medium">{metrics?.role === "store" ? metrics.store_name : "Cửa hàng được phân quyền"}</p>}
        {storeError && <p className="mt-1 text-xs text-danger" role="status">{storeError}</p>}
      </div>}
      {activeRole === "system" && <ViewSwitch label="Phân tích hệ thống" value={systemView} onChange={setSystemView} items={[{ value: "reconciliation", label: "Đối soát" }, { value: "http", label: "HTTP Metrics" }]} />}
      {showHttp ? <HttpMetricsPanel key={customerId} refreshVersion={refreshVersion} /> : <>
        {error && <div className="feedback-error" role="alert">Chưa tải được dữ liệu mới. {metrics ? "Đang giữ kết quả trước đó." : error} <button className="min-h-9 underline" onClick={() => setRefreshVersion(value => value + 1)} type="button">Thử lại</button></div>}
        {loading && <LoadingDashboard />}
        {metrics && <div aria-busy={pending} className="space-y-4" key={requestKey}>
          {["executive", "sales"].includes(metrics.role) && <PublicationStatus data={metrics} />}
          <RoleDashboards metrics={metrics} trend={activeRole === "executive" || activeRole === "sales" ? <SalesTrendPanel refreshVersion={refreshVersion} /> : undefined} />
        </div>}
        {!loading && !metrics && !error && <EmptyState />}
      </>}
    </>}
  </section>;
}

export default function AnalyticsHubPage() {
  return <Suspense fallback={<LoadingDashboard />}><AnalyticsHubContent /></Suspense>;
}
