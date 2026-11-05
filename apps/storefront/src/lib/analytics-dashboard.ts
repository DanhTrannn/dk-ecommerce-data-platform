import type { RoleMetricsResponse } from "@/lib/api";

export type AnalyticsRole = RoleMetricsResponse["role"];

export const ANALYTICS_ROLES: { id: AnalyticsRole; label: string }[] = [
  { id: "executive", label: "Tổng quan" }, { id: "sales", label: "Kinh doanh" },
  { id: "store", label: "Cửa hàng" }, { id: "inventory", label: "Tồn kho" },
  { id: "operations", label: "Vận hành" }, { id: "marketing", label: "Marketing" },
  { id: "system", label: "Hệ thống" },
];

export const CANONICAL_ROLES: Record<string, AnalyticsRole> = {
  admin: "executive", sales_manager: "sales", store_manager: "store", inventory_manager: "inventory",
  operations_manager: "operations", marketing_manager: "marketing", system_admin: "system",
};

export function isAnalyticsRole(value: string | null): value is AnalyticsRole {
  return ANALYTICS_ROLES.some(role => role.id === value);
}
