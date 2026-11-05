import type { EChartsOption } from "@/components/ui/EChart";
import { number } from "./AnalyticsUI";

export function barOption(rows: { name: string; value: number }[], seriesName: string, format: (value: number) => string = number, color = "#315b4f", unit = "", tooltipFormat: (value: number) => string = format): EChartsOption {
  return {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, renderMode: "richText", confine: true, valueFormatter: value => tooltipFormat(Number(value)) },
    grid: { left: 4, right: 36, top: 12, bottom: 36, containLabel: true },
    xAxis: { type: "value", name: unit, nameLocation: "middle", nameGap: 24, minInterval: unit.includes("%") ? undefined : 1, max: rows.every(row => row.value <= 0) ? (rows.every(row => row.value === 0) ? 1 : 0) : undefined, min: rows.some(row => row.value < 0) ? undefined : 0, axisLabel: { color: "#5d6965", fontSize: 12, formatter: format }, splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } },
    yAxis: { type: "category", inverse: true, data: rows.map(row => row.name), axisTick: { show: false }, axisLine: { show: false }, axisLabel: { color: "#152722", fontSize: 12, width: 160, overflow: "truncate" } },
    series: [{ name: seriesName, type: "bar", data: rows.map(row => row.value), barMaxWidth: 22, label: { show: true, position: "right", formatter: point => Number(point.value) === 0 ? "0" : "", color: "#152722" }, itemStyle: { color, borderRadius: [0, 3, 3, 0] } }],
  };
}

export function reconciliationOption(
  row: { oltp_value: number | null; lakehouse_value: number | null },
  unit: string,
  format: (value: number) => string = number,
  formatTooltip: (value: number) => string = format,
): EChartsOption {
  return {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, renderMode: "richText", confine: true, valueFormatter: value => formatTooltip(Number(value)) },
    grid: { left: 8, right: 16, top: 36, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: ["MySQL\n(OLTP)", "Lakehouse\n(Gold)"], axisTick: { show: false }, axisLine: { show: false }, axisLabel: { color: "#152722", fontSize: 12, interval: 0 } },
    yAxis: { type: "value", name: unit, min: ({ min }) => Math.min(0, min), minInterval: 1, nameTextStyle: { color: "#5d6965", fontSize: 12 }, axisLabel: { color: "#5d6965", fontSize: 12, formatter: format }, splitLine: { lineStyle: { color: "#ece6da", type: "dashed" } } },
    series: [{
      name: "Giá trị",
      type: "bar",
      barMaxWidth: 48,
      data: [
        { value: row.oltp_value, itemStyle: { color: "#315b4f" } },
        { value: row.lakehouse_value, itemStyle: { color: "#a94728" } },
      ],
      itemStyle: { borderRadius: [3, 3, 0, 0] },
      label: { show: true, position: "top", color: "#152722", fontSize: 12, formatter: params => format(Number(params.value)) },
    }],
  };
}
