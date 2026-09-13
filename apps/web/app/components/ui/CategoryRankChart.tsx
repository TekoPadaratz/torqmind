"use client";

import { useMemo, useState } from "react";
import { createPortal } from "react-dom";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";

import { ellipsizeLabel, shortCategoryLabels, shortPersonLabels } from "../../lib/chart-labels.mjs";

export type CategoryRankRow = {
  id: string | number;
  name: string;
  value: number;
  selected?: boolean;
  fill?: string;
  detail?: string;
};

type HoverHint = {
  x: number;
  y: number;
  title: string;
  valueText: string;
};

type Props = {
  rows: CategoryRankRow[];
  kind: "person" | "category";
  valueFormatter: (value: number, row: CategoryRankRow) => string;
  axisFormatter?: (value: number) => string;
  onSelect?: (id: string | number) => void;
  barFill?: string;
  selectedFill?: string;
  axisWidth?: number;
};

const ROW_PX = 44;
const MAX_VISIBLE = 10;
const DEFAULT_FILL = "var(--accent-copper, #b87333)";
const DEFAULT_SELECTED = "#f59e0b";

function clampHint(x: number, y: number): { left: number; top: number } {
  const pad = 8;
  const approxW = 300;
  const approxH = 88;
  let left = x + 14;
  let top = y + 14;
  if (typeof window !== "undefined") {
    if (left + approxW > window.innerWidth - pad) left = x - approxW - 8;
    if (top + approxH > window.innerHeight - pad) top = y - approxH - 8;
    left = Math.max(pad, left);
    top = Math.max(pad, top);
  }
  return { left, top };
}

function HoverCard({ hint }: { hint: HoverHint }) {
  if (typeof document === "undefined") return null;
  const pos = clampHint(hint.x, hint.y);
  return createPortal(
    <div
      className="chartTooltip"
      role="tooltip"
      style={{ position: "fixed", left: pos.left, top: pos.top, zIndex: 80, pointerEvents: "none", maxWidth: 320 }}
    >
      <div className="chartTooltipTitle">{hint.title}</div>
      <div className="chartTooltipValue">{hint.valueText}</div>
    </div>,
    document.body,
  );
}

export default function CategoryRankChart({
  rows,
  kind,
  valueFormatter,
  axisFormatter,
  onSelect,
  barFill = DEFAULT_FILL,
  selectedFill = DEFAULT_SELECTED,
  axisWidth,
}: Props) {
  const [hint, setHint] = useState<HoverHint | null>(null);

  const labeled = useMemo(() => {
    const uniqueRows = rows.map((row, index) => {
      const base = String(row.id ?? "").trim();
      return { ...row, chartId: base ? `${base}#${index}` : `row#${index}` };
    });
    const source = uniqueRows.map((row) => ({ id: row.chartId, name: row.name }));
    const shorts =
      kind === "person" ? shortPersonLabels(source) : shortCategoryLabels(source, 22);
    return uniqueRows.map((row) => {
      const full = String(row.name || "").trim() || (kind === "person" ? "Nome não cadastrado" : "—");
      const short = String(shorts.get(row.chartId) || ellipsizeLabel(full, kind === "person" ? 16 : 22));
      return {
        ...row,
        rowKey: row.chartId,
        shortLabel: short,
        fullLabel: full,
      };
    });
  }, [kind, rows]);

  const width = axisWidth ?? (kind === "person" ? 128 : 168);
  const plotHeight = Math.max(ROW_PX, labeled.length * ROW_PX);
  const viewport = Math.min(labeled.length, MAX_VISIBLE) * ROW_PX + 8;
  const selectable = typeof onSelect === "function";

  const showHint = (event: { clientX: number; clientY: number }, row: (typeof labeled)[number]) => {
    setHint({
      x: event.clientX,
      y: event.clientY,
      title: row.fullLabel,
      valueText: [valueFormatter(Number(row.value || 0), row), row.detail].filter(Boolean).join(" · "),
    });
  };

  return (
    <div className="chartCategoryScroll" style={{ maxHeight: viewport }}>
      <div className="chartCategoryGrid" style={{ ["--chart-axis-w" as string]: `${width}px`, height: plotHeight }}>
        <ul className="chartCategoryAxis">
          {labeled.map((row) => (
            <li key={row.rowKey}>
              <button
                type="button"
                className={`chartCategoryTickBtn${row.selected ? " is-selected" : ""}`}
                title={row.fullLabel}
                aria-label={row.fullLabel}
                aria-pressed={selectable ? Boolean(row.selected) : undefined}
                onClick={() => onSelect?.(row.id)}
                onMouseEnter={(event) => showHint(event, row)}
                onMouseMove={(event) => showHint(event, row)}
                onMouseLeave={() => setHint(null)}
                onFocus={(event) => {
                  const box = event.currentTarget.getBoundingClientRect();
                  showHint({ clientX: box.right, clientY: box.top + box.height / 2 }, row);
                }}
                onBlur={() => setHint(null)}
              >
                {row.shortLabel}
              </button>
            </li>
          ))}
        </ul>
        <div className="chartCategoryPlot">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={labeled}
              layout="vertical"
              margin={{ top: 4, right: 12, left: 4, bottom: 4 }}
            >
              <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
              <XAxis
                type="number"
                stroke="var(--muted)"
                tickFormatter={(value) =>
                  (axisFormatter || ((n: number) => String(n)))(Number(value))
                }
              />
              <YAxis type="category" dataKey="rowKey" hide />
              <Bar
                dataKey="value"
                radius={[0, 6, 6, 0]}
                cursor={selectable ? "pointer" : "default"}
                onClick={(entry) => {
                  const id = (entry as { payload?: CategoryRankRow })?.payload?.id;
                  if (id != null) onSelect?.(id);
                }}
                onMouseMove={(entry, _index, event) => {
                  const payload = (entry as { payload?: (typeof labeled)[number] })?.payload;
                  if (!payload) return;
                  const ev = event as unknown as MouseEvent;
                  showHint({ clientX: ev.clientX, clientY: ev.clientY }, payload);
                }}
                onMouseLeave={() => setHint(null)}
              >
                {labeled.map((row) => (
                  <Cell
                    key={row.rowKey}
                    fill={row.fill || (row.selected ? selectedFill : barFill)}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      {hint ? <HoverCard hint={hint} /> : null}
    </div>
  );
}
