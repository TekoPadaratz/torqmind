'use client';

type PayloadItem = {
  name?: string;
  value?: number | string;
  color?: string;
  dataKey?: string | number;
  payload?: Record<string, unknown>;
};

type Props = {
  active?: boolean;
  payload?: PayloadItem[];
  label?: string | number;
  valueFormatter?: (value: number | string, name: string, item: PayloadItem) => string;
  labelFormatter?: (label: string | number) => string;
};

/**
 * Tooltip Recharts tema-aware: título em cobre (visível no fundo do hint).
 */
export default function ChartTooltip({
  active,
  payload,
  label,
  valueFormatter,
  labelFormatter,
}: Props) {
  if (!active || !payload?.length) return null;
  const title = labelFormatter
    ? labelFormatter(label as string | number)
    : String(label ?? '');
  return (
    <div className="chartTooltip">
      {title ? <div className="chartTooltipTitle">{title}</div> : null}
      <ul className="chartTooltipList">
        {payload.map((item, idx) => {
          const row = item as PayloadItem;
          const name = String(row.name ?? row.dataKey ?? '');
          const raw = row.value ?? '';
          const text = valueFormatter
            ? valueFormatter(raw as number | string, name, row)
            : String(raw);
          return (
            <li key={`${name}-${idx}`} className="chartTooltipItem">
              <span
                className="chartTooltipSwatch"
                style={{ background: row.color || 'var(--accent-copper)' }}
              />
              <span className="chartTooltipName">{name}</span>
              <span className="chartTooltipValue">{text}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
