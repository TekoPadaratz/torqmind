"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import ChartTooltip from "./ui/ChartTooltip";
import { formatCurrency } from "../lib/format";

export type SalesFloorTotals = {
  vendas?: number;
  qtd_vendas?: number;
  cancelamentos?: number;
  qtd_cancelamentos?: number;
  devolucoes?: number;
  qtd_devolucoes?: number;
};

export type SalesFloorHourPoint = {
  hora: string;
  saidas: number;
};

type SalesFloorBoardProps = {
  title?: string;
  subtitle?: string;
  lastUpdated?: string | null;
  totals: SalesFloorTotals;
  hours: SalesFloorHourPoint[];
  loading?: boolean;
  error?: string | null;
  onLogout?: () => void;
  showLogout?: boolean;
  /** Quando true, omite o header (útil com AppNav em /sales). */
  embedded?: boolean;
};

function MetricCard({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint: string;
  tone: "good" | "warn" | "risk";
}) {
  return (
    <div className={`salesFloorMetric salesFloorMetric--${tone}`}>
      <div className="salesFloorMetricLabel">{label}</div>
      <div className="salesFloorMetricValue">{value}</div>
      <div className="salesFloorMetricHint">{hint}</div>
    </div>
  );
}

export default function SalesFloorBoard({
  title = "Vendas do dia",
  subtitle = "Acompanhamento operacional por hora",
  lastUpdated = null,
  totals,
  hours,
  loading = false,
  error = null,
  onLogout,
  showLogout = false,
  embedded = false,
}: SalesFloorBoardProps) {
  const hasHourValues = hours.some((row) => Number(row.saidas || 0) > 0);
  const vendas = Number(totals.vendas || 0);
  const cancelamentos = Number(totals.cancelamentos || 0);
  const devolucoes = Number(totals.devolucoes || 0);

  return (
    <div className={`salesFloor ${embedded ? "salesFloor--embedded" : ""}`}>
      {!embedded ? (
        <header className="salesFloorHeader">
          <div>
            <div className="salesFloorEyebrow">TorqMind · Piso de loja</div>
            <h1 className="salesFloorTitle">{title}</h1>
            <p className="salesFloorSubtitle">{subtitle}</p>
          </div>
          <div className="salesFloorHeaderActions">
            <span className="salesFloorUpdated">
              {lastUpdated
                ? `Atualizado às ${lastUpdated}`
                : "Atualização automática"}
            </span>
            {showLogout && onLogout ? (
              <button type="button" className="salesFloorLogout" onClick={onLogout}>
                Sair
              </button>
            ) : null}
          </div>
        </header>
      ) : null}

      {error ? <div className="salesFloorError">{error}</div> : null}

      <div className="salesFloorMetrics">
        <MetricCard
          label="Vendas"
          value={loading ? "…" : formatCurrency(vendas)}
          hint={`${Number(totals.qtd_vendas || 0).toLocaleString("pt-BR")} comprovante(s)`}
          tone="good"
        />
        <MetricCard
          label="Cancelamentos"
          value={loading ? "…" : formatCurrency(cancelamentos)}
          hint={`${Number(totals.qtd_cancelamentos || 0).toLocaleString("pt-BR")} comprovante(s)`}
          tone="warn"
        />
        <MetricCard
          label="Devoluções"
          value={loading ? "…" : formatCurrency(devolucoes)}
          hint={`${Number(totals.qtd_devolucoes || 0).toLocaleString("pt-BR")} nota(s)`}
          tone="risk"
        />
      </div>

      <section className="salesFloorChartCard">
        <div className="salesFloorChartHead">
          <h2>Vendas por hora</h2>
          <span className="muted">Distribuição do faturamento ao longo do dia</span>
        </div>
        {!loading && !hasHourValues ? (
          <div className="salesFloorEmpty">Sem vendas por hora neste período ainda.</div>
        ) : null}
        <div className="salesFloorChartWrap">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={hours} margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
              <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
              <XAxis dataKey="hora" stroke="var(--muted)" tick={{ fontSize: 12 }} interval={1} />
              <YAxis
                stroke="var(--muted)"
                tickFormatter={formatCurrency}
                width={108}
                tick={{ fontSize: 12 }}
              />
              <Tooltip
                content={<ChartTooltip valueFormatter={(value) => formatCurrency(value)} />}
              />
              <Bar dataKey="saidas" name="Vendas" fill="#34d399" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>
    </div>
  );
}
