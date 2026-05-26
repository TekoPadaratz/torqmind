"use client";

import { useEffect, useMemo, useState, useCallback } from "react";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import EmptyState from "../components/ui/EmptyState";
import { formatCurrency, formatPercent } from "../lib/format";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { readCachedSession } from "../lib/session";
import { useBiScopeData } from "../lib/use-bi-scope-data";

const ABC_COLORS: Record<string, string> = {
  A: "#22c55e",
  B: "#eab308",
  C: "#ef4444",
};

const CONCENTRATION_ICONS: Record<string, string> = {
  high: "⚠️",
  dispersed: "🔀",
  healthy: "✅",
  empty: "—",
};

interface AbcChartItem {
  posicao: number;
  nome_produto: string;
  nome_grupo: string;
  faturamento: number;
  participacao_pct: number;
  acumulado_pct: number;
  classe_abc: string;
}

interface AbcRankingItem extends AbcChartItem {
  id_produto: number;
  qtd: number;
  valor_unitario_medio: number;
  custo_total: number;
  margem: number;
}

interface AbcSummary {
  total_produtos: number;
  total_faturamento: number;
  classe_a_count: number;
  classe_a_pct: number;
  classe_b_count: number;
  classe_b_pct: number;
  classe_c_count: number;
  classe_c_pct: number;
  produto_lider: string;
  produto_lider_pct: number;
  produto_lider_faturamento: number;
  concentration: string;
  concentration_text: string;
}

interface AbcData {
  summary: AbcSummary;
  chart_data: AbcChartItem[];
  ranking: AbcRankingItem[];
  insights: Array<{ type: string; text: string } | string>;
  thresholds: { a: number; b: number; c: number };
  sort_by?: string;
  source: string;
  empty?: boolean;
}

export default function SalesAbcSection() {
  const scope = useScopeQuery();
  const [sortBy, setSortBy] = useState<"faturamento" | "quantidade" | "lucro">("faturamento");
  const [params, setParams] = useState({ abc_threshold_a: 80, abc_threshold_b: 95, abc_exclude_fuel: true });

  // Fetch params on scope change
  useEffect(() => {
    const fetchParams = async () => {
      try {
        const session = readCachedSession();
        if (!session?.token) return;
        const scopeParams = buildScopeParams(scope);
        const resp = await fetch(`/api/bi/params/filial?${scopeParams}`, {
          headers: { Authorization: `Bearer ${session.token}` },
        });
        if (resp.ok) {
          const p = await resp.json();
          setParams(p);
        }
      } catch { /* ignore */ }
    };
    fetchParams();
  }, [scope]);

  const saveParams = useCallback(async () => {
    try {
      const session = readCachedSession();
      if (!session?.token || !scope.id_filial) return;
      await fetch(`/api/bi/params/filial`, {
        method: "PUT",
        headers: { Authorization: `Bearer ${session.token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ ...params, id_filial: scope.id_filial }),
      });
    } catch { /* ignore */ }
  }, [params, scope]);

  const { data, loading } = useBiScopeData<AbcData>({
    moduleKey: `sales_abc_curve_${sortBy}`,
    scope,
    errorMessage: "Falha ao carregar Curva ABC",
    buildRequestUrl: (currentScope) =>
      `/bi/sales/abc-curve?sort_by=${sortBy}&${buildScopeParams(currentScope).toString()}`,
  });

  const chartItems = useMemo(() => {
    if (!data?.chart_data) return [];
    const w = typeof window !== "undefined" ? window.innerWidth : 1200;
    const limit = w >= 1200 ? 20 : w >= 768 ? 12 : 8;
    return data.chart_data.slice(0, limit);
  }, [data]);

  if (loading && !data) {
    return (
      <div className="card col-12" style={{ marginTop: 24 }}>
        <div className="sectionEyebrow">Curva ABC de Produtos</div>
        <p className="muted" style={{ marginTop: 8 }}>Carregando análise ABC...</p>
      </div>
    );
  }

  if (!data || data.empty || !data.chart_data?.length || !data.summary) {
    return (
      <div className="card col-12" style={{ marginTop: 24 }}>
        <div className="sectionEyebrow">Curva ABC de Produtos</div>
        <EmptyState
          title="Sem dados para Curva ABC."
          detail="Não há vendas suficientes no período selecionado para montar a Curva ABC."
        />
      </div>
    );
  }

  const summary = data.summary;

  return (
    <>
      {/* Section Header */}
      <div className="card col-12" style={{ marginTop: 24 }}>
        <div className="sectionEyebrow">Gestão de Produtos</div>
        <h2 style={{ marginTop: 4 }}>Curva ABC — Análise de Pareto</h2>
        <div className="muted" style={{ marginTop: 8 }}>
          Classifica produtos por contribuição ao {sortBy}. Classe A (até {params.abc_threshold_a}%), B ({params.abc_threshold_a}–{params.abc_threshold_b}%), C ({params.abc_threshold_b}–100%).
        </div>
      </div>

      {/* Executive Summary KPIs */}
      <div className="card kpi col-3">
        <div className="label">Produtos analisados</div>
        <div className="value">{summary.total_produtos}</div>
        <div className="muted" style={{ marginTop: 4 }}>
          Faturamento: {formatCurrency(summary.total_faturamento)}
        </div>
      </div>
      <div className="card kpi col-3">
        <div className="label">
          <span style={{ color: ABC_COLORS.A, fontWeight: 700 }}>■</span> Classe A
          <span className="muted" style={{ marginLeft: 4, fontSize: 10 }}>
            (até{" "}
            {scope.id_filial ? (
              <input
                type="number"
                value={params.abc_threshold_a}
                onChange={(e) => setParams(p => ({ ...p, abc_threshold_a: Math.max(1, Math.min(98, parseInt(e.target.value) || 80)) }))}
                onBlur={saveParams}
                style={{ width: 32, background: "transparent", border: "1px solid #555", borderRadius: 3, color: "#fff", textAlign: "center", fontSize: 10, padding: "1px 2px" }}
                min={1} max={98}
              />
            ) : (
              <span>{params.abc_threshold_a}</span>
            )}
            %)
          </span>
        </div>
        <div className="value">{summary.classe_a_count} prod.</div>
        <div className="muted" style={{ marginTop: 4 }}>
          {formatPercent(summary.classe_a_pct)} do faturamento
        </div>
      </div>
      <div className="card kpi col-3">
        <div className="label">
          <span style={{ color: ABC_COLORS.B, fontWeight: 700 }}>■</span> Classe B
          <span className="muted" style={{ marginLeft: 4, fontSize: 10 }}>
            ({params.abc_threshold_a}–
            {scope.id_filial ? (
              <input
                type="number"
                value={params.abc_threshold_b}
                onChange={(e) => setParams(p => ({ ...p, abc_threshold_b: Math.max(p.abc_threshold_a + 1, Math.min(99, parseInt(e.target.value) || 95)) }))}
                onBlur={saveParams}
                style={{ width: 32, background: "transparent", border: "1px solid #555", borderRadius: 3, color: "#fff", textAlign: "center", fontSize: 10, padding: "1px 2px" }}
                min={params.abc_threshold_a + 1} max={99}
              />
            ) : (
              <span>{params.abc_threshold_b}</span>
            )}
            %)
          </span>
        </div>
        <div className="value">{summary.classe_b_count} prod.</div>
        <div className="muted" style={{ marginTop: 4 }}>
          {formatPercent(summary.classe_b_pct)} do faturamento
        </div>
      </div>
      <div className="card kpi col-3">
        <div className="label">
          <span style={{ color: ABC_COLORS.C, fontWeight: 700 }}>■</span> Classe C
          <span className="muted" style={{ marginLeft: 4, fontSize: 10 }}>
            ({params.abc_threshold_b}–100%)
          </span>
        </div>
        <div className="value">{summary.classe_c_count} prod.</div>
        <div className="muted" style={{ marginTop: 4 }}>
          {formatPercent(summary.classe_c_pct)} do faturamento
        </div>
      </div>

      {/* Concentration insight */}
      <div className="card col-12">
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 18 }}>{CONCENTRATION_ICONS[summary.concentration] || ""}</span>
          <span style={{ fontWeight: 600 }}>
            Líder: {summary.produto_lider} ({formatPercent(summary.produto_lider_pct)})
          </span>
        </div>
        {summary.concentration_text ? (
          <div className="muted" style={{ marginTop: 4 }}>{summary.concentration_text}</div>
        ) : null}
      </div>

      {/* Pareto Chart: Bar (faturamento) + Line (acumulado) */}
      <div className="card col-12 chartCard" style={{ minHeight: 380 }}>
        <h2>Gráfico de Pareto</h2>
        <div className="muted" style={{ marginTop: 4 }}>
          Barras = {sortBy === "faturamento" ? "faturamento" : sortBy === "quantidade" ? "quantidade" : "lucro"} por produto | Linha = % acumulado
        </div>
        <div style={{ display: "flex", gap: 4, marginTop: 12 }}>
          {(["faturamento", "quantidade", "lucro"] as const).map((opt) => (
            <button
              key={opt}
              onClick={() => setSortBy(opt)}
              style={{
                padding: "6px 14px",
                borderRadius: 6,
                border: "1px solid #444",
                background: sortBy === opt ? "#3b82f6" : "transparent",
                color: sortBy === opt ? "#fff" : "#bbb",
                fontWeight: sortBy === opt ? 600 : 400,
                fontSize: 12,
                cursor: "pointer",
                transition: "all 0.15s ease",
              }}
            >
              {opt === "faturamento" ? "Faturamento" : opt === "quantidade" ? "Quantidade" : "Lucro"}
            </button>
          ))}
        </div>
        <div style={{ height: 300, marginTop: 12 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartItems} margin={{ top: 5, right: 40, bottom: 60, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#333" />
              <XAxis
                dataKey="nome_produto"
                tick={{ fill: "#bbb", fontSize: 10 }}
                angle={-45}
                textAnchor="end"
                interval={0}
                height={80}
              />
              <YAxis
                yAxisId="fat"
                tick={{ fill: "#bbb", fontSize: 10 }}
                tickFormatter={(v: number) => formatCurrency(v)}
                width={90}
              />
              <YAxis
                yAxisId="pct"
                orientation="right"
                domain={[0, 100]}
                tick={{ fill: "#bbb", fontSize: 10 }}
                tickFormatter={(v: number) => `${v}%`}
                width={50}
              />
              <Tooltip
                contentStyle={{ background: "#1e1e2e", border: "1px solid #444" }}
                formatter={(value: any, name: string) => {
                  if (name === "faturamento") return [formatCurrency(value), "Faturamento"];
                  if (name === "acumulado_pct") return [formatPercent(value), "Acumulado"];
                  return [value, name];
                }}
                labelFormatter={(label: string) => label}
              />
              <Bar
                yAxisId="fat"
                dataKey="faturamento"
                name="faturamento"
                radius={[3, 3, 0, 0]}
                fill="#3b82f6"
              />
              <Line
                yAxisId="pct"
                type="monotone"
                dataKey="acumulado_pct"
                name="acumulado_pct"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={false}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Ranking Table */}
      <div className="card col-12">
        <h2>Ranking completo</h2>
        <div className="muted" style={{ marginTop: 4 }}>
          Todos os produtos classificados pela Curva ABC
        </div>
        <div className="tableScroll" style={{ maxHeight: 400, overflowY: "auto", marginTop: 12 }}>
          <table className="table compact">
            <thead>
              <tr>
                <th>#</th>
                <th>Produto</th>
                <th>Grupo</th>
                <th>Faturamento</th>
                <th>% Part.</th>
                <th>% Acum.</th>
                <th>Classe</th>
              </tr>
            </thead>
            <tbody>
              {data.ranking.map((item) => (
                <tr key={item.id_produto}>
                  <td>{item.posicao}</td>
                  <td style={{ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {item.nome_produto}
                  </td>
                  <td>{item.nome_grupo}</td>
                  <td>{formatCurrency(item.faturamento)}</td>
                  <td>{formatPercent(item.participacao_pct)}</td>
                  <td>{formatPercent(item.acumulado_pct)}</td>
                  <td>
                    <span
                      style={{
                        color: ABC_COLORS[item.classe_abc] || "#999",
                        fontWeight: 700,
                        fontSize: 13,
                      }}
                    >
                      {item.classe_abc}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Auto-insights */}
      {data.insights?.length ? (
        <div className="card col-12">
          <h2>Insights automáticos</h2>
          <ul style={{ marginTop: 8, paddingLeft: 16 }}>
            {data.insights.map((insight: any, i: number) => (
              <li key={i} className="muted" style={{ marginBottom: 4 }}>
                {typeof insight === "string" ? insight : insight?.text || ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </>
  );
}
