"use client";

import { useEffect, useMemo, useState, useCallback, useRef } from "react";
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
  A: "var(--color-positive)",
  B: "var(--color-warning)",
  C: "var(--color-negative)",
};

const CONCENTRATION_ICONS: Record<string, string> = {
  high: "⚠️",
  dispersed: "🔀",
  healthy: "✅",
  empty: "—",
};

type SortMode = "faturamento" | "quantidade" | "lucro";

const SORT_LABELS: Record<SortMode, string> = {
  faturamento: "Faturamento",
  quantidade: "Quantidade",
  lucro: "Lucro",
};

// Map sort mode to the data field used for bar chart and table value column
const SORT_DATA_KEY: Record<SortMode, string> = {
  faturamento: "faturamento",
  quantidade: "qtd",
  lucro: "margem",
};

interface AbcChartItem {
  posicao: number;
  nome_produto: string;
  nome_grupo: string;
  faturamento: number;
  qtd: number;
  margem: number;
  participacao_pct: number;
  acumulado_pct: number;
  classe_abc: string;
}

interface AbcRankingItem extends AbcChartItem {
  id_produto: number;
  valor_unitario_medio: number;
  custo_total: number;
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
  groups?: Array<{ id_grupo_produto: number; grupo_nome: string; faturamento: number }>;
  selected_groups?: number[];
  sort_by?: string;
  source: string;
  empty?: boolean;
}

export default function SalesAbcSection() {
  const scope = useScopeQuery();
  const [sortBy, setSortBy] = useState<SortMode>("faturamento");
  const [params, setParams] = useState({ abc_threshold_a: 80, abc_threshold_b: 95, abc_exclude_fuel: true });
  // Grupos escolhidos para compor a curva (vazio = todos os grupos).
  const [selectedGroups, setSelectedGroups] = useState<number[]>([]);
  const [groupMenuOpen, setGroupMenuOpen] = useState(false);
  // Local edit state for threshold inputs (avoids constraints fighting during typing)
  const [editA, setEditA] = useState("80");
  const [editB, setEditB] = useState("95");
  const [paramsDirty, setParamsDirty] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

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
          setEditA(String(p.abc_threshold_a));
          setEditB(String(p.abc_threshold_b));
          setParamsDirty(false);
        }
      } catch { /* ignore */ }
    };
    fetchParams();
    // Ao trocar de escopo, volta a considerar todos os grupos.
    setSelectedGroups([]);
    setGroupMenuOpen(false);
  }, [scope]);

  const applyParams = useCallback(async () => {
    const a = Math.max(1, Math.min(98, parseInt(editA) || 80));
    const b = Math.max(a + 1, Math.min(99, parseInt(editB) || 95));
    const newParams = { ...params, abc_threshold_a: a, abc_threshold_b: b };
    setParams(newParams);
    setEditA(String(a));
    setEditB(String(b));
    setParamsDirty(false);

    // Save to backend
    try {
      const session = readCachedSession();
      if (session?.token && scope.id_filial) {
        await fetch(`/api/bi/params/filial`, {
          method: "PUT",
          headers: { Authorization: `Bearer ${session.token}`, "Content-Type": "application/json" },
          body: JSON.stringify({ ...newParams, id_filial: scope.id_filial }),
        });
      }
    } catch { /* ignore */ }

    // Always trigger re-fetch of ABC data with new thresholds
    setRefreshKey((k) => k + 1);
  }, [editA, editB, params, scope]);

  const { data, loading } = useBiScopeData<AbcData>({
    moduleKey: `sales_abc_curve_${sortBy}_${params.abc_threshold_a}_${params.abc_threshold_b}_g${selectedGroups.slice().sort((a, b) => a - b).join("-")}_${refreshKey}`,
    scope,
    errorMessage: "Falha ao carregar Curva ABC",
    buildRequestUrl: (currentScope) => {
      const base = `/bi/sales/abc-curve?sort_by=${sortBy}&threshold_a=${params.abc_threshold_a}&threshold_b=${params.abc_threshold_b}&${buildScopeParams(currentScope).toString()}`;
      const grupos = selectedGroups.map((id) => `&id_grupos=${id}`).join("");
      return base + grupos;
    },
  });

  const chartItems = useMemo(() => {
    if (!data?.chart_data) return [];
    const w = typeof window !== "undefined" ? window.innerWidth : 1200;
    const limit = w >= 1200 ? 20 : w >= 768 ? 12 : 8;
    return data.chart_data.slice(0, limit);
  }, [data]);

  // Dynamic field for the selected metric
  const metricKey = SORT_DATA_KEY[sortBy];
  const metricLabel = SORT_LABELS[sortBy];

  const formatMetricValue = useCallback(
    (value: number) => {
      if (sortBy === "quantidade") return value.toLocaleString("pt-BR", { maximumFractionDigits: 0 });
      return formatCurrency(value);
    },
    [sortBy],
  );

  const availableGroups = data?.groups || [];
  const toggleGroup = (id: number) =>
    setSelectedGroups((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  const groupSelector = availableGroups.length ? (
    <div style={{ marginTop: 12, position: "relative" }}>
      <button
        type="button"
        onClick={() => setGroupMenuOpen((o) => !o)}
        style={{
          padding: "7px 14px",
          borderRadius: 8,
          border: "1px solid var(--accent-copper)",
          background: "rgba(184,115,51,0.12)",
          color: "var(--text)",
          fontSize: 13,
          cursor: "pointer",
          display: "inline-flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        {selectedGroups.length === 0
          ? "Grupos: todos"
          : `Grupos: ${selectedGroups.length} selecionado(s)`}
        <span style={{ opacity: 0.7 }}>▾</span>
      </button>
      {groupMenuOpen ? (
        <div
          style={{
            position: "absolute",
            zIndex: 30,
            marginTop: 6,
            minWidth: 300,
            maxHeight: 340,
            overflowY: "auto",
            background: "#12191f",
            border: "1px solid var(--border)",
            borderRadius: 10,
            padding: 8,
            boxShadow: "0 12px 34px rgba(0,0,0,0.45)",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 6 }}>
            <button
              type="button"
              onClick={() => setSelectedGroups([])}
              style={{ background: "transparent", border: "none", color: "var(--accent-copper)", fontSize: 12, cursor: "pointer", padding: 4 }}
            >
              Todos os grupos
            </button>
            <button
              type="button"
              onClick={() => setGroupMenuOpen(false)}
              style={{ background: "transparent", border: "none", color: "var(--muted)", fontSize: 12, cursor: "pointer", padding: 4 }}
            >
              Fechar
            </button>
          </div>
          {availableGroups.map((g) => (
            <label
              key={g.id_grupo_produto}
              style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 4px", fontSize: 13, cursor: "pointer" }}
            >
              <input
                type="checkbox"
                checked={selectedGroups.includes(g.id_grupo_produto)}
                onChange={() => toggleGroup(g.id_grupo_produto)}
              />
              <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {g.grupo_nome}
              </span>
              <span className="muted" style={{ fontSize: 11 }}>{formatCurrency(g.faturamento)}</span>
            </label>
          ))}
        </div>
      ) : null}
      <div className="muted" style={{ marginTop: 6, fontSize: 11 }}>
        Escolha quais grupos entram na curva. Vazio = todos os grupos (combustíveis seguem a configuração da filial).
      </div>
    </div>
  ) : null;

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
        {groupSelector}
        <EmptyState
          title="Sem dados para Curva ABC."
          detail="Não há vendas suficientes no período (ou nos grupos selecionados) para montar a Curva ABC."
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
          Classifica produtos por contribuição ao {metricLabel.toLowerCase()}. Classe A (até {params.abc_threshold_a}%), B ({params.abc_threshold_a}–{params.abc_threshold_b}%), C ({params.abc_threshold_b}–100%).
        </div>
        {groupSelector}
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
                value={editA}
                onChange={(e) => { setEditA(e.target.value); setParamsDirty(true); }}
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
          {formatPercent(summary.classe_a_pct)} do {metricLabel.toLowerCase()}
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
                value={editB}
                onChange={(e) => { setEditB(e.target.value); setParamsDirty(true); }}
                style={{ width: 32, background: "transparent", border: "1px solid #555", borderRadius: 3, color: "#fff", textAlign: "center", fontSize: 10, padding: "1px 2px" }}
                min={1} max={99}
              />
            ) : (
              <span>{params.abc_threshold_b}</span>
            )}
            %)
          </span>
        </div>
        <div className="value">{summary.classe_b_count} prod.</div>
        <div className="muted" style={{ marginTop: 4 }}>
          {formatPercent(summary.classe_b_pct)} do {metricLabel.toLowerCase()}
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
          {formatPercent(summary.classe_c_pct)} do {metricLabel.toLowerCase()}
        </div>
      </div>

      {/* Apply button for thresholds */}
      {scope.id_filial && paramsDirty ? (
        <div className="card col-12" style={{ padding: "10px 16px", display: "flex", alignItems: "center", gap: 12 }}>
          <span className="muted" style={{ fontSize: 12 }}>Percentuais alterados.</span>
          <button
            onClick={applyParams}
            style={{
              padding: "6px 18px",
              borderRadius: 6,
              border: "none",
              background: "#22c55e",
              color: "#fff",
              fontWeight: 600,
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            Aplicar
          </button>
        </div>
      ) : null}

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

      {/* Pareto Chart */}
      <div className="card col-12 chartCard" style={{ minHeight: 380 }}>
        <h2>Gráfico de Pareto</h2>
        <div className="muted" style={{ marginTop: 4 }}>
          Barras = {metricLabel.toLowerCase()} por produto | Linha = % acumulado
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
              {SORT_LABELS[opt]}
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
                yAxisId="val"
                tick={{ fill: "#bbb", fontSize: 10 }}
                tickFormatter={(v: number) => formatMetricValue(v)}
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
                  if (name === metricKey) return [formatMetricValue(value), metricLabel];
                  if (name === "acumulado_pct") return [formatPercent(value), "Acumulado"];
                  return [value, name];
                }}
                labelFormatter={(label: string) => label}
              />
              <Bar
                yAxisId="val"
                dataKey={metricKey}
                name={metricKey}
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
          Todos os produtos classificados pela Curva ABC — ordenados por {metricLabel.toLowerCase()}
        </div>
        <div className="tableScroll" style={{ maxHeight: 400, overflowY: "auto", marginTop: 12 }}>
          <table className="table compact">
            <thead>
              <tr>
                <th>#</th>
                <th>Produto</th>
                <th>Grupo</th>
                <th>{metricLabel}</th>
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
                  <td>{formatMetricValue((item as any)[metricKey])}</td>
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
