"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import AppNav from "../components/AppNav";
import SalesFloorBoard from "../components/SalesFloorBoard";
import ChartTooltip from "../components/ui/ChartTooltip";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel, formatCurrency } from "../lib/format";
import { formatSalesQuantity } from "../lib/sales-quantity.mjs";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { isSalesFloorMode } from "../lib/session";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";

export const dynamic = "force-dynamic";

const MONTH_LABELS = [
  "Jan",
  "Fev",
  "Mar",
  "Abr",
  "Mai",
  "Jun",
  "Jul",
  "Ago",
  "Set",
  "Out",
  "Nov",
  "Dez",
];

export default function SalesPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  // Top grupos como filtro do top produtos (multi-seleção por id_grupo).
  const [selectedGrupoIds, setSelectedGrupoIds] = useState<number[]>([]);
  const grupoFilterKey = selectedGrupoIds.length
    ? [...selectedGrupoIds].sort((a, b) => a - b).join(",")
    : "";
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: grupoFilterKey ? `sales_overview:g:${grupoFilterKey}` : "sales_overview",
      scope,
      errorMessage: "Falha ao carregar vendas",
      keepPreviousData: true,
      buildRequestUrl: (currentScope) => {
        const params = buildScopeParams(currentScope);
        for (const id of selectedGrupoIds) {
          params.append("id_grupos", String(id));
        }
        return `/bi/sales/overview?${params.toString()}`;
      },
    });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const floorMode = useMemo(() => isSalesFloorMode(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("vendas")
    : buildModuleLoadingCopy("vendas");

  const commercial = data?.commercial_kpis || {};
  const hourAgg = useMemo(() => {
    const rows = new Array(24).fill(0).map((_, hora) => ({
      hora: `${hora.toString().padStart(2, "0")}:00`,
      saidas: 0,
    }));
    for (const row of data?.commercial_by_hour || []) {
      const hour = Number(row?.hora || 0);
      if (hour >= 0 && hour < 24) rows[hour].saidas += Number(row?.saidas || 0);
    }
    return rows;
  }, [data]);
  const cfopBreakdown = useMemo(
    () =>
      (data?.cfop_breakdown || []).map((row: any) => ({
        label: row?.label || row?.cfop_class || "Outros",
        ativo: Number(row?.valor_ativo || 0),
        cancelado: Number(row?.valor_cancelado || 0),
      })),
    [data],
  );
  const annualComparison = data?.annual_comparison || {};
  const currentYear = Number(annualComparison?.current_year || 0);
  const previousYear = Number(annualComparison?.previous_year || 0);
  const evolutionSeries = useMemo(() => {
    const annualRows = annualComparison?.months || [];
    if (annualRows.length) {
      return annualRows.map((row: any, index: number) => ({
        mes: MONTH_LABELS[index] || MONTH_LABELS[Number(row?.mes || 1) - 1],
        atual:
          row?.coverage_atual === "ok" && row?.saidas_atual != null
            ? Number(row.saidas_atual || 0)
            : null,
        anterior:
          row?.coverage_anterior === "ok" && row?.saidas_anterior != null
            ? Number(row.saidas_anterior || 0)
            : null,
        coverageAtual: row?.coverage_atual || "ok",
        coverageAnterior: row?.coverage_anterior || "ok",
      }));
    }

    const rows = data?.monthly_evolution || [];
    const latestYear = rows.reduce(
      (max: number, row: any) => Math.max(max, Number(row?.ano || 0)),
      0,
    );
    const priorYear = latestYear ? latestYear - 1 : 0;
    return MONTH_LABELS.map((label, index) => {
      const current = rows.find(
        (row: any) =>
          Number(row?.ano || 0) === latestYear && Number(row?.mes || 0) === index + 1,
      );
      const previous = rows.find(
        (row: any) =>
          Number(row?.ano || 0) === priorYear &&
          Number(row?.mes || 0) === index + 1,
      );
      return {
        mes: label,
        atual: current ? Number(current?.saidas || 0) : null,
        anterior: previous ? Number(previous?.saidas || 0) : null,
        coverageAtual: current ? "ok" : "missing",
        coverageAnterior: previous ? "ok" : "missing",
      };
    });
  }, [annualComparison, data]);

  const hasCommercialData =
    Number(commercial?.saidas || 0) > 0 ||
    Number(commercial?.entradas || 0) > 0 ||
    Number(commercial?.cancelamentos || 0) > 0;
  const hasHourValues = hourAgg.some((row) => Number(row.saidas || 0) > 0);
  const hasEvolution = evolutionSeries.some(
    (row) =>
      (row.atual != null && Number(row.atual) > 0) ||
      (row.anterior != null && Number(row.anterior) > 0),
  );

  const { query: groupsQ, setQuery: setGroupsQ, filteredRows: filteredGroups } = useGridSearch(
    data?.top_groups as Record<string, unknown>[] | undefined,
  );
  // Servidor já filtra por id_grupos; search local só sobre o ranking retornado.
  const { query: productsQ, setQuery: setProductsQ, filteredRows: searchedProducts } = useGridSearch(
    (data?.top_products || []) as Record<string, unknown>[],
  );
  const toggleGrupo = (idGrupo: number) =>
    setSelectedGrupoIds((prev) =>
      prev.includes(idGrupo) ? prev.filter((id) => id !== idGrupo) : [...prev, idGrupo],
    );
  const productsDisplayLimit = selectedGrupoIds.length ? 50 : 15;
  if (floorMode) {
    const devolucoes = Number(
      data?.kpis?.devolucoes || commercial?.entradas || 0,
    );
    const qtdDevolucoes = Number(commercial?.qtd_entradas || 0);
    return (
      <div>
        <AppNav title="Vendas" userLabel={userLabel} />
        <div className="container">
          {error ? (
            <div className="card errorCard" style={{ marginTop: 12 }}>
              {error}
            </div>
          ) : null}
          {!data ? (
            <div style={{ marginTop: 12 }}>
              <ScopeTransitionState
                mode={pendingUnavailable ? "unavailable" : "loading"}
                headline={transitionCopy.headline}
                detail={transitionCopy.detail}
                metrics={3}
                panels={1}
              />
            </div>
          ) : (
            <SalesFloorBoard
              embedded
              title="Vendas"
              logoUrl={claims?.branding?.logo_url || null}
              totals={{
                vendas: Number(commercial?.saidas || 0),
                qtd_vendas: Number(commercial?.qtd_saidas || 0),
                cancelamentos: Number(commercial?.cancelamentos || 0),
                qtd_cancelamentos: Number(commercial?.qtd_cancelamentos || 0),
                devolucoes,
                qtd_devolucoes: qtdDevolucoes,
              }}
              hours={hourAgg}
              loading={loading}
            />
          )}
        </div>
      </div>
    );
  }

  return (
    <div>
      <AppNav title="Vendas" userLabel={userLabel} />
      <div className="container">
        {error ? (
          <div className="card errorCard" style={{ marginTop: 12 }}>
            {error}
          </div>
        ) : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={5}
              panels={4}
            />
          </div>
        ) : (
          <>
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card kpi col-4">
                <div className="label">Vendas</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercial?.saidas)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {Number(commercial?.qtd_saidas || 0)} comprovante(s)
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Cancelamentos</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercial?.cancelamentos)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {Number(commercial?.qtd_cancelamentos || 0)} comprovante(s)
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Devoluções do período</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.devolucoes)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {Number(commercial?.qtd_entradas || 0)} comprovante(s)
                </div>
              </div>

              <div className="card col-12 chartCard">
                <h2>Evolução de vendas</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Comparativo mensal fechado de janeiro a dezembro entre{" "}
                  {previousYear || "o ano anterior"} e {currentYear || "o ano atual"}.
                </div>
                {!loading && !hasEvolution ? (
                  <EmptyState
                    title="Sem série mensal suficiente para comparação."
                    detail="A evolução mensal aparece assim que houver meses comerciais válidos no histórico."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={evolutionSeries}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="mes" stroke="var(--muted)" />
                      <YAxis
                        stroke="var(--muted)"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        content={
                          <ChartTooltip
                            valueFormatter={(value, name, item) => {
                              const payload = item?.payload as
                                | {
                                    coverageAtual?: string;
                                    coverageAnterior?: string;
                                  }
                                | undefined;
                              const coverage =
                                String(name) === String(currentYear)
                                  ? payload?.coverageAtual
                                  : payload?.coverageAnterior;
                              if (
                                coverage === "missing" ||
                                coverage === "future" ||
                                value == null ||
                                value === ""
                              ) {
                                return "—";
                              }
                              return formatCurrency(value);
                            }}
                          />
                        }
                      />
                      <Legend />
                      <Bar
                        dataKey="atual"
                        name={currentYear ? String(currentYear) : "Ano atual"}
                        fill="#22d3ee"
                        radius={[6, 6, 0, 0]}
                      />
                      <Bar
                        dataKey="anterior"
                        name={previousYear ? String(previousYear) : "Ano anterior"}
                        fill="rgba(129,140,248,0.8)"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card kpi col-4">
                <div className="label">Margem analítica</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.margem)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Ticket Médio Produtos</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.ticket_medio)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Ticket Médio Combustível</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.ticket_combustivel?.ticket_medio)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {!loading ? (
                    <>
                      {Number(data?.ticket_combustivel?.qtd_abastecimentos || 0).toLocaleString("pt-BR")}{" "}
                      item(ns)
                      {Number(data?.ticket_combustivel?.valor_total || 0) > 0
                        ? ` · ${formatCurrency(data?.ticket_combustivel?.valor_total)} em combustível`
                        : ""}
                    </>
                  ) : null}
                </div>
              </div>

              <div className="card col-12 chartCard">
                <h2>Vendas por hora</h2>
                {!loading && !hasHourValues ? (
                  <EmptyState
                    title="Sem vendas por hora no período."
                    detail="A distribuição por hora aparece quando existem vendas normais no período."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={hourAgg}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="hora" stroke="var(--muted)" />
                      <YAxis
                        stroke="var(--muted)"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        content={<ChartTooltip valueFormatter={(value) => formatCurrency(value)} />}
                      />
                      <Bar
                        dataKey="saidas"
                        fill="#34d399"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-6">
                <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "baseline", justifyContent: "space-between" }}>
                  <h2>Top grupos</h2>
                  <span className="muted" style={{ fontSize: 12 }}>clique para filtrar os produtos →</span>
                </div>
                {!loading && !(data?.top_groups || []).length ? (
                  <EmptyState
                    title="Sem grupos ranqueados."
                    detail="A agregação por grupo não trouxe produtos ativos suficientes para este período."
                  />
                ) : null}
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap", margin: "8px 0" }}>
                  <button
                    type="button"
                    onClick={() => setSelectedGrupoIds([])}
                    aria-pressed={selectedGrupoIds.length === 0}
                    style={{
                      padding: "4px 12px",
                      borderRadius: 8,
                      fontSize: 12,
                      cursor: "pointer",
                      border: `1px solid ${selectedGrupoIds.length === 0 ? "var(--accent-copper)" : "var(--border)"}`,
                      background: selectedGrupoIds.length === 0 ? "var(--accent-copper-soft)" : "transparent",
                      color: selectedGrupoIds.length === 0 ? "var(--text)" : "var(--muted)",
                    }}
                  >
                    Todos os grupos
                  </button>
                  <GridSearchInput value={groupsQ} onChange={setGroupsQ} />
                </div>
                <div className="tableScroll">
                  <table className="table compact" style={{ minWidth: "max-content", width: "100%" }}>
                    <thead>
                      <tr>
                        <th style={{ whiteSpace: "nowrap" }}>Grupo</th>
                        <th style={{ whiteSpace: "nowrap" }}>Receita</th>
                        <th style={{ whiteSpace: "nowrap" }}>Margem</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredGroups.slice(0, 10).map((g: any) => {
                        const idGrupo = Number(g.id_grupo_produto);
                        const on = selectedGrupoIds.includes(idGrupo);
                        return (
                          <tr
                            key={g.grupo_key || `${g.id_grupo_produto}-${g.grupo_nome}`}
                            onClick={() => toggleGrupo(idGrupo)}
                            style={{ cursor: "pointer", background: on ? "var(--accent-copper-soft)" : undefined }}
                          >
                            <td
                              style={{
                                whiteSpace: "nowrap",
                                color: on ? "var(--accent-copper)" : undefined,
                                fontWeight: on ? 700 : undefined,
                              }}
                            >
                              {g.grupo_nome}
                            </td>
                            <td style={{ whiteSpace: "nowrap" }}>{formatCurrency(g.faturamento)}</td>
                            <td style={{ whiteSpace: "nowrap" }}>{formatCurrency(g.margem)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-6">
                <h2>Top produtos{selectedGrupoIds.length ? ` · ${selectedGrupoIds.length} grupo(s)` : ""}</h2>
                <div style={{ margin: "8px 0" }}>
                  <GridSearchInput value={productsQ} onChange={setProductsQ} />
                </div>
                {!loading && !searchedProducts.length ? (
                  <EmptyState
                    title={selectedGrupoIds.length ? "Sem produtos no(s) grupo(s) selecionado(s)." : "Sem produtos ranqueados."}
                    detail={selectedGrupoIds.length ? "Nenhum produto vendido no(s) grupo(s) escolhido(s) neste período." : "A leitura por item não trouxe produtos ativos para este período."}
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact" style={{ minWidth: "max-content", width: "100%" }}>
                    <thead>
                      <tr>
                        <th style={{ whiteSpace: "nowrap" }}>Produto</th>
                        <th style={{ whiteSpace: "nowrap" }}>Receita</th>
                        <th style={{ whiteSpace: "nowrap" }}>Custo</th>
                        <th style={{ whiteSpace: "nowrap" }}>Margem</th>
                      </tr>
                    </thead>
                    <tbody>
                      {searchedProducts.slice(0, productsDisplayLimit).map((p: any) => (
                        <tr key={p.id_produto}>
                          <td style={{ whiteSpace: "nowrap" }}>
                            <div>{p.produto_nome}</div>
                            <div className="muted" style={{ marginTop: 4 }}>
                              {formatSalesQuantity(p.qtd, p)} · preço médio{" "}
                              {formatCurrency(p.valor_unitario_medio)}
                            </div>
                          </td>
                          <td style={{ whiteSpace: "nowrap" }}>{formatCurrency(p.faturamento)}</td>
                          <td style={{ whiteSpace: "nowrap" }}>{formatCurrency(p.custo_total)}</td>
                          <td style={{ whiteSpace: "nowrap" }}>{formatCurrency(p.margem)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {!loading && !hasCommercialData ? (
                <div className="card col-12">
                  <EmptyState
                    title="Sem movimento comercial relevante no período."
                    detail="A leitura por comprovante não encontrou vendas, entradas ou cancelamentos comerciais no período selecionado."
                  />
                </div>
              ) : null}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
