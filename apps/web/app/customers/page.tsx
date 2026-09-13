"use client";

import { useEffect, useMemo, useState, Fragment } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import AppNav from "../components/AppNav";
import CategoryRankChart from "../components/ui/CategoryRankChart";
import ChartTooltip from "../components/ui/ChartTooltip";
import EmptyState from "../components/ui/EmptyState";
import GridChrome from "../components/ui/GridChrome";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import SortableTh from "../components/ui/SortableTh";
import { compareGridRows } from "../lib/grid-sort";
import { useRecordGrid } from "../lib/use-record-grid";
import { apiGet } from "../lib/api";
import { buildUserLabel, formatCurrency, formatDateOnly } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { rowMatchesGridSearch, useGridSearch } from "../lib/use-grid-search";
import { canViewSensitiveFinancials } from "../lib/session";

export const dynamic = "force-dynamic";

function buildChurnSignal(customer: any) {
  const reasons = customer?.reasons || {};
  const recencyDays = Number(reasons.recency_days || 0);
  const expectedCycleDays = Number(reasons.expected_cycle_days || 0);
  const frequencyDrop = Number(reasons.frequency_drop || 0);
  const monetaryDrop = Number(reasons.monetary_drop || 0);
  const compras30 = Number(customer?.compras_30d || 0);
  const comprasPrev = Number(customer?.compras_60_30 || 0);
  const faturamento30 = Number(customer?.faturamento_30d || 0);
  const faturamentoPrev = Number(customer?.faturamento_60_30 || 0);

  if (expectedCycleDays > 0 && recencyDays > expectedCycleDays * 2) {
    return "Não voltou no intervalo esperado para a rotina do posto.";
  }
  if (comprasPrev > 0 && compras30 === 0) {
    return "Deixou de retornar no ciclo recente e pede reativação comercial.";
  }
  if (frequencyDrop >= 15) {
    return "Reduziu a frequência de visitas nas últimas semanas.";
  }
  if (comprasPrev > compras30 && compras30 > 0) {
    return "Perdeu ritmo de compra em relação ao padrão anterior.";
  }
  if (monetaryDrop >= 20) {
    return "Perdeu força de ticket médio e merece reativação comercial.";
  }
  if (faturamentoPrev > faturamento30 && faturamento30 > 0) {
    return "Reduziu gasto no posto e merece abordagem personalizada.";
  }
  return (
    customer?.recommendation ||
    "Vale retomar contato e monitorar a próxima visita."
  );
}

export default function CustomersPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const [delinquencySort, setDelinquencySort] = useState<"gravity" | "valor" | "atraso" | "comprando">("gravity");
  // Filtro-sobre-filtro: postos selecionados nos cards (vazio = todos os postos do escopo).
  const [selectedFiliais, setSelectedFiliais] = useState<Set<number>>(new Set());
  const [precoFixoPage, setPrecoFixoPage] = useState(0);
  const [precoFixoLoading, setPrecoFixoLoading] = useState(false);
  const [precoFixoData, setPrecoFixoData] = useState<any>(null);
  const [precoFixoExpanded, setPrecoFixoExpanded] = useState<string | null>(null);
  const [precoFixoDetail, setPrecoFixoDetail] = useState<any>(null);
  const [precoFixoDetailLoading, setPrecoFixoDetailLoading] = useState(false);
  const [precoFixoDetailQ, setPrecoFixoDetailQ] = useState("");
  const [inativosDays, setInativosDays] = useState<15 | 30 | 60>(30);
  const [inativosLoading, setInativosLoading] = useState(false);
  const [inativosData, setInativosData] = useState<any>(null);
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "customers_overview",
      scope,
      errorMessage: "Falha ao carregar clientes",
      buildRequestUrl: (currentScope) =>
        `/bi/customers/overview?${buildScopeParams(currentScope).toString()}`,
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("clientes")
    : buildModuleLoadingCopy("clientes");

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

  const topChart = useMemo(
    () =>
      (data?.top_customers || []).slice(0, 10).map((c: any, index: number) => ({
        id: Number(c.id_cliente) > 0 ? `${c.id_cliente}:${index}` : `cliente:${index}`,
        name: String(c.cliente_nome || "").trim() || `Cliente #${c.id_cliente || index + 1}`,
        value: Number(c.faturamento || 0),
      })),
    [data],
  );
  const churnSnapshot = data?.churn_snapshot || {};
  const delinquency = data?.delinquency || {};
  const delinquencyByFilial = delinquency?.by_filial || [];
  const showFilialColumn = delinquencyByFilial.length > 1;
  const delinquencyCustomers = useMemo(() => {
    let customers = [...(delinquency?.customers || [])];
    if (selectedFiliais.size > 0) {
      customers = customers.filter((c: any) => selectedFiliais.has(Number(c.id_filial)));
    }
    switch (delinquencySort) {
      case "valor":
        customers.sort((a: any, b: any) => (b.valor_total_aberto || (b.valor_total_vencido || 0) + (b.valor_a_vencer || 0)) - (a.valor_total_aberto || (a.valor_total_vencido || 0) + (a.valor_a_vencer || 0)));
        break;
      case "atraso":
        customers.sort((a: any, b: any) => (b.max_dias_atraso || 0) - (a.max_dias_atraso || 0));
        break;
      case "comprando":
        customers.sort((a: any, b: any) => {
          const aCompras = a.compras_30d || 0;
          const bCompras = b.compras_30d || 0;
          if (bCompras !== aCompras) return bCompras - aCompras;
          return (b.valor_total_vencido || 0) - (a.valor_total_vencido || 0);
        });
        break;
      default: // gravity - default from API
        break;
    }
    return customers;
  }, [delinquency?.customers, delinquencySort, selectedFiliais]);
  const { query: delinquencySearch, setQuery: setDelinquencySearch, filteredRows: filteredDelinquency } =
    useGridSearch(delinquencyCustomers as Record<string, unknown>[]);
  const delinquencyGrid = useRecordGrid<any>({
    rows: filteredDelinquency,
    resetKey: `${delinquencySearch}:${delinquencySort}:${selectedFiliais.size}`,
    getTieId: (row) => `${row.id_filial ?? 0}-${row.id_cliente}`,
    defaultCompare: (a, b) => {
      const ia = filteredDelinquency.indexOf(a);
      const ib = filteredDelinquency.indexOf(b);
      return ia - ib;
    },
    summableKeys: [
      "valor_ate_30d",
      "valor_acima_30d",
      "valor_total_vencido",
      "valor_a_vencer",
      "valor_total_aberto",
    ],
    columns: {
      cliente_nome: { type: "text" },
      filial_label: { type: "text" },
      titulos_ate_30d: { type: "number" },
      valor_ate_30d: { type: "number" },
      titulos_acima_30d: { type: "number" },
      valor_acima_30d: { type: "number" },
      valor_total_vencido: { type: "number", getValue: (r) => r.valor_total_vencido || ((r.valor_ate_30d || 0) + (r.valor_acima_30d || 0)) },
      titulos_a_vencer: { type: "number" },
      valor_a_vencer: { type: "number" },
      valor_total_aberto: { type: "number", getValue: (r) => r.valor_total_aberto || ((r.valor_total_vencido || 0) + (r.valor_a_vencer || 0)) },
      max_dias_atraso: { type: "number" },
      compras_30d: { type: "number" },
    },
  });
  const delinquencyChart = useMemo(
    () =>
      (delinquency?.buckets || []).map((bucket: any) => ({
        bucket: bucket?.label || bucket?.bucket || "Bucket",
        valor: Number(bucket?.valor || 0),
        titulos: Number(bucket?.titulos || 0),
      })),
    [delinquency],
  );
  // Limpa a selecao de postos quando o escopo/janela muda (respeita o filtro global).
  useEffect(() => {
    setSelectedFiliais(new Set());
  }, [data?.commercial_coverage?.effective_dt_fim]);

  useEffect(() => {
    setPrecoFixoPage(0);
    setPrecoFixoExpanded(null);
    setPrecoFixoDetail(null);
    setPrecoFixoDetailQ("");
  }, [scope.dt_ini, scope.dt_fim, scope.id_empresa, scope.id_filial, scope.id_filiais]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setPrecoFixoLoading(true);
      try {
        const params = buildScopeParams(scope);
        params.set("page", String(precoFixoPage));
        params.set("page_size", "30");
        const res = await apiGet(`/bi/customers/preco-fixo?${params.toString()}`);
        if (!cancelled) setPrecoFixoData(res);
      } catch {
        if (!cancelled) {
          setPrecoFixoData({
            items: [],
            total: 0,
            page: 0,
            page_size: 30,
            total_pages: 0,
            summary: { clientes: 0, desconto_total: 0 },
          });
        }
      } finally {
        if (!cancelled) setPrecoFixoLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [scope, precoFixoPage]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setInativosLoading(true);
      try {
        const params = buildScopeParams(scope);
        params.set("days_without", String(inativosDays));
        const res = await apiGet(`/bi/customers/preco-fixo/inativos?${params.toString()}`);
        if (!cancelled) setInativosData(res);
      } catch {
        if (!cancelled) setInativosData({ items: [], total: 0, days_without: inativosDays });
      } finally {
        if (!cancelled) setInativosLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [scope, inativosDays]);

  const openPrecoFixoDetail = async (row: any) => {
    const key = `${row.id_filial}-${row.id_entidade}`;
    if (precoFixoExpanded === key) {
      setPrecoFixoExpanded(null);
      setPrecoFixoDetail(null);
      return;
    }
    setPrecoFixoExpanded(key);
    setPrecoFixoDetailLoading(true);
    setPrecoFixoDetail(null);
    setPrecoFixoDetailQ("");
    try {
      const params = buildScopeParams(scope);
      params.set("id_filial", String(row.id_filial));
      params.set("id_entidade", String(row.id_entidade));
      params.set("page", "0");
      // Exceção: detalhe preço fixo page_size 200 — não cortar grupo/subtotal.
      params.set("page_size", "200");
      const res = await apiGet(`/bi/customers/preco-fixo/detail?${params.toString()}`);
      setPrecoFixoDetail(res);
    } catch {
      setPrecoFixoDetail({ items: [], total: 0, summary: { desconto_total: 0 } });
    } finally {
      setPrecoFixoDetailLoading(false);
    }
  };

  const precoFixoItems = precoFixoData?.items || [];
  const { query: precoFixoSearch, setQuery: setPrecoFixoSearch, filteredRows: filteredPrecoFixo } =
    useGridSearch(precoFixoItems as Record<string, unknown>[]);
  const precoFixoPageCount = Math.max(1, Number(precoFixoData?.total_pages || 1));
  const precoFixoTotal = Number(precoFixoData?.total || 0);
  const precoFixoRangeFrom = precoFixoTotal === 0 ? 0 : precoFixoPage * 30 + 1;
  const precoFixoRangeTo = Math.min((precoFixoPage + 1) * 30, precoFixoTotal);
  const inativosItems = (inativosData?.items || []) as any[];
  const inativosGrid = useRecordGrid<any>({
    rows: inativosItems,
    resetKey: inativosDays,
    getTieId: (row) => `${row.id_filial}-${row.id_entidade}-${row.id_produto}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.ultima_compra, nome: a.cliente_nome },
        { filial: b.filial_label ?? b.id_filial, data: b.ultima_compra, nome: b.cliente_nome },
      ),
    columns: {
      filial_label: { type: "text" },
      cliente_nome: { type: "text" },
      ultima_compra: { type: "date" },
      dias_sem: { type: "number" },
      produto_nome: { type: "text" },
    },
  });
  const churnTopRows = useMemo(
    () => (Array.isArray(data?.churn_top) ? data.churn_top : []) as Record<string, unknown>[],
    [data?.churn_top],
  );
  const { query: churnQ, setQuery: setChurnQ, filteredRows: filteredChurn } = useGridSearch(churnTopRows);
  const churnGrid = useRecordGrid<any>({
    rows: filteredChurn,
    resetKey: churnQ,
    getTieId: (row) => row.id_cliente,
    defaultCompare: (a, b) => Number(b.churn_score || 0) - Number(a.churn_score || 0),
    summableKeys: ["compras_30d", "compras_60_30", "faturamento_30d", "faturamento_60_30"],
    columns: {
      cliente_nome: { type: "text" },
      churn_score: { type: "number" },
      last_purchase: { type: "date" },
      compras_30d: { type: "number" },
      compras_60_30: { type: "number" },
      faturamento_30d: { type: "number" },
      faturamento_60_30: { type: "number" },
    },
  });
  const topCustomersRows = useMemo(
    () =>
      (Array.isArray(data?.top_customers) ? data.top_customers : [])
        .slice(0, 10) as Record<string, unknown>[],
    [data?.top_customers],
  );
  const { query: topCustomersQ, setQuery: setTopCustomersQ, filteredRows: filteredTopCustomers } =
    useGridSearch(topCustomersRows);
  const topCustomersGrid = useRecordGrid<any>({
    rows: filteredTopCustomers,
    resetKey: topCustomersQ,
    getTieId: (row) => row.id_cliente,
    defaultCompare: (a, b) => Number(b.compras || 0) - Number(a.compras || 0),
    summableKeys: ["compras"],
    columns: {
      cliente_nome: { type: "text" },
      compras: { type: "number" },
      ticket_medio: { type: "number" },
    },
  });
  return (
    <div>
      <AppNav title="Análise de Clientes" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={7}
              panels={4}
            />
          </div>
        ) : (
          <>
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card kpi col-3">
                <div className="label">Clientes identificados</div>
                <div className="value">
                  {loading ? "..." : (data?.rfm?.clientes_identificados ?? 0)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Ativos 7d</div>
                <div className="value">
                  {loading ? "..." : (data?.rfm?.ativos_7d ?? 0)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Em risco 30d</div>
                <div className="value">
                  {loading ? "..." : (data?.rfm?.em_risco_30d ?? 0)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Fat. 90d</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.rfm?.faturamento_90d)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Clientes em atraso</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.clientes_em_aberto || 0)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Títulos vencidos</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.titulos_em_aberto || 0)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Total vencido</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(delinquency?.summary?.valor_total)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Maior atraso</div>
                <div className="value">
                  {loading
                    ? "..."
                    : `${Number(delinquency?.summary?.max_dias_atraso || 0)} dias`}
                </div>
              </div>

              <div className="card kpi col-4">
                <div className="label">Títulos a vencer</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.titulos_a_vencer || 0)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Valor a vencer</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(delinquency?.summary?.valor_a_vencer)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Clientes a vencer</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.clientes_a_vencer || 0)}
                </div>
              </div>

              <div className="card col-6 chartCard">
                <h2>Distribuição por faixa de atraso</h2>
                {!loading && !delinquencyChart.length ? (
                  <EmptyState
                    title="Sem inadimplência relevante no período."
                    detail="Os buckets aparecem assim que houver contas a receber vencidas na rede."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={delinquencyChart}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="bucket" stroke="var(--muted)" />
                      <YAxis
                        stroke="var(--muted)"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        content={<ChartTooltip valueFormatter={(value) => formatCurrency(value)} />}
                      />
                      <Bar
                        dataKey="valor"
                        fill="#f97316"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
              <div className="card col-6">
                <h2>Faixas em atraso</h2>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 10, marginTop: 12 }}>
                  {(delinquency?.buckets || []).map((b: any) => (
                    <div className="card kpi riskCard" key={b?.bucket || b?.label}>
                      <div className="label">{`Vencido ${b?.label || b?.bucket || ""}`}</div>
                      <div className="value">{loading ? "..." : formatCurrency(Number(b?.valor || 0))}</div>
                      <div className="muted" style={{ marginTop: 8 }}>
                        {loading ? "..." : `${Number(b?.titulos || 0)} título(s)`}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="card col-12">
                <div className="panelHead">
                  <div>
                    <h2>Prioridades de cobrança</h2>
                    <div className="muted" style={{ marginTop: 8 }}>
                      Clientes inadimplentes ordenados por gravidade. Mostra títulos vencidos (até 30d e 30+) e títulos ainda a vencer desses mesmos clientes — se está em atraso, não deveria estar comprando.
                    </div>
                    <div style={{ marginTop: 8, display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {([
                        { key: "gravity", label: "Gravidade" },
                        { key: "valor", label: "Maior valor" },
                        { key: "atraso", label: "Maior atraso" },
                        { key: "comprando", label: "Atrasado comprando" },
                      ] as const).map((opt) => (
                        <button
                          key={opt.key}
                          type="button"
                          className="btn"
                          style={{
                            fontSize: 12,
                            padding: "4px 10px",
                            background: delinquencySort === opt.key ? "var(--color-accent, #3b82f6)" : undefined,
                            color: delinquencySort === opt.key ? "var(--on-accent)" : undefined,
                            borderColor: delinquencySort === opt.key ? "var(--color-accent, #3b82f6)" : undefined,
                          }}
                          onClick={() => setDelinquencySort(opt.key)}
                        >
                          {opt.label}
                        </button>
                      ))}
                    </div>
                    <div style={{ marginTop: 8 }}>
                      <GridSearchInput
                        value={delinquencySearch}
                        onChange={setDelinquencySearch}
                      />
                    </div>
                  </div>
                </div>
                {!loading && !(delinquency?.customers || []).length ? (
                  <EmptyState
                    title="Sem clientes em atraso para priorizar."
                    detail="Quando houver recebíveis vencidos, os maiores riscos aparecem aqui."
                  />
                ) : null}
                {!loading && delinquencyCustomers.length > 0 && filteredDelinquency.length === 0 ? (
                  <EmptyState
                    title="Nenhum cliente encontrado para a busca."
                    detail={`Nenhum resultado para "${delinquencySearch}" nas prioridades de cobrança.`}
                  />
                ) : null}
                {showFilialColumn ? (
                  <div style={{ marginBottom: 12 }}>
                    <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                      Dívida vencida por posto (clique para filtrar o ranking; o mesmo cliente pode dever em mais de um posto):
                    </div>
                    <div style={{ display: "flex", flexWrap: "nowrap", gap: 8, overflowX: "auto", paddingBottom: 6 }}>
                      <button
                        type="button"
                        onClick={() => setSelectedFiliais(new Set())}
                        aria-pressed={selectedFiliais.size === 0}
                        style={{
                          textAlign: "left",
                          cursor: "pointer",
                          flex: "0 0 auto",
                          border: selectedFiliais.size === 0 ? "1px solid var(--accent-copper)" : "1px solid var(--border)",
                          borderRadius: 10,
                          padding: "8px 12px",
                          background: selectedFiliais.size === 0 ? "var(--accent-copper-soft)" : "var(--surface-faint)",
                          color: "inherit",
                          minWidth: 120,
                        }}
                      >
                        <div className="muted" style={{ fontSize: 11 }}>Todos os postos</div>
                        <div style={{ fontWeight: 700 }}>{formatCurrency(delinquency?.summary?.valor_total)}</div>
                        <div className="muted" style={{ fontSize: 11 }}>
                          {Number(delinquency?.summary?.clientes_em_aberto || 0)} cliente(s)
                        </div>
                      </button>
                      {delinquencyByFilial.map((f: any) => {
                        const fid = Number(f.id_filial);
                        const active = selectedFiliais.has(fid);
                        return (
                          <button
                            type="button"
                            key={f.id_filial}
                            onClick={() =>
                              setSelectedFiliais((prev) => {
                                const next = new Set(prev);
                                if (next.has(fid)) next.delete(fid);
                                else next.add(fid);
                                return next;
                              })
                            }
                            aria-pressed={active}
                            style={{
                              textAlign: "left",
                              cursor: "pointer",
                              flex: "0 0 auto",
                              border: active ? "1px solid var(--accent-copper)" : "1px solid var(--border)",
                              borderRadius: 10,
                              padding: "8px 12px",
                              background: active ? "var(--accent-copper-soft)" : "var(--surface-faint)",
                              color: "inherit",
                              minWidth: 168,
                            }}
                          >
                            <div className="muted" style={{ fontSize: 11 }}>{f.filial_label}</div>
                            <div style={{ fontWeight: 700 }}>{formatCurrency(f.valor_vencido)}</div>
                            <div className="muted" style={{ fontSize: 11 }}>
                              {f.clientes} cliente(s) · aberto {formatCurrency(f.valor_aberto)}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <SortableTh label="Cliente" sortKey="cliente_nome" ariaSort={delinquencyGrid.ariaSort("cliente_nome")} onToggle={delinquencyGrid.toggleSort} />
                        {showFilialColumn ? (
                          <SortableTh label="Filial" sortKey="filial_label" ariaSort={delinquencyGrid.ariaSort("filial_label")} onToggle={delinquencyGrid.toggleSort} />
                        ) : null}
                        <SortableTh label="Até 30d" sortKey="titulos_ate_30d" ariaSort={delinquencyGrid.ariaSort("titulos_ate_30d")} onToggle={delinquencyGrid.toggleSort} />
                        <SortableTh label="R$ até 30d" sortKey="valor_ate_30d" ariaSort={delinquencyGrid.ariaSort("valor_ate_30d")} onToggle={delinquencyGrid.toggleSort} align="right" />
                        <SortableTh label="30+ dias" sortKey="titulos_acima_30d" ariaSort={delinquencyGrid.ariaSort("titulos_acima_30d")} onToggle={delinquencyGrid.toggleSort} />
                        <SortableTh label="R$ 30+" sortKey="valor_acima_30d" ariaSort={delinquencyGrid.ariaSort("valor_acima_30d")} onToggle={delinquencyGrid.toggleSort} align="right" />
                        <SortableTh label="Total vencido" sortKey="valor_total_vencido" ariaSort={delinquencyGrid.ariaSort("valor_total_vencido")} onToggle={delinquencyGrid.toggleSort} align="right" />
                        <SortableTh label="A vencer" sortKey="titulos_a_vencer" ariaSort={delinquencyGrid.ariaSort("titulos_a_vencer")} onToggle={delinquencyGrid.toggleSort} />
                        <SortableTh label="R$ a vencer" sortKey="valor_a_vencer" ariaSort={delinquencyGrid.ariaSort("valor_a_vencer")} onToggle={delinquencyGrid.toggleSort} align="right" />
                        <SortableTh label="Total aberto" sortKey="valor_total_aberto" ariaSort={delinquencyGrid.ariaSort("valor_total_aberto")} onToggle={delinquencyGrid.toggleSort} align="right" />
                        <SortableTh label="Maior atraso" sortKey="max_dias_atraso" ariaSort={delinquencyGrid.ariaSort("max_dias_atraso")} onToggle={delinquencyGrid.toggleSort} />
                        <SortableTh label="Compras 30d" sortKey="compras_30d" ariaSort={delinquencyGrid.ariaSort("compras_30d")} onToggle={delinquencyGrid.toggleSort} />
                      </tr>
                    </thead>
                    <tbody>
                      {delinquencyGrid.slice.map((item: any) => {
                        const totalVencido = item.valor_total_vencido || ((item.valor_ate_30d || 0) + (item.valor_acima_30d || 0));
                        const totalAberto = item.valor_total_aberto || (totalVencido + (item.valor_a_vencer || 0));
                        return (
                        <tr key={`${item.id_filial ?? 0}-${item.id_cliente}`}>
                          <td>{item.cliente_nome}</td>
                          {showFilialColumn ? <td>{item.filial_label || "—"}</td> : null}
                          <td>{item.titulos_ate_30d ?? 0}</td>
                          <td>{formatCurrency(item.valor_ate_30d ?? 0)}</td>
                          <td style={{ fontWeight: (item.titulos_acima_30d || 0) > 0 ? 700 : 400, color: (item.titulos_acima_30d || 0) > 0 ? 'var(--color-negative)' : undefined }}>{item.titulos_acima_30d ?? 0}</td>
                          <td style={{ color: (item.valor_acima_30d || 0) > 0 ? 'var(--color-negative)' : undefined }}>{formatCurrency(item.valor_acima_30d ?? 0)}</td>
                          <td style={{ fontWeight: 700 }}>{formatCurrency(totalVencido)}</td>
                          <td style={{ fontWeight: (item.titulos_a_vencer || 0) > 0 ? 700 : 400, color: (item.titulos_a_vencer || 0) > 0 ? 'var(--color-warning)' : undefined }}>{item.titulos_a_vencer || 0}</td>
                          <td style={{ color: (item.valor_a_vencer || 0) > 0 ? 'var(--color-warning)' : undefined }}>{formatCurrency(item.valor_a_vencer || 0)}</td>
                          <td style={{ fontWeight: 700 }}>{formatCurrency(totalAberto)}</td>
                          <td>{item.max_dias_atraso}d</td>
                          <td style={{ fontWeight: (item.compras_30d || 0) > 0 ? 700 : 400, color: (item.compras_30d || 0) > 0 ? 'var(--color-warning)' : undefined }}>{item.compras_30d || 0}</td>
                        </tr>
                        );
                      })}
                    </tbody>
                    {delinquencyGrid.slice.length ? (
                      <tfoot>
                        <tr>
                          <td colSpan={showFilialColumn ? 3 : 2}>Total da página ({delinquencyGrid.slice.length})</td>
                          <td>{formatCurrency(delinquencyGrid.pageTotals.valor_ate_30d)}</td>
                          <td />
                          <td>{formatCurrency(delinquencyGrid.pageTotals.valor_acima_30d)}</td>
                          <td>{formatCurrency(delinquencyGrid.pageTotals.valor_total_vencido)}</td>
                          <td />
                          <td>{formatCurrency(delinquencyGrid.pageTotals.valor_a_vencer)}</td>
                          <td>{formatCurrency(delinquencyGrid.pageTotals.valor_total_aberto)}</td>
                          <td colSpan={2} />
                        </tr>
                      </tfoot>
                    ) : null}
                  </table>
                </div>
                <GridChrome
                  page={delinquencyGrid.page}
                  totalPages={delinquencyGrid.totalPages}
                  total={delinquencyGrid.total}
                  from={delinquencyGrid.range.from}
                  to={delinquencyGrid.range.to}
                  onPrev={delinquencyGrid.onPrev}
                  onNext={delinquencyGrid.onNext}
                  onResetOrder={delinquencyGrid.resetOrder}
                  isDefaultOrder={delinquencyGrid.isDefaultOrder}
                />
              </div>

                            <div className="card col-12">
                <div className="panelHead">
                  <div>
                    <h2>Clientes com preço fixo</h2>
                    <div className="muted" style={{ marginTop: 8 }}>
                      Desconto econômico implícito em combustível no período do filtro
                      (preço da bomba do dia menos o preço pago pelo cliente).
                    </div>
                  </div>
                </div>
                <div style={{ marginTop: 12, display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
                  <div className="muted" style={{ fontSize: 13 }}>
                    {precoFixoLoading
                      ? "Carregando…"
                      : `${Number(precoFixoData?.summary?.clientes || 0)} cliente(s) · ${Number(precoFixoData?.summary?.qtd_litros || 0).toLocaleString("pt-BR", { maximumFractionDigits: 0 })} L · total ${formatCurrency(precoFixoData?.summary?.desconto_total || 0)}`}
                  </div>
                  <GridSearchInput value={precoFixoSearch} onChange={setPrecoFixoSearch} />
                </div>
                {!precoFixoLoading && precoFixoItems.length === 0 ? (
                  <EmptyState
                    title="Nenhum cliente com preço fixo no período."
                    detail="Quando houver vendas de combustível abaixo do preço da bomba para clientes cadastrados com valor fixo, o acumulado aparece aqui."
                  />
                ) : null}
                {precoFixoItems.length > 0 ? (
                  <>
                    <div className="tableScroll" style={{ marginTop: 12 }}>
                      <table className="table compact">
                        <thead>
                          <tr>
                            {/* Paginação server-side (30); ordem da API — não reordenar só a página. */}
                            <th>Filial</th>
                            <th>Cliente</th>
                            <th>Vendas</th>
                            <th>Litros</th>
                            <th>Desconto acumulado</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filteredPrecoFixo.map((row: any) => {
                            const key = `${row.id_filial}-${row.id_entidade}`;
                            const open = precoFixoExpanded === key;
                            return (
                              <Fragment key={key}>
                                <tr
                                  onClick={() => void openPrecoFixoDetail(row)}
                                  style={{ cursor: "pointer" }}
                                  aria-expanded={open}
                                >
                                  <td>{row.filial_label || "—"}</td>
                                  <td>{row.cliente_nome}</td>
                                  <td>{row.qtd_vendas ?? 0}</td>
                                  <td>{Number(row.qtd_litros || 0).toLocaleString("pt-BR", { maximumFractionDigits: 1 })}</td>
                                  <td style={{ fontWeight: 700 }}>{formatCurrency(row.desconto_total)}</td>
                                </tr>
                                {open ? (
                                  <tr>
                                    <td colSpan={5} style={{ padding: 12, background: "var(--surface-faint)" }}>
                                      {precoFixoDetailLoading ? (
                                        <div className="muted">Carregando detalhe…</div>
                                      ) : !(precoFixoDetail?.items || []).length ? (
                                        <div className="muted">Sem itens no período.</div>
                                      ) : (
                                        <div className="tableScroll">
                                          <div style={{ marginBottom: 8 }}>
                                            <GridSearchInput
                                              value={precoFixoDetailQ}
                                              onChange={setPrecoFixoDetailQ}
                                              aria-label="Pesquisar itens de preço fixo"
                                            />
                                          </div>
                                          <table className="table compact">
                                            <thead>
                                              <tr>
                                                <th>Data</th>
                                                <th>Documento</th>
                                                <th>Produto</th>
                                                <th>Preço bomba</th>
                                                <th>Preço cliente</th>
                                                <th>Qtd</th>
                                                <th>Desconto</th>
                                                {canViewSensitiveFinancials(claims) ? <th>Margem</th> : null}
                                              </tr>
                                            </thead>
                                            <tbody>
                                              {(precoFixoDetail?.items || [])
                                                .filter((item: any) => rowMatchesGridSearch(item, precoFixoDetailQ))
                                                .map((item: any, idx: number) => {
                                                if (item.row_kind === "subtotal") {
                                                  return (
                                                    <tr
                                                      key={`sub-${item.id_produto}-${idx}`}
                                                      style={{ background: "var(--filter-bg)", fontWeight: 700 }}
                                                    >
                                                      <td colSpan={3}>Subtotal {item.produto_nome || `#${item.id_produto}`}</td>
                                                      <td>—</td>
                                                      <td>—</td>
                                                      <td>{Number(item.qtd || 0).toFixed(3)}</td>
                                                      <td>{formatCurrency(item.desconto_total)}</td>
                                                      {canViewSensitiveFinancials(claims) ? <td>—</td> : null}
                                                    </tr>
                                                  );
                                                }
                                                return (
                                                  <tr key={`${item.id_comprovante}-${item.id_itemcomprovante}-${idx}`}>
                                                    <td>{formatDateOnly(item.dt_venda)}</td>
                                                    <td>{item.documento_label || "—"}</td>
                                                    <td>{item.produto_nome || `#${item.id_produto}`}</td>
                                                    <td>{formatCurrency(item.preco_bomba)}</td>
                                                    <td>{formatCurrency(item.preco_pago)}</td>
                                                    <td>{Number(item.qtd || 0).toFixed(3)}</td>
                                                    <td style={{ fontWeight: 700 }}>{formatCurrency(item.desconto_total)}</td>
                                                    {canViewSensitiveFinancials(claims) ? (
                                                      <td>
                                                        {item.margem_unitaria_pct == null
                                                          ? "—"
                                                          : `${Number(item.margem_unitaria_pct).toFixed(1)}%`}
                                                      </td>
                                                    ) : null}
                                                  </tr>
                                                );
                                              })}
                                            </tbody>
                                          </table>
                                        </div>
                                      )}
                                    </td>
                                  </tr>
                                ) : null}
                              </Fragment>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <GridChrome
                      page={precoFixoPage + 1}
                      totalPages={precoFixoPageCount}
                      total={precoFixoTotal}
                      from={precoFixoRangeFrom}
                      to={precoFixoRangeTo}
                      onPrev={() => setPrecoFixoPage((p) => Math.max(0, p - 1))}
                      onNext={() => setPrecoFixoPage((p) => Math.min(p + 1, precoFixoPageCount - 1))}
                      onResetOrder={() => setPrecoFixoPage(0)}
                      isDefaultOrder={precoFixoPage === 0}
                    />
                  </>
                ) : null}
              </div>

              <div className="card col-12" style={{ marginTop: 12 }}>
                <div className="panelHead">
                  <div>
                    <h2>Clientes com preço fixo sem abastecimento</h2>
                  </div>
                </div>
                <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                  {([15, 30, 60] as const).map((d) => (
                    <button
                      key={d}
                      type="button"
                      className="btn"
                      aria-pressed={inativosDays === d}
                      onClick={() => setInativosDays(d)}
                      style={{
                        opacity: inativosDays === d ? 1 : 0.7,
                        borderColor: inativosDays === d ? "var(--accent-copper, #b8722c)" : undefined,
                      }}
                    >
                      {d} dias
                    </button>
                  ))}
                  <span className="muted" style={{ fontSize: 12 }}>
                    {inativosLoading
                      ? "Carregando…"
                      : `${Number(inativosData?.total || 0)} cliente(s)`}
                  </span>
                </div>
                {!inativosLoading && !(inativosData?.items || []).length ? (
                  <EmptyState
                    title="Nenhum alerta neste período."
                    detail="Clientes com preço fixo ativo e abastecimento recente não aparecem aqui."
                  />
                ) : (
                  <div className="tableScroll" style={{ marginTop: 10 }}>
                    <table className="table compact">
                      <thead>
                        <tr>
                          <SortableTh label="Filial" sortKey="filial_label" ariaSort={inativosGrid.ariaSort("filial_label")} onToggle={inativosGrid.toggleSort} />
                          <SortableTh label="Cliente" sortKey="cliente_nome" ariaSort={inativosGrid.ariaSort("cliente_nome")} onToggle={inativosGrid.toggleSort} />
                          <SortableTh label="Última compra" sortKey="ultima_compra" ariaSort={inativosGrid.ariaSort("ultima_compra")} onToggle={inativosGrid.toggleSort} />
                          <SortableTh label="Dias sem comprar" sortKey="dias_sem" ariaSort={inativosGrid.ariaSort("dias_sem")} onToggle={inativosGrid.toggleSort} />
                          <SortableTh label="Produto / regra" sortKey="produto_nome" ariaSort={inativosGrid.ariaSort("produto_nome")} onToggle={inativosGrid.toggleSort} />
                        </tr>
                      </thead>
                      <tbody>
                        {inativosGrid.slice.map((row: any) => (
                          <tr key={`${row.id_filial}-${row.id_entidade}-${row.id_produto}`}>
                            <td>{row.filial_label || "—"}</td>
                            <td>{row.cliente_nome || "—"}</td>
                            <td>{row.ultima_compra ? formatDateOnly(row.ultima_compra) : "—"}</td>
                            <td>{row.dias_sem ?? "—"}</td>
                            <td>
                              {row.produto_nome || "—"}
                              {row.valor_fixo
                                ? ` · ${formatCurrency(Number(row.valor_fixo))}`
                                : ""}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <GridChrome
                      page={inativosGrid.page}
                      totalPages={inativosGrid.totalPages}
                      total={inativosGrid.total}
                      from={inativosGrid.range.from}
                      to={inativosGrid.range.to}
                      onPrev={inativosGrid.onPrev}
                      onNext={inativosGrid.onNext}
                      onResetOrder={inativosGrid.resetOrder}
                      isDefaultOrder={inativosGrid.isDefaultOrder}
                    />
                  </div>
                )}
              </div>

<div className="card col-12">
                <h2>Clientes em risco de saída</h2>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    Data-base pedida:{" "}
                    {formatDateOnly(
                      churnSnapshot?.requested_dt_ref || claims?.server_today,
                    )}
                    . Leitura usada:{" "}
                    {formatDateOnly(
                      churnSnapshot?.effective_dt_ref ||
                        churnSnapshot?.requested_dt_ref ||
                        claims?.server_today,
                    )}
                    .
                  </div>
                ) : null}
                <div style={{ margin: "8px 0" }}>
                  <GridSearchInput value={churnQ} onChange={setChurnQ} />
                </div>
                {!loading && !churnTopRows.length ? (
                  <EmptyState
                    title="Nenhum cliente em risco relevante."
                    detail="A base identificada não trouxe sinais fortes de saída para este período."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <SortableTh label="Cliente" sortKey="cliente_nome" ariaSort={churnGrid.ariaSort("cliente_nome")} onToggle={churnGrid.toggleSort} />
                      <SortableTh label="Score" sortKey="churn_score" ariaSort={churnGrid.ariaSort("churn_score")} onToggle={churnGrid.toggleSort} />
                      <SortableTh label="Última compra" sortKey="last_purchase" ariaSort={churnGrid.ariaSort("last_purchase")} onToggle={churnGrid.toggleSort} />
                      <th>Sinal principal</th>
                      <SortableTh label="Compras 30d" sortKey="compras_30d" ariaSort={churnGrid.ariaSort("compras_30d")} onToggle={churnGrid.toggleSort} />
                      <SortableTh label="Compras 60-30d" sortKey="compras_60_30" ariaSort={churnGrid.ariaSort("compras_60_30")} onToggle={churnGrid.toggleSort} />
                      <SortableTh label="Fat. 30d" sortKey="faturamento_30d" ariaSort={churnGrid.ariaSort("faturamento_30d")} onToggle={churnGrid.toggleSort} align="right" />
                      <SortableTh label="Fat. 60-30d" sortKey="faturamento_60_30" ariaSort={churnGrid.ariaSort("faturamento_60_30")} onToggle={churnGrid.toggleSort} align="right" />
                    </tr>
                  </thead>
                  <tbody>
                    {churnGrid.slice.map((c: any) => (
                      <tr key={c.id_cliente}>
                        <td>{c.cliente_nome}</td>
                        <td>
                          <span
                            className={`badge ${Number(c.churn_score || 0) >= 80 ? "warn" : "ok"}`}
                          >
                            {c.churn_score}
                          </span>
                        </td>
                        <td>{formatDateOnly(c.last_purchase)}</td>
                        <td>{buildChurnSignal(c)}</td>
                        <td>{c.compras_30d}</td>
                        <td>{c.compras_60_30}</td>
                        <td>{formatCurrency(c.faturamento_30d)}</td>
                        <td>{formatCurrency(c.faturamento_60_30)}</td>
                      </tr>
                    ))}
                  </tbody>
                  {churnGrid.slice.length ? (
                    <tfoot>
                      <tr>
                        <td colSpan={4}>Total da página ({churnGrid.slice.length})</td>
                        <td>{churnGrid.pageTotals.compras_30d}</td>
                        <td>{churnGrid.pageTotals.compras_60_30}</td>
                        <td>{formatCurrency(churnGrid.pageTotals.faturamento_30d)}</td>
                        <td>{formatCurrency(churnGrid.pageTotals.faturamento_60_30)}</td>
                      </tr>
                    </tfoot>
                  ) : null}
                </table>
                <GridChrome
                  page={churnGrid.page}
                  totalPages={churnGrid.totalPages}
                  total={churnGrid.total}
                  from={churnGrid.range.from}
                  to={churnGrid.range.to}
                  onPrev={churnGrid.onPrev}
                  onNext={churnGrid.onNext}
                  onResetOrder={churnGrid.resetOrder}
                  isDefaultOrder={churnGrid.isDefaultOrder}
                />
              </div>

              <div className="card col-7 chartCard">
                <h2>Maiores clientes por faturamento</h2>
                {!loading && !topChart.length ? (
                  <EmptyState
                    title="Sem clientes identificados com faturamento."
                    detail="A filial não trouxe clientes nomeados para este período."
                  />
                ) : null}
                <CategoryRankChart
                  kind="category"
                  barFill="#818cf8"
                  axisWidth={168}
                  rows={topChart}
                  axisFormatter={(value) => formatCurrency(value)}
                  valueFormatter={(value) => formatCurrency(value)}
                />
              </div>

              <div className="card col-5">
                <h2>Maiores clientes</h2>
                <div style={{ margin: "8px 0" }}>
                  <GridSearchInput value={topCustomersQ} onChange={setTopCustomersQ} />
                </div>
                {!loading && !topCustomersRows.length ? (
                  <EmptyState
                    title="Sem top clientes no período."
                    detail="Não houve base identificada suficiente para ranqueamento."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <SortableTh label="Cliente" sortKey="cliente_nome" ariaSort={topCustomersGrid.ariaSort("cliente_nome")} onToggle={topCustomersGrid.toggleSort} />
                      <SortableTh label="Compras" sortKey="compras" ariaSort={topCustomersGrid.ariaSort("compras")} onToggle={topCustomersGrid.toggleSort} />
                      <SortableTh label="Ticket" sortKey="ticket_medio" ariaSort={topCustomersGrid.ariaSort("ticket_medio")} onToggle={topCustomersGrid.toggleSort} align="right" />
                    </tr>
                  </thead>
                  <tbody>
                    {topCustomersGrid.slice.map((c: any) => (
                      <tr key={c.id_cliente}>
                        <td>{c.cliente_nome}</td>
                        <td>{c.compras}</td>
                        <td>{formatCurrency(c.ticket_medio)}</td>
                      </tr>
                    ))}
                  </tbody>
                  {topCustomersGrid.slice.length ? (
                    <tfoot>
                      <tr>
                        <td>Total da página ({topCustomersGrid.slice.length})</td>
                        <td>{topCustomersGrid.pageTotals.compras}</td>
                        <td />
                      </tr>
                    </tfoot>
                  ) : null}
                </table>
                <GridChrome
                  page={topCustomersGrid.page}
                  totalPages={topCustomersGrid.totalPages}
                  total={topCustomersGrid.total}
                  from={topCustomersGrid.range.from}
                  to={topCustomersGrid.range.to}
                  onPrev={topCustomersGrid.onPrev}
                  onNext={topCustomersGrid.onNext}
                  onResetOrder={topCustomersGrid.resetOrder}
                  isDefaultOrder={topCustomersGrid.isDefaultOrder}
                />
              </div>

            </div>
          </>
        )}
      </div>
    </div>
  );
}
