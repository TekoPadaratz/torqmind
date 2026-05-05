"use client";

import { useMemo } from "react";
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
import EmptyState from "../components/ui/EmptyState";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel, formatCurrency } from "../lib/format";
import { formatSalesQuantity } from "../lib/sales-quantity.mjs";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

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
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "sales_overview",
      scope,
      errorMessage: "Falha ao carregar vendas",
      buildRequestUrl: (currentScope) =>
        `/bi/sales/overview?${buildScopeParams(currentScope).toString()}`,
    });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
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
        atual: Number(row?.saidas_atual || 0),
        anterior: Number(row?.saidas_anterior || 0),
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
        atual: Number(current?.saidas || 0),
        anterior: Number(previous?.saidas || 0),
      };
    });
  }, [annualComparison, data]);

  const hasCommercialData =
    Number(commercial?.saidas || 0) > 0 ||
    Number(commercial?.entradas || 0) > 0 ||
    Number(commercial?.cancelamentos || 0) > 0;
  const hasHourValues = hourAgg.some((row) => Number(row.saidas || 0) > 0);
  const hasEvolution = evolutionSeries.some(
    (row) => Number(row.atual || 0) > 0 || Number(row.anterior || 0) > 0,
  );

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
              <div className="card col-12">
                <div className="sectionEyebrow">Resumo comercial</div>
                <h2 style={{ marginTop: 4 }}>Vendas, entradas e cancelamentos por comprovante</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  A tela usa comprovantes comerciais e separa movimentos válidos, entradas e cancelamentos.
                  Margem e ticket seguem abaixo pela leitura por item.
                </div>
              </div>

              <div className="card kpi col-4">
                <div className="label">Vendas normais</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercial?.saidas)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {Number(commercial?.qtd_saidas || 0)} comprovante(s)
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Entradas registradas</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercial?.entradas)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {Number(commercial?.qtd_entradas || 0)} comprovante(s)
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

              <div className="card col-8 chartCard">
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
                      <XAxis dataKey="mes" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip formatter={(value: any) => formatCurrency(value)} />
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

              <div className="card col-4">
                <h2>Classificação por CFOP</h2>
                {!loading && !cfopBreakdown.length ? (
                  <EmptyState
                    title="Sem classificação comercial no período."
                    detail="A classificação aparece quando há comprovantes válidos com CFOP comercial no período."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Classe</th>
                      <th>Ativo</th>
                      <th>Cancelado</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cfopBreakdown.map((row: any) => (
                      <tr key={row.label}>
                        <td>{row.label}</td>
                        <td>{formatCurrency(row.ativo)}</td>
                        <td>{formatCurrency(row.cancelado)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card kpi col-4">
                <div className="label">Margem analítica</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.margem)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  Calculada pelos itens dos comprovantes.
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Ticket médio analítico</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.ticket_medio)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  Receita média por documento válido no período analisado.
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Devoluções do período</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.devolucoes)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  Valor tratado como devolução na leitura por item.
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
                      <XAxis dataKey="hora" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip formatter={(value: any) => formatCurrency(value)} />
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
                <h2>Top produtos</h2>
                {!loading && !(data?.top_products || []).length ? (
                  <EmptyState
                    title="Sem produtos ranqueados."
                    detail="A leitura por item não trouxe produtos ativos para este período."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Produto</th>
                        <th>Receita</th>
                        <th>Custo</th>
                        <th>Margem</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.top_products || []).slice(0, 10).map((p: any) => (
                        <tr key={p.id_produto}>
                          <td>
                            <div>{p.produto_nome}</div>
                            <div className="muted" style={{ marginTop: 4 }}>
                              {formatSalesQuantity(p.qtd, p)} · preço médio{" "}
                              {formatCurrency(p.valor_unitario_medio)}
                            </div>
                          </td>
                          <td>{formatCurrency(p.faturamento)}</td>
                          <td>{formatCurrency(p.custo_total)}</td>
                          <td>{formatCurrency(p.margem)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-6">
                <h2>Top grupos</h2>
                {!loading && !(data?.top_groups || []).length ? (
                  <EmptyState
                    title="Sem grupos ranqueados."
                    detail="A agregação por grupo não trouxe produtos ativos suficientes para este período."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Grupo</th>
                        <th>Fat.</th>
                        <th>Margem</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.top_groups || []).slice(0, 10).map((g: any) => (
                        <tr key={g.grupo_key || `${g.id_grupo_produto}-${g.grupo_nome}`}>
                          <td>{g.grupo_nome}</td>
                          <td>{formatCurrency(g.faturamento)}</td>
                          <td>{formatCurrency(g.margem)}</td>
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
