"use client";

import { useEffect, useMemo, useState } from "react";
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
import EmptyState from "../components/ui/EmptyState";
import ReadingStatusBanner from "../components/ui/ReadingStatusBanner";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel, formatCurrency, formatDateOnly } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import {
  describeDataFreshness,
  describeChurnCoverage,
} from "../lib/reading-copy.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

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
  const [delinquencyPage, setDelinquencyPage] = useState(0);
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
      (data?.top_customers || []).slice(0, 10).map((c: any) => ({
        cliente: c.cliente_nome || `#ID ${c.id_cliente}`,
        faturamento: Number(c.faturamento || 0),
      })),
    [data],
  );
  const anon = data?.anonymous_retention || {};
  const anonKpis = anon?.kpis || {};
  const churnSnapshot = data?.churn_snapshot || {};
  const delinquency = data?.delinquency || {};
  const delinquencyCustomers = delinquency?.customers || [];
  const delinquencyChart = useMemo(
    () =>
      (delinquency?.buckets || []).map((bucket: any) => ({
        bucket: bucket?.label || bucket?.bucket || "Bucket",
        valor: Number(bucket?.valor || 0),
        titulos: Number(bucket?.titulos || 0),
      })),
    [delinquency],
  );
  const delinquencyPageSize = 8;
  const delinquencyPageCount = Math.max(
    1,
    Math.ceil(delinquencyCustomers.length / delinquencyPageSize),
  );
  const delinquencyPageItems = useMemo(() => {
    const safePage = Math.min(delinquencyPage, Math.max(delinquencyPageCount - 1, 0));
    const start = safePage * delinquencyPageSize;
    return delinquencyCustomers.slice(start, start + delinquencyPageSize);
  }, [delinquencyCustomers, delinquencyPage, delinquencyPageCount]);
  const customersBanner =
    describeDataFreshness(data, "clientes")
    || (String(churnSnapshot?.snapshot_status || "").toLowerCase() === "exact"
      ? null
      : describeChurnCoverage(churnSnapshot));

  useEffect(() => {
    setDelinquencyPage(0);
  }, [data?.commercial_coverage?.effective_dt_fim, delinquencyCustomers.length]);

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
            <ReadingStatusBanner
              message={customersBanner}
            />

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
              <div className="card kpi col-4 riskCard">
                <div className="label">Recorrência anônima</div>
                <div className="value">
                  {loading
                    ? "..."
                    : `${Number(anonKpis?.trend_pct || 0).toFixed(1)}%`}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Impacto estimado (7d)</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(anonKpis?.impact_estimated_7d)}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Índice de recorrência anônima</div>
                <div className="value">
                  {loading
                    ? "..."
                    : `${Number(anonKpis?.repeat_proxy_idx || 0).toFixed(1)}%`}
                </div>
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Inadimplência</div>
                <h2 style={{ marginTop: 4 }}>Clientes com maior exposição em aberto</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  A leitura executiva mostra buckets de atraso e prioriza os clientes com maior valor e maior risco.
                </div>
              </div>

              <div className="card kpi col-3 riskCard">
                <div className="label">Clientes em aberto</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.clientes_em_aberto || 0)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Títulos em aberto</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(delinquency?.summary?.titulos_em_aberto || 0)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Valor vencido</div>
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

              <div className="card col-12">
                <h2>Buckets por atraso</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Os buckets mostram concentracao de titulos e valores para orientar a ordem de cobranca.
                </div>
              </div>

              <div className="card kpi col-4 riskCard">
                <div className="label">Bucket 30 dias</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(delinquency?.summary?.valor_30)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {loading ? "..." : `${Number(delinquency?.summary?.titulos_30 || 0)} titulo(s)`}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Bucket 60 dias</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(delinquency?.summary?.valor_60)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {loading ? "..." : `${Number(delinquency?.summary?.titulos_60 || 0)} titulo(s)`}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Bucket 90+ dias</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(delinquency?.summary?.valor_90_plus)}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {loading ? "..." : `${Number(delinquency?.summary?.titulos_90_plus || 0)} titulo(s)`}
                </div>
              </div>

              <div className="card col-12 chartCard">
                <h2>Buckets por atraso</h2>
                {!loading && !delinquencyChart.length ? (
                  <EmptyState
                    title="Sem inadimplência relevante no recorte."
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
                      <XAxis dataKey="bucket" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip formatter={(value: any) => formatCurrency(value)} />
                      <Bar
                        dataKey="valor"
                        fill="#f97316"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-12">
                <div className="panelHead">
                  <div>
                    <h2>Prioridades de cobrança</h2>
                    <div className="muted" style={{ marginTop: 8 }}>
                      Ranking executivo dos clientes com maior pressao em aberto, separado pelos buckets 30, 60 e 90+ dias.
                    </div>
                  </div>
                  {delinquencyCustomers.length > delinquencyPageSize ? (
                    <div className="inlinePager">
                      <button
                        className="btn"
                        type="button"
                        onClick={() => setDelinquencyPage((current) => Math.max(current - 1, 0))}
                        disabled={delinquencyPage <= 0}
                      >
                        Pagina anterior
                      </button>
                      <div className="muted">
                        Pagina {Math.min(delinquencyPage + 1, delinquencyPageCount)} de {delinquencyPageCount}
                      </div>
                      <button
                        className="btn"
                        type="button"
                        onClick={() =>
                          setDelinquencyPage((current) => Math.min(current + 1, delinquencyPageCount - 1))
                        }
                        disabled={delinquencyPage >= delinquencyPageCount - 1}
                      >
                        Proxima pagina
                      </button>
                    </div>
                  ) : null}
                </div>
                {!loading && !(delinquency?.customers || []).length ? (
                  <EmptyState
                    title="Sem clientes em atraso para priorizar."
                    detail="Quando houver recebíveis vencidos, os maiores riscos aparecem aqui."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Cliente</th>
                        <th>Bucket</th>
                        <th>Titulos 30</th>
                        <th>Titulos 60</th>
                        <th>Titulos 90+</th>
                        <th>Valores 30</th>
                        <th>Valores 60</th>
                        <th>Valores 90+</th>
                        <th>Titulos totais</th>
                        <th>Valor total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {delinquencyPageItems.map((item: any) => (
                        <tr key={item.id_cliente}>
                          <td>{item.cliente_nome}</td>
                          <td>{item.bucket_label}</td>
                          <td>{item.titulos_30}</td>
                          <td>{item.titulos_60}</td>
                          <td>{item.titulos_90_plus}</td>
                          <td>{formatCurrency(item.valor_30)}</td>
                          <td>{formatCurrency(item.valor_60)}</td>
                          <td>{formatCurrency(item.valor_90_plus)}</td>
                          <td>{item.titulos_totais}</td>
                          <td>{formatCurrency(item.valor_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <h2>Risco de churn (top 10)</h2>
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
                {!loading && !(data?.churn_top || []).length ? (
                  <EmptyState
                    title="Nenhum cliente em risco relevante."
                    detail="A base identificada não trouxe sinais fortes de churn para este recorte."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Cliente</th>
                      <th>Score</th>
                      <th>Última compra</th>
                      <th>Sinal principal</th>
                      <th>Compras 30d</th>
                      <th>Compras 60-30d</th>
                      <th>Fat. 30d</th>
                      <th>Fat. 60-30d</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data?.churn_top || []).map((c: any) => (
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
                </table>
              </div>

              <div className="card col-7 chartCard">
                <h2>Top clientes por faturamento</h2>
                {!loading && !topChart.length ? (
                  <EmptyState
                    title="Sem clientes identificados com faturamento."
                    detail="A filial não trouxe clientes nomeados para este recorte."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={topChart}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="cliente" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        formatter={(value: any) => formatCurrency(value)}
                      />
                      <Bar
                        dataKey="faturamento"
                        fill="#818cf8"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-5">
                <h2>Top clientes</h2>
                {!loading && !(data?.top_customers || []).length ? (
                  <EmptyState
                    title="Sem top clientes no período."
                    detail="Não houve base identificada suficiente para ranqueamento."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Cliente</th>
                      <th>Compras</th>
                      <th>Ticket</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data?.top_customers || []).slice(0, 10).map((c: any) => (
                      <tr key={c.id_cliente}>
                        <td>{c.cliente_nome}</td>
                        <td>{c.compras}</td>
                        <td>{formatCurrency(c.ticket_medio)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card col-12">
                <h2>Radar de recorrência anônima</h2>
                <div className="muted" style={{ marginBottom: 8 }}>
                  {loading
                    ? "..."
                    : anonKpis?.recommendation ||
                      "Sem leitura adicional para o período."}
                </div>
                {!loading && !(anon?.breakdown_dow || []).length ? (
                  <EmptyState
                    title="Sem leitura anônima suficiente neste recorte."
                    detail="A integração ainda não trouxe volume confiável para comparar recorrência sem identificação nominal."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Dia da semana</th>
                      <th>Atual</th>
                      <th>Período anterior</th>
                      <th>Tendência</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(anon?.breakdown_dow || []).map((r: any) => (
                      <tr key={r.dow}>
                        <td>{r.dow}</td>
                        <td>{formatCurrency(r.anon_current)}</td>
                        <td>{formatCurrency(r.anon_prev)}</td>
                        <td>{Number(r.trend_pct || 0).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="muted">
                Recorrência, churn e oportunidades de reativação da base, com
                leitura própria de comportamento do cliente e sem misturar
                sinais de caixa ou cancelamento operacional.
              </div>
              {!loading ? (
                <div style={{ marginTop: 10, fontWeight: 700 }}>
                  {describeChurnCoverage(churnSnapshot)}
                </div>
              ) : null}
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="muted">
                A recorrência anônima compara o movimento recente de clientes
                sem identificação nominal com a semana comparável anterior.
                Quando o percentual fica negativo, a frequência caiu. Quando
                sobe, a rotina de retorno ficou mais forte. O índice de
                recorrência junta estabilidade e repetição do fluxo para mostrar
                onde vale agir primeiro.
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
