"use client";

import { useMemo } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  Cell,
  CartesianGrid,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKeyShort,
  formatTurnoLabel,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { describeFinanceCoverage } from "../lib/reading-copy.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

export const dynamic = "force-dynamic";

export default function FinancePage() {
  const scope = useScopeQuery();
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "finance_overview",
      scope,
      errorMessage: "Falha ao carregar financeiro",
      buildRequestUrl: (currentScope) =>
        `/bi/finance/overview?${buildScopeParams(currentScope).toString()}&include_operational=false`,
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("financeiro")
    : buildModuleLoadingCopy("financeiro");

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

  const chartData = useMemo(
    () =>
      (data?.by_day || []).map((r: any) => ({
        data: formatDateKeyShort(r.data_key),
        aberto: Number(r.valor_aberto || 0),
        pago: Number(r.valor_pago || 0),
      })),
    [data],
  );

  const hasFinance = useMemo(() => (data?.by_day || []).length > 0, [data]);
  const paymentsKpis = data?.payments?.kpis || {};
  const paymentMixTotal = Number(paymentsKpis.total_valor || 0);
  const paymentsByDay = useMemo(() => {
    const totals = new Map<string, number>();
    for (const row of data?.payments?.by_day || []) {
      const total = Number(row?.total_valor || 0);
      const key = String(row?.data_key || "");
      if (!key || total <= 0) continue;
      totals.set(key, (totals.get(key) || 0) + total);
    }
    return Array.from(totals.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([dataKey, valor]) => ({
        data: formatDateKeyShort(dataKey),
        valor,
      }));
  }, [data]);
  const paymentsByTurno = useMemo(
    () =>
      (data?.payments?.by_turno || [])
        .filter((r: any) => Number(r.total_valor || 0) > 0)
        .map((r: any) => ({
          ...r,
          share_pct:
            paymentMixTotal > 0
              ? (Number(r.total_valor || 0) / paymentMixTotal) * 100
              : 0,
        })),
    [data, paymentMixTotal],
  );
  const paymentsAnomalies = data?.payments?.anomalies || [];
  const aging = data?.aging || {};
  const financeDefinitions = data?.definitions || {};
  const businessClock = data?.business_clock || {};
  const receberVencido = Number(aging.receber_total_vencido || 0);
  const pagarVencido = Number(aging.pagar_total_vencido || 0);
  const cashPressure = receberVencido + pagarVencido;
  const top5Concentration = Number(aging.top5_concentration_pct || 0);
  const actionHeadline =
    receberVencido >= pagarVencido && receberVencido > 0
      ? "Cobrar os recebíveis vencidos mais concentrados."
      : pagarVencido > 0
        ? "Renegociar compromissos vencidos para proteger o caixa."
        : top5Concentration >= 45
          ? "Desconcentrar a carteira antes que o risco aumente."
          : "Manter disciplina de cobrança e acompanhamento diário.";
  const actionDetail =
    receberVencido >= pagarVencido && receberVencido > 0
      ? "Priorize os maiores títulos vencidos e a filial com mais caixa travado."
      : pagarVencido > 0
        ? "Reordene pagamentos, preserve caixa operacional e trate os maiores vencidos primeiro."
        : top5Concentration >= 45
          ? "Os maiores títulos já pesam demais na exposição atual e pedem ação preventiva."
          : "Sem ruptura material no período, mas o cockpit continua focado em pressão, atraso e concentração.";
  const paymentMixPreview = (paymentsKpis?.mix || [])
    .slice(0, 3)
    .map(
      (item: any) =>
        `${item.category_label || item.label || item.category}: ${formatCurrency(item.total_valor)}`,
    )
    .join(" · ");
  const paymentMixChart = useMemo(() => {
    const rows = (paymentsKpis?.mix || []).filter(
      (item: any) => Number(item.total_valor || 0) > 0,
    );
    const topRows = rows.slice(0, 5).map((item: any) => ({
      label: item.category_label || item.label || item.category,
      value: Number(item.total_valor || 0),
    }));
    const othersValue = rows
      .slice(5)
      .reduce((acc: number, item: any) => acc + Number(item.total_valor || 0), 0);
    if (othersValue > 0) {
      topRows.push({ label: "Outras formas", value: othersValue });
    }
    return topRows;
  }, [paymentsKpis]);
  const topPaymentMethod = paymentMixChart[0];
  const paymentsStatus = String(paymentsKpis?.source_status || "unavailable");
  const paymentMixColors = [
    "#38bdf8",
    "#34d399",
    "#f59e0b",
    "#818cf8",
    "#fb7185",
    "#94a3b8",
  ];

  return (
    <div>
      <AppNav title="Financeiro" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={6}
              panels={4}
            />
          </div>
        ) : (
          <>
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card col-6">
                <h2>Ação prioritária</h2>
                <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>
                  {loading ? "..." : actionHeadline}
                </div>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    {actionDetail}
                  </div>
                ) : null}
              </div>

              <div className="card kpi col-3">
                <div className="label">Receber em aberto</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.receber_aberto)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Receber vencido</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(receberVencido)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Pagar em aberto</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(data?.kpis?.pagar_aberto)}
                </div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Pagar vencido</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(pagarVencido)}
                </div>
              </div>

              <div className="card col-4">
                <h2>Pressão imediata de caixa</h2>
                <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>
                  {loading ? "..." : formatCurrency(cashPressure)}
                </div>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    {cashPressure > 0
                      ? "Vencidos que já exigem cobrança, renegociação ou reordenação de pagamentos."
                      : aging?.data_gaps
                        ? "A leitura mostrada não encontrou exposição aberta relevante na referência efetiva."
                        : "Sem concentração crítica de vencidos no período analisado."}
                  </div>
                ) : null}
              </div>

              <div className="card col-4">
                <h2>Concentração da carteira</h2>
                <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>
                  {loading ? "..." : `${top5Concentration.toFixed(1)}%`}
                </div>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    {top5Concentration > 0
                      ? "Os 5 maiores títulos concentram esse percentual da exposição atual."
                      : "A carteira segue distribuída, sem concentração material no topo."}
                  </div>
                ) : null}
              </div>

              <div className="card col-4">
                <h2>Leitura dos pagamentos</h2>
                <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>
                  {loading ? "..." : formatCurrency(paymentsKpis.total_valor)}
                </div>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    {paymentsKpis.summary ||
                      paymentMixPreview ||
                      "Sem movimento financeiro conciliado no período."}
                  </div>
                ) : null}
              </div>
            </div>
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card col-12 chartCard">
                <h2>Fluxo por vencimento</h2>
                {!loading && !hasFinance ? (
                  <p className="muted">
                    Sem dados financeiros para o período selecionado.
                  </p>
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="data" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        formatter={(value: any) => formatCurrency(value)}
                      />
                      <Area
                        type="monotone"
                        dataKey="pago"
                        stackId="1"
                        stroke="#22d3ee"
                        fill="#22d3ee"
                        fillOpacity={0.35}
                      />
                      <Area
                        type="monotone"
                        dataKey="aberto"
                        stackId="1"
                        stroke="#fb7185"
                        fill="#fb7185"
                        fillOpacity={0.35}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card kpi col-4">
                <div className="label">Variação dos pagamentos</div>
                <div className="value">
                  {loading
                    ? "..."
                    : `${Number(paymentsKpis.delta_pct || 0).toFixed(1)}%`}
                </div>
                {!loading ? (
                  <div className="muted">
                    Comparação com o período imediatamente anterior.
                  </div>
                ) : null}
              </div>
              <div className="card kpi col-4" id="payment-mapping">
                <div className="label">Pagamentos sem classificação</div>
                <div className="value">
                  {loading
                    ? "..."
                    : `${Number(paymentsKpis.unknown_share_pct || 0).toFixed(1)}%`}
                </div>
                {!loading ? (
                  <div className="muted">
                    Parcela ainda sem correspondência oficial de meio de
                    pagamento na leitura recebida.
                  </div>
                ) : null}
              </div>
              <div className="card kpi col-4">
                <div className="label">Forma líder do período</div>
                <div className="value" style={{ fontSize: 22 }}>
                  {loading
                    ? "..."
                    : topPaymentMethod?.label || "Sem liderança"}
                </div>
                {!loading ? (
                  <div className="muted">
                    {topPaymentMethod
                      ? `${formatCurrency(topPaymentMethod.value)} no período selecionado.`
                      : "Sem concentração material entre as formas conciliadas."}
                  </div>
                ) : null}
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Meios e formas de pagamento</div>
                <h2 style={{ marginTop: 4 }}>Leitura conciliada do período</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Volume diário agregado, mix por forma e concentração por turno com rótulos executivos.
                </div>
              </div>

              <div className="card col-7 chartCard">
                <h2>Volume recebido por dia</h2>
                {!loading && paymentsStatus === "value_gap" ? (
                  <EmptyState
                    title="Valores de pagamentos ainda em validação da carga."
                    detail="Os registros da operação chegaram, mas a leitura monetária ainda não está estável o bastante para decisão."
                  />
                ) : null}
                {!loading &&
                paymentsStatus !== "value_gap" &&
                !paymentsByDay.length ? (
                  <EmptyState
                    title="Sem pagamentos recebidos no período."
                    detail="A consolidação diária de pagamentos ainda não trouxe movimento para este período."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={paymentsByDay}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="data" stroke="#9fb0d0" />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        formatter={(value: any) => formatCurrency(value)}
                      />
                      <Bar
                        dataKey="valor"
                        fill="#60a5fa"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-5 chartCard">
                <h2>Mix do período por forma</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Legenda clara para separar o que já está mapeado do que ainda exige saneamento.
                </div>
                {!loading && paymentsStatus === "value_gap" ? (
                  <EmptyState
                    title="Mix monetário aguardando validação."
                    detail="A composição por forma aparece assim que a leitura monetária dos pagamentos estiver estável."
                  />
                ) : null}
                {!loading &&
                paymentsStatus !== "value_gap" &&
                !paymentMixChart.length ? (
                  <EmptyState
                    title="Sem mix conciliado no período."
                    detail="A legenda aparece quando há pagamentos monetizados no período."
                  />
                ) : null}
                <div className="chartWrap" style={{ height: 230 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={paymentMixChart}
                        dataKey="value"
                        nameKey="label"
                        innerRadius={56}
                        outerRadius={88}
                        paddingAngle={3}
                      >
                        {paymentMixChart.map((entry: any, index: number) => (
                          <Cell
                            key={`${entry.label}-${index}`}
                            fill={paymentMixColors[index % paymentMixColors.length]}
                          />
                        ))}
                      </Pie>
                      <Tooltip formatter={(value: any) => formatCurrency(value)} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                  {paymentMixChart.map((item: any, index: number) => (
                    <div
                      key={`${item.label}-${index}`}
                      style={{
                        display: "grid",
                        gridTemplateColumns: "10px minmax(0, 1fr) auto",
                        gap: 10,
                        alignItems: "center",
                      }}
                    >
                      <span
                        style={{
                          width: 10,
                          height: 10,
                          borderRadius: 999,
                          background: paymentMixColors[index % paymentMixColors.length],
                        }}
                      />
                      <span className="muted" style={{ fontSize: 12 }}>
                        {item.label}
                      </span>
                      <strong>{formatCurrency(item.value)}</strong>
                    </div>
                  ))}
                </div>
              </div>

              <div className="card col-7">
                <h2>Turnos e formas com maior exposição</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Ranking consolidado do período, sem repetir uma linha por dia.
                </div>
                {!loading && paymentsStatus === "value_gap" ? (
                  <EmptyState
                    title="A leitura por turno ainda não está confiável."
                    detail="Os vínculos chegaram da operação, mas o valor monetário ainda está em correção no pipeline."
                  />
                ) : null}
                {!loading &&
                paymentsStatus !== "value_gap" &&
                !paymentsByTurno.length ? (
                  <EmptyState
                    title="Sem leitura por turno no período."
                    detail="A fonte de pagamentos por turno ainda não retornou registros para o período selecionado."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Filial</th>
                        <th>Turno</th>
                        <th>Forma</th>
                        <th>Valor</th>
                        <th>Share</th>
                        <th>Dias</th>
                        <th>Comprovantes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paymentsByTurno.slice(0, 15).map((r: any, idx: number) => (
                        <tr
                          key={`${r.id_filial}-${r.id_turno}-${r.category}-${idx}`}
                        >
                          <td>{r.filial_label}</td>
                          <td>{formatTurnoLabel(r.id_turno, r.turno_label)}</td>
                          <td>{r.category_label || r.label || r.category}</td>
                          <td>{formatCurrency(r.total_valor)}</td>
                          <td>{Number(r.share_pct || 0).toFixed(1)}%</td>
                          <td>{Number(r.dias_com_movimento || 0)}</td>
                          <td>{Number(r.qtd_comprovantes || 0)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-5">
                <h2>Sinais de pagamento fora do padrão</h2>
                {!loading && paymentsStatus === "value_gap" ? (
                  <EmptyState
                    title="Motor de anomalias aguardando valor monetário confiável."
                    detail="Enquanto a carga monetária de pagamentos estiver em correção, esta leitura fica em monitoramento técnico."
                  />
                ) : null}
                {!loading &&
                paymentsStatus !== "value_gap" &&
                !paymentsAnomalies.length ? (
                  <EmptyState
                    title="Sem anomalias relevantes no período."
                    detail="A leitura de pagamentos seguiu estável neste período."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Evento</th>
                      <th>Severidade</th>
                      <th>Score</th>
                      <th>Impacto</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paymentsAnomalies
                      .slice(0, 10)
                      .map((a: any, idx: number) => (
                        <tr key={`${a.insight_id || a.event_type}-${idx}`}>
                          <td>{a.event_label || a.event_type}</td>
                          <td>{a.severity}</td>
                          <td>{Number(a.score || 0)}</td>
                          <td>{formatCurrency(a.impacto_estimado)}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="muted">
                Cockpit financeiro para decidir cobrança, renegociação,
                concentração e pressão imediata de caixa sem misturar
                turnos/caixas abertos do módulo operacional.
              </div>
              {!loading ? (
                <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
                  <div style={{ fontWeight: 700 }}>
                    {describeFinanceCoverage(aging)}
                  </div>
                  <div className="muted">
                    <strong>Data-base do negócio:</strong>{" "}
                    {businessClock.business_date || scope.dt_fim || "-"}{" "}
                    {businessClock.timezone
                      ? `(${businessClock.timezone})`
                      : ""}
                  </div>
                </div>
              ) : null}
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="muted">
                Receber em aberto e pagar em aberto cobrem os títulos que ainda
                estão dentro do prazo, enquanto vencido é o que já extrapolou. A
                pressão imediata de caixa mostra o quanto dessas posições
                vencidas exigem ação urgente para evitar gap financeiro.
              </div>
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="panelHead">
                <h2>Glossário auditável</h2>
                <span className="muted">
                  Origem, cálculo e impacto de cada KPI principal.
                </span>
              </div>
              <div style={{ display: "grid", gap: 10, marginTop: 12 }}>
                {Object.entries(financeDefinitions).map(
                  ([key, item]: [string, any]) => (
                    <div
                      key={key}
                      style={{
                        padding: "12px 14px",
                        borderRadius: 16,
                        border: "1px solid rgba(148,163,184,0.14)",
                        background: "rgba(7,18,31,0.36)",
                      }}
                    >
                      <div style={{ fontWeight: 800 }}>{item.label}</div>
                      <div className="muted" style={{ marginTop: 4 }}>
                        <strong>Como calcula:</strong> {item.formula}
                      </div>
                      <div className="muted" style={{ marginTop: 4 }}>
                        <strong>Origem:</strong> {item.source}
                      </div>
                      <div className="muted" style={{ marginTop: 4 }}>
                        <strong>Por que importa:</strong> {item.impact}
                      </div>
                    </div>
                  ),
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
