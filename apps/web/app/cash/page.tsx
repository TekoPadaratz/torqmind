"use client";

import { useMemo } from "react";
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
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateTime,
  formatHoursLabel,
  formatTurnoLabel,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { describeDataFreshness } from "../lib/reading-copy.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

export const dynamic = "force-dynamic";

function severityTone(value: string) {
  const severity = String(value || "").toUpperCase();
  if (severity === "CRITICAL") {
    return { bg: "rgba(239, 68, 68, 0.14)", border: "rgba(248, 113, 113, 0.32)" };
  }
  if (severity === "HIGH") {
    return { bg: "rgba(245, 158, 11, 0.14)", border: "rgba(251, 191, 36, 0.28)" };
  }
  if (severity === "WARN") {
    return { bg: "rgba(56, 189, 248, 0.14)", border: "rgba(96, 165, 250, 0.28)" };
  }
  return { bg: "rgba(52, 211, 153, 0.12)", border: "rgba(74, 222, 128, 0.24)" };
}

function formatStockQuantity(value: any) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return numeric.toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 3,
  });
}

export default function CashPage() {
  const scope = useScopeQuery();
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "cash_overview",
      scope,
      errorMessage: "Falha ao carregar o módulo de Caixa",
      buildRequestUrl: (currentScope) =>
        `/bi/cash/overview?${buildScopeParams(currentScope).toString()}`,
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("caixa")
    : buildModuleLoadingCopy("caixa");

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const historical = data?.historical || {};
  const commercial = data?.commercial || {};
  const commercialKpis = commercial?.kpis || {};
  const liveNow = data?.live_now || {};
  const liveKpis = liveNow?.kpis || {};
  const dreSummary = data?.dre_summary || {};
  const paymentMix = historical?.payment_mix || [];
  const commercialByDay = commercial?.by_day || [];
  const topTurnos = commercial?.top_turnos || [];
  const openBoxes = liveNow?.open_boxes || data?.open_boxes || [];
  const staleBoxes = liveNow?.stale_boxes || data?.stale_boxes || [];
  const alerts = liveNow?.alerts || data?.alerts || [];
  const paymentMixChartHeight = Math.max(280, paymentMix.length * 44);

  return (
    <div>
      <AppNav title="Caixa" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={5}
              panels={5}
            />
          </div>
        ) : (
          <>
            <ReadingStatusBanner
              message={describeDataFreshness(data, "caixa")}
            />

            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div
                className="card col-12"
                style={{
                  background:
                    "linear-gradient(135deg, rgba(14,116,144,0.22), rgba(15,23,42,0.92) 45%, rgba(16,185,129,0.16))",
                  borderColor: "rgba(56, 189, 248, 0.24)",
                }}
              >
                <div className="sectionEyebrow">Visão comercial e financeira</div>
                <h2 style={{ marginTop: 4 }}>Caixa do período e caixa agora</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  {commercial?.summary || data?.summary}
                </div>
                <div className="muted" style={{ marginTop: 8 }}>
                  {liveNow?.summary || "Leitura dos turnos indisponível no momento."}
                </div>
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Caixa do período selecionado</div>
                <h2 style={{ marginTop: 4 }}>Vendas, cancelamentos e recebimentos</h2>
              </div>

              <div className="card kpi col-3">
                <div className="label">Vendas no período</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercialKpis?.total_vendas)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Cancelamentos</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(commercialKpis?.total_cancelamentos)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Recebimentos</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercialKpis?.total_pagamentos)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Saldo comercial</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercialKpis?.saldo_comercial)}
                </div>
              </div>

              <div className="card col-5">
                <h2>Fluxo de Caixa / DRE resumido</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Consolida o que entrou, o que saiu e o que ainda depende de composição financeira.
                </div>
                <div className="bi-grid" style={{ marginTop: 12 }}>
                  {(dreSummary?.cards || []).map((card: any) => (
                    <div
                      key={card.key}
                      className="card kpi col-12"
                      style={
                        String(card?.status || "").toLowerCase() === "unavailable"
                          ? {
                              background: "rgba(148,163,184,0.08)",
                              borderColor: "rgba(148,163,184,0.18)",
                            }
                          : undefined
                      }
                    >
                      <div className="label">{card.label}</div>
                      <div className="value">
                        {card.amount === null || card.amount === undefined
                          ? "Aguardando base"
                          : formatCurrency(card.amount)}
                      </div>
                      {card.quantity !== null && card.quantity !== undefined ? (
                        <div className="muted" style={{ marginTop: 8 }}>
                          Posição: {formatStockQuantity(card.quantity)}
                        </div>
                      ) : null}
                      <div className="muted" style={{ marginTop: 8 }}>
                        {card.detail}
                      </div>
                    </div>
                  ))}
                  {(dreSummary?.pending || []).map((item: any) => (
                    <div key={item.key} className="card col-12" style={{ opacity: 0.78 }}>
                      <div className="label">{item.label}</div>
                      <div className="muted" style={{ marginTop: 8 }}>
                        {item.detail}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="card col-7 chartCard">
                <h2>Formas de pagamento do período</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Distribuição conciliada dos recebimentos que sustentam o resumo financeiro ao lado.
                </div>
                {!loading && !paymentMix.length ? (
                  <EmptyState
                    title="Sem pagamentos conciliados no período."
                    detail="A distribuição por forma aparece quando existem recebimentos conciliados no recorte."
                  />
                ) : null}
                <div
                  style={{
                    maxHeight: 520,
                    overflowY: paymentMix.length > 6 ? "auto" : "visible",
                    paddingRight: paymentMix.length > 6 ? 6 : 0,
                  }}
                >
                  <div className="chartWrap" style={{ height: paymentMixChartHeight }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={paymentMix} layout="vertical">
                        <CartesianGrid
                          stroke="rgba(255,255,255,0.08)"
                          strokeDasharray="3 3"
                        />
                        <XAxis
                          type="number"
                          stroke="#9fb0d0"
                          tickFormatter={formatCurrency}
                        />
                        <YAxis
                          dataKey="label"
                          type="category"
                          stroke="#9fb0d0"
                          width={140}
                        />
                        <Tooltip formatter={(value: any) => formatCurrency(value)} />
                        <Bar
                          dataKey="total_valor"
                          fill="#818cf8"
                          radius={[0, 6, 6, 0]}
                        />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              <div className="card col-12 chartCard">
                <h2>Série diária comercial</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Evolução diária de vendas e cancelamentos para explicar a formação do período.
                </div>
                {!loading && !commercialByDay.length ? (
                  <EmptyState
                    title="Sem série diária para o recorte."
                    detail="A leitura comercial do caixa aparece quando existem comprovantes válidos no período."
                  />
                ) : null}
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={commercialByDay}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis
                        dataKey="data_key"
                        stroke="#9fb0d0"
                        tickFormatter={formatDateKey}
                      />
                      <YAxis
                        stroke="#9fb0d0"
                        tickFormatter={formatCurrency}
                        width={112}
                      />
                      <Tooltip
                        labelFormatter={(value: any) => formatDateKey(value)}
                        formatter={(value: any) => formatCurrency(value)}
                      />
                      <Bar
                        dataKey="total_vendas"
                        name="Vendas"
                        fill="#22d3ee"
                        radius={[6, 6, 0, 0]}
                      />
                      <Bar
                        dataKey="total_cancelamentos"
                        name="Cancelamentos"
                        fill="rgba(248,113,113,0.9)"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-12">
                <h2>Turnos com maior fluxo comercial</h2>
                {!loading && !topTurnos.length ? (
                  <EmptyState
                    title="Sem turnos comerciais no período."
                    detail="Os turnos aparecem quando comprovantes e recebimentos ficam vinculados ao recorte."
                  />
                ) : null}
                {topTurnos.length ? (
                  <div className="tableScroll">
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Filial</th>
                          <th>Turno</th>
                          <th>Operador</th>
                          <th>Vendas</th>
                          <th>Cancel.</th>
                          <th>Receb.</th>
                          <th>Saldo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {topTurnos.map((item: any) => (
                          <tr key={`${item.id_filial}-${item.id_turno}`}>
                            <td>{item.filial_label}</td>
                            <td>
                              {formatTurnoLabel(item.id_turno, item.turno_label)}
                            </td>
                            <td>{item.usuario_label}</td>
                            <td>{formatCurrency(item.total_vendas)}</td>
                            <td>{formatCurrency(item.total_cancelamentos)}</td>
                            <td>{formatCurrency(item.total_pagamentos)}</td>
                            <td>{formatCurrency(item.saldo_comercial)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Caixa agora</div>
                <h2 style={{ marginTop: 4 }}>Turnos em aberto agora</h2>
              </div>

              <div className="card kpi col-3">
                <div className="label">Caixas abertos</div>
                <div className="value">
                  {loading ? "..." : Number(liveKpis.caixas_abertos || 0)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Caixas a revisar</div>
                <div className="value">
                  {loading ? "..." : Number(liveKpis.caixas_stale || 0)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Vendas abertas</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(liveKpis.total_vendas_abertas)}
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Cancelamentos abertos</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(liveKpis.total_cancelamentos_abertos)}
                </div>
              </div>

              <div className="card col-8">
                <h2>Turnos abertos agora</h2>
                {!loading && !openBoxes.length ? (
                  <EmptyState
                    title="Nenhum turno aberto na janela operacional."
                    detail="Quando houver caixa aberto com atividade recente, ele aparece aqui."
                  />
                ) : null}
                {openBoxes.length ? (
                  <div className="tableScroll">
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Filial</th>
                          <th>Turno</th>
                          <th>Operador</th>
                          <th>Aberto há</th>
                          <th>Sem movimento</th>
                          <th>Vendas</th>
                          <th>Cancel.</th>
                          <th>Receb.</th>
                        </tr>
                      </thead>
                      <tbody>
                        {openBoxes.map((item: any) => (
                          <tr key={`${item.id_filial}-${item.id_turno}`}>
                            <td>{item.filial_label}</td>
                            <td>{formatTurnoLabel(item.id_turno, item.turno_label)}</td>
                            <td>{item.usuario_label}</td>
                            <td>{formatHoursLabel(item.horas_aberto)}</td>
                            <td>{formatHoursLabel(item.horas_sem_movimento)}</td>
                            <td>{formatCurrency(item.total_vendas)}</td>
                            <td>{formatCurrency(item.total_cancelamentos)}</td>
                            <td>{formatCurrency(item.total_pagamentos)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </div>

              <div className="card col-4">
                <h2>Alertas do agora</h2>
                {!loading && !alerts.length && !staleBoxes.length ? (
                  <EmptyState
                    title="Nenhum alerta operacional agora."
                    detail="Críticos, atrasos e turnos stale aparecem assim que houver sinal real."
                  />
                ) : null}
                <div style={{ display: "grid", gap: 12 }}>
                  {alerts.map((item: any, index: number) => {
                    const tone = severityTone(item?.severity || "OK");
                    return (
                      <div
                        key={`${item?.id_filial}-${item?.id_turno}-${index}`}
                        className="card"
                        style={{
                          background: tone.bg,
                          borderColor: tone.border,
                        }}
                      >
                        <div style={{ fontWeight: 700 }}>{item?.title || "Alerta operacional"}</div>
                        <div className="muted" style={{ marginTop: 8 }}>
                          {item?.body}
                        </div>
                        <div className="muted" style={{ marginTop: 8 }}>
                          {formatDateTime(item?.last_activity_ts || item?.abertura_ts)}
                        </div>
                      </div>
                    );
                  })}
                  {staleBoxes.slice(0, 3).map((item: any) => (
                    <div
                      key={`${item.id_filial}-${item.id_turno}-stale`}
                      className="card"
                      style={{
                        background: "rgba(56, 189, 248, 0.12)",
                        borderColor: "rgba(96, 165, 250, 0.24)",
                      }}
                    >
                      <div style={{ fontWeight: 700 }}>
                        {item.filial_label} · turno {formatTurnoLabel(item.id_turno, item.turno_label)}
                      </div>
                      <div className="muted" style={{ marginTop: 8 }}>
                        Sem movimento há {formatHoursLabel(item.horas_sem_movimento)}.
                      </div>
                      <div className="muted" style={{ marginTop: 8 }}>
                        Última atividade: {formatDateTime(item.last_activity_ts)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
