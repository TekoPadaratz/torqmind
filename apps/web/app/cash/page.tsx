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
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateTime,
  formatHoursLabel,
  formatTurnoLabel,
  formatTurnoPeriod,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { sortGridRows } from "../lib/grid-sort";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
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
  useEnsureScopedProductUrl();
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
  const topTurnos = (commercial?.top_turnos || [])
    .filter((item: any) => Number(item?.id_turno || 0) > 0)
    .slice(0, 15);
  const openBoxes = useMemo(
    () =>
      sortGridRows(
        (liveNow?.open_boxes || data?.open_boxes || []).filter(
          (item: any) => Number(item?.id_turno || 0) > 0,
        ),
        (i: any) => ({
          filial: i.filial_label ?? i.id_filial,
          data: i.abertura_ts,
          nome: i.usuario_label,
        }),
      ),
    [liveNow?.open_boxes, data?.open_boxes],
  );
  const staleBoxes = (liveNow?.stale_boxes || data?.stale_boxes || []).filter((item: any) => Number(item?.id_turno || 0) > 0);
  const alerts = liveNow?.alerts || data?.alerts || [];
  const inutilizacoes = data?.inutilizacoes || {};
  const inutItems = useMemo(
    () =>
      sortGridRows(inutilizacoes?.items || [], (i: any) => ({
        filial: i.filial_label ?? i.id_filial,
        data: i.data_emissao_nfe || i.dt,
        nome: i.usuario_label,
      })),
    [inutilizacoes?.items],
  );
  const hasInutilizacoes = Number(inutilizacoes?.qtd || 0) > 0;
  const paymentMixChartHeight = Math.max(280, paymentMix.length * 44);

  function formatNfeDateTime(item: any) {
    if (item?.data_emissao_nfe) return formatDateTime(item.data_emissao_nfe);
    if (item?.dt) {
      const hourValue = Number(item?.hora);
      if (Number.isFinite(hourValue) && hourValue >= 0) {
        const hour = String(Math.trunc(hourValue)).padStart(2, "0");
        return formatDateTime(`${item.dt}T${hour}:00:00`);
      }
      return formatDateTime(`${item.dt}T00:00:00`);
    }
    return "-";
  }

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
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div
                className="card col-12"
                style={{
                  background:
                    "linear-gradient(135deg, rgba(14,116,144,0.22), var(--bg) 45%, rgba(16,185,129,0.16))",
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
                {!loading && Math.abs(Number(commercialKpis?.diferenca_conciliacao || 0)) > 0.01 ? (
                  <div className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                    Não conciliado: {formatCurrency(commercialKpis?.diferenca_conciliacao)}
                  </div>
                ) : null}
              </div>
              <div className="card kpi col-3">
                <div className="label">Saldo comercial</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(commercialKpis?.saldo_comercial)}
                </div>
              </div>

              <div className="card col-12 chartCard">
                <h2>Formas de Pagamento Vendas</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Distribuição conciliada dos recebimentos de vendas do período por forma de pagamento.
                </div>
                {!loading && !paymentMix.length ? (
                  <EmptyState
                    title="Sem pagamentos conciliados no período."
                    detail="A distribuição por forma aparece quando existem recebimentos conciliados no período."
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
                          stroke="var(--muted)"
                          tickFormatter={formatCurrency}
                        />
                        <YAxis
                          dataKey="label"
                          type="category"
                          stroke="var(--muted)"
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

              <div className="card col-12">
                <h2>Fluxo do período selecionado</h2>
                {!loading && !topTurnos.length ? (
                  <EmptyState
                    title="Sem turnos comerciais no período."
                    detail="Os turnos aparecem quando comprovantes e recebimentos ficam vinculados ao período."
                  />
                ) : null}
                {topTurnos.length ? (
                  <div className="tableScroll">
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Filial</th>
                          <th>Turno</th>
                          <th>Período do turno</th>
                          <th>Operador</th>
                          <th>Qtd. vendas</th>
                          <th>Faturamento</th>
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
                            <td>{formatTurnoPeriod(item.abertura_ts, item.fechamento_ts)}</td>
                            <td>{item.usuario_label}</td>
                            <td>{Number(item.qtd_vendas || 0)}</td>
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

              {hasInutilizacoes ? (
                <div className="card col-12">
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                    <h2 style={{ margin: 0, color: "var(--accent-copper)" }}>Notas Fiscais Inutilizadas</h2>
                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                      <div className="card" style={{ padding: "8px 14px" }}>
                        <div className="label">Qtd. inutilizadas</div>
                        <div style={{ fontSize: 20, fontWeight: 800 }}>{inutilizacoes.qtd || 0}</div>
                      </div>
                      <div className="card" style={{ padding: "8px 14px" }}>
                        <div className="label">Valor total</div>
                        <div style={{ fontSize: 20, fontWeight: 800 }}>{formatCurrency(inutilizacoes.valor_total)}</div>
                      </div>
                    </div>
                  </div>
                  {!loading && !inutItems.length ? (
                    <EmptyState
                      title="Lista detalhada em preparação"
                      detail="Existem notas inutilizadas no período, mas a lista detalhada ainda está sendo preparada."
                    />
                  ) : null}
                  {inutItems.length ? (
                    <div className="tableScroll">
                      <table className="table compact">
                        <thead>
                          <tr>
                            <th>Filial</th>
                            <th>Data/hora</th>
                            <th>Turno/caixa</th>
                            <th>Operador</th>
                            <th>Documento</th>
                            <th>Valor</th>
                            <th>Chave / protocolo</th>
                          </tr>
                        </thead>
                        <tbody>
                          {inutItems.map((item: any, idx: number) => (
                            <tr key={`inut-${item.id_comprovante}-${item.id_nfe}-${idx}`}>
                              <td>{item.filial_label}</td>
                              <td>{formatNfeDateTime(item)}</td>
                              <td>{formatTurnoLabel(item.id_turno, item.turno_label)}</td>
                              <td>{item.usuario_label}</td>
                              <td>{item.numero_nfe || "-"}</td>
                              <td>{formatCurrency(item.valor_comprovante)}</td>
                              <td>{item.protocolo || item.chave_nfe || "-"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div className="card col-12">
                <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                  <h2 style={{ margin: 0, color: "var(--accent-copper)" }}>Caixa Agora</h2>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Caixas abertos</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : Number(liveKpis.caixas_abertos || 0)}</div>
                    </div>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Vendas abertas</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : formatCurrency(liveKpis.total_vendas_abertas)}</div>
                    </div>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Cancelamentos abertos</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : formatCurrency(liveKpis.total_cancelamentos_abertos)}</div>
                    </div>
                  </div>
                </div>
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
            </div>
          </>
        )}
      </div>
    </div>
  );
}
