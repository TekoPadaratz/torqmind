"use client";

import { useMemo } from "react";
import {
  Bar,
  BarChart,
  Cell,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Line,
  LineChart,
  Pie,
  PieChart,
} from "recharts";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ReadingStatusBanner from "../components/ui/ReadingStatusBanner";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatDateTime,
  formatFilialLabel,
  formatHoursLabel,
  formatTurnoLabel,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { describeCacheBanner } from "../lib/reading-copy.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

export const dynamic = "force-dynamic";

function operationalSourceLabel(source: string) {
  const normalized = String(source || "").toLowerCase();
  if (normalized === "turno") return "Resolvido pelo turno";
  if (normalized === "comprovante") return "Apoio do documento";
  return "Sem resolução";
}

function riskCategoryLabel(eventType: string) {
  const normalized = String(eventType || "").toUpperCase();
  if (normalized === "CANCELAMENTO") return "Cancelamento da venda";
  if (normalized === "CANCELAMENTO_SEGUIDO_VENDA")
    return "Cancelou e refez logo depois";
  if (normalized === "DESCONTO_ALTO") return "Desconto fora do padrão";
  if (normalized === "HORARIO_RISCO") return "Operação em horário incomum";
  if (normalized === "FUNCIONARIO_OUTLIER") return "Colaborador fora da curva";
  return "Outro alerta relevante";
}

function riskGridReference(event: any) {
  if (event?.id_comprovante) return `Documento ${event.id_comprovante}`;
  if (event?.id_movprodutos) return `Movimento ${event.id_movprodutos}`;
  const turnoLabel = formatTurnoLabel(event?.id_turno, event?.turno_label);
  if (turnoLabel && turnoLabel !== "Turno não identificado")
    return turnoLabel.startsWith("Turno ") ? turnoLabel : `Turno ${turnoLabel}`;
  return "Sem referência operacional";
}

export default function FraudPage() {
  const scope = useScopeQuery();
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "fraud_overview",
      scope,
      errorMessage: "Falha ao carregar fraude",
      buildRequestUrl: (currentScope) =>
        `/bi/fraud/overview?${buildScopeParams(currentScope).toString()}`,
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("antifraude")
    : buildModuleLoadingCopy("antifraude");

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  const byDay = useMemo(
    () =>
      (data?.by_day || []).map((r: any) => ({
        ...r,
        data: formatDateKeyShort(r.data_key),
        cancelamentos: Number(r.cancelamentos || 0),
      })),
    [data],
  );
  const riskByDay = useMemo(
    () =>
      (data?.risk_by_day || []).map((r: any) => ({
        ...r,
        data: formatDateKeyShort(r.data_key),
        eventos_alto_risco: Number(r.eventos_alto_risco || 0),
        impacto_estimado_total: Number(r.impacto_estimado_total || 0),
      })),
    [data],
  );

  const definitions = data?.definitions || {};
  const modelCoverage = data?.model_coverage || {};
  const modelCoverageStatus = String(modelCoverage.status || "unavailable");
  const modelCoverageMessage =
    modelCoverage.message || "A leitura modelada ainda está sendo preparada.";
  const businessClock = data?.business_clock || {};

  const openCash = data?.open_cash || {};
  const topOperationalUser = (data?.top_users || [])[0];
  const latestOperationalEvent = (data?.last_events || [])[0];
  const topEmployee = (data?.risk_top_employees || [])[0];
  const modeledEvents = data?.risk_last_events || [];
  const paymentsRiskRows = data?.payments_risk || [];
  const topModeledEvent = modeledEvents[0];
  const cancelationRows = useMemo(
    () =>
      modeledEvents
        .filter((row: any) =>
          ["CANCELAMENTO", "CANCELAMENTO_SEGUIDO_VENDA"].includes(
            String(row?.event_type || "").toUpperCase(),
          ),
        )
        .slice(0, 8),
    [modeledEvents],
  );
  const suspiciousOperationRows = useMemo(
    () =>
      modeledEvents
        .filter(
          (row: any) =>
            !["CANCELAMENTO", "CANCELAMENTO_SEGUIDO_VENDA"].includes(
              String(row?.event_type || "").toUpperCase(),
            ),
        )
        .slice(0, 8),
    [modeledEvents],
  );
  const highlightRows = useMemo(
    () =>
      modeledEvents.slice(0, 12).map((row: any) => ({
        id: `${row.id || row.id_comprovante || row.id_movprodutos}`,
        categoria: riskCategoryLabel(row.event_type),
        referencia: riskGridReference(row),
        operador:
          row.operador_caixa_label ||
          row.responsavel_label ||
          "Operador não identificado",
        frentista: row.funcionario_label || "Sem frentista associado",
        valor: row.impacto_estimado,
        data: row.data,
        motivo: row.reason_summary || "Evento destacado pelo motor de risco.",
      })),
    [modeledEvents],
  );
  const alertMix = useMemo(
    () =>
      [
        {
          label: "Cancelamentos modelados",
          value: cancelationRows.length,
        },
        {
          label: "Outras suspeitas",
          value: suspiciousOperationRows.length,
        },
        {
          label: "Pagamentos fora do padrão",
          value: paymentsRiskRows.length,
        },
      ].filter((item) => item.value > 0),
    [cancelationRows.length, suspiciousOperationRows.length, paymentsRiskRows.length],
  );
  const operationalResolutionMix = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of data?.last_events || []) {
      const source = operationalSourceLabel(String(row?.usuario_source || ""));
      counts.set(source, (counts.get(source) || 0) + 1);
    }
    return Array.from(counts.entries()).map(([label, value]) => ({ label, value }));
  }, [data]);
  const priorityHeadline =
    topModeledEvent?.event_label ||
    latestOperationalEvent?.usuario_label ||
    "Sem foco crítico imediato";
  const priorityDetail = topModeledEvent
    ? topModeledEvent.reason_summary ||
      "O motor de risco encontrou um evento que merece entrar no topo da fila de revisão."
    : latestOperationalEvent
      ? `${latestOperationalEvent.usuario_label} lidera a revisão operacional mais recente em ${formatDateTime(latestOperationalEvent.data)}.`
      : "O período não trouxe concentração material para puxar a fila imediatamente.";
  const fraudDonutColors = ["#f97316", "#ef4444", "#38bdf8", "#94a3b8"];

  return (
    <div>
      <AppNav title="Antifraude" userLabel={userLabel} />
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
            <ReadingStatusBanner
              message={describeCacheBanner(data?._snapshot_cache, "antifraude")}
            />
            {modelCoverageStatus !== "covered" ? (
              <div
                className="card"
                style={{ marginTop: 12, borderColor: "#f59e0b" }}
              >
                <strong>
                  Leitura modelada com cobertura{" "}
                  {modelCoverageStatus === "partial"
                    ? "parcial"
                    : "indisponível"}
                  .
                </strong>{" "}
                {modelCoverageMessage}
              </div>
            ) : null}

            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card col-5">
                <div className="sectionEyebrow">Radar executivo</div>
                <h2 style={{ marginTop: 4 }}>Onde agir primeiro</h2>
                <div style={{ marginTop: 12, fontSize: 26, fontWeight: 800 }}>
                  {priorityHeadline}
                </div>
                <div className="muted" style={{ marginTop: 10 }}>
                  {priorityDetail}
                </div>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                    gap: 10,
                    marginTop: 16,
                  }}
                >
                  <div className="card" style={{ padding: 12 }}>
                    <div className="label">Cancelamentos</div>
                    <div style={{ fontSize: 22, fontWeight: 800 }}>
                      {Number(data?.kpis?.cancelamentos || 0)}
                    </div>
                  </div>
                  <div className="card" style={{ padding: 12 }}>
                    <div className="label">Alto risco</div>
                    <div style={{ fontSize: 22, fontWeight: 800 }}>
                      {Number(data?.risk_kpis?.eventos_alto_risco || 0)}
                    </div>
                  </div>
                  <div className="card" style={{ padding: 12 }}>
                    <div className="label">Caixas abertos</div>
                    <div style={{ fontSize: 22, fontWeight: 800 }}>
                      {Number(openCash?.total_open || 0)}
                    </div>
                  </div>
                </div>
              </div>

              <div className="card col-3 chartCard">
                <h2>Composição dos alertas</h2>
                <div className="chartWrap" style={{ height: 220 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={alertMix}
                        dataKey="value"
                        nameKey="label"
                        innerRadius={42}
                        outerRadius={78}
                        paddingAngle={3}
                      >
                        {alertMix.map((entry: any, index: number) => (
                          <Cell
                            key={`${entry.label}-${index}`}
                            fill={fraudDonutColors[index % fraudDonutColors.length]}
                          />
                        ))}
                      </Pie>
                      <Tooltip />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                  {alertMix.length ? (
                    alertMix.map((item: any, index: number) => (
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
                            background: fraudDonutColors[index % fraudDonutColors.length],
                          }}
                        />
                        <span className="muted">{item.label}</span>
                        <strong>{item.value}</strong>
                      </div>
                    ))
                  ) : (
                    <div className="muted">Sem alertas modelados no recorte.</div>
                  )}
                </div>
              </div>

              <div className="card col-4 chartCard">
                <h2>Responsabilização operacional</h2>
                <div className="chartWrap" style={{ height: 220 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={operationalResolutionMix}
                        dataKey="value"
                        nameKey="label"
                        innerRadius={42}
                        outerRadius={78}
                        paddingAngle={3}
                      >
                        {operationalResolutionMix.map((entry: any, index: number) => (
                          <Cell
                            key={`${entry.label}-${index}`}
                            fill={fraudDonutColors[(index + 1) % fraudDonutColors.length]}
                          />
                        ))}
                      </Pie>
                      <Tooltip />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                  {operationalResolutionMix.length ? (
                    operationalResolutionMix.map((item: any, index: number) => (
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
                            background: fraudDonutColors[(index + 1) % fraudDonutColors.length],
                          }}
                        />
                        <span className="muted">{item.label}</span>
                        <strong>{item.value}</strong>
                      </div>
                    ))
                  ) : (
                    <div className="muted">Sem cancelamentos operacionais no recorte.</div>
                  )}
                </div>
              </div>

              <div className="card kpi col-4">
                <div className="label">Leitura operacional: cancelamentos</div>
                <div className="value">
                  {loading ? "..." : Number(data?.kpis?.cancelamentos || 0)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">
                  Leitura operacional: valor cancelado
                </div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(data?.kpis?.valor_cancelado)}
                </div>
              </div>
              <div className="card kpi col-4">
                <div className="label">Caixas abertos agora</div>
                <div className="value">
                  {loading ? "..." : Number(openCash?.total_open || 0)}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Leitura modelada: impacto estimado</div>
                <div className="value">
                  {loading
                    ? "..."
                    : formatCurrency(data?.risk_kpis?.impacto_total)}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">
                  Leitura modelada: eventos alto risco
                </div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(data?.risk_kpis?.eventos_alto_risco || 0)}
                </div>
              </div>
              <div className="card kpi col-4 riskCard">
                <div className="label">Leitura modelada: score médio</div>
                <div className="value">
                  {loading
                    ? "..."
                    : Number(data?.risk_kpis?.score_medio || 0).toFixed(1)}
                </div>
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Responsáveis e prioridade</div>
                <h2>Exposição humana</h2>
                <div className="muted">
                  Quem mais aparece vinculado às operações que merecem revisão,
                  separando o operador logado do caixa do frentista associado à
                  venda.
                </div>
              </div>

              <div className="card col-4">
                <h2>Operador mais exposto</h2>
                {topOperationalUser ? (
                  <>
                    <div
                      style={{ fontSize: 20, fontWeight: 800, marginTop: 8 }}
                    >
                      {topOperationalUser.usuario_label}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {topOperationalUser.cancelamentos} cancelamento(s), sendo{" "}
                      {topOperationalUser.resolvidos_por_turno} resolvido(s)
                      diretamente pelo turno.
                    </div>
                    <div
                      style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}
                    >
                      {formatCurrency(topOperationalUser.valor_cancelado)}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {topOperationalUser.fallback_comprovante > 0
                        ? `${topOperationalUser.fallback_comprovante} evento(s) ainda dependem do apoio do documento para fechar o responsável.`
                        : "Todos os eventos deste operador foram resolvidos pela verdade do turno."}
                    </div>
                  </>
                ) : (
                  <EmptyState
                    title="Sem operador destacado."
                    detail="O período não trouxe cancelamentos operacionais com concentração material."
                  />
                )}
              </div>

              <div className="card col-4">
                <h2>Frentista mais exposto</h2>
                {topEmployee ? (
                  <>
                    <div
                      style={{ fontSize: 20, fontWeight: 800, marginTop: 8 }}
                    >
                      {topEmployee.funcionario_nome}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {topEmployee.eventos} evento(s) associados às operações do
                      período
                    </div>
                    <div
                      style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}
                    >
                      {formatCurrency(topEmployee.impacto_estimado)}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      Score médio de{" "}
                      {Number(topEmployee.score_medio || 0).toFixed(1)} e
                      prioridade para revisar desconto, cancelamento e contexto
                      da operação.
                    </div>
                  </>
                ) : (
                  <EmptyState
                    title="Sem frentista destacado."
                    detail="Nenhum colaborador apareceu com concentração material no recorte atual."
                  />
                )}
              </div>

              <div className="card col-4">
                <h2>Como interpretar o score médio</h2>
                <div style={{ marginTop: 12, fontSize: 22, fontWeight: 800 }}>
                  {loading
                    ? "..."
                    : `${Number(data?.risk_kpis?.score_medio || 0).toFixed(1)} / 100`}
                </div>
                {!loading ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    O score médio não prova fraude sozinho. Ele resume a força
                    dos sinais encontrados no período e ajuda a ordenar a fila
                    de revisão. Acima de 60, a investigação merece prioridade.
                  </div>
                ) : null}
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Operação confirmada</div>
                <h2>Cancelamentos de venda e caixa</h2>
                <div className="muted">
                  Sempre mostrando o operador logado responsável pela operação
                  do caixa para bater com o módulo de Caixa.
                </div>
              </div>

              <div className="card col-4">
                <h2>Último cancelamento operacional</h2>
                {latestOperationalEvent ? (
                  <>
                    <div
                      style={{ fontSize: 18, fontWeight: 800, marginTop: 8 }}
                    >
                      {latestOperationalEvent.usuario_label}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {formatFilialLabel(
                        latestOperationalEvent.id_filial,
                        latestOperationalEvent.filial_nome,
                      )}{" "}
                      · {formatDateTime(latestOperationalEvent.data)}
                    </div>
                    <div
                      style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}
                    >
                      {formatCurrency(latestOperationalEvent.valor_total)}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {operationalSourceLabel(
                        latestOperationalEvent.usuario_source,
                      )}{" "}
                      ·{" "}
                      {formatTurnoLabel(
                        latestOperationalEvent.id_turno,
                        latestOperationalEvent.turno_label,
                      )}
                    </div>
                  </>
                ) : (
                  <EmptyState
                    title="Sem cancelamento recente."
                    detail="Não houve cancelamento operacional no recorte atual."
                  />
                )}
              </div>

              <div className="card col-4">
                <h2>Evento mais crítico do período</h2>
                {topModeledEvent ? (
                  <>
                    <div
                      style={{ fontSize: 18, fontWeight: 800, marginTop: 8 }}
                    >
                      {topModeledEvent.event_label}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {topModeledEvent.operador_caixa_label ||
                        topModeledEvent.responsavel_label ||
                        "Responsável não identificado"}{" "}
                      · {formatDateTime(topModeledEvent.data)}
                    </div>
                    <div
                      style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}
                    >
                      {formatCurrency(topModeledEvent.impacto_estimado)}
                    </div>
                    <div className="muted" style={{ marginTop: 6 }}>
                      {topModeledEvent.reason_summary ||
                        "Evento priorizado pelo motor de risco para revisão imediata."}
                    </div>
                  </>
                ) : (
                  <EmptyState
                    title="Sem foco crítico modelado."
                    detail={
                      modelCoverageStatus === "covered"
                        ? "O período não trouxe evento modelado com criticidade suficiente para liderar a fila de revisão."
                        : "O modelo não cobre este recorte por completo. Use os eventos operacionais e a janela coberta pelo modelo como referência."
                    }
                  />
                )}
              </div>

              <div className="card col-4">
                <h2>O que agir primeiro</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Priorize cancelamentos recentes com valor alto, operador
                  recorrente e turnos ainda abertos. Essa combinação costuma
                  indicar revisão operacional urgente.
                </div>
                <div style={{ marginTop: 14, display: "grid", gap: 8 }}>
                  <div>
                    <strong>{Number(data?.kpis?.cancelamentos || 0)}</strong>{" "}
                    cancelamento(s) operacional(is)
                  </div>
                  <div>
                    <strong>
                      {Number(data?.risk_kpis?.eventos_alto_risco || 0)}
                    </strong>{" "}
                    evento(s) de alto risco
                  </div>
                  <div>
                    <strong>{Number(openCash?.total_open || 0)}</strong>{" "}
                    caixa(s) ainda aberto(s)
                  </div>
                </div>
              </div>

              <div className="card col-12">
                <h2>Monitor de turnos</h2>
                {loading ? null : (
                  <>
                    <div className="muted" style={{ marginBottom: 8 }}>
                      {openCash.summary ||
                        "Monitoramento operacional indisponível."}
                    </div>
                    {openCash.source_status === "ok" &&
                    openCash.items?.length ? (
                      <div className="tableScroll">
                        <table className="table compact">
                          <thead>
                            <tr>
                              <th>Filial</th>
                              <th>Turno</th>
                              <th>Operador</th>
                              <th>Horas aberto</th>
                              <th>Severidade</th>
                            </tr>
                          </thead>
                          <tbody>
                            {openCash.items.map((item: any) => (
                              <tr key={`${item.id_filial}-${item.id_turno}`}>
                                <td>
                                  {formatFilialLabel(
                                    item.id_filial,
                                    item.filial_nome,
                                  )}
                                </td>
                                <td>
                                  {formatTurnoLabel(
                                    item.id_turno,
                                    item.turno_label,
                                  )}
                                </td>
                                <td>
                                  {item.usuario_label ||
                                    item.usuario_nome ||
                                    "Operador não identificado"}
                                </td>
                                <td>{formatHoursLabel(item.horas_aberto)}</td>
                                <td>{item.status_label || item.severity}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <EmptyState
                        title={
                          openCash.source_status === "unavailable"
                            ? "Monitor de turnos em integração."
                            : openCash.source_status === "unmapped"
                              ? "Fonte operacional ainda não mapeada."
                              : "Nenhuma ocorrência relevante."
                        }
                        detail={
                          openCash.summary ||
                          "Assim que a base operacional estiver pronta, esta leitura passa a destacar turnos abertos e antigos automaticamente."
                        }
                      />
                    )}
                  </>
                )}
              </div>

              <div className="card col-6 chartCard">
                <h2>Cancelamentos por dia</h2>
                <div className="muted" style={{ marginBottom: 8 }}>
                  Série operacional de cancelamentos reconciliados por turno,
                  usando a mesma base semântica do Caixa.
                </div>
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={byDay}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="data" stroke="#9fb0d0" />
                      <YAxis stroke="#9fb0d0" />
                      <Tooltip />
                      <Bar
                        dataKey="cancelamentos"
                        fill="#f97316"
                        radius={[6, 6, 0, 0]}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-6">
                <h2>Operadores de caixa com mais cancelamentos</h2>
                {!loading && !(data?.top_users || []).length ? (
                  <EmptyState
                    title="Sem operadores destacados."
                    detail="Não houve concentração operacional relevante por operador de caixa."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Operador</th>
                        <th>Cancelamentos</th>
                        <th>Valor</th>
                        <th>Apoio do documento</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.top_users || []).slice(0, 10).map((u: any) => (
                        <tr key={`${u.id_usuario}-${u.usuario_label}`}>
                          <td>{u.usuario_label}</td>
                          <td>{Number(u.cancelamentos || 0)}</td>
                          <td>{formatCurrency(u.valor_cancelado)}</td>
                          <td>{Number(u.fallback_comprovante || 0)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <h2>Últimos cancelamentos operacionais</h2>
                {!loading && !(data?.last_events || []).length ? (
                  <EmptyState
                    title="Sem eventos operacionais recentes."
                    detail="Não houve cancelamentos reconciliados por turno no recorte analisado."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Data</th>
                        <th>Filial</th>
                        <th>Turno</th>
                        <th>Operador</th>
                        <th>Origem da resolução</th>
                        <th>Valor</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.last_events || []).slice(0, 20).map((e: any) => (
                        <tr key={`${e.id_filial}-${e.id_db}-${e.id_comprovante}`}>
                          <td>{formatDateTime(e.data)}</td>
                          <td>
                            {e.filial_label ||
                              formatFilialLabel(e.id_filial, e.filial_nome)}
                          </td>
                          <td>{formatTurnoLabel(e.id_turno, e.turno_label)}</td>
                          <td>{e.usuario_label}</td>
                          <td>{operationalSourceLabel(e.usuario_source)}</td>
                          <td>{formatCurrency(e.valor_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Modelagem e suspeitas</div>
                <h2>Outras operações suspeitas</h2>
                <div className="muted">
                  Aqui entram os alertas que não são cancelamento puro:
                  descontos fora do padrão, horários incomuns, repetições
                  suspeitas e outras operações administrativas que pedem
                  revisão.
                </div>
              </div>

              <div className="card col-6">
                <h2>Cancelamentos e refações suspeitas</h2>
                {!loading && !cancelationRows.length ? (
                  <EmptyState
                    title="Sem sequência suspeita de cancelamento."
                    detail="O período não trouxe cancelamentos modelados com padrão recorrente acima do limiar."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Quando</th>
                        <th>Categoria</th>
                        <th>Operador</th>
                        <th>Valor</th>
                        <th>Motivo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cancelationRows.map((row: any) => (
                        <tr key={row.id}>
                          <td>{formatDateTime(row.data)}</td>
                          <td>{riskCategoryLabel(row.event_type)}</td>
                          <td>
                            {row.operador_caixa_label ||
                              row.responsavel_label ||
                              "Operador não identificado"}
                          </td>
                          <td>{formatCurrency(row.impacto_estimado)}</td>
                          <td>
                            {row.reason_summary ||
                              "Evento destacado para revisão."}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-6">
                <h2>Outras suspeitas com operador responsável</h2>
                {!loading && !suspiciousOperationRows.length ? (
                  <EmptyState
                    title="Sem outras suspeitas materiais."
                    detail="Os demais sinais modelados ficaram abaixo do limiar de destaque no recorte atual."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Quando</th>
                        <th>Categoria</th>
                        <th>Operador</th>
                        <th>Frentista</th>
                        <th>Impacto</th>
                      </tr>
                    </thead>
                    <tbody>
                      {suspiciousOperationRows.map((row: any) => (
                        <tr key={row.id}>
                          <td>{formatDateTime(row.data)}</td>
                          <td>{riskCategoryLabel(row.event_type)}</td>
                          <td>
                            {row.operador_caixa_label ||
                              row.responsavel_label ||
                              "Operador não identificado"}
                          </td>
                          <td>
                            {row.funcionario_label || "Sem frentista associado"}
                          </td>
                          <td>{formatCurrency(row.impacto_estimado)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-8 chartCard">
                <h2>Série temporal de alto risco</h2>
                <div className="muted" style={{ marginBottom: 8 }}>
                  O gráfico abaixo mostra a camada modelada. Use junto com os
                  cancelamentos operacionais acima, nunca como substituto da
                  verdade do caixa.
                </div>
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={riskByDay}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="data" stroke="#9fb0d0" />
                      <YAxis stroke="#9fb0d0" />
                      <Tooltip
                        formatter={(value: any, _name: any, item: any) =>
                          item?.dataKey === "impacto_estimado_total"
                            ? formatCurrency(value)
                            : Number(value || 0)
                        }
                      />
                      <Line
                        type="monotone"
                        dataKey="eventos_alto_risco"
                        stroke="#ef4444"
                        strokeWidth={2}
                        dot={false}
                      />
                      <Line
                        type="monotone"
                        dataKey="impacto_estimado_total"
                        stroke="#f59e0b"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-4">
                <h2>Anomalias de pagamento</h2>
                {!loading && !paymentsRiskRows.length ? (
                  <EmptyState
                    title="Sem anomalias de pagamento."
                    detail="O período não trouxe evento de pagamento acima do limiar de atenção."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Data</th>
                        <th>Turno</th>
                        <th>Evento</th>
                        <th>Score</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paymentsRiskRows
                        .slice(0, 8)
                        .map((e: any, idx: number) => (
                          <tr key={`${e.insight_id || e.event_type}-${idx}`}>
                            <td>{formatDateKey(e.data_key)}</td>
                            <td>{formatTurnoLabel(e.id_turno, e.turno_label)}</td>
                            <td>{e.event_label || e.event_type}</td>
                            <td>{Number(e.score || 0)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <h2>Concentração por turno e canal</h2>
                {!loading && !(data?.risk_by_turn_local || []).length ? (
                  <EmptyState
                    title="Sem concentração por turno e canal."
                    detail="Nenhum agrupamento material apareceu no período selecionado."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Filial</th>
                        <th>Turno</th>
                        <th>Canal</th>
                        <th>Eventos</th>
                        <th>Alto risco</th>
                        <th>Impacto</th>
                        <th>Score médio</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.risk_by_turn_local || [])
                        .slice(0, 10)
                        .map((r: any, idx: number) => (
                          <tr key={`${r.id_turno}-${r.id_local_venda}-${idx}`}>
                            <td>
                              {r.filial_label ||
                                formatFilialLabel(r.id_filial, r.filial_nome)}
                            </td>
                            <td>{formatTurnoLabel(r.id_turno, r.turno_label)}</td>
                            <td>{r.local_label || "Canal não informado"}</td>
                            <td>{r.eventos}</td>
                            <td>{r.alto_risco}</td>
                            <td>{formatCurrency(r.impacto_estimado)}</td>
                            <td>{Number(r.score_medio || 0).toFixed(1)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <div className="sectionEyebrow">Fila de revisão</div>
                <h2>Destaques para localizar no ERP</h2>
                <div className="muted">
                  Esta grade organiza os eventos mais relevantes para que a
                  equipe localize rapidamente o documento, veja o operador
                  logado responsável e entenda por que o alerta foi levantado.
                </div>
              </div>

              <div className="card col-12">
                {!loading && !highlightRows.length ? (
                  <EmptyState
                    title="Sem destaques no período."
                    detail="Nenhum evento entrou na fila principal de revisão para o recorte atual."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Categoria</th>
                        <th>Referência</th>
                        <th>Operador</th>
                        <th>Frentista</th>
                        <th>Valor</th>
                        <th>Quando</th>
                        <th>Motivo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {highlightRows.map((row: any) => (
                        <tr key={row.id}>
                          <td>{row.categoria}</td>
                          <td>{row.referencia}</td>
                          <td>{row.operador}</td>
                          <td>{row.frentista}</td>
                          <td>{formatCurrency(row.valor)}</td>
                          <td>{formatDateTime(row.data)}</td>
                          <td>{row.motivo}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
            <div className="card" style={{ marginTop: 12 }}>
              <div className="muted">
                Central antifraude com duas leituras complementares: a leitura
                operacional mostra o que realmente aconteceu no período, e a
                leitura modelada prioriza o que vale investigar primeiro quando
                a janela do modelo cobre o recorte.
              </div>
              {!loading ? (
                <div
                  className="muted"
                  style={{ marginTop: 10, display: "grid", gap: 8 }}
                >
                  <div>
                    <strong>Data-base do negócio:</strong>{" "}
                    {businessClock.business_date || scope.dt_fim || "-"}{" "}
                    {businessClock.timezone
                      ? `(${businessClock.timezone})`
                      : ""}
                  </div>
                  <div>
                    <strong>Cancelamento operacional:</strong>{" "}
                    {definitions.operational_cancelamentos ||
                      "Venda cancelada que precisa de revisão, sempre reconciliada com o turno real do caixa para manter o responsável correto."}
                  </div>
                  <div>
                    <strong>Operador responsável:</strong>{" "}
                    {definitions.cashier_operator ||
                      "Mostramos o operador logado responsável pela operação do caixa. O documento só entra como apoio quando o turno não resolve o responsável."}
                  </div>
                  <div>
                    <strong>Eventos de alto risco:</strong>{" "}
                    {definitions.high_risk_events ||
                      "Camada modelada que sinaliza comportamentos atípicos em sequência, descontos ou turnos suscetíveis."}
                  </div>
                  <div>
                    <strong>Impacto estimado:</strong>{" "}
                    {definitions.estimated_impact ||
                      "Estimativa financeira usada para priorizar a investigação, não representa perda definitiva."}
                  </div>
                  <div>
                    <strong>Fórmula do impacto:</strong>{" "}
                    {definitions.impact_formulas ||
                      "Cancelamento modelado usa parte do valor da venda; desconto alto usa a exposição monetária do próprio desconto."}
                  </div>
                  <div>
                    <strong>Score médio:</strong>{" "}
                    {definitions.score_meaning ||
                      "Leitura média do nível de alerta do período."}
                  </div>
                  <div>
                    <strong>Cobertura do modelo:</strong>{" "}
                    {definitions.coverage ||
                      "A tela separa leitura operacional e leitura modelada para evitar zero semântico falso."}
                  </div>
                </div>
              ) : null}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
