"use client";

import { useEffect, useMemo, useState } from "react";
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
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatDateOnly,
  formatDateTime,
  formatFilialLabel,
  formatHoursLabel,
  formatTurnoLabel,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
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
  if (event?.documento_label) return event.documento_label;
  if (event?.documento_venda) return `Comprovante ${event.documento_venda}`;
  if (event?.id_comprovante) return `Comprovante #${event.id_comprovante}`;
  return "Sem comprovante";
}

function scoreLevelLabel(level: string) {
  const normalized = String(level || "").toUpperCase();
  if (normalized === "CRITICAL") return { label: "Crítico", color: "var(--color-negative)" };
  if (normalized === "HIGH") return { label: "Alto", color: "var(--color-warning)" };
  if (normalized === "MEDIUM" || normalized === "MEDIO") return { label: "Médio", color: "var(--color-info)" };
  if (normalized === "LOW" || normalized === "BAIXO") return { label: "Baixo", color: "#94a3b8" };
  return { label: normalized || "—", color: "#94a3b8" };
}

export default function FraudPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const [trocaSoSuspeitas, setTrocaSoSuspeitas] = useState(true);
  const [trocaFormaNova, setTrocaFormaNova] = useState<"todos" | "prazo" | "cheque_pre">("todos");
  const [creditoRisco, setCreditoRisco] = useState<"suspeitas" | "normais" | "todas">("suspeitas");
  const scopeKey = useMemo(
    () => buildScopeParams(scope).toString(),
    [scope],
  );
  useEffect(() => {
    setTrocaSoSuspeitas(true);
    setTrocaFormaNova("todos");
    setCreditoRisco("suspeitas");
  }, [scopeKey]);

  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: `fraud_overview:${creditoRisco}:${trocaFormaNova}:${trocaSoSuspeitas ? "susp" : "all"}`,
      scope,
      errorMessage: "Falha ao carregar fraude",
      buildRequestUrl: (currentScope) => {
        const params = buildScopeParams(currentScope);
        params.set("credito_risco", creditoRisco);
        params.set("troca_forma_nova", trocaFormaNova);
        params.set("troca_only_suspeita", trocaSoSuspeitas ? "true" : "false");
        return `/bi/fraud/overview?${params.toString()}`;
      },
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("antifraude")
    : buildModuleLoadingCopy("antifraude");

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  // Antifraude — troca de forma de pagamento (sensível: só master/owner).
  const trocaAllowed = useMemo(
    () =>
      ["platform_master", "owner"].includes(
        String((claims as any)?.user_role || (claims as any)?.role || "").toLowerCase(),
      ),
    [claims],
  );

  const trocaRows: any[] = data?.troca_forma_pgto || [];
  const trocaTotais = data?.troca_forma_pgto_totais || {};
  const trocaTotalQtd = trocaSoSuspeitas
    ? Number(trocaTotais.suspeitas_qtd || 0)
    : Number(trocaTotais.todas_qtd || 0);
  const trocaTotalValor = trocaSoSuspeitas
    ? Number(trocaTotais.suspeitas_valor || 0)
    : Number(trocaTotais.todas_valor || 0);
  const trocaLoading = loading;

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
  const creditos = data?.lancamentos_creditos || {};
  const creditosSummary = creditos?.summary || {};
  const creditosRows = creditos?.lancamentos || [];
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
      modeledEvents.slice(0, 20).map((row: any) => ({
        id: `${row.id || row.id_comprovante || row.id_movprodutos}`,
        prioridade: scoreLevelLabel(row.score_level),
        score: Number(row.score ?? row.score_risco ?? 0),
        categoria: riskCategoryLabel(row.event_type),
        referencia: riskGridReference(row),
        filial:
          row.filial_label || formatFilialLabel(row.id_filial, row.filial_nome),
        turno: formatTurnoLabel(row.turno_numero, row.turno_label),
        operador:
          row.operador_label ||
          row.operador_caixa_label ||
          row.responsavel_label ||
          "Operador sem cadastro",
        frentista: row.frentista_label || row.funcionario_label || "Sem frentista associado",
        valor: row.impacto_estimado ?? row.valor,
        data: row.data,
        motivo: row.motivo || row.reason_summary || "Evento destacado pelo motor de risco.",
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
  const fraudDonutColors = ["var(--color-warning)", "var(--color-negative)", "var(--color-info)", "#94a3b8"];

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
            <div className="bi-grid" style={{ marginTop: 12 }}>
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
                        <th>Filial</th>
                        <th>Operador</th>
                        <th>Cancelamentos</th>
                        <th>Valor</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data?.top_users || []).slice(0, 10).map((u: any) => (
                        <tr key={`${u.id_filial}-${u.id_usuario}-${u.usuario_label}`}>
                          <td>{u.filial_label || formatFilialLabel(u.id_filial, u.filial_nome)}</td>
                          <td>{u.usuario_label}</td>
                          <td>{Number(u.cancelamentos || 0)}</td>
                          <td>{formatCurrency(u.valor_cancelado)}</td>
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
                    detail="Não houve cancelamentos reconciliados por turno no período analisado."
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
                        <th>Documento</th>
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
                          <td>{e.documento_label || e.documento_venda || (e.id_comprovante ? String(e.id_comprovante) : "—")}</td>
                          <td>{formatCurrency(e.valor_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card col-12">
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 12,
                    alignItems: "flex-start",
                    justifyContent: "space-between",
                  }}
                >
                  <div>
                    <div className="sectionEyebrow">Risco financeiro</div>
                    <h2 style={{ marginTop: 4 }}>Lançamentos de créditos</h2>
                    <div className="muted" style={{ marginTop: 4 }}>
                      Créditos lançados no cadastro do cliente. A injeção manual de crédito
                      (&ldquo;Crédito adicionado manualmente&rdquo;), sem troco, cheque ou fatura por
                      trás, é o padrão clássico de golpe: o operador cria saldo e depois usa para dar
                      baixa em vendas. As linhas em vermelho são injeções manuais.
                    </div>
                  </div>
                  <div className="profitFilterBar" style={{ marginBottom: 0 }}>
                    <select
                      value={creditoRisco}
                      onChange={(e) => setCreditoRisco(e.target.value as "suspeitas" | "normais" | "todas")}
                      aria-label="Filtro de risco dos créditos"
                    >
                      <option value="suspeitas">Só suspeitas</option>
                      <option value="normais">Só normais</option>
                      <option value="todas">Todas</option>
                    </select>
                  </div>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 10, marginTop: 12 }}>
                  <div className="card" style={{ padding: 12 }}>
                    <div className="label">Crédito injetado</div>
                    <div style={{ fontSize: 20, fontWeight: 800 }}>{formatCurrency(creditosSummary.injetado)}</div>
                    <div className="muted" style={{ fontSize: 11 }}>{Number(creditosSummary.injecoes_qtd || 0)} lançamento(s)</div>
                  </div>
                  <div className="card" style={{ padding: 12, borderColor: "var(--color-negative)" }}>
                    <div className="label">Injeção manual (suspeita)</div>
                    <div style={{ fontSize: 20, fontWeight: 800, color: "var(--color-negative)" }}>{formatCurrency(creditosSummary.injetado_manual)}</div>
                    <div className="muted" style={{ fontSize: 11 }}>{Number(creditosSummary.manuais_qtd || 0)} manual(is)</div>
                  </div>
                  <div className="card" style={{ padding: 12 }}>
                    <div className="label">Crédito aplicado</div>
                    <div style={{ fontSize: 20, fontWeight: 800 }}>{formatCurrency(creditosSummary.aplicado)}</div>
                  </div>
                </div>
                {!loading && !creditosRows.length ? (
                  <EmptyState title="Sem injeções de crédito no recorte." detail="Nenhum crédito corresponde ao filtro atual na janela analisada." />
                ) : (
                  <div className="tableScroll" style={{ marginTop: 12 }}>
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Data</th>
                          <th>Filial</th>
                          <th>Cliente</th>
                          <th>Operador</th>
                          <th>Forma</th>
                          <th style={{ textAlign: "right" }}>Injetado</th>
                          <th style={{ textAlign: "right" }}>Saldo atual</th>
                          <th>Histórico</th>
                          <th>Risco</th>
                        </tr>
                      </thead>
                      <tbody>
                        {creditosRows.map((c: any, idx: number) => (
                          <tr key={`${c.id_filial}-${c.id_cliente || idx}-${idx}`} style={c.suspeita ? { background: "rgba(239,68,68,0.07)" } : undefined}>
                            <td style={{ whiteSpace: "nowrap" }}>
                              {c.data_ts ? formatDateTime(c.data_ts) : c.data ? formatDateOnly(c.data) : "—"}
                            </td>
                            <td>{c.filial_label}</td>
                            <td>{c.cliente}</td>
                            <td>{c.operador}</td>
                            <td>{c.forma_pagamento || "—"}</td>
                            <td style={{ textAlign: "right", fontWeight: 700 }}>{formatCurrency(c.injetado)}</td>
                            <td style={{ textAlign: "right" }}>{formatCurrency(c.saldo_cliente)}</td>
                            <td style={{ minWidth: 220 }}>{c.historico}</td>
                            <td>{c.suspeita ? <span style={{ color: "var(--color-negative)", fontWeight: 700 }}>Suspeita</span> : <span className="muted">Normal</span>}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {trocaAllowed ? (
                <div className="card col-12">
                  <div
                    style={{
                      display: "flex",
                      flexWrap: "wrap",
                      gap: 12,
                      alignItems: "center",
                      justifyContent: "space-between",
                    }}
                  >
                    <div>
                      <div className="sectionEyebrow">Risco financeiro</div>
                      <h2 style={{ marginTop: 4 }}>Troca de forma de pagamento</h2>
                      <div className="muted" style={{ marginTop: 4 }}>
                        Pagamentos que foram recebidos e depois trocados para uma
                        forma a receber (prazo, cheque, convênio, crediário). A
                        troca de uma venda já quitada para &quot;a receber&quot; é
                        o padrão clássico de desvio de caixa.
                      </div>
                    </div>
                    <div className="profitFilterBar" style={{ marginBottom: 0 }}>
                      <select
                        value={trocaSoSuspeitas ? "suspeitas" : "todas"}
                        onChange={(e) => setTrocaSoSuspeitas(e.target.value === "suspeitas")}
                        aria-label="Filtro de risco das trocas"
                      >
                        <option value="suspeitas">Só suspeitas</option>
                        <option value="todas">Todas</option>
                      </select>
                      <select
                        value={trocaFormaNova}
                        onChange={(e) =>
                          setTrocaFormaNova(e.target.value as "todos" | "prazo" | "cheque_pre")
                        }
                        aria-label="Filtro da forma nova"
                      >
                        <option value="todos">Forma nova: todas</option>
                        <option value="prazo">Forma nova: prazo</option>
                        <option value="cheque_pre">Forma nova: cheque pré</option>
                      </select>
                    </div>
                  </div>

                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
                      gap: 10,
                      marginTop: 12,
                    }}
                  >
                    <div className="card" style={{ padding: 12 }}>
                      <div className="label">
                        {trocaSoSuspeitas ? "Trocas suspeitas" : "Trocas no período"}
                      </div>
                      <div style={{ fontSize: 22, fontWeight: 800 }}>
                        {trocaTotalQtd}
                      </div>
                    </div>
                    <div className="card" style={{ padding: 12 }}>
                      <div className="label">Valor envolvido</div>
                      <div style={{ fontSize: 22, fontWeight: 800 }}>
                        {formatCurrency(trocaTotalValor)}
                      </div>
                    </div>
                  </div>

                  <div style={{ marginTop: 12 }}>
                    {trocaLoading ? (
                      <div className="muted">Carregando trocas…</div>
                    ) : !trocaRows.length ? (
                      <EmptyState
                        title={
                          trocaSoSuspeitas
                            ? "Sem trocas suspeitas no período."
                            : "Sem trocas de forma de pagamento no período."
                        }
                        detail="Nenhum registro de alteração de forma de pagamento foi encontrado para a janela atual."
                      />
                    ) : (
                      <div className="tableScroll">
                        {trocaTotalQtd > trocaRows.length ? (
                          <div className="muted" style={{ marginBottom: 8, fontSize: 12 }}>
                            Exibindo as {trocaRows.length} trocas mais recentes de{" "}
                            {trocaTotalQtd} no período.
                          </div>
                        ) : null}
                        <table className="table compact">
                          <thead>
                            <tr>
                              <th>Documento</th>
                              <th>Forma anterior</th>
                              <th>Forma nova</th>
                              <th>Usuário</th>
                              <th>Quando</th>
                              <th style={{ textAlign: "right" }}>Valor</th>
                              <th>Risco</th>
                            </tr>
                          </thead>
                          <tbody>
                            {trocaRows.map((row: any) => (
                              <tr
                                key={`${row.troca_id}`}
                                style={
                                  row.is_suspeita
                                    ? { background: "rgba(239,68,68,0.06)" }
                                    : undefined
                                }
                              >
                                <td>{row.documento || "—"}</td>
                                <td>{row.forma_de || "—"}</td>
                                <td>
                                  <strong>{row.forma_para || "—"}</strong>
                                </td>
                                <td>
                                  {row.nome_operador ||
                                    (row.id_usuario
                                      ? `Operador #${row.id_usuario}`
                                      : "Não resolvido")}
                                </td>
                                <td>
                                  {row.data_troca_ts
                                    ? formatDateTime(row.data_troca_ts)
                                    : formatDateKey(row.data_key)}
                                </td>
                                <td style={{ textAlign: "right" }}>
                                  {formatCurrency(row.valor)}
                                </td>
                                <td>
                                  {row.is_suspeita ? (
                                    <span
                                      style={{
                                        color: "var(--color-negative)",
                                        fontWeight: 700,
                                      }}
                                    >
                                      Suspeita
                                    </span>
                                  ) : (
                                    <span className="muted">Normal</span>
                                  )}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
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
