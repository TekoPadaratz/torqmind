"use client";

import { Fragment, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
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
import ChartTooltip from "../components/ui/ChartTooltip";
import EmptyState from "../components/ui/EmptyState";
import GridChrome from "../components/ui/GridChrome";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import SortableTh from "../components/ui/SortableTh";
import { compareGridRows } from "../lib/grid-sort";
import { useRecordGrid } from "../lib/use-record-grid";
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
import { sortGridRows } from "../lib/grid-sort";
import { canAccessScreenKey } from "../lib/session";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { rowMatchesGridSearch, useGridSearch } from "../lib/use-grid-search";
import MonthYearSelect from "../components/ui/MonthYearSelect";
import { currentAnoMesSP } from "../lib/month-year.mjs";

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
  const label = String(event?.documento_label || event?.documento_fiscal || "").trim();
  if (label && label !== "—" && label !== "-") return label;
  return "—";
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
  // Default "Todas": em muitos dias (ex. hoje) há trocas/créditos normais e 0 suspeitas —
  // "Só suspeitas" zera a grade e parece "sem dado". Usuário ainda pode filtrar suspeitas.
  const [trocaSoSuspeitas, setTrocaSoSuspeitas] = useState(false);
  const [trocaFormaNova, setTrocaFormaNova] = useState<"todos" | "prazo" | "cheque_pre">("todos");
  const [creditoRisco, setCreditoRisco] = useState<"suspeitas" | "normais" | "todas">("todas");
  const [creditoUsoQuery, setCreditoUsoQuery] = useState("");
  const [creditoExpandido, setCreditoExpandido] = useState<string | null>(null);
  const [credFuncStatus, setCredFuncStatus] = useState<"todos" | "suspeitos" | "normais">("todos");
  const [credFuncMonth, setCredFuncMonth] = useState<number>(() => currentAnoMesSP());
  const [credFuncExpandido, setCredFuncExpandido] = useState<number | null>(null);
  const [credFuncUsoQuery, setCredFuncUsoQuery] = useState("");
  const riscoFinanceiroRef = useRef<HTMLDivElement | null>(null);
  const scrollAnchorElRef = useRef<HTMLElement | null>(null);
  const scrollAnchorTopRef = useRef<number | null>(null);
  const scopeKey = useMemo(
    () => buildScopeParams(scope).toString(),
    [scope],
  );

  const pinFilterScroll = (from?: EventTarget | null) => {
    const host =
      (from instanceof HTMLElement ? from.closest(".card") : null) ||
      riscoFinanceiroRef.current;
    if (!host) return;
    scrollAnchorElRef.current = host as HTMLElement;
    scrollAnchorTopRef.current = host.getBoundingClientRect().top;
  };

  useEffect(() => {
    setTrocaSoSuspeitas(false);
    setTrocaFormaNova("todos");
    setCreditoRisco("todas");
    setCreditoExpandido(null);
    setCredFuncStatus("todos");
    setCredFuncExpandido(null);
  }, [scopeKey]);
  useEffect(() => {
    setCredFuncExpandido(null);
  }, [credFuncStatus, credFuncMonth]);

  // Core operacional (operadores/cancelamentos/gráficos) — isolado dos filtros de risco.
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "fraud_overview_core",
      scope,
      errorMessage: "Falha ao carregar fraude",
      requestTimeoutMs: 60_000,
      keepPreviousData: true,
      buildRequestUrl: (currentScope, session) => {
        if (!canAccessScreenKey(session, "fraud.core")) return null;
        const params = buildScopeParams(currentScope);
        params.set("sections", "core");
        return `/bi/fraud/overview?${params.toString()}`;
      },
    });

  // Risco financeiro (créditos + troca) — único bloco afetado pelos filtros locais.
  const {
    data: riscoData,
    loading: riscoLoading,
    error: riscoError,
  } = useBiScopeData<any>({
    moduleKey: `fraud_risco:${creditoRisco}:${trocaFormaNova}:${trocaSoSuspeitas ? "susp" : "all"}`,
    scope,
    errorMessage: "Falha ao carregar risco financeiro",
    requestTimeoutMs: 60_000,
    keepPreviousData: true,
    buildRequestUrl: (currentScope, session) => {
      if (!canAccessScreenKey(session, "fraud.risco_financeiro")) return null;
      const params = buildScopeParams(currentScope);
      params.set("sections", "risco_financeiro");
      params.set("credito_risco", creditoRisco);
      params.set("troca_forma_nova", trocaFormaNova);
      params.set("troca_only_suspeita", trocaSoSuspeitas ? "true" : "false");
      return `/bi/fraud/overview?${params.toString()}`;
    },
  });

  const {
    data: credFuncData,
    loading: credFuncLoading,
    error: credFuncError,
  } = useBiScopeData<any>({
    moduleKey: `fraud_cred_func:${credFuncMonth}:${credFuncStatus}:${scope?.id_filial || scope?.id_filiais?.join(",") || "all"}`,
    scope,
    errorMessage: "Falha ao carregar crédito de funcionário",
    requestTimeoutMs: 60_000,
    keepPreviousData: true,
    buildRequestUrl: (currentScope, session) => {
      if (!canAccessScreenKey(session, "fraud.credito_funcionario")) return null;
      const params = buildScopeParams(currentScope);
      params.set("ano_mes", String(credFuncMonth));
      params.set("status", credFuncStatus);
      // Mart refresh sob demanda no backend se vazia; evita timeout no GET.
      return `/bi/fraud/credito-funcionario?${params.toString()}`;
    },
  });

  const canSeeFraudCore = canAccessScreenKey(claims, "fraud.core");
  const canSeeFraudRisco = canAccessScreenKey(claims, "fraud.risco_financeiro");
  const canSeeCredFunc = canAccessScreenKey(claims, "fraud.credito_funcionario");

  const credFuncPayload = credFuncData?.data || credFuncData || {};
  const credFuncRows: any[] = Array.isArray(credFuncPayload?.funcionarios)
    ? credFuncPayload.funcionarios
    : [];
  const credFuncSearch = useGridSearch(credFuncRows);
  const credFuncSummary = credFuncPayload?.summary || {};
  const credFuncExtraMonths = useMemo(
    () =>
      Array.isArray(credFuncPayload?.meses_disponiveis)
        ? credFuncPayload.meses_disponiveis.map((m: any) => Number(m)).filter((m: number) => Number.isFinite(m) && m > 0)
        : [],
    [credFuncPayload?.meses_disponiveis],
  );
  const credFuncGrid = useRecordGrid<any>({
    rows: credFuncSearch.filteredRows,
    resetKey: `${credFuncSearch.query}:${credFuncStatus}:${credFuncMonth}`,
    getTieId: (row) => row.id_funcionario,
    defaultCompare: (a, b) =>
      compareGridRows({ nome: a.nome }, { nome: b.nome }),
    summableKeys: [
      "limite_prazo",
      "limite_vale",
      "usado_vale",
      "usado_prazo",
      "usado_geral",
      "pago_mes",
      "saldo_aberto_geral",
      "saldo_aberto_mes",
      "qtd_usos_mes",
    ],
    columns: {
      nome: { type: "text" },
      limite_prazo: { type: "number" },
      limite_vale: { type: "number" },
      usado_vale: { type: "number" },
      usado_prazo: { type: "number" },
      usado_geral: { type: "number" },
      pago_mes: { type: "number" },
      saldo_aberto_geral: { type: "number" },
      saldo_aberto_mes: { type: "number" },
      qtd_usos_mes: { type: "number" },
    },
  });

  // Mantém o card do filtro na mesma posição do viewport após o refresh do risco.
  useLayoutEffect(() => {
    const el = scrollAnchorElRef.current || riscoFinanceiroRef.current;
    const expectedTop = scrollAnchorTopRef.current;
    if (!el || expectedTop == null || typeof window === "undefined") return;
    const delta = el.getBoundingClientRect().top - expectedTop;
    if (Math.abs(delta) > 1) {
      window.scrollBy(0, delta);
    }
    if (!riscoLoading) {
      scrollAnchorTopRef.current = null;
      scrollAnchorElRef.current = null;
    } else {
      scrollAnchorTopRef.current = el.getBoundingClientRect().top;
    }
  }, [riscoData, riscoLoading]);
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

  const trocaRows = useMemo(
    () =>
      sortGridRows(
        Array.isArray(riscoData?.troca_forma_pgto) ? riscoData.troca_forma_pgto : [],
        (r: any) => ({
          filial: r.filial_label ?? r.filial_nome ?? r.id_filial,
          data: r.data_troca_ts || r.data_key,
          nome: r.nome_operador,
        }),
      ),
    [riscoData?.troca_forma_pgto],
  );
  const trocaSearch = useGridSearch(trocaRows);
  const trocaTotais = riscoData?.troca_forma_pgto_totais || {};
  const trocaTotalQtd = trocaSoSuspeitas
    ? Number(trocaTotais.suspeitas_qtd || 0)
    : Number(trocaTotais.todas_qtd || 0);
  const trocaTotalValor = trocaSoSuspeitas
    ? Number(trocaTotais.suspeitas_valor || 0)
    : Number(trocaTotais.todas_valor || 0);
  const trocaLoading = riscoLoading;

  const devolucaoRows = useMemo(
    () =>
      sortGridRows(
        Array.isArray(riscoData?.devolucao_entrada?.items)
          ? riscoData.devolucao_entrada.items
          : [],
        (r: any) => ({
          filial: r.filial_label ?? r.id_filial,
          data: r.dt || r.data_key,
          nome: r.nome_operador,
        }),
      ),
    [riscoData?.devolucao_entrada?.items],
  );
  const devolucaoSearch = useGridSearch(devolucaoRows);
  const devolucaoSummary = riscoData?.devolucao_entrada?.summary || {};
  const devolucaoGrid = useRecordGrid<any>({
    rows: devolucaoSearch.filteredRows,
    resetKey: devolucaoSearch.query,
    getTieId: (row) => `${row.id_filial}-${row.id_comprovante}-${row.documento}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.dt || a.data_key, nome: a.nome_operador },
        { filial: b.filial_label ?? b.id_filial, data: b.dt || b.data_key, nome: b.nome_operador },
      ),
    summableKeys: ["valor"],
    columns: {
      filial_label: { type: "text" },
      dt: { type: "date" },
      documento: { type: "text" },
      nome_operador: { type: "text" },
      valor: { type: "number" },
    },
  });

  const transferenciaRows = useMemo(
    () =>
      sortGridRows(
        Array.isArray(riscoData?.transferencia_cr?.items)
          ? riscoData.transferencia_cr.items
          : [],
        (r: any) => ({
          filial: r.filial_label ?? r.id_filial,
          data: r.dt || r.data_key,
          nome: r.entidade_para || r.entidade_de,
        }),
      ),
    [riscoData?.transferencia_cr?.items],
  );
  const transferenciaSearch = useGridSearch(transferenciaRows);
  const transferenciaSummary = riscoData?.transferencia_cr?.summary || {};
  const transferenciaGrid = useRecordGrid<any>({
    rows: transferenciaSearch.filteredRows,
    resetKey: transferenciaSearch.query,
    getTieId: (row) =>
      `${row.id_filial}-${row.id_contasreceber}-${row.id_entidade_de}-${row.id_entidade_para}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.dt || a.data_key, nome: a.entidade_para || a.entidade_de },
        { filial: b.filial_label ?? b.id_filial, data: b.dt || b.data_key, nome: b.entidade_para || b.entidade_de },
      ),
    summableKeys: ["valor"],
    columns: {
      filial_label: { type: "text" },
      dt: { type: "date" },
      documento: { type: "text" },
      entidade_de: { type: "text" },
      entidade_para: { type: "text" },
      valor: { type: "number" },
    },
  });

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
  const creditos = riscoData?.lancamentos_creditos || {};
  const creditosSummary = creditos?.summary || {};
  const creditosRows = creditos?.lancamentos || [];
  const creditosSearch = useGridSearch(creditosRows);
  const creditosFiltered = useMemo(() => {
    return sortGridRows(creditosSearch.filteredRows, (c: any) => ({
      filial: c.filial_label ?? c.id_filial,
      data: c.data_ts || c.data || c.data_key,
      nome: c.cliente,
    }));
  }, [creditosSearch.filteredRows]);
  const creditosGrid = useRecordGrid<any>({
    rows: creditosFiltered,
    resetKey: `${creditosSearch.query}:${creditoRisco}`,
    getTieId: (row, idx) => `${row.id_filial}-${row.id_cliente || idx}-${row.id_mov || idx}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.data_ts || a.data || a.data_key, nome: a.cliente },
        { filial: b.filial_label ?? b.id_filial, data: b.data_ts || b.data || b.data_key, nome: b.cliente },
      ),
    summableKeys: ["injetado"],
    columns: {
      filial_label: { type: "text" },
      data_ts: { type: "date" },
      cliente: { type: "text" },
      operador: { type: "text" },
      injetado: { type: "number" },
      saldo_operacao: { type: "number" },
      saldo_atual: { type: "number" },
    },
  });

  const lastEventsOperational = useMemo(() => {
    // Mesmo universo do gráfico (fraud_daily): não descartar turno 0 / NF ausente.
    // Sem NF → "—" honesto; turno sem resolução → label da API.
    const rows = Array.isArray(data?.last_events) ? data.last_events : [];
    const filtered = rows.filter((e: any) => Boolean(e?.data || e?.data_key || e?.id_comprovante));
    return sortGridRows(filtered, (e: any) => ({
      filial: e.filial_label ?? e.filial_nome ?? e.id_filial,
      data: e.data || e.data_key,
      nome: e.usuario_label || e.operador_label,
    }));
  }, [data?.last_events]);
  const cancelSearch = useGridSearch(lastEventsOperational);

  const cancelGrid = useRecordGrid<any>({
    rows: cancelSearch.filteredRows,
    resetKey: cancelSearch.query,
    getTieId: (row) => `${row.id_filial}-${row.id_db}-${row.id_comprovante}-${row.event_id || row.id || ""}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.filial_nome ?? a.id_filial, data: a.data || a.data_key, nome: a.usuario_label || a.operador_label },
        { filial: b.filial_label ?? b.filial_nome ?? b.id_filial, data: b.data || b.data_key, nome: b.usuario_label || b.operador_label },
      ),
    summableKeys: ["valor_total"],
    columns: {
      filial_label: { type: "text" },
      data: { type: "date" },
      turno_label: { type: "text" },
      usuario_label: { type: "text" },
      documento_label: { type: "text" },
      valor_total: { type: "number" },
    },
  });

  const topUsersRows = useMemo(
    () => (Array.isArray(data?.top_users) ? data.top_users : []),
    [data?.top_users],
  );
  const operadorSearch = useGridSearch(topUsersRows);
  const operadorGrid = useRecordGrid<any>({
    rows: operadorSearch.filteredRows,
    resetKey: operadorSearch.query,
    getTieId: (row) => `${row.id_filial}-${row.id_usuario}-${row.usuario_label}`,
    defaultSort: { key: "cancelamentos", type: "number", dir: "desc" },
    summableKeys: ["cancelamentos", "valor_cancelado"],
    columns: {
      filial_label: { type: "text" },
      usuario_label: { type: "text" },
      cancelamentos: { type: "number" },
      valor_cancelado: { type: "number" },
    },
  });

  const trocaGrid = useRecordGrid<any>({
    rows: trocaSearch.filteredRows,
    resetKey: `${trocaSearch.query}:${trocaSoSuspeitas}:${trocaFormaNova}`,
    getTieId: (row) => row.troca_id,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.filial_nome ?? a.id_filial, data: a.data_troca_ts || a.data_key, nome: a.nome_operador },
        { filial: b.filial_label ?? b.filial_nome ?? b.id_filial, data: b.data_troca_ts || b.data_key, nome: b.nome_operador },
      ),
    summableKeys: ["valor"],
    columns: {
      filial_label: { type: "text" },
      data_troca_ts: { type: "date" },
      documento: { type: "text" },
      forma_de: { type: "text" },
      forma_para: { type: "text" },
      nome_operador: { type: "text" },
      valor: { type: "number" },
    },
  });
  const topOperationalUser = topUsersRows[0];
  const latestOperationalEvent = lastEventsOperational[0];
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
        {riscoError && data ? (
          <div className="card errorCard" style={{ marginTop: 8 }}>
            {riscoError}
          </div>
        ) : null}
        {!data && canSeeFraudCore ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={6}
              panels={4}
            />
          </div>
        ) : !canSeeFraudCore && !canSeeFraudRisco && !canSeeCredFunc ? (
          <div style={{ marginTop: 12 }}>
            <EmptyState
              title="Sem painéis liberados"
              detail="Seu usuário não tem painéis do Antifraude liberados."
            />
          </div>
        ) : (
          <>
            {canSeeFraudCore && data ? (
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card col-6 chartCard">
                <h2>Cancelamentos por dia</h2>
                <div className="chartWrap">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={byDay}>
                      <CartesianGrid
                        stroke="rgba(255,255,255,0.08)"
                        strokeDasharray="3 3"
                      />
                      <XAxis dataKey="data" stroke="var(--muted)" />
                      <YAxis stroke="var(--muted)" />
                      <Tooltip content={<ChartTooltip />} />
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
                <GridSearchInput value={operadorSearch.query} onChange={operadorSearch.setQuery} aria-label="Pesquisar operadores com cancelamentos" />
                {!loading && !operadorSearch.filteredRows.length ? (
                  <EmptyState
                    title="Sem operadores destacados."
                    detail="Não houve concentração operacional relevante por operador de caixa."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <SortableTh label="Filial" sortKey="filial_label" ariaSort={operadorGrid.ariaSort("filial_label")} onToggle={operadorGrid.toggleSort} />
                        <SortableTh label="Operador" sortKey="usuario_label" ariaSort={operadorGrid.ariaSort("usuario_label")} onToggle={operadorGrid.toggleSort} />
                        <SortableTh label="Cancelamentos" sortKey="cancelamentos" ariaSort={operadorGrid.ariaSort("cancelamentos")} onToggle={operadorGrid.toggleSort} align="right" />
                        <SortableTh label="Valor" sortKey="valor_cancelado" ariaSort={operadorGrid.ariaSort("valor_cancelado")} onToggle={operadorGrid.toggleSort} align="right" />
                      </tr>
                    </thead>
                    <tbody>
                      {operadorGrid.slice.map((u: any) => (
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
                <GridChrome
                  page={operadorGrid.page}
                  totalPages={operadorGrid.totalPages}
                  total={operadorGrid.total}
                  from={operadorGrid.range.from}
                  to={operadorGrid.range.to}
                  onPrev={operadorGrid.onPrev}
                  onNext={operadorGrid.onNext}
                  onResetOrder={operadorGrid.resetOrder}
                  isDefaultOrder={operadorGrid.isDefaultOrder}
                />
              </div>

              <div className="card col-12">
                <h2>Últimos cancelamentos operacionais</h2>
                <GridSearchInput value={cancelSearch.query} onChange={cancelSearch.setQuery} aria-label="Pesquisar cancelamentos operacionais" />
                {!loading && !cancelSearch.filteredRows.length ? (
                  <EmptyState
                    title="Sem cancelamentos no período."
                    detail="Não há comprovantes cancelados reconciliados no intervalo selecionado."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <SortableTh label="Filial" sortKey="filial_label" ariaSort={cancelGrid.ariaSort("filial_label")} onToggle={cancelGrid.toggleSort} />
                        <SortableTh label="Data" sortKey="data" ariaSort={cancelGrid.ariaSort("data")} onToggle={cancelGrid.toggleSort} />
                        <SortableTh label="Turno" sortKey="turno_label" ariaSort={cancelGrid.ariaSort("turno_label")} onToggle={cancelGrid.toggleSort} />
                        <SortableTh label="Operador" sortKey="usuario_label" ariaSort={cancelGrid.ariaSort("usuario_label")} onToggle={cancelGrid.toggleSort} />
                        <SortableTh label="Documento" sortKey="documento_label" ariaSort={cancelGrid.ariaSort("documento_label")} onToggle={cancelGrid.toggleSort} />
                        <SortableTh label="Valor" sortKey="valor_total" ariaSort={cancelGrid.ariaSort("valor_total")} onToggle={cancelGrid.toggleSort} align="right" />
                      </tr>
                    </thead>
                    <tbody>
                      {cancelGrid.slice.map((e: any) => (
                        <tr key={`${e.id_filial}-${e.id_db}-${e.id_comprovante}-${e.event_id || e.id || ""}`}>
                          <td>
                            {e.filial_label ||
                              formatFilialLabel(e.id_filial, e.filial_nome)}
                          </td>
                          <td>{e.data ? formatDateTime(e.data) : e.data_key ? formatDateKey(e.data_key) : "—"}</td>
                          <td>
                            {e.turno_label ||
                              formatTurnoLabel(e.turno_numero, e.turno_label)}
                          </td>
                          <td>{e.usuario_label || e.operador_label}</td>
                          <td>{e.documento_label || e.documento_fiscal || "—"}</td>
                          <td>{formatCurrency(e.valor_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <GridChrome
                  page={cancelGrid.page}
                  totalPages={cancelGrid.totalPages}
                  total={cancelGrid.total}
                  from={cancelGrid.range.from}
                  to={cancelGrid.range.to}
                  onPrev={cancelGrid.onPrev}
                  onNext={cancelGrid.onNext}
                  onResetOrder={cancelGrid.resetOrder}
                  isDefaultOrder={cancelGrid.isDefaultOrder}
                />
              </div>
            </div>
            ) : null}

            {canSeeFraudRisco ? (
              <>
              <div
                ref={riscoFinanceiroRef}
                className="card col-12"
                style={{
                  marginTop: canSeeFraudCore ? undefined : 12,
                  opacity: riscoLoading && riscoData ? 0.92 : 1,
                  transition: "opacity 0.15s",
                }}
              >
                <div>
                  <div className="sectionEyebrow" style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                    Risco financeiro
                    {riscoLoading && riscoData ? (
                      <span className="muted" style={{ fontSize: 11, fontWeight: 500 }}>
                        Atualizando…
                      </span>
                    ) : null}
                  </div>
                  <h2 style={{ marginTop: 4 }}>Lançamentos de créditos</h2>
                  <div
                    className="profitFilterBar"
                    style={{ marginTop: 10, marginBottom: 0, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}
                  >
                    {(
                      [
                        { value: "suspeitas", label: "Só suspeitas" },
                        { value: "normais", label: "Só normais" },
                        { value: "todas", label: "Todas" },
                      ] as const
                    ).map((opt) => {
                      const active = creditoRisco === opt.value;
                      return (
                        <button
                          key={opt.value}
                          type="button"
                          aria-pressed={active}
                          onClick={(e) => {
                            if (opt.value === creditoRisco) return;
                            pinFilterScroll(e.currentTarget);
                            setCreditoRisco(opt.value);
                          }}
                          style={{
                            border: active
                              ? "1px solid var(--color-accent, var(--accent-copper, #3b82f6))"
                              : "1px solid var(--border)",
                            background: active
                              ? "var(--accent-copper-soft, rgba(59,130,246,0.12))"
                              : "transparent",
                            color: "var(--text)",
                            borderRadius: 6,
                            padding: "6px 12px",
                            cursor: "pointer",
                            fontSize: 12,
                            fontWeight: active ? 700 : 500,
                          }}
                        >
                          {opt.label}
                        </button>
                      );
                    })}
                    <GridSearchInput value={creditosSearch.query} onChange={creditosSearch.setQuery} aria-label="Pesquisar lançamentos de crédito" />
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
                {riscoLoading && !riscoData ? (
                  <div className="muted" style={{ marginTop: 12 }}>Carregando créditos…</div>
                ) : !riscoLoading && !creditosFiltered.length ? (
                  <EmptyState title="Sem injeções de crédito no período selecionado." detail="Nenhum crédito corresponde ao filtro atual na janela analisada." />
                ) : (
                  <div className="tableScroll" style={{ marginTop: 12 }}>
                    <table className="table compact">
                      <thead>
                        <tr>
                          <SortableTh label="Filial" sortKey="filial_label" ariaSort={creditosGrid.ariaSort("filial_label")} onToggle={creditosGrid.toggleSort} />
                          <SortableTh label="Data" sortKey="data_ts" ariaSort={creditosGrid.ariaSort("data_ts")} onToggle={creditosGrid.toggleSort} />
                          <SortableTh label="Cliente" sortKey="cliente" ariaSort={creditosGrid.ariaSort("cliente")} onToggle={creditosGrid.toggleSort} />
                          <SortableTh label="Operador" sortKey="operador" ariaSort={creditosGrid.ariaSort("operador")} onToggle={creditosGrid.toggleSort} />
                          <SortableTh label="Injetado" sortKey="injetado" ariaSort={creditosGrid.ariaSort("injetado")} onToggle={creditosGrid.toggleSort} align="right" />
                          <SortableTh label="Saldo na operação" sortKey="saldo_operacao" ariaSort={creditosGrid.ariaSort("saldo_operacao")} onToggle={creditosGrid.toggleSort} align="right" />
                          <SortableTh label="Saldo atual" sortKey="saldo_atual" ariaSort={creditosGrid.ariaSort("saldo_atual")} onToggle={creditosGrid.toggleSort} align="right" />
                          <th>Histórico</th>
                          <th>Risco</th>
                        </tr>
                      </thead>
                      <tbody>
                        {creditosGrid.slice.map((c: any, idx: number) => {
                          const rowKey = `${c.id_filial}-${c.id_cliente || idx}-${c.id_mov || idx}`;
                          const expanded = creditoExpandido === rowKey;
                          const consumos: any[] = Array.isArray(c.consumos) ? c.consumos : [];
                          return (
                            <Fragment key={rowKey}>
                              <tr
                                onClick={() =>
                                  setCreditoExpandido(expanded ? null : rowKey)
                                }
                                style={{
                                  cursor: "pointer",
                                  ...(c.suspeita
                                    ? { background: "rgba(239,68,68,0.07)" }
                                    : {}),
                                }}
                              >
                                <td>{c.filial_label}</td>
                                <td style={{ whiteSpace: "nowrap" }}>
                                  {c.data_ts && c.hora_conhecida
                                    ? formatDateTime(c.data_ts)
                                    : c.data
                                      ? formatDateOnly(c.data)
                                      : "—"}
                                </td>
                                <td>{c.cliente}</td>
                                <td>{c.operador}</td>
                                <td style={{ textAlign: "right", fontWeight: 700 }}>{formatCurrency(c.injetado)}</td>
                                <td style={{ textAlign: "right", fontWeight: 600 }}>
                                  {c.saldo_operacao != null ? formatCurrency(c.saldo_operacao) : "—"}
                                </td>
                                <td style={{ textAlign: "right" }}>{formatCurrency(c.saldo_atual ?? c.saldo_cliente)}</td>
                                <td style={{ minWidth: 220 }}>{c.historico}</td>
                                <td>{c.suspeita ? <span style={{ color: "var(--color-negative)", fontWeight: 700 }}>Suspeita</span> : <span className="muted">Normal</span>}</td>
                              </tr>
                              {expanded ? (
                                <tr>
                                  <td colSpan={10} style={{ padding: "10px 12px", background: "var(--surface-faint)" }}>
                                    {consumos.length ? (
                                      <>
                                      <GridSearchInput value={creditoUsoQuery} onChange={setCreditoUsoQuery} aria-label="Pesquisar usos do crédito" />
                                      {/* Exceção: detalhe expandido do crédito, não listagem paginada. */}
                                      <table className="table compact" style={{ margin: 0 }}>
                                        <thead>
                                          <tr>
                                            <th>Data</th>
                                            <th>Tipo</th>
                                            <th style={{ textAlign: "right" }}>Valor</th>
                                            <th style={{ textAlign: "right" }}>Saldo na operação</th>
                                            <th>Histórico</th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {consumos.filter((u: any) => rowMatchesGridSearch(u, creditoUsoQuery)).map((u: any, uIdx: number) => (
                                            <tr key={`${rowKey}-consumo-${uIdx}`}>
                                              <td style={{ whiteSpace: "nowrap" }}>
                                                {u.data_ts && u.hora_conhecida
                                                  ? formatDateTime(u.data_ts)
                                                  : u.data
                                                    ? formatDateOnly(u.data)
                                                    : "—"}
                                              </td>
                                              <td>{u.tipo_label || "Uso do crédito"}</td>
                                              <td style={{ textAlign: "right" }}>{formatCurrency(u.valor)}</td>
                                              <td style={{ textAlign: "right" }}>
                                                {u.saldo_operacao != null ? formatCurrency(u.saldo_operacao) : "—"}
                                              </td>
                                              <td>{u.historico || "—"}</td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                      </>
                                    ) : (
                                      <div className="muted" style={{ fontSize: 13 }}>
                                        Nenhum uso deste crédito encontrado após a injeção.
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
                    <GridChrome
                      page={creditosGrid.page}
                      totalPages={creditosGrid.totalPages}
                      total={creditosGrid.total}
                      from={creditosGrid.range.from}
                      to={creditosGrid.range.to}
                      onPrev={creditosGrid.onPrev}
                      onNext={creditosGrid.onNext}
                      onResetOrder={creditosGrid.resetOrder}
                      isDefaultOrder={creditosGrid.isDefaultOrder}
                    />
                  </div>
                )}
              </div>

              {trocaAllowed ? (
                <div
                  className="card col-12"
                  style={{
                    opacity: riscoLoading && riscoData ? 0.92 : 1,
                    transition: "opacity 0.15s",
                  }}
                >
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
                    </div>
                    <div className="profitFilterBar" style={{ marginBottom: 0 }}>
                      <select
                        value={trocaSoSuspeitas ? "suspeitas" : "todas"}
                        onChange={(e) => {
                          pinFilterScroll(e.currentTarget);
                          setTrocaSoSuspeitas(e.target.value === "suspeitas");
                        }}
                        aria-label="Filtro de risco das trocas"
                      >
                        <option value="suspeitas">Só suspeitas</option>
                        <option value="todas">Todas</option>
                      </select>
                      <select
                        value={trocaFormaNova}
                        onChange={(e) => {
                          pinFilterScroll(e.currentTarget);
                          setTrocaFormaNova(e.target.value as "todos" | "prazo" | "cheque_pre");
                        }}
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
                    {trocaLoading && !riscoData ? (
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
                        <GridSearchInput value={trocaSearch.query} onChange={trocaSearch.setQuery} aria-label="Pesquisar trocas de pagamento" />
                        {trocaTotalQtd > trocaRows.length ? (
                          <div className="muted" style={{ marginBottom: 8, fontSize: 12 }}>
                            Exibindo as {trocaRows.length} trocas mais recentes de{" "}
                            {trocaTotalQtd} no período.
                          </div>
                        ) : null}
                        {!trocaSearch.filteredRows.length ? (
                          <EmptyState
                            title="Nenhuma troca encontrada para a busca."
                            detail="Ajuste o termo ou limpe a pesquisa."
                          />
                        ) : null}
                        <table className="table compact">
                          <thead>
                            <tr>
                              <SortableTh label="Filial" sortKey="filial_label" ariaSort={trocaGrid.ariaSort("filial_label")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Data" sortKey="data_troca_ts" ariaSort={trocaGrid.ariaSort("data_troca_ts")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Documento" sortKey="documento" ariaSort={trocaGrid.ariaSort("documento")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Forma anterior" sortKey="forma_de" ariaSort={trocaGrid.ariaSort("forma_de")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Forma nova" sortKey="forma_para" ariaSort={trocaGrid.ariaSort("forma_para")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Usuário" sortKey="nome_operador" ariaSort={trocaGrid.ariaSort("nome_operador")} onToggle={trocaGrid.toggleSort} />
                              <SortableTh label="Valor" sortKey="valor" ariaSort={trocaGrid.ariaSort("valor")} onToggle={trocaGrid.toggleSort} align="right" />
                              <th>Venda</th>
                              <th>Risco</th>
                            </tr>
                          </thead>
                          <tbody>
                            {trocaGrid.slice.map((row: any) => (
                              <tr
                                key={`${row.troca_id}`}
                                style={
                                  row.venda_cancelada
                                    ? { background: "rgba(239,68,68,0.12)" }
                                    : row.is_suspeita
                                      ? { background: "rgba(239,68,68,0.06)" }
                                      : undefined
                                }
                              >
                                <td>
                                  {row.filial_label ||
                                    formatFilialLabel(row.id_filial, row.filial_nome)}
                                </td>
                                <td>
                                  {row.data_troca_ts
                                    ? formatDateTime(row.data_troca_ts)
                                    : formatDateKey(row.data_key)}
                                </td>
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
                                <td style={{ textAlign: "right" }}>
                                  {formatCurrency(row.valor)}
                                </td>
                                <td>
                                  {row.venda_cancelada ||
                                  row.venda_status === "Cancelada" ? (
                                    <span
                                      style={{
                                        color: "var(--color-negative)",
                                        fontWeight: 700,
                                      }}
                                    >
                                      Cancelada
                                    </span>
                                  ) : (
                                    row.venda_status || "Ativa"
                                  )}
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
                        <GridChrome
                          page={trocaGrid.page}
                          totalPages={trocaGrid.totalPages}
                          total={trocaGrid.total}
                          from={trocaGrid.range.from}
                          to={trocaGrid.range.to}
                          onPrev={trocaGrid.onPrev}
                          onNext={trocaGrid.onNext}
                          onResetOrder={trocaGrid.resetOrder}
                          isDefaultOrder={trocaGrid.isDefaultOrder}
                          truncatedNote={trocaTotalQtd > trocaRows.length ? `Exibindo as ${trocaRows.length} trocas mais recentes de ${trocaTotalQtd} no período.` : null}
                        />
                      </div>
                    )}
                  </div>
                </div>
              ) : null}

              <div className="card col-12" style={{ marginTop: 12 }}>
                <div className="platformSectionHead" style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between" }}>
                  <div>
                    <div className="sectionEyebrow">Risco financeiro</div>
                    <h2 style={{ marginTop: 4 }}>Devolução de Vendas</h2>
                  </div>
                  <div className="muted" style={{ fontSize: 13 }}>
                    {Number(devolucaoSummary.qtd || 0)} nota(s) ·{" "}
                    {formatCurrency(Number(devolucaoSummary.valor_total || 0))}
                  </div>
                </div>
                <div style={{ marginTop: 12 }}>
                  {riscoLoading && !riscoData ? (
                    <div className="muted">Carregando…</div>
                  ) : !devolucaoRows.length ? (
                    <EmptyState title="Sem devoluções de vendas no período." />
                  ) : (
                    <div className="tableScroll">
                      <div style={{ marginBottom: 8 }}>
                        <GridSearchInput
                          value={devolucaoSearch.query}
                          onChange={devolucaoSearch.setQuery}
                          aria-label="Pesquisar devoluções"
                        />
                      </div>
                      <table className="table compact">
                        <thead>
                          <tr>
                            <SortableTh label="Filial" sortKey="filial_label" ariaSort={devolucaoGrid.ariaSort("filial_label")} onToggle={devolucaoGrid.toggleSort} />
                            <SortableTh label="Data" sortKey="dt" ariaSort={devolucaoGrid.ariaSort("dt")} onToggle={devolucaoGrid.toggleSort} />
                            <SortableTh label="Documento" sortKey="documento" ariaSort={devolucaoGrid.ariaSort("documento")} onToggle={devolucaoGrid.toggleSort} />
                            <SortableTh label="Operador" sortKey="nome_operador" ariaSort={devolucaoGrid.ariaSort("nome_operador")} onToggle={devolucaoGrid.toggleSort} />
                            <SortableTh label="Valor" sortKey="valor" ariaSort={devolucaoGrid.ariaSort("valor")} onToggle={devolucaoGrid.toggleSort} align="right" />
                          </tr>
                        </thead>
                        <tbody>
                          {devolucaoGrid.slice.map((row: any) => (
                            <tr key={`${row.id_filial}-${row.id_comprovante}-${row.documento}`}>
                              <td>
                                {row.filial_label ||
                                  formatFilialLabel(row.id_filial, row.filial_nome)}
                              </td>
                              <td>{row.dt ? formatDateOnly(row.dt) : formatDateKey(row.data_key)}</td>
                              <td>{row.documento || row.documento_label || "—"}</td>
                              <td>{row.nome_operador || "—"}</td>
                              <td style={{ textAlign: "right" }}>{formatCurrency(row.valor)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      <GridChrome
                        page={devolucaoGrid.page}
                        totalPages={devolucaoGrid.totalPages}
                        total={devolucaoGrid.total}
                        from={devolucaoGrid.range.from}
                        to={devolucaoGrid.range.to}
                        onPrev={devolucaoGrid.onPrev}
                        onNext={devolucaoGrid.onNext}
                        onResetOrder={devolucaoGrid.resetOrder}
                        isDefaultOrder={devolucaoGrid.isDefaultOrder}
                      />
                    </div>
                  )}
                </div>
              </div>

              <div className="card col-12" style={{ marginTop: 12 }}>
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
                    <h2 style={{ marginTop: 4 }}>Transferência de contas a receber</h2>
                  </div>
                  <div className="muted" style={{ fontSize: 13 }}>
                    {Number(transferenciaSummary.qtd || 0)} título(s) ·{" "}
                    {formatCurrency(Number(transferenciaSummary.valor_total || 0))}
                  </div>
                </div>
                <div style={{ marginTop: 12 }}>
                  {riscoLoading && !riscoData ? (
                    <div className="muted">Carregando…</div>
                  ) : !transferenciaRows.length ? (
                    <EmptyState title="Sem transferências de contas a receber no período." />
                  ) : (
                    <div className="tableScroll">
                      <div style={{ marginBottom: 8 }}>
                        <GridSearchInput
                          value={transferenciaSearch.query}
                          onChange={transferenciaSearch.setQuery}
                          aria-label="Pesquisar transferências de contas a receber"
                        />
                      </div>
                      <table className="table compact">
                        <thead>
                          <tr>
                            <SortableTh label="Filial" sortKey="filial_label" ariaSort={transferenciaGrid.ariaSort("filial_label")} onToggle={transferenciaGrid.toggleSort} />
                            <SortableTh label="Data" sortKey="dt" ariaSort={transferenciaGrid.ariaSort("dt")} onToggle={transferenciaGrid.toggleSort} />
                            <SortableTh label="Título" sortKey="documento" ariaSort={transferenciaGrid.ariaSort("documento")} onToggle={transferenciaGrid.toggleSort} />
                            <SortableTh label="De" sortKey="entidade_de" ariaSort={transferenciaGrid.ariaSort("entidade_de")} onToggle={transferenciaGrid.toggleSort} />
                            <SortableTh label="Para" sortKey="entidade_para" ariaSort={transferenciaGrid.ariaSort("entidade_para")} onToggle={transferenciaGrid.toggleSort} />
                            <SortableTh label="Valor" sortKey="valor" ariaSort={transferenciaGrid.ariaSort("valor")} onToggle={transferenciaGrid.toggleSort} align="right" />
                          </tr>
                        </thead>
                        <tbody>
                          {transferenciaGrid.slice.map((row: any) => (
                            <tr
                              key={`${row.id_filial}-${row.id_contasreceber}-${row.id_entidade_de}-${row.id_entidade_para}`}
                            >
                              <td>
                                {row.filial_label ||
                                  formatFilialLabel(row.id_filial, row.filial_nome)}
                              </td>
                              <td>
                                {row.dt ? formatDateOnly(row.dt) : formatDateKey(row.data_key)}
                              </td>
                              <td>{row.documento || row.documento_label || "—"}</td>
                              <td>{row.entidade_de || "—"}</td>
                              <td>{row.entidade_para || "—"}</td>
                              <td style={{ textAlign: "right" }}>
                                {formatCurrency(row.valor)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      <GridChrome
                        page={transferenciaGrid.page}
                        totalPages={transferenciaGrid.totalPages}
                        total={transferenciaGrid.total}
                        from={transferenciaGrid.range.from}
                        to={transferenciaGrid.range.to}
                        onPrev={transferenciaGrid.onPrev}
                        onNext={transferenciaGrid.onNext}
                        onResetOrder={transferenciaGrid.resetOrder}
                        isDefaultOrder={transferenciaGrid.isDefaultOrder}
                      />
                    </div>
                  )}
                </div>
              </div>
              </>
            ) : null}

            {canSeeCredFunc ? (
              <div className="card col-12" style={{ marginTop: 12 }}>
                <div>
                  <div className="sectionEyebrow" style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                    Crédito funcionário
                    {credFuncLoading && credFuncData ? (
                      <span className="muted" style={{ fontSize: 11, fontWeight: 500 }}>
                        Atualizando…
                      </span>
                    ) : null}
                  </div>
                  <h2 style={{ marginTop: 4 }}>Vale / a prazo de colaboradores</h2>
                  <div
                    className="profitFilterBar"
                    style={{ marginTop: 10, marginBottom: 8, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}
                  >
                    {(
                      [
                        { value: "todos", label: "Todos" },
                        { value: "suspeitos", label: "Só suspeitos" },
                        { value: "normais", label: "Só normais" },
                      ] as const
                    ).map((opt) => {
                      const active = credFuncStatus === opt.value;
                      return (
                        <button
                          key={opt.value}
                          type="button"
                          aria-pressed={active}
                          onClick={() => {
                            if (opt.value === credFuncStatus) return;
                            setCredFuncStatus(opt.value);
                          }}
                          style={{
                            border: active
                              ? "1px solid var(--color-accent, var(--accent-copper, #3b82f6))"
                              : "1px solid var(--border)",
                            background: active
                              ? "var(--accent-copper-soft, rgba(59,130,246,0.12))"
                              : "transparent",
                            color: "var(--text)",
                            borderRadius: 6,
                            padding: "6px 12px",
                            cursor: "pointer",
                            fontSize: 12,
                            fontWeight: active ? 700 : 500,
                          }}
                        >
                          {opt.label}
                        </button>
                      );
                    })}
                    <MonthYearSelect
                      value={credFuncMonth}
                      onChange={setCredFuncMonth}
                      extraMonths={credFuncExtraMonths}
                      title="Mês de referência do crédito funcionário"
                      aria-label="Mês do crédito funcionário"
                    />
                    <span className="profitFilterCount">
                      {Number(credFuncSummary.suspeitos || 0)} suspeito(s) ·{" "}
                      {formatCurrency(Number(credFuncSummary.usado_total || 0))} usados
                    </span>
                  </div>
                  {credFuncError ? (
                    <div className="muted" style={{ color: "var(--color-negative)" }}>
                      {String(credFuncError)}
                    </div>
                  ) : credFuncLoading && !credFuncData ? (
                    <div className="muted">Carregando crédito de funcionário…</div>
                  ) : !credFuncRows.length ? (
                    <EmptyState
                      title="Sem colaboradores com limite a prazo/vale no mês."
                      detail="Quando houver vale ou crédito a prazo de funcionário no mês, a lista aparece aqui."
                    />
                  ) : (
                    <div className="tableScroll">
                      <GridSearchInput value={credFuncSearch.query} onChange={credFuncSearch.setQuery} aria-label="Pesquisar créditos de funcionário" />
                      {!credFuncSearch.filteredRows.length ? (
                        <EmptyState
                          title="Nenhum colaborador encontrado para a busca."
                          detail="Ajuste o termo ou limpe a pesquisa."
                        />
                      ) : null}
                      <table className="table compact">
                        <thead>
                          <tr>
                            <th></th>
                            <SortableTh label="Funcionário" sortKey="nome" ariaSort={credFuncGrid.ariaSort("nome")} onToggle={credFuncGrid.toggleSort} />
                            <th>Uso no mês</th>
                            <SortableTh label="Limite crédito" sortKey="limite_prazo" ariaSort={credFuncGrid.ariaSort("limite_prazo")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Limite vale" sortKey="limite_vale" ariaSort={credFuncGrid.ariaSort("limite_vale")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Vale (mês)" sortKey="usado_vale" ariaSort={credFuncGrid.ariaSort("usado_vale")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Crédito (mês)" sortKey="usado_prazo" ariaSort={credFuncGrid.ariaSort("usado_prazo")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Usado geral" sortKey="usado_geral" ariaSort={credFuncGrid.ariaSort("usado_geral")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Pago mês" sortKey="pago_mes" ariaSort={credFuncGrid.ariaSort("pago_mes")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Saldo aberto" sortKey="saldo_aberto_geral" ariaSort={credFuncGrid.ariaSort("saldo_aberto_geral")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Saldo mês" sortKey="saldo_aberto_mes" ariaSort={credFuncGrid.ariaSort("saldo_aberto_mes")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <SortableTh label="Lançamentos" sortKey="qtd_usos_mes" ariaSort={credFuncGrid.ariaSort("qtd_usos_mes")} onToggle={credFuncGrid.toggleSort} align="right" />
                            <th>Status</th>
                          </tr>
                        </thead>
                        <tbody>
                          {credFuncGrid.slice.map((row: any) => {
                            const expanded = credFuncExpandido === Number(row.id_funcionario);
                            const suspeito = String(row.status || "") === "Suspeito";
                            const usadoVale = Number(row.usado_vale || 0);
                            const usadoPrazo = Number(row.usado_prazo || 0);
                            const tipoUso =
                              usadoVale > 0 && usadoPrazo <= 0
                                ? "Vale"
                                : usadoPrazo > 0 && usadoVale <= 0
                                  ? "A prazo"
                                  : usadoVale > 0 && usadoPrazo > 0
                                    ? "Misto"
                                    : "—";
                            return (
                              <Fragment key={row.id_funcionario}>
                                <tr
                                  onClick={() =>
                                    setCredFuncExpandido(expanded ? null : Number(row.id_funcionario))
                                  }
                                  style={{
                                    cursor: "pointer",
                                    background: suspeito ? "rgba(239,68,68,0.06)" : undefined,
                                  }}
                                >
                                  <td style={{ width: 28 }}>{expanded ? "▾" : "▸"}</td>
                                  <td>
                                    <strong>{row.nome || "—"}</strong>
                                    {Array.isArray(row.motivos) && row.motivos.length ? (
                                      <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>
                                        {row.motivos.join(" · ")}
                                      </div>
                                    ) : null}
                                  </td>
                                  <td>{tipoUso}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.limite_prazo)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.limite_vale)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(usadoVale)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(usadoPrazo)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.usado_geral)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.pago_mes)}</td>
                                  <td style={{ textAlign: "right", fontWeight: 700 }}>
                                    {formatCurrency(row.saldo_aberto_geral ?? row.saldo_restante)}
                                  </td>
                                  <td style={{ textAlign: "right" }}>
                                    {formatCurrency(row.saldo_aberto_mes)}
                                  </td>
                                  <td style={{ textAlign: "right" }}>{Number(row.qtd_usos_mes || 0)}</td>
                                  <td>
                                    {suspeito ? (
                                      <span style={{ color: "var(--color-negative)", fontWeight: 700 }}>
                                        Suspeito
                                      </span>
                                    ) : (
                                      <span className="muted" style={{ color: "var(--color-positive, #16a34a)" }}>
                                        Normal
                                      </span>
                                    )}
                                  </td>
                                </tr>
                                {expanded ? (
                                  <tr>
                                    <td colSpan={13} style={{ padding: "8px 12px 14px", background: "var(--surface-faint)" }}>
                                      {!(row.usos_abertos_mes?.length || row.usos_pagos_mes?.length || row.usos?.length) ? (
                                        <div className="muted" style={{ fontSize: 12 }}>
                                          Sem lançamentos em aberto ou pagos no mês para este colaborador.
                                        </div>
                                      ) : (
                                        <>
                                        <GridSearchInput value={credFuncUsoQuery} onChange={setCredFuncUsoQuery} aria-label="Pesquisar usos de crédito de funcionário" />
                                        {(() => {
                                          const abertos = row.usos_abertos_mes?.length
                                            ? row.usos_abertos_mes
                                            : (row.usos || []).filter((u: any) => u.grupo_lista === "aberto_mes");
                                          const pagos = row.usos_pagos_mes?.length
                                            ? row.usos_pagos_mes
                                            : (row.usos || []).filter((u: any) => u.grupo_lista === "pago_mes");
                                          const totAberto = Number(row.totalizadores?.abertos_mes ?? abertos.reduce((s: number, u: any) => s + Number(u.saldo_aberto || 0), 0));
                                          const totPago = Number(row.totalizadores?.pagos_mes ?? pagos.reduce((s: number, u: any) => s + Number(u.vlr_pago || 0), 0));
                                          const renderUsoTable = (title: string, items: any[], totalLabel: string, totalValue: number) => {
                                            const filtered = sortGridRows(items, (u: any) => ({
                                              filial: u.filial_label ?? u.id_filial,
                                              data: u.dt_evento,
                                              nome: String(u.observacao || u.historico || u.operador_caixa || "").trim(),
                                            })).filter((u: any) => rowMatchesGridSearch(u, credFuncUsoQuery));
                                            if (!filtered.length) return null;
                                            return (
                                              <div style={{ marginTop: 12 }}>
                                                <div style={{ fontWeight: 650, marginBottom: 6 }}>
                                                  {title} · {formatCurrency(totalValue)}
                                                </div>
                                                {/* Exceção: detalhe expandido do funcionário, não listagem paginada. */}
                                                <table className="table compact">
                                                  <thead>
                                                    <tr>
                                                      <th>Filial</th>
                                                      <th>Data</th>
                                                      <th>NF-e / NFC-e</th>
                                                      <th>Histórico</th>
                                                      <th>Tipo</th>
                                                      <th>Situação</th>
                                                      <th style={{ textAlign: "right" }}>Valor</th>
                                                      <th style={{ textAlign: "right" }}>Pago</th>
                                                      <th style={{ textAlign: "right" }}>Saldo</th>
                                                    </tr>
                                                  </thead>
                                                  <tbody>
                                                    {filtered.map((u: any, idx: number) => (
                                                      <tr key={`${title}-${row.id_funcionario}-${u.id_contasreceber || idx}`}>
                                                        <td>{u.filial_label || formatFilialLabel(u.id_filial, u.filial_nome)}</td>
                                                        <td>{u.dt_evento ? formatDateOnly(u.dt_evento) : "—"}</td>
                                                        <td>{u.documento_label || u.documento_fiscal || "—"}</td>
                                                        <td style={{ minWidth: 160, maxWidth: 360, whiteSpace: "normal" }}>
                                                          {String(u.observacao || u.historico || "").trim() || "—"}
                                                        </td>
                                                        <td>{u.tipo_uso === "vale" ? "Vale" : "Crédito a prazo"}</td>
                                                        <td>{u.situacao === "pago" ? "Pago" : "Em aberto"}</td>
                                                        <td style={{ textAlign: "right" }}>{formatCurrency(u.valor)}</td>
                                                        <td style={{ textAlign: "right" }}>{formatCurrency(u.vlr_pago)}</td>
                                                        <td style={{ textAlign: "right" }}>{formatCurrency(u.saldo_aberto)}</td>
                                                      </tr>
                                                    ))}
                                                  </tbody>
                                                </table>
                                              </div>
                                            );
                                          };
                                          return (
                                            <>
                                              {renderUsoTable("Em aberto neste mês", abertos, "Total em aberto", totAberto)}
                                              {renderUsoTable("Pagos neste mês", pagos, "Total pago", totPago)}
                                            </>
                                          );
                                        })()}
                                        </>
                                      )}
                                    </td>
                                  </tr>
                                ) : null}
                              </Fragment>
                            );
                          })}
                        </tbody>
                      </table>
                      <GridChrome
                        page={credFuncGrid.page}
                        totalPages={credFuncGrid.totalPages}
                        total={credFuncGrid.total}
                        from={credFuncGrid.range.from}
                        to={credFuncGrid.range.to}
                        onPrev={credFuncGrid.onPrev}
                        onNext={credFuncGrid.onNext}
                        onResetOrder={credFuncGrid.resetOrder}
                        isDefaultOrder={credFuncGrid.isDefaultOrder}
                      />
                    </div>
                  )}
                </div>
              </div>
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}
