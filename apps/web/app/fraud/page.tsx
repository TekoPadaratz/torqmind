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
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
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
import { sortGridRows } from "../lib/grid-sort";
import { canAccessScreenKey } from "../lib/session";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { rowMatchesGridSearch, useGridSearch } from "../lib/use-grid-search";

export const dynamic = "force-dynamic";

function currentAnoMesSP(): number {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
  });
  const parts = fmt.formatToParts(new Date());
  const y = Number(parts.find((p) => p.type === "year")?.value || 0);
  const m = Number(parts.find((p) => p.type === "month")?.value || 0);
  return y * 100 + m;
}

function fmtAnoMes(ym: number): string {
  const y = Math.floor(ym / 100);
  const m = ym % 100;
  const nomes = [
    "", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
  ];
  return `${nomes[m] || m}/${y}`;
}

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

function GridPager({
  page,
  totalPages,
  total,
  pageSize,
  onPrev,
  onNext,
}: {
  page: number;
  totalPages: number;
  total: number;
  pageSize: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  if (total <= pageSize) return null;
  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: 8,
        alignItems: "center",
        justifyContent: "flex-end",
        marginTop: 10,
      }}
    >
      <span className="muted" style={{ fontSize: 12 }}>
        Página {page} de {totalPages}
      </span>
      <button
        type="button"
        disabled={page <= 1}
        onClick={onPrev}
        style={{
          border: "1px solid var(--border)",
          background: "transparent",
          color: "var(--text)",
          borderRadius: 6,
          padding: "6px 12px",
          cursor: page <= 1 ? "not-allowed" : "pointer",
          fontSize: 12,
          opacity: page <= 1 ? 0.5 : 1,
        }}
      >
        Anterior
      </button>
      <button
        type="button"
        disabled={page >= totalPages}
        onClick={onNext}
        style={{
          border: "1px solid var(--border)",
          background: "transparent",
          color: "var(--text)",
          borderRadius: 6,
          padding: "6px 12px",
          cursor: page >= totalPages ? "not-allowed" : "pointer",
          fontSize: 12,
          opacity: page >= totalPages ? 0.5 : 1,
        }}
      >
        Próxima
      </button>
    </div>
  );
}

export default function FraudPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  // Default "Todas": suspeitas dependem do join movlcto; com sync atrasado o filtro
  // "Só suspeitas" esconde o período inteiro (gap jul/2026+).
  const [trocaSoSuspeitas, setTrocaSoSuspeitas] = useState(false);
  const [trocaFormaNova, setTrocaFormaNova] = useState<"todos" | "prazo" | "cheque_pre">("todos");
  const [creditoRisco, setCreditoRisco] = useState<"suspeitas" | "normais" | "todas">("suspeitas");
  const [creditoUsoQuery, setCreditoUsoQuery] = useState("");
  const [creditoPage, setCreditoPage] = useState(1);
  const [creditoExpandido, setCreditoExpandido] = useState<string | null>(null);
  const [credFuncStatus, setCredFuncStatus] = useState<"todos" | "suspeitos" | "normais">("todos");
  const [credFuncMonth, setCredFuncMonth] = useState<number>(() => currentAnoMesSP());
  const [credFuncPage, setCredFuncPage] = useState(1);
  const [credFuncExpandido, setCredFuncExpandido] = useState<number | null>(null);
  const [credFuncUsoQuery, setCredFuncUsoQuery] = useState("");
  const [trocaPage, setTrocaPage] = useState(1);
  const [cancelPage, setCancelPage] = useState(1);
  const [operadorPage, setOperadorPage] = useState(1);
  const pageSize = 20;
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
    setTrocaSoSuspeitas(true);
    setTrocaFormaNova("todos");
    setCreditoRisco("suspeitas");
    setCreditoPage(1);
    setTrocaPage(1);
    setCancelPage(1);
    setOperadorPage(1);
    setCreditoExpandido(null);
    setCredFuncStatus("todos");
    setCredFuncPage(1);
    setCredFuncExpandido(null);
  }, [scopeKey]);
  useEffect(() => {
    setCreditoPage(1);
  }, [creditoRisco]);
  useEffect(() => {
    setCredFuncPage(1);
    setCredFuncExpandido(null);
  }, [credFuncStatus, credFuncMonth]);
  useEffect(() => {
    setTrocaPage(1);
  }, [trocaSoSuspeitas, trocaFormaNova]);

  // Core operacional (operadores/cancelamentos/gráficos) — isolado dos filtros de risco.
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "fraud_overview_core",
      scope,
      errorMessage: "Falha ao carregar fraude",
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
  const credFuncMeses = useMemo(() => {
    const fromApi = Array.isArray(credFuncPayload?.meses_disponiveis)
      ? credFuncPayload.meses_disponiveis.map((m: any) => Number(m)).filter((m: number) => Number.isFinite(m) && m > 0)
      : [];
    const set = new Set<number>([credFuncMonth, ...fromApi]);
    // Garante janela recente (padrão DRE/Solvência) mesmo sem mash antigo.
    let cursor = currentAnoMesSP();
    for (let i = 0; i < 18; i += 1) {
      set.add(cursor);
      const y = Math.floor(cursor / 100);
      const m = cursor % 100;
      cursor = m <= 1 ? (y - 1) * 100 + 12 : y * 100 + (m - 1);
    }
    return Array.from(set).sort((a, b) => b - a);
  }, [credFuncMonth, credFuncPayload?.meses_disponiveis]);
  const credFuncTotalPages = Math.max(1, Math.ceil(credFuncSearch.filteredRows.length / pageSize));
  const credFuncPageSafe = Math.min(Math.max(1, credFuncPage), credFuncTotalPages);
  const credFuncPageRows = credFuncSearch.filteredRows.slice(
    (credFuncPageSafe - 1) * pageSize,
    credFuncPageSafe * pageSize,
  );

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
  const [devolucaoPage, setDevolucaoPage] = useState(1);
  const devolucaoTotalPages = Math.max(1, Math.ceil(devolucaoSearch.filteredRows.length / pageSize));
  const devolucaoPageSafe = Math.min(Math.max(1, devolucaoPage), devolucaoTotalPages);
  const devolucaoPageRows = devolucaoSearch.filteredRows.slice(
    (devolucaoPageSafe - 1) * pageSize,
    devolucaoPageSafe * pageSize,
  );

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
  const [transferenciaPage, setTransferenciaPage] = useState(1);
  const transferenciaTotalPages = Math.max(
    1,
    Math.ceil(transferenciaSearch.filteredRows.length / pageSize),
  );
  const transferenciaPageSafe = Math.min(
    Math.max(1, transferenciaPage),
    transferenciaTotalPages,
  );
  const transferenciaPageRows = transferenciaSearch.filteredRows.slice(
    (transferenciaPageSafe - 1) * pageSize,
    transferenciaPageSafe * pageSize,
  );

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
  const creditoTotalPages = Math.max(1, Math.ceil(creditosFiltered.length / pageSize));
  const creditoPageSafe = Math.min(Math.max(1, creditoPage), creditoTotalPages);
  const creditosPageRows = creditosFiltered.slice(
    (creditoPageSafe - 1) * pageSize,
    creditoPageSafe * pageSize,
  );

  const lastEventsOperational = useMemo(() => {
    const rows = Array.isArray(data?.last_events) ? data.last_events : [];
    const filtered = rows.filter((e: any) => {
      const tn = Number(e?.turno_numero);
      const label = String(e?.turno_label || "").toLowerCase();
      if (Number.isFinite(tn) && tn < 1) return false;
      if (
        label.includes("sem cadastro") ||
        label.includes("não resolvido") ||
        label.includes("nao resolvido") ||
        label === "caixa geral"
      ) {
        return false;
      }
      if (!e?.data && !e?.data_key) return false;
      const doc = e?.documento_label || e?.documento_fiscal;
      if (!doc || String(doc).trim() === "—" || String(doc).trim() === "-") return false;
      return true;
    });
    return sortGridRows(filtered, (e: any) => ({
      filial: e.filial_label ?? e.filial_nome ?? e.id_filial,
      data: e.data || e.data_key,
      nome: e.usuario_label || e.operador_label,
    }));
  }, [data?.last_events]);
  const cancelSearch = useGridSearch(lastEventsOperational);

  const cancelTotalPages = Math.max(1, Math.ceil(cancelSearch.filteredRows.length / pageSize));
  const cancelPageSafe = Math.min(Math.max(1, cancelPage), cancelTotalPages);
  const cancelPageRows = cancelSearch.filteredRows.slice(
    (cancelPageSafe - 1) * pageSize,
    cancelPageSafe * pageSize,
  );

  const topUsersRows = useMemo(
    () => (Array.isArray(data?.top_users) ? data.top_users : []),
    [data?.top_users],
  );
  const operadorSearch = useGridSearch(topUsersRows);
  const operadorTotalPages = Math.max(1, Math.ceil(operadorSearch.filteredRows.length / pageSize));
  const operadorPageSafe = Math.min(Math.max(1, operadorPage), operadorTotalPages);
  const operadorPageRows = operadorSearch.filteredRows.slice(
    (operadorPageSafe - 1) * pageSize,
    operadorPageSafe * pageSize,
  );

  const trocaTotalPages = Math.max(1, Math.ceil(trocaSearch.filteredRows.length / pageSize));
  const trocaPageSafe = Math.min(Math.max(1, trocaPage), trocaTotalPages);
  const trocaPageRows = trocaSearch.filteredRows.slice(
    (trocaPageSafe - 1) * pageSize,
    trocaPageSafe * pageSize,
  );

  useEffect(() => {
    setCreditoPage(1);
  }, [creditosSearch.query]);
  useEffect(() => {
    setCredFuncPage(1);
  }, [credFuncSearch.query]);
  useEffect(() => {
    setTrocaPage(1);
  }, [trocaSearch.query]);
  useEffect(() => {
    setCancelPage(1);
  }, [cancelSearch.query]);
  useEffect(() => {
    setOperadorPage(1);
  }, [operadorSearch.query]);
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
                        <th>Filial</th>
                        <th>Operador</th>
                        <th>Cancelamentos</th>
                        <th>Valor</th>
                      </tr>
                    </thead>
                    <tbody>
                      {operadorPageRows.map((u: any) => (
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
                <GridPager
                  page={operadorPageSafe}
                  totalPages={operadorTotalPages}
                  total={operadorSearch.filteredRows.length}
                  pageSize={pageSize}
                  onPrev={() => setOperadorPage((p) => Math.max(1, p - 1))}
                  onNext={() =>
                    setOperadorPage((p) => Math.min(operadorTotalPages, p + 1))
                  }
                />
              </div>

              <div className="card col-12">
                <h2>Últimos cancelamentos operacionais</h2>
                <GridSearchInput value={cancelSearch.query} onChange={cancelSearch.setQuery} aria-label="Pesquisar cancelamentos operacionais" />
                {!loading && !cancelSearch.filteredRows.length ? (
                  <EmptyState
                    title="Sem eventos operacionais recentes."
                    detail="Não houve cancelamentos reconciliados por turno no período analisado."
                  />
                ) : null}
                <div className="tableScroll">
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Filial</th>
                        <th>Data</th>
                        <th>Turno</th>
                        <th>Operador</th>
                        <th>Documento</th>
                        <th>Valor</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cancelPageRows.map((e: any) => (
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
                <GridPager
                  page={cancelPageSafe}
                  totalPages={cancelTotalPages}
                  total={cancelSearch.filteredRows.length}
                  pageSize={pageSize}
                  onPrev={() => setCancelPage((p) => Math.max(1, p - 1))}
                  onNext={() =>
                    setCancelPage((p) => Math.min(cancelTotalPages, p + 1))
                  }
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
                          <th>Filial</th>
                          <th>Data</th>
                          <th>Cliente</th>
                          <th>Operador</th>
                          <th style={{ textAlign: "right" }}>Injetado</th>
                          <th style={{ textAlign: "right" }}>Saldo na operação</th>
                          <th style={{ textAlign: "right" }}>Saldo atual</th>
                          <th>Histórico</th>
                          <th>Risco</th>
                        </tr>
                      </thead>
                      <tbody>
                        {creditosPageRows.map((c: any, idx: number) => {
                          const rowKey = `${c.id_filial}-${c.id_cliente || idx}-${c.id_mov || (creditoPageSafe - 1) * pageSize + idx}`;
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
                    <GridPager
                      page={creditoPageSafe}
                      totalPages={creditoTotalPages}
                      total={creditosFiltered.length}
                      pageSize={pageSize}
                      onPrev={() => setCreditoPage((p) => Math.max(1, p - 1))}
                      onNext={() =>
                        setCreditoPage((p) => Math.min(creditoTotalPages, p + 1))
                      }
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
                              <th>Filial</th>
                              <th>Data</th>
                              <th>Documento</th>
                              <th>Forma anterior</th>
                              <th>Forma nova</th>
                              <th>Usuário</th>
                              <th style={{ textAlign: "right" }}>Valor</th>
                              <th>Venda</th>
                              <th>Risco</th>
                            </tr>
                          </thead>
                          <tbody>
                            {trocaPageRows.map((row: any) => (
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
                        <GridPager
                          page={trocaPageSafe}
                          totalPages={trocaTotalPages}
                          total={trocaSearch.filteredRows.length}
                          pageSize={pageSize}
                          onPrev={() => setTrocaPage((p) => Math.max(1, p - 1))}
                          onNext={() =>
                            setTrocaPage((p) => Math.min(trocaTotalPages, p + 1))
                          }
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
                    <h2 style={{ marginTop: 4 }}>Devoluções de entrada</h2>
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
                    <EmptyState title="Sem devoluções de entrada no período." />
                  ) : (
                    <div className="tableScroll">
                      <div style={{ marginBottom: 8 }}>
                        <GridSearchInput
                          value={devolucaoSearch.query}
                          onChange={(v) => {
                            devolucaoSearch.setQuery(v);
                            setDevolucaoPage(1);
                          }}
                          aria-label="Pesquisar devoluções"
                        />
                      </div>
                      <table className="table compact">
                        <thead>
                          <tr>
                            <th>Filial</th>
                            <th>Data</th>
                            <th>Documento</th>
                            <th>Operador</th>
                            <th style={{ textAlign: "right" }}>Valor</th>
                          </tr>
                        </thead>
                        <tbody>
                          {devolucaoPageRows.map((row: any) => (
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
                      <GridPager
                        page={devolucaoPageSafe}
                        totalPages={devolucaoTotalPages}
                        total={devolucaoSearch.filteredRows.length}
                        pageSize={pageSize}
                        onPrev={() => setDevolucaoPage((p) => Math.max(1, p - 1))}
                        onNext={() =>
                          setDevolucaoPage((p) => Math.min(devolucaoTotalPages, p + 1))
                        }
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
                          onChange={(v) => {
                            transferenciaSearch.setQuery(v);
                            setTransferenciaPage(1);
                          }}
                          aria-label="Pesquisar transferências de contas a receber"
                        />
                      </div>
                      <table className="table compact">
                        <thead>
                          <tr>
                            <th>Filial</th>
                            <th>Data</th>
                            <th>Título</th>
                            <th>De</th>
                            <th>Para</th>
                            <th style={{ textAlign: "right" }}>Valor</th>
                          </tr>
                        </thead>
                        <tbody>
                          {transferenciaPageRows.map((row: any) => (
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
                      <GridPager
                        page={transferenciaPageSafe}
                        totalPages={transferenciaTotalPages}
                        total={transferenciaSearch.filteredRows.length}
                        pageSize={pageSize}
                        onPrev={() => setTransferenciaPage((p) => Math.max(1, p - 1))}
                        onNext={() =>
                          setTransferenciaPage((p) =>
                            Math.min(transferenciaTotalPages, p + 1),
                          )
                        }
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
                    <label className="profitScopeMonth" title="Mês de referência do crédito funcionário">
                      <span className="profitScopeMonthLabel">Mês</span>
                      <select
                        className="profitScopeMonthSelect"
                        value={credFuncMonth}
                        onChange={(e) => setCredFuncMonth(Number(e.target.value))}
                        aria-label="Mês do crédito funcionário"
                      >
                        {credFuncMeses.map((m) => (
                          <option key={m} value={m}>
                            {fmtAnoMes(m)}
                          </option>
                        ))}
                      </select>
                    </label>
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
                            <th>Funcionário</th>
                            <th style={{ textAlign: "right" }}>Limite a prazo</th>
                            <th style={{ textAlign: "right" }}>Limite vale</th>
                            <th style={{ textAlign: "right" }}>Limite total</th>
                            <th style={{ textAlign: "right" }}>Usado a prazo</th>
                            <th style={{ textAlign: "right" }}>Usado vale</th>
                            <th style={{ textAlign: "right" }}>Usado total</th>
                            <th style={{ textAlign: "right" }}>Saldo</th>
                            <th style={{ textAlign: "right" }}>Usos</th>
                            <th>Status</th>
                          </tr>
                        </thead>
                        <tbody>
                          {credFuncPageRows.map((row: any) => {
                            const expanded = credFuncExpandido === Number(row.id_funcionario);
                            const suspeito = String(row.status || "") === "Suspeito";
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
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.limite_prazo)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.limite_vale)}</td>
                                  <td style={{ textAlign: "right", fontWeight: 700 }}>
                                    {formatCurrency(row.limite_total ?? row.limite)}
                                  </td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.usado_prazo)}</td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.usado_vale)}</td>
                                  <td style={{ textAlign: "right", fontWeight: 700 }}>
                                    {formatCurrency(row.usado_mes)}
                                  </td>
                                  <td style={{ textAlign: "right" }}>{formatCurrency(row.saldo_restante)}</td>
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
                                    <td colSpan={11} style={{ padding: "8px 12px 14px", background: "var(--surface-faint)" }}>
                                      {!row.usos?.length ? (
                                        <div className="muted" style={{ fontSize: 12 }}>
                                          Sem usos resolvidos no mês para este colaborador.
                                        </div>
                                      ) : (
                                        <>
                                        <GridSearchInput value={credFuncUsoQuery} onChange={setCredFuncUsoQuery} aria-label="Pesquisar usos de crédito de funcionário" />
                                        <table className="table compact">
                                          <thead>
                                            <tr>
                                              <th>Filial</th>
                                              <th>Data</th>
                                              <th>NF-e / NFC-e</th>
                                              <th>Cliente</th>
                                              <th>Tipo</th>
                                              <th>Operador de caixa</th>
                                              <th style={{ textAlign: "right" }}>Valor</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {sortGridRows(row.usos || [], (u: any) => ({
                                              filial: u.filial_label ?? u.id_filial,
                                              data: u.dt_evento,
                                              nome: u.cliente_nome || u.operador_caixa,
                                            })).filter((u: any) => rowMatchesGridSearch(u, credFuncUsoQuery)).map((u: any, idx: number) => (
                                              <tr key={`${row.id_funcionario}-${u.id_contasreceber || idx}`}>
                                                <td>
                                                  {u.filial_label ||
                                                    formatFilialLabel(u.id_filial, u.filial_nome)}
                                                </td>
                                                <td>
                                                  {u.dt_evento
                                                    ? formatDateOnly(u.dt_evento)
                                                    : "—"}
                                                </td>
                                                <td>
                                                  {u.documento_label || u.documento_fiscal || "—"}
                                                  {u.atipico ? (
                                                    <span style={{ color: "var(--color-negative)", marginLeft: 6, fontWeight: 600 }}>
                                                      atípico
                                                    </span>
                                                  ) : null}
                                                </td>
                                                <td>{u.cliente_nome || "—"}</td>
                                                <td>{u.tipo_uso === "vale" ? "Vale" : "A prazo"}</td>
                                                <td>{u.operador_caixa || "—"}</td>
                                                <td style={{ textAlign: "right" }}>
                                                  {formatCurrency(u.valor)}
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
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
                      <GridPager
                        page={credFuncPageSafe}
                        totalPages={credFuncTotalPages}
                        total={credFuncSearch.filteredRows.length}
                        pageSize={pageSize}
                        onPrev={() => setCredFuncPage((p) => Math.max(1, p - 1))}
                        onNext={() =>
                          setCredFuncPage((p) => Math.min(credFuncTotalPages, p + 1))
                        }
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
