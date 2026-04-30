"use client";

import { useMemo } from "react";
import Link from "next/link";

import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import {
  buildExecutiveCards,
  buildExecutiveSummary,
  buildPriorityCards,
} from "../lib/dashboard-home.mjs";
import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import HeroMoneyCard from "../components/ui/HeroMoneyCard";
import RiskBadge from "../components/ui/RiskBadge";
import Skeleton from "../components/ui/Skeleton";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";

export const dynamic = "force-dynamic";

export default function Dashboard() {
  const scope = useScopeQuery();
  const {
    claims,
    data: homeData,
    error,
    loading,
    pendingUnavailable,
  } = useBiScopeData<any>({
    moduleKey: "dashboard_home",
    scope,
    errorMessage: "Falha ao carregar dashboard",
    buildRequestUrl: (currentScope) =>
      `/bi/dashboard/home?${buildScopeParams(currentScope).toString()}`,
    requestTimeoutMs: 60_000,
  });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("o dashboard geral")
    : buildModuleLoadingCopy("o dashboard geral");

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const overview = homeData?.overview || {};
  const churnData = homeData?.churn || {};
  const financeData = homeData?.finance || {};
  const fraudOverview = overview?.fraud || {};
  const fraudOperational = fraudOverview?.operational || {};
  const fraudOperationalKpis = fraudOperational?.kpis || {};
  const modeledRisk = fraudOverview?.modeled_risk || overview?.risk || {};
  const modeledRiskKpis = modeledRisk?.kpis || {};
  const modeledRiskWindow = modeledRisk?.window || {};
  const cashBundle = homeData?.cash || {};
  const cashLiveNow = cashBundle?.live_now || overview?.cash?.live_now || {};
  const churnTop = churnData?.top_risk || [];
  const financeAging = financeData?.aging || {};

  const fraudeImpacto = Number(fraudOperationalKpis?.valor_cancelado || 0);
  const fraudeCancelamentos = Number(fraudOperationalKpis?.cancelamentos || 0);
  const riscoModeladoImpacto = Number(modeledRiskKpis?.impacto_total || 0);
  const revenueAtRisk = churnTop.reduce(
    (acc: number, item: any) => acc + Number(item.revenue_at_risk_30d || 0),
    0,
  );
  const caixaRisco =
    Number(financeAging?.receber_total_vencido || 0) +
    Number(financeAging?.pagar_total_vencido || 0);
  const heroRecoverable = fraudeImpacto + revenueAtRisk + caixaRisco;

  const maxRiskDateKey = Number(modeledRiskWindow?.max_data_key || 0);
  const maxRiskDate = formatDateKey(maxRiskDateKey);
  const scopeEndKey = Number(String(scope.dt_fim || "").replaceAll("-", ""));
  const scopeOutdatedForRisk =
    maxRiskDateKey > 0 &&
    scopeEndKey > 0 &&
    scopeEndKey < maxRiskDateKey &&
    Number(modeledRiskKpis?.total_eventos || 0) === 0;

  const priorityCards = useMemo(
    () =>
      buildPriorityCards({
        fraudImpact: fraudeImpacto,
        fraudCancelamentos: fraudeCancelamentos,
        cashLiveNow,
        churnImpact: revenueAtRisk,
        modeledRiskKpis,
        churnTop,
        cashPressure: caixaRisco,
        financeData,
        scope,
      }),
    [
      fraudeImpacto,
      fraudeCancelamentos,
      cashLiveNow,
      revenueAtRisk,
      modeledRiskKpis,
      churnTop,
      caixaRisco,
      financeData,
      scope,
    ],
  );

  const executiveCards = useMemo(
    () =>
      buildExecutiveCards({
        fraudImpact: fraudeImpacto,
        fraudCancelamentos: fraudeCancelamentos,
        modeledRiskKpis,
        cashLiveNow,
        financeData,
        churnData,
        scope,
      }),
    [
      fraudeImpacto,
      fraudeCancelamentos,
      modeledRiskKpis,
      cashLiveNow,
      financeData,
      churnData,
      scope,
    ],
  );

  const aiSummary = useMemo(
    () =>
      buildExecutiveSummary({
        overview,
        scope,
        priorityCards,
      }),
    [overview, scope, priorityCards],
  );

  return (
    <div>
      <AppNav
        title="Dashboard Geral"
        userLabel={userLabel}
        initialUnread={homeData?.notifications_unread}
        deferAuxiliaryLoads
      />

      <div className="container dashboardHome">
        {error ? <div className="card errorCard homeBlock">{error}</div> : null}
        {!homeData ? (
          <section className="homeBlock">
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={3}
              panels={3}
              onRetry={pendingUnavailable ? () => window.location.reload() : undefined}
            />
          </section>
        ) : (
          <>
            {scopeOutdatedForRisk ? (
              <div
                className="card homeBlock"
                style={{ borderColor: "#f59e0b" }}
              >
                <strong>
                  Período além da última janela de risco modelado.
                </strong>{" "}
                Os dados modelados mais recentes vão até{" "}
                <strong>{maxRiskDate}</strong>, mas a home continua mostrando
                cancelamentos operacionais, churn, financeiro e caixa com as
                melhores fontes disponíveis.
              </div>
            ) : null}

            <section className="homeBlock">
              {loading ? (
                <Skeleton height={164} />
              ) : (
                <HeroMoneyCard
                  title="Valor em jogo agora"
                  value={formatCurrency(heroRecoverable)}
                  subtitle={
                    heroRecoverable > 0
                      ? "Montante que pode ser protegido ao agir hoje em fraude operacional, retenção de clientes e pressão de caixa."
                      : "Sem exposição crítica material no período. A home segue como leitura rápida para ritmo comercial, disciplina de caixa e sinais precoces."
                  }
                />
              )}
            </section>

            <section className="kpiStrip homeBlock">
              <article className="card kpi riskCard">
                <div className="label">Fraude operacional</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(fraudeImpacto)}
                </div>
                <div className="muted">
                  {loading
                    ? "..."
                    : fraudeImpacto > 0 || riscoModeladoImpacto > 0
                      ? `${fraudeCancelamentos} cancelamento(s) materiais no período. Risco modelado: ${formatCurrency(riscoModeladoImpacto)}.`
                      : "Sem cancelamentos ou eventos de alto risco acima da linha de intervenção no período."}
                </div>
              </article>
              <article className="card kpi">
                <div className="label">Clientes em risco</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(revenueAtRisk)}
                </div>
                <div className="muted">
                  {revenueAtRisk > 0
                    ? "Receita estimada em risco entre clientes que saíram do padrão."
                    : "Nenhum grupo material de clientes saiu do padrão de retorno neste período."}
                </div>
              </article>
              <article className="card kpi">
                <div className="label">Caixa sob pressão</div>
                <div className="value">
                  {loading ? "..." : formatCurrency(caixaRisco)}
                </div>
                <div className="muted">
                  {caixaRisco > 0
                    ? "Recebíveis e obrigações vencidas já pedem cobrança, renegociação ou reordenação de pagamentos."
                    : "Sem vencidos materiais no período; o monitor de turnos e caixas abertos segue no módulo de Caixa."}
                </div>
              </article>
            </section>

            <section className="homeBlock">
              <div className="panelHead">
                <div>
                  <div className="sectionEyebrow">Radar executivo</div>
                  <h2>Impacto, causa provável e próximo clique</h2>
                </div>
              </div>
              {loading ? <Skeleton height={248} /> : null}
              {!loading ? (
                <div className="executiveGrid">
                  {executiveCards.map((item: any) => (
                    <article className="card focusCard executiveCard" key={item.key}>
                      <div className="focusLabel">{item.section}</div>
                      <div className="priorityTop">
                        <RiskBadge level={item.severity} />
                        <span className="priorityImpact">{item.value}</span>
                      </div>
                      <h3 className="executiveTitle">{item.title}</h3>
                      <p className="focusDetail">{item.detail}</p>
                      <p className="executiveAction">{item.action}</p>
                      <Link href={item.href} className="subtleLink">
                        {item.cta}
                      </Link>
                    </article>
                  ))}
                </div>
              ) : null}
            </section>

            <section className="homePrimeGrid homeBlock">
              <div className="card">
                <div className="panelHead">
                  <div>
                    <div className="sectionEyebrow">Prioridades do dia</div>
                    <h2>O que exige ação hoje</h2>
                  </div>
                </div>
                {loading ? <Skeleton height={220} /> : null}
                {!loading && !priorityCards.length ? (
                  <EmptyState
                    title="Nenhuma frente exigiu intervenção acima da linha de corte."
                    detail="Use o radar executivo e o copiloto para manter disciplina de execução, acompanhar sinais precoces e agir se o dia mudar de rumo."
                  />
                ) : null}
                {!loading ? (
                  <div className="priorityGrid">
                    {priorityCards.map((card: any) => (
                      <article className="priorityCard" key={card.title}>
                        <div className="priorityTop">
                          <RiskBadge level={card.severity} />
                          <span className="priorityImpact">{card.impact}</span>
                        </div>
                        <h3>{card.title}</h3>
                        <p>{card.summary}</p>
                        <Link href={card.href} className="btn">
                          {card.cta}
                        </Link>
                      </article>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className="card aiSummaryCard">
                <div className="sectionEyebrow">{aiSummary.title}</div>
                <h2>{aiSummary.headline}</h2>
                {loading ? <Skeleton height={180} /> : null}
                {!loading ? (
                  <>
                    <div className="priorityTop">
                      <RiskBadge
                        level={String(aiSummary.status || "ok").toUpperCase()}
                      />
                      <span className="priorityImpact">
                        {aiSummary.impactLabel}
                      </span>
                    </div>
                    <p className="aiSummaryLead">{aiSummary.summary}</p>
                    {aiSummary.highlights?.length ? (
                      <div className="aiHighlights">
                        {aiSummary.highlights.map((item: string, idx: number) => (
                          <div key={`${item}-${idx}`} className="aiHighlight">
                            {item}
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <div className="actionCard" style={{ marginTop: 4 }}>
                      <div className="actionHead">
                        <div className="focusLabel">Problema principal</div>
                        <strong>{aiSummary.problem}</strong>
                      </div>
                      <div className="muted">
                        Impacto: {aiSummary.impactLabel}
                      </div>
                    </div>
                    <div className="actionCard">
                      <div className="actionHead">
                        <div className="focusLabel">Causa provável</div>
                        <strong>{aiSummary.cause}</strong>
                      </div>
                      <div className="muted">
                        Prioridade: {aiSummary.priority}
                      </div>
                    </div>
                    <div className="actionCard">
                      <div className="actionHead">
                        <div className="focusLabel">Ação recomendada</div>
                        <strong>{aiSummary.action}</strong>
                      </div>
                      {aiSummary.evidence?.length ? (
                        <div className="evidenceRow">
                          {aiSummary.evidence.map(
                            (item: string, idx: number) => (
                              <span
                                key={`${item}-${idx}`}
                                className="evidenceChip"
                              >
                                {item}
                              </span>
                            ),
                          )}
                        </div>
                      ) : null}
                    </div>
                    {aiSummary.primaryShortcut ? (
                      <div className="actionCard">
                        <div className="actionHead">
                          <div className="focusLabel">Próximo clique útil</div>
                          <strong>
                            Abra o módulo que resolve primeiro a prioridade do dia.
                          </strong>
                        </div>
                        <div className="shortcutRow">
                          <Link href={aiSummary.primaryShortcut.href} className="btn">
                            {aiSummary.primaryShortcut.label}
                          </Link>
                          {(aiSummary.secondaryShortcuts || []).map(
                            (item: any, idx: number) => (
                              <Link
                                href={item.href}
                                key={`${item.label}-${idx}`}
                                className="secondaryShortcut"
                              >
                                {item.label}
                                {item.impactLabel ? ` · ${item.impactLabel}` : ""}
                              </Link>
                            ),
                          )}
                        </div>
                      </div>
                    ) : null}
                    <div className="actionCard">
                      <div className="actionHead">
                        <div className="focusLabel">Confiança da leitura</div>
                        <strong>{aiSummary.confidenceLabel}</strong>
                      </div>
                      <div className="muted">{aiSummary.confidenceReason}</div>
                      </div>
                    {aiSummary.secondaryFocus?.length ? (
                      <div className="aiSummaryFooter">
                        <span>Próximos focos</span>
                        {aiSummary.secondaryFocus.map(
                          (item: any, idx: number) => (
                            <strong key={`${item.label}-${idx}`}>
                              {item.label} · {item.impactLabel} ·{" "}
                              {item.priority}
                            </strong>
                          ),
                        )}
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>
            </section>
          </>
        )}
      </div>
    </div>
  );
}
