'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateOnly,
  formatFilialLabel,
} from '../lib/format';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { loadSession, readCachedSession } from '../lib/session';
import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import HeroMoneyCard from '../components/ui/HeroMoneyCard';
import RiskBadge from '../components/ui/RiskBadge';
import Skeleton from '../components/ui/Skeleton';

export const dynamic = 'force-dynamic';

function detailsHref(path: string, scope: any) {
  return buildScopeParams(scope).toString()
    ? `${path}?${buildScopeParams(scope).toString()}`
    : path;
}

function buildPriorityCards({
  fraudImpact,
  fraudCancelamentos,
  churnImpact,
  cashPressure,
  modeledRiskKpis,
  churnTop,
  financeData,
  scope,
}: {
  fraudImpact: number;
  fraudCancelamentos: number;
  churnImpact: number;
  cashPressure: number;
  modeledRiskKpis: any;
  churnTop: any[];
  financeData: any;
  scope: any;
}) {
  const cards = [];

  if (fraudImpact > 0) {
    cards.push({
      title: 'Auditar cancelamentos materiais do período',
      severity: Number(modeledRiskKpis?.eventos_alto_risco || 0) > 0 ? 'HIGH' : 'WARN',
      impact: formatCurrency(fraudImpact),
      summary:
        Number(modeledRiskKpis?.eventos_alto_risco || 0) > 0
          ? `${fraudCancelamentos} cancelamentos relevantes no período e ${Number(modeledRiskKpis?.eventos_alto_risco || 0)} evento(s) de alto risco modelado.`
          : `${fraudCancelamentos} cancelamentos relevantes concentram a materialidade operacional do período.`,
      cta: 'Abrir antifraude',
      href: detailsHref('/fraud', scope),
    });
  }

  if (cashPressure > 0) {
    cards.push({
      title: 'Cobrar e renegociar vencidos mais concentrados',
      severity: Number(financeData?.aging?.receber_total_vencido || 0) > 0 ? 'WARN' : 'INFO',
      impact: formatCurrency(cashPressure),
      summary: 'Recebíveis e obrigações vencidas já pressionam caixa e exigem régua de ação imediata.',
      cta: 'Abrir financeiro',
      href: detailsHref('/finance', scope),
    });
  }

  if (churnImpact > 0) {
    const topCustomer = churnTop[0];
    cards.push({
      title: 'Recuperar clientes que saíram do padrão de retorno',
      severity: Number(topCustomer?.churn_score || 0) >= 70 ? 'HIGH' : 'WARN',
      impact: formatCurrency(churnImpact),
      summary: `${churnTop.length} clientes identificados já mostram perda de frequência, ticket ou intervalo de recompra.`,
      cta: 'Abrir clientes',
      href: detailsHref('/customers', scope),
    });
  }

  return cards
    .sort((a, b) => {
      const severityRank = { HIGH: 3, WARN: 2, INFO: 1 } as Record<string, number>;
      return severityRank[b.severity] - severityRank[a.severity];
    })
    .slice(0, 2);
}

function buildOperationalFocus({
  overview,
  fraudImpact,
  churnImpact,
  cashPressure,
  churnTop,
  financeData,
  scope,
}: {
  overview: any;
  fraudImpact: number;
  churnImpact: number;
  cashPressure: number;
  churnTop: any[];
  financeData: any;
  scope: any;
}) {
  const latestInsight = (overview?.insights_generated || [])[0];
  const topCustomer = churnTop[0];

  return [
    {
      title: 'Maior foco do período',
      value: formatCurrency(fraudImpact),
      detail:
        fraudImpact > 0
          ? 'A leitura executiva está ancorada em cancelamentos operacionais reais, não apenas no motor modelado de risco.'
          : 'Sem materialidade operacional relevante de cancelamentos no recorte atual.',
      href: detailsHref('/fraud', scope),
      cta: 'Ver investigação',
    },
    {
      title: 'Oportunidade de recuperação',
      value: formatCurrency(churnImpact),
      detail:
        topCustomer?.cliente_nome
          ? `${topCustomer.cliente_nome} lidera a fila de reativação e ajuda a recuperar receita ainda neste ciclo.`
          : 'A fila de recuperação comercial já está pronta para priorização pela equipe.',
      href: detailsHref('/customers', scope),
      cta: 'Ver clientes',
    },
    {
      title: 'Pressão imediata de caixa',
      value: formatCurrency(cashPressure),
      detail:
        cashPressure > 0
          ? `${Number(financeData?.aging?.receber_total_vencido || 0)} recebíveis vencidos concentram a necessidade de cobrança e renegociação.`
          : latestInsight?.message || 'O caixa segue estável no período auditado.',
      href: detailsHref('/finance', scope),
      cta: 'Ver financeiro',
    },
  ];
}

function buildExecutiveSummary({
  overview,
}: {
  overview: any;
}) {
  const copiloto = overview?.jarvis || {};
  return {
    title: copiloto.title || 'Copiloto operacional',
    headline: copiloto.headline || 'Operação estável no recorte atual.',
    summary: copiloto.summary || 'Sem foco crítico acima da linha de corte.',
    impactLabel: copiloto.impact_label || 'Sem exposição crítica material',
    action: copiloto.action || 'Manter a rotina de acompanhamento diário.',
    priority: copiloto.priority || 'Acompanhar',
    evidence: copiloto.evidence || [],
    highlights: copiloto.highlights || [],
    secondaryFocus: copiloto.secondary_focus || [],
    status: copiloto.status || 'ok',
  };
}

export default function Dashboard() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [homeData, setHomeData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  useEffect(() => {
    if (!scope.ready) return;

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const me = await loadSession(router, 'product');
        if (!me) return;
        setClaims(me);
        if (!scope.dt_ini || !scope.dt_fim) {
          router.replace(me?.home_path || '/dashboard');
          return;
        }

        const homeRes = await apiGet(`/bi/dashboard/home?${buildScopeParams(scope).toString()}`);
        setHomeData(homeRes);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar dashboard'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filiais_key, scope.id_empresa, scope.ready]);

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
  const cashHistorical = cashBundle?.historical || overview?.cash?.historical || {};
  const cashLiveNow = cashBundle?.live_now || overview?.cash?.live_now || {};
  const filialLabel =
    homeData?.scope?.filial_label
    || (scope.id_filiais.length > 1 ? `${scope.id_filiais.length} filiais selecionadas` : formatFilialLabel(scope.id_filial));
  const churnTop = churnData?.top_risk || [];
  const financeAging = financeData?.aging || {};

  const fraudeImpacto = Number(fraudOperationalKpis?.valor_cancelado || 0);
  const fraudeCancelamentos = Number(fraudOperationalKpis?.cancelamentos || 0);
  const riscoModeladoImpacto = Number(modeledRiskKpis?.impacto_total || 0);
  const revenueAtRisk = churnTop.reduce((acc: number, item: any) => acc + Number(item.revenue_at_risk_30d || 0), 0);
  const caixaRisco = Number(financeAging?.receber_total_vencido || 0) + Number(financeAging?.pagar_total_vencido || 0);
  const heroRecoverable = fraudeImpacto + revenueAtRisk + caixaRisco;

  const maxRiskDateKey = Number(modeledRiskWindow?.max_data_key || 0);
  const maxRiskDate = formatDateKey(maxRiskDateKey);
  const scopeEndKey = Number(String(scope.dt_fim || '').replaceAll('-', ''));
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
        churnImpact: revenueAtRisk,
        cashPressure: caixaRisco,
        modeledRiskKpis,
        churnTop,
        financeData,
        scope,
      }),
    [fraudeImpacto, fraudeCancelamentos, revenueAtRisk, caixaRisco, modeledRiskKpis, churnTop, financeData, scope]
  );

  const operationalFocus = useMemo(
    () =>
      buildOperationalFocus({
        overview,
        fraudImpact: fraudeImpacto,
        churnImpact: revenueAtRisk,
        cashPressure: caixaRisco,
        churnTop,
        financeData,
        scope,
      }),
    [overview, fraudeImpacto, revenueAtRisk, caixaRisco, churnTop, financeData, scope]
  );

  const aiSummary = useMemo(
    () =>
      buildExecutiveSummary({
        overview,
      }),
    [overview]
  );

  const sourceCards = [
    {
      title: 'Fraude executiva',
      value: formatCurrency(fraudeImpacto),
      detail:
        riscoModeladoImpacto > 0
          ? `Cancelamentos operacionais do período com apoio do motor modelado (${formatCurrency(riscoModeladoImpacto)}).`
          : 'Baseada em cancelamentos operacionais reais do período, mesmo sem risco modelado material.',
    },
    {
      title: 'Churn',
      value: String(churnData?.snapshot_meta?.snapshot_status || churnData?.snapshot_status || churnData?.summary?.total_top_risk || 'missing'),
      detail: `Fonte ${churnData?.snapshot_meta?.source_kind || churnData?.snapshot_meta?.source_table || 'indisponível'} em ${formatDateOnly(
        churnData?.snapshot_meta?.effective_dt_ref || churnData?.snapshot_meta?.requested_dt_ref || claims?.server_today
      )}.`,
    },
    {
      title: 'Financeiro',
      value: String(financeAging?.snapshot_status || 'missing'),
      detail: `Precisão ${financeAging?.precision_mode || 'missing'} com referência efetiva em ${formatDateOnly(
        financeAging?.effective_dt_ref || financeAging?.requested_dt_ref || claims?.server_today
      )}.`,
    },
    {
      title: 'Caixa',
      value: `${String(cashHistorical?.source_status || 'unavailable')} / ${String(cashLiveNow?.source_status || 'unavailable')}`,
      detail: `Histórico do período e monitor de agora ficam separados para evitar falso zero; ${Number(cashLiveNow?.kpis?.caixas_stale || 0)} turno(s) stale ficam fora do ao vivo.`,
    },
  ];

  return (
    <div>
      <AppNav title="Dashboard Geral" userLabel={userLabel} initialUnread={homeData?.notifications_unread} />

      <div className="container dashboardHome">
        <div className="card toolbar">
          <div>
            <div className="muted">Escopo ativo</div>
            <div className="scopeLine">
              <strong>{formatDateOnly(scope.dt_ini)}</strong> até <strong>{formatDateOnly(scope.dt_fim)}</strong> · Base do servidor{' '}
              <strong>{formatDateOnly(homeData?.scope?.requested_dt_ref || claims?.server_today || scope.dt_fim)}</strong> · Filial{' '}
              <strong>{filialLabel || formatFilialLabel(scope.id_filial)}</strong> · Empresa{' '}
              <strong>{scope.id_empresa || claims?.id_empresa || '1'}</strong>
            </div>
          </div>
        </div>

        {error ? <div className="card errorCard homeBlock">{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card homeBlock" style={{ borderColor: '#f59e0b' }}>
            <strong>Período além da última janela de risco modelado.</strong> Os dados modelados mais recentes vão até <strong>{maxRiskDate}</strong>, mas a home continua mostrando cancelamentos operacionais, churn, financeiro e caixa com as melhores fontes disponíveis.
          </div>
        ) : null}

        <section className="homeBlock">
          {loading ? (
            <Skeleton height={164} />
          ) : (
            <HeroMoneyCard
              title="Valor em jogo agora"
              value={formatCurrency(heroRecoverable)}
              subtitle="Montante que pode ser protegido ao agir hoje em fraude operacional, retenção de clientes e pressão de caixa."
            />
          )}
        </section>

        <section className="kpiStrip homeBlock">
          <article className="card kpi riskCard">
            <div className="label">Fraude operacional</div>
            <div className="value">{loading ? '...' : formatCurrency(fraudeImpacto)}</div>
            <div className="muted">
              {loading
                ? '...'
                : `${fraudeCancelamentos} cancelamento(s) materiais no período. Risco modelado: ${formatCurrency(riscoModeladoImpacto)}.`}
            </div>
          </article>
          <article className="card kpi">
            <div className="label">Clientes em risco</div>
            <div className="value">{loading ? '...' : formatCurrency(revenueAtRisk)}</div>
            <div className="muted">Receita estimada em risco entre clientes que saíram do padrão.</div>
          </article>
          <article className="card kpi">
            <div className="label">Caixa sob pressão</div>
            <div className="value">{loading ? '...' : formatCurrency(caixaRisco)}</div>
            <div className="muted">Recebíveis e obrigações vencidas. O monitor de turnos e caixas abertos segue no módulo de Caixa.</div>
          </article>
        </section>

        <section className="homeBlock">
          <div className="focusGrid">
            {sourceCards.map((item) => (
              <article className="card focusCard" key={item.title}>
                <div className="focusLabel">{item.title}</div>
                <div className="focusValue" style={{ fontSize: 24 }}>{loading ? '...' : item.value}</div>
                <p className="focusDetail">{item.detail}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="homePrimeGrid homeBlock">
          <div className="card">
            <div className="panelHead">
              <div>
                <div className="sectionEyebrow">Prioridades do dia</div>
                <h2>Onde agir primeiro</h2>
              </div>
            </div>
            {loading ? <Skeleton height={220} /> : null}
            {!loading && !priorityCards.length ? (
              <EmptyState
                title="Nenhuma frente crítica superou a linha de corte."
                detail="A operação segue estável neste recorte, sem necessidade de intervenção imediata acima do padrão."
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
                  <RiskBadge level={String(aiSummary.status || 'ok').toUpperCase()} />
                  <span className="priorityImpact">{aiSummary.impactLabel}</span>
                </div>
                <p className="aiSummaryLead">{aiSummary.summary}</p>
                <div className="actionCard" style={{ marginTop: 4 }}>
                  <div className="actionHead">
                    <div className="focusLabel">Ação recomendada</div>
                    <strong>{aiSummary.action}</strong>
                  </div>
                  <div className="muted">Prioridade: {aiSummary.priority}</div>
                  {aiSummary.evidence?.length ? (
                    <div className="evidenceRow">
                      {aiSummary.evidence.map((item: string, idx: number) => (
                        <span key={`${item}-${idx}`} className="evidenceChip">
                          {item}
                        </span>
                      ))}
                    </div>
                  ) : null}
                </div>
                <div className="aiHighlights">
                  {aiSummary.highlights.map((item: string, idx: number) => (
                    <div className="aiHighlight" key={`${item}-${idx}`}>
                      {item}
                    </div>
                  ))}
                </div>
                {aiSummary.secondaryFocus?.length ? (
                  <div className="aiSummaryFooter">
                    <span>Próximos focos</span>
                    {aiSummary.secondaryFocus.map((item: any, idx: number) => (
                      <strong key={`${item.label}-${idx}`}>
                        {item.label} · {item.impactLabel} · {item.priority}
                      </strong>
                    ))}
                  </div>
                ) : null}
              </>
            ) : null}
          </div>
        </section>

        <section className="homeBlock">
          <div className="panelHead">
            <div>
              <div className="sectionEyebrow">Foco operacional</div>
              <h2>Dinheiro, risco e oportunidade em uma leitura só</h2>
            </div>
          </div>
          {loading ? <Skeleton height={220} /> : null}
          {!loading ? (
            <div className="focusGrid">
              {operationalFocus.map((item) => (
                <article className="card focusCard" key={item.title}>
                  <div className="focusLabel">{item.title}</div>
                  <div className="focusValue">{item.value}</div>
                  <p className="focusDetail">{item.detail}</p>
                  <Link href={item.href} className="subtleLink">
                    {item.cta}
                  </Link>
                </article>
              ))}
            </div>
          ) : null}
        </section>
      </div>
    </div>
  );
}
