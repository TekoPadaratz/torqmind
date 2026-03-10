'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateOnly,
  formatFilialLabel,
} from '../lib/format';
import { useScopeQuery } from '../lib/scope';
import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import HeroMoneyCard from '../components/ui/HeroMoneyCard';
import RiskBadge from '../components/ui/RiskBadge';
import Skeleton from '../components/ui/Skeleton';

function detailsHref(path: string, scope: any) {
  const qs = new URLSearchParams({
    dt_ini: scope.dt_ini,
    dt_fim: scope.dt_fim,
    dt_ref: scope.dt_ref || scope.dt_fim,
  });
  if (scope.id_filial) qs.set('id_filial', scope.id_filial);
  if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
  return `${path}?${qs.toString()}`;
}

function buildPriorityCards({
  fraudImpact,
  churnImpact,
  cashPressure,
  riskKpis,
  churnTop,
  financeData,
  scope,
}: {
  fraudImpact: number;
  churnImpact: number;
  cashPressure: number;
  riskKpis: any;
  churnTop: any[];
  financeData: any;
  scope: any;
}) {
  const cards = [];

  if (fraudImpact > 0) {
    cards.push({
      title: 'Auditar descontos e cancelamentos fora da curva',
      severity: Number(riskKpis?.eventos_alto_risco || 0) > 0 ? 'HIGH' : 'WARN',
      impact: formatCurrency(fraudImpact),
      summary: `${Number(riskKpis?.eventos_alto_risco || 0)} eventos de alto risco concentram o maior impacto operacional do período.`,
      cta: 'Abrir antifraude',
      href: detailsHref('/fraud', scope),
    });
  }

  if (cashPressure > 0) {
    cards.push({
      title: 'Cobrar e renegociar vencidos mais concentrados',
      severity: Number(financeData?.aging?.receber_titulos_vencidos || 0) > 0 ? 'WARN' : 'INFO',
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
          ? 'Fraude operacional segue sendo o maior ponto de atenção financeira no recorte atual.'
          : 'Nenhum desvio crítico de fraude apareceu acima da linha de corte.',
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
          ? `${Number(financeData?.aging?.receber_titulos_vencidos || 0)} recebíveis vencidos concentram a necessidade de cobrança e renegociação.`
          : latestInsight?.message || 'O caixa segue estável no período auditado.',
      href: detailsHref('/finance', scope),
      cta: 'Ver financeiro',
    },
  ];
}

function buildExecutiveSummary({
  overview,
  fraudImpact,
  churnImpact,
  cashPressure,
}: {
  overview: any;
  fraudImpact: number;
  churnImpact: number;
  cashPressure: number;
}) {
  const bullets = Array.isArray(overview?.jarvis?.bullets) ? overview.jarvis.bullets.filter(Boolean) : [];
  if (bullets.length) {
    return {
      title: 'Resumo IA',
      summary: String(bullets[0]),
      highlights: bullets.slice(1, 3),
    };
  }

  const ordered = [
    { label: 'fraude operacional', value: fraudImpact },
    { label: 'retenção de clientes', value: churnImpact },
    { label: 'pressão de caixa', value: cashPressure },
  ].sort((a, b) => b.value - a.value);

  return {
    title: 'Resumo IA',
    summary: `A maior frente do período está em ${ordered[0].label}, seguida por ${ordered[1].label}.`,
    highlights: ['Priorize o maior desvio financeiro primeiro.', 'Mantenha o segundo foco em plano de execução ainda hoje.'],
  };
}

export default function Dashboard() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [overview, setOverview] = useState<any>(null);
  const [churnData, setChurnData] = useState<any>(null);
  const [financeData, setFinanceData] = useState<any>(null);
  const [filialLabel, setFilialLabel] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  useEffect(() => {
    if (!scope.ready) return;

    if (!requireAuth()) {
      router.push('/');
      return;
    }

    if (!scope.dt_ini || !scope.dt_fim) {
      router.push('/scope');
      return;
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const me = await apiGet('/auth/me');
        setClaims(me);

        const qs = new URLSearchParams({
          dt_ini: scope.dt_ini,
          dt_fim: scope.dt_fim,
          dt_ref: scope.dt_ref || scope.dt_fim,
        });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const [overviewRes, churnRes, financeRes] = await Promise.all([
          apiGet(`/bi/dashboard/overview?${qs.toString()}`),
          apiGet(`/bi/clients/churn?${qs.toString()}&min_score=40&limit=10`),
          apiGet(`/bi/finance/overview?${qs.toString()}`),
        ]);

        if (scope.id_filial) {
          const branchList = await apiGet(`/bi/filiais${scope.id_empresa ? `?id_empresa=${scope.id_empresa}` : ''}`);
          const selected = (branchList?.items || []).find((item: any) => String(item.id_filial) === String(scope.id_filial));
          setFilialLabel(formatFilialLabel(scope.id_filial, selected?.nome));
        } else {
          setFilialLabel('Todas as filiais');
        }

        setOverview(overviewRes);
        setChurnData(churnRes);
        setFinanceData(financeRes);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar dashboard'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa, scope.ready]);

  const riskKpis = overview?.risk?.kpis || {};
  const riskWindow = overview?.risk?.window || {};
  const generatedInsights = overview?.insights_generated || [];
  const churnTop = churnData?.top_risk || [];
  const financeAging = financeData?.aging || {};

  const fraudeImpacto = Number(riskKpis?.impacto_total || 0);
  const revenueAtRisk = churnTop.reduce((acc: number, item: any) => acc + Number(item.revenue_at_risk_30d || 0), 0);
  const caixaRisco = Number(financeAging?.receber_total_vencido || 0) + Number(financeAging?.pagar_total_vencido || 0);
  const heroRecoverable = fraudeImpacto + revenueAtRisk + caixaRisco;

  const maxRiskDateKey = Number(riskWindow?.max_data_key || 0);
  const maxRiskDate = formatDateKey(maxRiskDateKey);
  const scopeEndKey = Number(String(scope.dt_fim || '').replaceAll('-', ''));
  const scopeOutdatedForRisk =
    maxRiskDateKey > 0 &&
    scopeEndKey > 0 &&
    scopeEndKey < maxRiskDateKey &&
    Number(riskKpis?.total_eventos || 0) === 0;

  const priorityCards = useMemo(
    () =>
      buildPriorityCards({
        fraudImpact: fraudeImpacto,
        churnImpact: revenueAtRisk,
        cashPressure: caixaRisco,
        riskKpis,
        churnTop,
        financeData,
        scope,
      }),
    [fraudeImpacto, revenueAtRisk, caixaRisco, riskKpis, churnTop, financeData, scope]
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
        fraudImpact: fraudeImpacto,
        churnImpact: revenueAtRisk,
        cashPressure: caixaRisco,
      }),
    [overview, fraudeImpacto, revenueAtRisk, caixaRisco]
  );

  return (
    <div>
      <AppNav title="Dashboard Geral" userLabel={userLabel} />

      <div className="container dashboardHome">
        <div className="card toolbar">
          <div>
            <div className="muted">Escopo ativo</div>
            <div className="scopeLine">
              <strong>{formatDateOnly(scope.dt_ini)}</strong> até <strong>{formatDateOnly(scope.dt_fim)}</strong> · Ref.{' '}
              <strong>{formatDateOnly(scope.dt_ref || scope.dt_fim)}</strong> · Filial{' '}
              <strong>{filialLabel || formatFilialLabel(scope.id_filial)}</strong> · Empresa{' '}
              <strong>{scope.id_empresa || claims?.id_empresa || '1'}</strong>
            </div>
          </div>
        </div>

        {error ? <div className="card errorCard homeBlock">{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card homeBlock" style={{ borderColor: '#f59e0b' }}>
            <strong>Escopo fora da janela de risco.</strong> Os dados mais recentes de risco vão até <strong>{maxRiskDate}</strong>. Ajuste o período em{' '}
            <Link href="/scope">Definir escopo</Link>.
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
            <div className="label">Fraude em risco</div>
            <div className="value">{loading ? '...' : formatCurrency(fraudeImpacto)}</div>
            <div className="muted">Eventos críticos e alto risco já identificados no período.</div>
          </article>
          <article className="card kpi">
            <div className="label">Clientes em risco</div>
            <div className="value">{loading ? '...' : formatCurrency(revenueAtRisk)}</div>
            <div className="muted">Receita estimada em risco entre clientes que saíram do padrão.</div>
          </article>
          <article className="card kpi">
            <div className="label">Caixa sob pressão</div>
            <div className="value">{loading ? '...' : formatCurrency(caixaRisco)}</div>
            <div className="muted">Recebíveis e obrigações vencidas que já pedem ação comercial e financeira.</div>
          </article>
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
            <h2>Leitura executiva do período</h2>
            {loading ? <Skeleton height={180} /> : null}
            {!loading ? (
              <>
                <p className="aiSummaryLead">{aiSummary.summary}</p>
                <div className="aiHighlights">
                  {aiSummary.highlights.map((item: string, idx: number) => (
                    <div className="aiHighlight" key={`${item}-${idx}`}>
                      {item}
                    </div>
                  ))}
                </div>
                {generatedInsights[0]?.title ? (
                  <div className="aiSummaryFooter">
                    <span>Último sinal relevante</span>
                    <strong>{generatedInsights[0].title}</strong>
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
