'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { apiGet, apiPost } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatDateOnly,
  formatFilialLabel,
  formatTurnoLabel,
} from '../lib/format';
import { useScopeQuery } from '../lib/scope';
import AppNav from '../components/AppNav';
import ActionCard from '../components/ui/ActionCard';
import EmptyState from '../components/ui/EmptyState';
import HeroMoneyCard from '../components/ui/HeroMoneyCard';
import RadarPanel from '../components/ui/RadarPanel';
import RiskBadge from '../components/ui/RiskBadge';
import Skeleton from '../components/ui/Skeleton';

function detailsHref(path: string, scope: any) {
  const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim, dt_ref: scope.dt_ref || scope.dt_fim });
  if (scope.id_filial) qs.set('id_filial', scope.id_filial);
  if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
  return `${path}?${qs.toString()}`;
}

function insightDetailsPath(insightType: string) {
  const t = String(insightType || '').toUpperCase();
  if (t.includes('CHURN') || t.includes('CLIENTE')) return '/customers';
  if (t.includes('CANCEL') || t.includes('DESCONTO') || t.includes('RISCO') || t.includes('FRAUDE')) return '/fraud';
  if (t.includes('PAGAR') || t.includes('RECEBER') || t.includes('INADIMPL') || t.includes('FINANC')) return '/finance';
  if (t.includes('MARGEM') || t.includes('TICKET') || t.includes('FATURAMENTO') || t.includes('VENDA')) return '/sales';
  return '/dashboard';
}

function actionHeadline(insight: any) {
  const title = String(insight?.title || '').trim();
  if (title.includes('Faturamento abaixo do ritmo')) {
    return title.replace('Faturamento abaixo do ritmo', 'Recuperar ritmo comercial');
  }
  if (title.includes('Ticket médio em queda')) {
    return title.replace('Ticket médio em queda', 'Reagir à queda de ticket médio');
  }
  if (title.includes('Cancel')) {
    return 'Auditar risco operacional de cancelamentos';
  }
  return title || 'Atuar no principal desvio do período';
}

function fallbackActions({
  fraudeImpacto,
  revenueAtRisk,
  caixaRisco,
  openCash,
}: {
  fraudeImpacto: number;
  revenueAtRisk: number;
  caixaRisco: number;
  openCash: any;
}) {
  const items: Array<{ title: string; severity: string; impact: number; evidence: string[]; checklist: string[]; href: string }> = [];

  if (fraudeImpacto > 0) {
    items.push({
      title: 'Auditar descontos e cancelamentos fora da curva',
      severity: 'HIGH',
      impact: fraudeImpacto,
      evidence: ['Fraude operacional ativa', 'Impacto estimado no período'],
      checklist: ['Revisar os eventos mais recentes', 'Priorizar filial e colaborador com maior exposição', 'Validar desconto, cancelamento e nova venda'],
      href: '/fraud',
    });
  }
  if (revenueAtRisk > 0) {
    items.push({
      title: 'Ativar clientes com maior risco de saída',
      severity: 'WARN',
      impact: revenueAtRisk,
      evidence: ['Receita em risco de churn', 'Clientes identificados já ranqueados'],
      checklist: ['Priorizar os 10 maiores riscos', 'Rodar contato comercial em até 7 dias', 'Acompanhar retorno de frequência'],
      href: '/customers',
    });
  }
  if (caixaRisco > 0) {
    items.push({
      title: 'Cobrar e renegociar vencidos com maior concentração',
      severity: 'WARN',
      impact: caixaRisco,
      evidence: ['Contas vencidas em aberto', 'Pressão em caixa e inadimplência'],
      checklist: ['Separar top 5 maiores valores', 'Definir régua de cobrança', 'Monitorar concentração de recebíveis'],
      href: '/finance',
    });
  }
  if (String(openCash?.source_status) === 'ok' && Number(openCash?.total_open || 0) > 0) {
    items.push({
      title: 'Fechar turnos operacionais antigos',
      severity: String(openCash?.severity || 'WARN'),
      impact: Number(openCash?.total_open || 0),
      evidence: ['Turnos em aberto', 'Leitura operacional de exceção'],
      checklist: ['Validar turnos críticos primeiro', 'Conferir abertura e fechamento', 'Corrigir pendências antes do próximo turno'],
      href: '/fraud',
    });
  }

  return items.slice(0, 3);
}

export default function Dashboard() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [overview, setOverview] = useState<any>(null);
  const [churnData, setChurnData] = useState<any>(null);
  const [anonRetention, setAnonRetention] = useState<any>(null);
  const [financeData, setFinanceData] = useState<any>(null);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [filialLabel, setFilialLabel] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

  const chartData = useMemo(() => {
    const byDay = overview?.by_day || [];
    return byDay.map((r: any) => ({
      ...r,
      data: formatDateKeyShort(r.data_key),
      faturamento: Number(r.faturamento || 0),
      margem: Number(r.margem || 0),
    }));
  }, [overview]);

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

        const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim, dt_ref: scope.dt_ref || scope.dt_fim });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const [overviewRes, churnRes, anonRes, financeRes, notificationsRes] = await Promise.all([
          apiGet(`/bi/dashboard/overview?${qs.toString()}`),
          apiGet(`/bi/clients/churn?${qs.toString()}&min_score=40&limit=10`),
          apiGet(`/bi/clients/retention-anonymous?${qs.toString()}`),
          apiGet(`/bi/finance/overview?${qs.toString()}`),
          apiGet(`/bi/notifications?${qs.toString()}&limit=10`),
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
        setAnonRetention(anonRes);
        setFinanceData(financeRes);
        setNotifications(notificationsRes?.items || []);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar dashboard'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa, scope.ready]);

  const kpis = overview?.kpis || {};
  const jarvis = overview?.jarvis;
  const riskKpis = overview?.risk?.kpis || {};
  const riskWindow = overview?.risk?.window || {};
  const openCash = overview?.open_cash || {};
  const generatedInsights = overview?.insights_generated || [];

  const churnTop = churnData?.top_risk || [];
  const anonKpis = anonRetention?.kpis || {};
  const revenueAtRisk = churnTop.reduce((acc: number, c: any) => acc + Number(c.revenue_at_risk_30d || 0), 0);
  const payments = overview?.payments || {};
  const paymentsKpis = payments?.kpis || {};
  const paymentsByDay = (payments?.by_day || [])
    .filter((r: any) => Number(r.total_valor || 0) > 0)
    .map((r: any) => ({
    data: formatDateKeyShort(r.data_key),
    valor: Number(r.total_valor || 0),
  }));
  const paymentsAnomalies = (payments?.anomalies || []).slice(0, 8);
  const canMapPaymentTypes = ['MASTER', 'OWNER'].includes(String(claims?.role || ''));
  const paymentMixPreview = (paymentsKpis?.mix || [])
    .slice(0, 3)
    .map((item: any) => `${item.category}: ${formatCurrency(item.total_valor)}`)
    .join(' · ');

  const financeAging = financeData?.aging || {};
  const caixaRisco = Number(financeAging?.receber_total_vencido || 0) + Number(financeAging?.pagar_total_vencido || 0);
  const fraudeImpacto = Number(riskKpis?.impacto_total || 0);

  const heroRecoverable = fraudeImpacto + revenueAtRisk + caixaRisco;
  const maxRiskDateKey = Number(riskWindow?.max_data_key || 0);
  const maxRiskDate = formatDateKey(maxRiskDateKey);
  const scopeEndKey = Number(String(scope.dt_fim || '').replaceAll('-', ''));
  const scopeOutdatedForRisk =
    maxRiskDateKey > 0 &&
    scopeEndKey > 0 &&
    scopeEndKey < maxRiskDateKey &&
    Number(riskKpis?.total_eventos || 0) === 0;
  const topActions = [...generatedInsights]
    .sort((a: any, b: any) => Number(b.impacto_estimado || 0) - Number(a.impacto_estimado || 0))
    .slice(0, 3);
  const priorityActions = useMemo(() => {
    if (topActions.length) {
      return topActions.map((ins: any) => ({
        key: `ins-${ins.id}`,
        title: actionHeadline(ins),
        severity: ins.severity,
        impactLabel: formatCurrency(ins.impacto_estimado),
        evidence: [
          `Tipo: ${ins.insight_type || 'INSIGHT'}`,
          `Data: ${formatDateOnly(ins.dt_ref)}`,
          `Impacto: ${formatCurrency(ins.impacto_estimado)}`,
        ],
        checklist: (ins.ai_plan?.actions_today || []).length
          ? ins.ai_plan.actions_today
          : [ins.recommendation || 'Investigar, corrigir e acompanhar ainda hoje'],
        detailsHref: detailsHref(insightDetailsPath(ins.insight_type), scope),
      }));
    }

    return fallbackActions({ fraudeImpacto, revenueAtRisk, caixaRisco, openCash }).map((item, index) => ({
      key: `fallback-${index}`,
      title: item.title,
      severity: item.severity,
      impactLabel: item.href === '/fraud' && item.title.includes('Fechar turnos') ? `${item.impact} turno(s)` : formatCurrency(item.impact),
      evidence: item.evidence,
      checklist: item.checklist,
      detailsHref: detailsHref(item.href, scope),
    }));
  }, [topActions, scope, fraudeImpacto, revenueAtRisk, caixaRisco, openCash]);
  const displayedAlerts = useMemo(() => {
    if (notifications.length) {
      return notifications.map((item) => ({ ...item, synthetic: false }));
    }
    return generatedInsights
      .filter((ins: any) => ['CRITICAL', 'HIGH'].includes(String(ins.severity || '').toUpperCase()))
      .slice(0, 4)
      .map((ins: any) => ({
        id: `insight-${ins.id}`,
        title: ins.title,
        body: ins.message || ins.recommendation || 'Acompanhar desvio relevante do período.',
        severity: ins.severity,
        url: insightDetailsPath(ins.insight_type),
        read_at: null,
        synthetic: true,
      }));
  }, [notifications, generatedInsights]);
  const recoveryFocus = useMemo(
    () => [
      {
        label: 'Fraude evitável',
        value: formatCurrency(fraudeImpacto),
        detail: 'Desvios e eventos de risco com impacto estimado.',
      },
      {
        label: 'Clientes a recuperar',
        value: formatCurrency(revenueAtRisk),
        detail: 'Receita potencial em risco nos próximos 30 dias.',
      },
      {
        label: 'Caixa sob pressão',
        value: formatCurrency(caixaRisco),
        detail: 'Vencidos a receber e a pagar exigindo atenção.',
      },
    ],
    [fraudeImpacto, revenueAtRisk, caixaRisco]
  );

  const markNotificationRead = async (id: number) => {
    try {
      const qs = new URLSearchParams();
      if (scope.id_filial) qs.set('id_filial', scope.id_filial);
      if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
      await apiPost(`/bi/notifications/${id}/read?${qs.toString()}`, {});
      setNotifications((prev) => prev.map((n) => (n.id === id ? { ...n, read_at: new Date().toISOString() } : n)));
    } catch {
      // no-op
    }
  };

  return (
    <div>
      <AppNav title="Dashboard Geral" userLabel={userLabel} />

      <div className="container">
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

        <div className="card" style={{ marginTop: 12 }}>
          <div className="muted">Centro de decisão com vendas, risco, clientes e prioridade operacional.</div>
        </div>
        {error ? <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card" style={{ marginTop: 12, borderColor: '#f59e0b' }}>
            <strong>Escopo fora da janela de risco.</strong> Seus dados de risco mais recentes vão até{' '}
            <strong>{maxRiskDate}</strong>. Ajuste o período em <Link href="/scope">Definir Escopo</Link>.
          </div>
        ) : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="col-12">
            {loading ? (
              <Skeleton height={140} />
            ) : (
              <HeroMoneyCard
                title="HOJE"
                value={formatCurrency(heroRecoverable)}
                subtitle="Valor potencialmente protegido ao agir em fraude, churn e caixa"
              />
            )}
          </div>

          <div className="card kpi col-4 riskCard">
            <div className="label">Fraude em risco</div>
            <div className="value">{loading ? '...' : formatCurrency(fraudeImpacto)}</div>
          </div>
          <div className="card kpi col-4">
            <div className="label">Churn em risco (30d)</div>
            <div className="value">{loading ? '...' : formatCurrency(revenueAtRisk)}</div>
          </div>
          <div className="card kpi col-4">
            <div className="label">Caixa vencido (AR/AP)</div>
            <div className="value">{loading ? '...' : formatCurrency(caixaRisco)}</div>
          </div>
          <div className="card col-12">
            <div className="panelHead">
              <h2>Monitor de turnos</h2>
              <Link className="btn" href={detailsHref('/fraud', scope)}>Ver monitor</Link>
            </div>
            {!loading ? (
              <>
                <div className="muted" style={{ marginBottom: 8 }}>{openCash.summary || 'Monitoramento de turnos indisponível.'}</div>
                {openCash.source_status === 'ok' && openCash.items?.length ? (
                  <table className="table compact">
                    <thead><tr><th>Filial</th><th>Turno</th><th>Horas aberto</th><th>Severidade</th></tr></thead>
                    <tbody>
                      {openCash.items.map((item: any) => (
                        <tr key={`${item.id_filial}-${item.id_turno}`}>
                          <td>{formatFilialLabel(item.id_filial, item.filial_nome)}</td>
                          <td>{formatTurnoLabel(item.id_turno)}</td>
                          <td>{Number(item.open_hours || 0).toFixed(1)}h</td>
                          <td>{item.severity}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState
                    title={
                      openCash.source_status === 'unavailable'
                        ? 'Monitor de turnos em integração.'
                        : openCash.source_status === 'unmapped'
                          ? 'Fonte operacional ainda não mapeada.'
                          : 'Nenhum turno em aberto acima do limite esperado.'
                    }
                    detail={openCash.summary || 'Assim que a base operacional estiver conectada, esta leitura aparece aqui com prioridade automática.'}
                  />
                )}
              </>
            ) : null}
          </div>

          <div className="card kpi col-4">
            <div className="label">Mix de pagamentos</div>
            <div className="value">{loading ? '...' : formatCurrency(paymentsKpis.total_valor)}</div>
            {!loading ? (
              <div className="muted">
                {paymentMixPreview || `${Number(paymentsKpis.delta_pct || 0).toFixed(1)}% vs. período anterior`}
              </div>
            ) : null}
          </div>
          <div className="card kpi col-4">
            <div className="label">Formas em validação</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.unknown_share_pct || 0).toFixed(1)}%`}</div>
            {!loading && canMapPaymentTypes && Number(paymentsKpis.unknown_share_pct || 0) > 0 ? (
              <Link className="btn" href={detailsHref('/finance#payment-mapping', scope)}>Refinar mapeamento</Link>
            ) : null}
          </div>
          <div className="card col-4">
            <h2>Anomalias de pagamento</h2>
            {!loading && !paymentsAnomalies.length ? (
              <EmptyState title="Sem anomalias de pagamento no período." detail="A leitura financeira seguiu estável neste recorte." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Evento</th><th>Severidade</th><th>Score</th></tr></thead>
              <tbody>
                {paymentsAnomalies.map((a: any, idx: number) => (
                  <tr key={`${a.insight_id || a.event_type}-${idx}`}>
                    <td>{a.event_label || a.event_type}</td>
                    <td>{a.severity}</td>
                    <td>{Number(a.score || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card col-8 chartCard">
            <h2>Mix de pagamentos por dia</h2>
            {!loading && !paymentsByDay.length ? (
              <EmptyState title="Sem pagamentos recebidos no período." detail="A consolidação diária de pagamentos ainda não retornou movimento neste recorte." />
            ) : null}
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={paymentsByDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="valor" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-12">
            <div className="panelHead">
              <h2>Top 3 ações de hoje</h2>
              <span className="muted">Prioridades com impacto e encaminhamento objetivo</span>
            </div>
            {!loading && !priorityActions.length ? (
              <EmptyState title="Sem ações críticas no período." detail="O painel volta a priorizar automaticamente assim que surgir uma nova frente relevante." />
            ) : null}
            <div className="actionsGrid">
              {priorityActions.map((action: any) => (
                <ActionCard
                  key={action.key}
                  title={action.title}
                  severity={action.severity}
                  impactLabel={action.impactLabel}
                  evidence={action.evidence}
                  checklist={action.checklist}
                  detailsHref={action.detailsHref}
                />
              ))}
            </div>
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Fraude"
              href={detailsHref('/fraud', scope)}
              metrics={[
                { label: 'Impacto estimado', value: formatCurrency(fraudeImpacto) },
                { label: 'Eventos alto risco', value: String(Number(riskKpis.eventos_alto_risco || 0)) },
                { label: 'Score médio', value: Number(riskKpis.score_medio || 0).toFixed(1) },
              ]}
            />
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Churn"
              href={detailsHref('/customers', scope)}
              metrics={[
                { label: 'Clientes em risco', value: String(churnTop.length) },
                { label: 'Receita em risco 30d', value: formatCurrency(revenueAtRisk) },
                { label: 'Recorrência anônima', value: `${Number(anonKpis.repeat_proxy_idx || 0).toFixed(1)}%` },
              ]}
            />
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Caixa"
              href={detailsHref('/finance', scope)}
              metrics={[
                { label: 'Receber vencido', value: formatCurrency(financeAging.receber_total_vencido) },
                { label: 'Pagar vencido', value: formatCurrency(financeAging.pagar_total_vencido) },
                { label: 'Concentração top 5', value: `${Number(financeAging.top5_concentration_pct || 0).toFixed(1)}%` },
              ]}
            />
          </div>

          <div className="col-12">
            <RadarPanel
              title="Radar Recorrência Anônima"
              href={detailsHref('/customers', scope)}
              metrics={[
                { label: 'Tendência', value: `${Number(anonKpis.trend_pct || 0).toFixed(1)}%` },
                { label: 'Impacto estimado 7d', value: formatCurrency(anonKpis.impact_estimated_7d) },
                { label: 'Severidade', value: String(anonKpis.severity || 'OK') },
              ]}
            />
          </div>

          <div className="card col-12">
            <div className="panelHead">
              <h2>Oportunidades de recuperação</h2>
              <Link className="btn" href={detailsHref('/dashboard', scope)}>Priorizar agora</Link>
            </div>
            <div className="actionsGrid" style={{ marginTop: 0 }}>
              {recoveryFocus.map((item) => (
                <div className="actionCard" key={item.label}>
                  <div className="actionHead">
                    <strong>{item.label}</strong>
                    <span className="actionImpact">{item.value}</span>
                  </div>
                  <div className="muted">{item.detail}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="card col-12" id="alerts">
            <div className="panelHead">
              <h2>Alertas</h2>
              <span className="muted">Gerados automaticamente para riscos críticos</span>
            </div>
            {!displayedAlerts.length ? <p className="muted">Sem alertas relevantes no período.</p> : null}
            {displayedAlerts.map((n: any) => (
              <div className="insightItem" key={n.id}>
                <div>
                  <RiskBadge level={n.severity} />
                </div>
                <div>
                  <div><strong>{n.title}</strong></div>
                  <div className="muted">{n.body}</div>
                  <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
                    <Link className="btn" href={detailsHref(n.url || '/dashboard', scope)}>Abrir alerta</Link>
                    {!n.synthetic && !n.read_at ? (
                      <button className="btn" onClick={() => markNotificationRead(n.id)}>Marcar como lido</button>
                    ) : !n.synthetic ? (
                      <span className="pill">Lido</span>
                    ) : (
                      <span className="pill">Insight crítico</span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="card col-8 chartCard">
            <h2>Evolucao diaria</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Line type="monotone" dataKey="faturamento" stroke="#6ee7ff" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="margem" stroke="#4ade80" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-4">
            <h2>Resumo IA</h2>
            {loading ? <Skeleton /> : null}
            {!loading && jarvis?.bullets?.length ? (
              <ul className="insightList">
                {jarvis.bullets.map((b: string, idx: number) => (
                  <li key={idx}>{b}</li>
                ))}
              </ul>
            ) : null}
            {!loading && !jarvis?.bullets?.length ? (
              <p className="muted">Sem briefing executivo para o período selecionado.</p>
            ) : null}
          </div>

          <div className="card col-12 chartCard">
            <h2>Comparativo faturamento x margem</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData.slice(-14)}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="faturamento" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="margem" fill="#34d399" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-12">
            <div className="panelHead">
              <h2>Insights recentes</h2>
              <span className="muted">Resumo dos pontos mais relevantes</span>
            </div>
            {!loading && !generatedInsights.length ? <p className="muted">Sem insights para o período.</p> : null}
            {(generatedInsights || []).slice(0, 6).map((ins: any) => (
              <div className="insightItem" key={ins.id}>
                <div>
                  <RiskBadge level={ins.severity} />
                </div>
                <div>
                  <div><strong>{ins.title}</strong> · {formatCurrency(ins.impacto_estimado)}</div>
                  <div className="muted">{ins.message}</div>
                  <div className="cta">Corrigir hoje: {ins.recommendation}</div>
                  {(ins.ai_plan?.actions_today || []).length ? (
                    <ul className="insightList">
                      {(ins.ai_plan.actions_today || []).slice(0, 3).map((a: string, idx: number) => (
                        <li key={`${ins.id}-ai-${idx}`}>{a}</li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
