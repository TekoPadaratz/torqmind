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
import { useScopeQuery } from '../lib/scope';
import AppNav from '../components/AppNav';
import ActionCard from '../components/ui/ActionCard';
import HeroMoneyCard from '../components/ui/HeroMoneyCard';
import RadarPanel from '../components/ui/RadarPanel';
import RiskBadge from '../components/ui/RiskBadge';
import Skeleton from '../components/ui/Skeleton';

function money(v: any) {
  const n = Number(v || 0);
  return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function shortDateKey(key: number) {
  const s = String(key || '');
  if (s.length !== 8) return s;
  return `${s.slice(6, 8)}/${s.slice(4, 6)}`;
}

function dataKeyToISO(key: any) {
  const s = String(key || '');
  if (s.length !== 8) return '';
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function detailsHref(path: string, scope: any) {
  const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim });
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

export default function Dashboard() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [overview, setOverview] = useState<any>(null);
  const [churnData, setChurnData] = useState<any>(null);
  const [anonRetention, setAnonRetention] = useState<any>(null);
  const [financeData, setFinanceData] = useState<any>(null);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [etlMsg, setEtlMsg] = useState('');
  const [aiMsg, setAiMsg] = useState('');

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    const parts = [
      claims.role,
      claims.id_empresa ? `E${claims.id_empresa}` : '',
      claims.id_filial ? `F${claims.id_filial}` : '',
    ].filter(Boolean);
    return parts.join(' · ');
  }, [claims]);

  const chartData = useMemo(() => {
    const byDay = overview?.by_day || [];
    return byDay.map((r: any) => ({
      ...r,
      data: shortDateKey(r.data_key),
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

        const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const [overviewRes, churnRes, anonRes, financeRes, notificationsRes] = await Promise.all([
          apiGet(`/bi/dashboard/overview?${qs.toString()}`),
          apiGet(`/bi/clients/churn?${qs.toString()}&min_score=40&limit=10`),
          apiGet(`/bi/clients/retention-anonymous?${qs.toString()}`),
          apiGet(`/bi/finance/overview?${qs.toString()}`),
          apiGet(`/bi/notifications?${qs.toString()}&limit=10`),
        ]);

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
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa, scope.ready]);

  const runEtl = async () => {
    try {
      setEtlMsg('Rodando ETL...');
      const qs = new URLSearchParams({ refresh_mart: 'true', force_full: 'false' });
      if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
      const res = await apiPost(`/etl/run?${qs.toString()}`, {});
      setEtlMsg(`ETL OK: ${JSON.stringify(res.meta || {})}`);
      setTimeout(() => window.location.reload(), 700);
    } catch (err: any) {
      setEtlMsg(`ETL falhou: ${extractApiError(err, 'erro inesperado')}`);
    }
  };

  const runJarvis = async () => {
    try {
      setAiMsg('Gerando planos IA...');
      const qs = new URLSearchParams({ dt_ref: scope.dt_fim, limit: '10', force: 'false' });
      if (scope.id_filial) qs.set('id_filial', scope.id_filial);
      if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
      const res = await apiPost(`/bi/jarvis/generate?${qs.toString()}`, {});
      const stats = res?.stats || {};
      setAiMsg(
        `Jarvis OK: ${Number(stats.processed || 0)} processados, ${Number(stats.cache_hits || 0)} cache, custo estimado US$ ${Number(stats.estimated_cost_usd || 0).toFixed(6)}`,
      );
      setTimeout(() => window.location.reload(), 700);
    } catch (err: any) {
      setAiMsg(`Jarvis falhou: ${extractApiError(err, 'erro inesperado')}`);
    }
  };

  const kpis = overview?.kpis || {};
  const jarvis = overview?.jarvis;
  const riskKpis = overview?.risk?.kpis || {};
  const riskWindow = overview?.risk?.window || {};
  const generatedInsights = overview?.insights_generated || [];

  const churnTop = churnData?.top_risk || [];
  const anonKpis = anonRetention?.kpis || {};
  const revenueAtRisk = churnTop.reduce((acc: number, c: any) => acc + Number(c.revenue_at_risk_30d || 0), 0);
  const payments = overview?.payments || {};
  const paymentsKpis = payments?.kpis || {};
  const paymentsByDay = (payments?.by_day || []).map((r: any) => ({
    data: shortDateKey(r.data_key),
    valor: Number(r.total_valor || 0),
  }));
  const paymentsAnomalies = (payments?.anomalies || []).slice(0, 8);
  const canMapPaymentTypes = ['MASTER', 'OWNER'].includes(String(claims?.role || ''));

  const financeAging = financeData?.aging || {};
  const caixaRisco = Number(financeAging?.receber_total_vencido || 0) + Number(financeAging?.pagar_total_vencido || 0);
  const fraudeImpacto = Number(riskKpis?.impacto_total || 0);

  const heroRecoverable = fraudeImpacto + revenueAtRisk + caixaRisco;
  const maxRiskDate = dataKeyToISO(riskWindow?.max_data_key);
  const scopeOutdatedForRisk =
    !!maxRiskDate &&
    !!scope.dt_fim &&
    scope.dt_fim < maxRiskDate &&
    Number(riskKpis?.total_eventos || 0) === 0;
  const topActions = [...generatedInsights]
    .sort((a: any, b: any) => Number(b.impacto_estimado || 0) - Number(a.impacto_estimado || 0))
    .slice(0, 3);

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
              <strong>{scope.dt_ini}</strong> ate <strong>{scope.dt_fim}</strong> · Filial{' '}
              <strong>{scope.id_filial || 'Todas'}</strong> · Empresa{' '}
              <strong>{scope.id_empresa || claims?.id_empresa || '1'}</strong>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <button className="btn" onClick={runEtl}>Atualizar dados (ETL)</button>
            <button className="btn" onClick={runJarvis}>Gerar planos IA</button>
          </div>
        </div>

        {etlMsg ? <div className="card" style={{ marginTop: 12 }}>{etlMsg}</div> : null}
        {aiMsg ? <div className="card" style={{ marginTop: 12 }}>{aiMsg}</div> : null}
        {error ? <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card" style={{ marginTop: 12, borderColor: '#f59e0b' }}>
            <strong>Escopo fora da janela de risco.</strong> Seus dados de risco mais recentes vao ate{' '}
            <strong>{maxRiskDate}</strong>. Ajuste o periodo em <Link href="/scope">Definir Escopo</Link>.
          </div>
        ) : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="col-12">
            {loading ? (
              <Skeleton height={140} />
            ) : (
              <HeroMoneyCard
                title="HOJE"
                value={money(heroRecoverable)}
                subtitle="Voce recupera/evita perder ao agir em Fraude + Churn + Caixa"
              />
            )}
          </div>

          <div className="card kpi col-4 riskCard">
            <div className="label">Fraude em risco</div>
            <div className="value">{loading ? '...' : money(fraudeImpacto)}</div>
          </div>
          <div className="card kpi col-4">
            <div className="label">Churn em risco (30d)</div>
            <div className="value">{loading ? '...' : money(revenueAtRisk)}</div>
          </div>
          <div className="card kpi col-4">
            <div className="label">Caixa vencido (AR/AP)</div>
            <div className="value">{loading ? '...' : money(caixaRisco)}</div>
          </div>

          <div className="card kpi col-4">
            <div className="label">Mix de pagamentos</div>
            <div className="value">{loading ? '...' : money(paymentsKpis.total_valor)}</div>
            {!loading ? (
              <div className="muted">
                {Number(paymentsKpis.delta_pct || 0).toFixed(1)}% vs período anterior
              </div>
            ) : null}
          </div>
          <div className="card kpi col-4">
            <div className="label">TIPO_FORMA desconhecido</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.unknown_share_pct || 0).toFixed(1)}%`}</div>
            {!loading && canMapPaymentTypes && Number(paymentsKpis.unknown_share_pct || 0) > 0 ? (
              <Link className="btn" href={detailsHref('/finance#payment-mapping', scope)}>Mapear tipos</Link>
            ) : null}
          </div>
          <div className="card col-4">
            <h2>Anomalias de pagamento</h2>
            {!loading && !paymentsAnomalies.length ? <p className="muted">Sem anomalias de pagamento no período.</p> : null}
            <table className="table compact">
              <thead><tr><th>Evento</th><th>Sev</th><th>Score</th></tr></thead>
              <tbody>
                {paymentsAnomalies.map((a: any, idx: number) => (
                  <tr key={`${a.insight_id || a.event_type}-${idx}`}>
                    <td>{a.event_type}</td>
                    <td>{a.severity}</td>
                    <td>{Number(a.score || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card col-8 chartCard">
            <h2>Mix de pagamentos por dia</h2>
            {!loading && !paymentsByDay.length ? <p className="muted">Sem pagamentos recebidos nesse período.</p> : null}
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
              <h2>TOP 3 ACOES DE HOJE</h2>
              <span className="muted">Checklist com impacto e evidencia</span>
            </div>
            {!loading && !topActions.length ? <p className="muted">Sem acoes para o periodo selecionado.</p> : null}
            <div className="actionsGrid">
              {topActions.map((ins: any) => {
                const checklist = (ins.ai_plan?.actions_today || []).length
                  ? ins.ai_plan.actions_today
                  : [ins.recommendation || 'Investigar e corrigir hoje'];
                const evidence = [
                  `Tipo: ${ins.insight_type || 'INSIGHT'}`,
                  `Data: ${ins.dt_ref || '-'}`,
                  `Impacto: ${money(ins.impacto_estimado)}`,
                ];
                const path = insightDetailsPath(ins.insight_type);
                return (
                  <ActionCard
                    key={ins.id}
                    title={ins.title}
                    severity={ins.severity}
                    impactLabel={money(ins.impacto_estimado)}
                    evidence={evidence}
                    checklist={checklist}
                    detailsHref={detailsHref(path, scope)}
                  />
                );
              })}
            </div>
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Fraude"
              href={detailsHref('/fraud', scope)}
              metrics={[
                { label: 'Impacto estimado', value: money(fraudeImpacto) },
                { label: 'Eventos alto risco', value: String(Number(riskKpis.eventos_alto_risco || 0)) },
                { label: 'Score medio', value: Number(riskKpis.score_medio || 0).toFixed(1) },
              ]}
            />
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Churn"
              href={detailsHref('/customers', scope)}
              metrics={[
                { label: 'Clientes em risco', value: String(churnTop.length) },
                { label: 'Receita em risco 30d', value: money(revenueAtRisk) },
                { label: 'Recorrencia anonima', value: `${Number(anonKpis.repeat_proxy_idx || 0).toFixed(1)}%` },
              ]}
            />
          </div>

          <div className="col-4">
            <RadarPanel
              title="Radar Caixa"
              href={detailsHref('/finance', scope)}
              metrics={[
                { label: 'Receber vencido', value: money(financeAging.receber_total_vencido) },
                { label: 'Pagar vencido', value: money(financeAging.pagar_total_vencido) },
                { label: 'Concentracao top5', value: `${Number(financeAging.top5_concentration_pct || 0).toFixed(1)}%` },
              ]}
            />
          </div>

          <div className="col-12">
            <RadarPanel
              title="Radar Recorrencia Anonima"
              href={detailsHref('/customers', scope)}
              metrics={[
                { label: 'Tendencia', value: `${Number(anonKpis.trend_pct || 0).toFixed(1)}%` },
                { label: 'Impacto estimado 7d', value: money(anonKpis.impact_estimated_7d) },
                { label: 'Severidade', value: String(anonKpis.severity || 'OK') },
              ]}
            />
          </div>

          <div className="card col-12">
            <div className="panelHead">
              <h2>Perda Invisivel</h2>
              <Link className="btn" href={detailsHref('/scope', scope)}>Configurar integracoes</Link>
            </div>
            <p className="muted">
              Modulo disponivel ao integrar tanques, entregas e afericoes. Enquanto isso, foque nas acoes de Fraude, Churn e Caixa.
            </p>
          </div>

          <div className="card col-12" id="alerts">
            <div className="panelHead">
              <h2>Alertas</h2>
              <span className="muted">Gerados automaticamente para riscos criticos</span>
            </div>
            {!notifications.length ? <p className="muted">Sem alertas para o periodo.</p> : null}
            {notifications.map((n) => (
              <div className="insightItem" key={n.id}>
                <div>
                  <RiskBadge level={n.severity} />
                </div>
                <div>
                  <div><strong>{n.title}</strong></div>
                  <div className="muted">{n.body}</div>
                  <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
                    <Link className="btn" href={detailsHref(n.url || '/dashboard', scope)}>Abrir alerta</Link>
                    {!n.read_at ? (
                      <button className="btn" onClick={() => markNotificationRead(n.id)}>Marcar como lido</button>
                    ) : (
                      <span className="pill">Lido</span>
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
            <h2>Jarvis briefing</h2>
            {loading ? <Skeleton /> : null}
            {!loading && jarvis?.bullets?.length ? (
              <ul className="insightList">
                {jarvis.bullets.map((b: string, idx: number) => (
                  <li key={idx}>{b}</li>
                ))}
              </ul>
            ) : null}
            {!loading && !jarvis?.bullets?.length ? (
              <p className="muted">Sem briefing para o periodo selecionado.</p>
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
              <span className="muted">Evidencias e plano gerado</span>
            </div>
            {!loading && !generatedInsights.length ? <p className="muted">Sem insights persistidos para o periodo.</p> : null}
            {(generatedInsights || []).slice(0, 6).map((ins: any) => (
              <div className="insightItem" key={ins.id}>
                <div>
                  <RiskBadge level={ins.severity} />
                </div>
                <div>
                  <div><strong>{ins.title}</strong> · {money(ins.impacto_estimado)}</div>
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
