'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateTime,
  formatFilialLabel,
  formatHoursLabel,
} from '../lib/format';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { loadSession, readCachedSession } from '../lib/session';

export const dynamic = 'force-dynamic';

const PIE_COLORS = ['#38bdf8', '#34d399', '#f59e0b', '#f87171', '#a78bfa', '#f472b6', '#94a3b8'];

function severityTone(value: string) {
  const severity = String(value || '').toUpperCase();
  if (severity === 'CRITICAL') return { bg: 'rgba(239, 68, 68, 0.14)', border: 'rgba(248, 113, 113, 0.32)', label: 'Crítico' };
  if (severity === 'HIGH') return { bg: 'rgba(245, 158, 11, 0.14)', border: 'rgba(251, 191, 36, 0.28)', label: 'Atenção alta' };
  if (severity === 'WARN') return { bg: 'rgba(56, 189, 248, 0.14)', border: 'rgba(96, 165, 250, 0.28)', label: 'Monitorar' };
  return { bg: 'rgba(52, 211, 153, 0.12)', border: 'rgba(74, 222, 128, 0.24)', label: 'Dentro da janela' };
}

export default function CashPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [data, setData] = useState<any>(null);
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

        const qs = buildScopeParams(scope).toString();
        const res = await apiGet(`/bi/cash/overview?${qs}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar o módulo de Caixa'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.ready, scope.dt_ini, scope.dt_fim, scope.id_filiais_key, scope.id_empresa]);

  const historical = data?.historical || {};
  const liveNow = data?.live_now || {};
  const kpis = historical?.kpis || data?.kpis || {};
  const liveKpis = liveNow?.kpis || {};
  const openBoxes = liveNow?.open_boxes || data?.open_boxes || [];
  const paymentMix = historical?.payment_mix || data?.payment_mix || [];
  const cancelamentos = historical?.cancelamentos || data?.cancelamentos || [];
  const alerts = liveNow?.alerts || data?.alerts || [];
  const topTurnos = historical?.top_turnos || [];
  const byDay = historical?.by_day || [];
  const historicalStatus = String(historical?.source_status || data?.source_status || 'unavailable');
  const liveStatus = String(liveNow?.source_status || 'unavailable');

  return (
    <div>
      <AppNav title="Caixa" userLabel={userLabel} />
      <div className="container">
        <div
          className="card"
          style={{
            background:
              'linear-gradient(135deg, rgba(37,99,235,0.20), rgba(15,23,42,0.92) 45%, rgba(16,185,129,0.14))',
            borderColor: 'rgba(96, 165, 250, 0.22)',
          }}
        >
          <div className="muted">Painel de caixa com visão histórica do período filtrado e monitor operacional em tempo real, sem misturar os dois conceitos.</div>
          {!loading ? <div style={{ marginTop: 10, fontSize: 18, fontWeight: 700 }}>{historical?.summary || data?.summary}</div> : null}
          {!loading ? <div className="muted" style={{ marginTop: 8 }}>{liveNow?.summary || 'Monitor em tempo real indisponível no momento.'}</div> : null}
        </div>

        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3">
            <div className="label">Caixas no período</div>
            <div className="value">{loading ? '...' : Number(kpis.caixas_periodo || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Dias com movimento</div>
            <div className="value">{loading ? '...' : Number(kpis.dias_com_movimento || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Vendas do período</div>
            <div className="value">{loading ? '...' : formatCurrency(kpis.total_vendas)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Cancelamentos do período</div>
            <div className="value">{loading ? '...' : formatCurrency(kpis.total_cancelamentos)}</div>
          </div>

          <div className="card kpi col-3">
            <div className="label">Caixas abertos agora</div>
            <div className="value">{loading ? '...' : Number(liveKpis.caixas_abertos || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Acima de 24h</div>
            <div className="value">{loading ? '...' : Number(liveKpis.caixas_criticos || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Vendas expostas agora</div>
            <div className="value">{loading ? '...' : formatCurrency(liveKpis.total_vendas_abertas)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Cancelamentos em aberto</div>
            <div className="value">{loading ? '...' : formatCurrency(liveKpis.total_cancelamentos_abertos)}</div>
          </div>

          <div className="card col-8">
            <h2>Turnos com maior movimento no período</h2>
            {!loading && historicalStatus === 'unavailable' ? (
              <EmptyState
                title="Sem histórico de caixa para o período selecionado."
                detail="O recorte atual não trouxe turnos vinculados a comprovantes ou pagamentos."
              />
            ) : null}
            {!loading && historicalStatus !== 'unavailable' && !topTurnos.length ? (
              <EmptyState
                title="Sem turnos históricos destacados neste recorte."
                detail="O período não gerou ranking relevante de turnos com comprovantes e pagamentos vinculados."
              />
            ) : null}
            {topTurnos.length ? (
              <table className="table compact">
                <thead>
                  <tr>
                    <th>Filial</th>
                    <th>Turno</th>
                    <th>Operador</th>
                    <th>Último evento</th>
                    <th>Vendas</th>
                    <th>Pagamentos</th>
                    <th>Cancelamentos</th>
                  </tr>
                </thead>
                <tbody>
                  {topTurnos.map((item: any) => (
                    <tr key={`${item.id_filial}-${item.id_turno}`}>
                      <td>{item.filial_label}</td>
                      <td>Caixa {item.id_turno}</td>
                      <td>{item.usuario_label}</td>
                      <td>{formatDateTime(item.last_event_at)}</td>
                      <td>{formatCurrency(item.total_vendas)}</td>
                      <td>{formatCurrency(item.total_pagamentos)}</td>
                      <td>{formatCurrency(item.total_cancelamentos)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : null}
          </div>

          <div className="card col-4 chartCard">
            <h2>Formas de pagamento do período</h2>
            {!loading && !paymentMix.length ? (
              <EmptyState
                title="Sem pagamentos conciliados no período."
                detail="A distribuição por forma aparece quando a carga por turno retorna pagamentos vinculados ao recorte."
              />
            ) : null}
            <div className="chartWrap" style={{ height: 280 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={paymentMix} dataKey="total_valor" nameKey="label" innerRadius={60} outerRadius={95} paddingAngle={2}>
                    {paymentMix.map((_: any, idx: number) => (
                      <Cell key={`cash-pay-${idx}`} fill={PIE_COLORS[idx % PIE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: any) => formatCurrency(value)} />
                </PieChart>
              </ResponsiveContainer>
            </div>
            {paymentMix.length ? (
              <div style={{ display: 'grid', gap: 8 }}>
                {paymentMix.slice(0, 6).map((item: any) => (
                  <div key={item.label} style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                    <span className="muted">{item.label}</span>
                    <strong>{formatCurrency(item.total_valor)}</strong>
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          <div className="card col-6">
            <h2>Caixas abertos agora</h2>
            {!loading && liveStatus === 'unavailable' ? (
              <EmptyState
                title="Monitor operacional indisponível."
                detail="O DW ainda não possui turnos em tempo real suficientes para formar a visão de agora."
              />
            ) : null}
            {!loading && liveStatus !== 'unavailable' && !openBoxes.length ? (
              <EmptyState
                title="Nenhum caixa aberto neste momento."
                detail="A visão em tempo real está íntegra, mas a operação não tem pendências abertas agora."
              />
            ) : null}
            {openBoxes.length ? (
              <div className="cashBoxList">
                {openBoxes.map((item: any) => {
                  const tone = severityTone(item.severity);
                  return (
                    <div
                      key={`${item.id_filial}-${item.id_turno}`}
                      className="cashBoxCard"
                      style={{
                        border: `1px solid ${tone.border}`,
                        background: tone.bg,
                      }}
                    >
                      <div className="cashBoxHead">
                        <div className="cashBoxIdentity">
                          <div className="cashBoxTitle">{formatFilialLabel(item.id_filial, item.filial_nome)}</div>
                          <div className="muted">
                            Caixa {item.id_turno} • {item.usuario_label || item.usuario_nome || 'Operador não identificado'}
                          </div>
                        </div>
                        <div className="pill" style={{ borderColor: tone.border, background: 'transparent' }}>{tone.label}</div>
                      </div>
                      <div className="cashBoxMetrics">
                        <div className="cashMetric cashMetricWide">
                          <div className="label">Abertura</div>
                          <div className="cashMetricValue">{formatDateTime(item.abertura_ts)}</div>
                        </div>
                        <div className="cashMetric">
                          <div className="label">Tempo aberto</div>
                          <div className="cashMetricValue">{formatHoursLabel(item.horas_aberto)}</div>
                        </div>
                        <div className="cashMetric">
                          <div className="label">Status</div>
                          <div className="cashMetricValue">{item.status_label || tone.label}</div>
                        </div>
                      </div>
                      <div className="cashBoxMetrics cashBoxMetricsSecondary">
                        <div className="cashMetric cashMetricWide">
                          <div className="label">Total vendido</div>
                          <div className="cashMetricValue">{formatCurrency(item.total_vendas)}</div>
                        </div>
                        <div className="cashMetric">
                          <div className="label">Pagamentos</div>
                          <div className="cashMetricValue">{formatCurrency(item.total_pagamentos)}</div>
                        </div>
                        <div className="cashMetric">
                          <div className="label">Cancelamentos</div>
                          <div className="cashMetricValue">{formatCurrency(item.total_cancelamentos)}</div>
                        </div>
                        <div className="cashMetric">
                          <div className="label">Comprovantes válidos</div>
                          <div className="cashMetricValue">{Number(item.qtd_vendas || 0)}</div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>

          <div className="card col-6">
            <h2>Alertas de caixa aberto</h2>
            {!loading && !alerts.length ? (
              <EmptyState
                title="Nenhum caixa em situação crítica."
                detail="Quando um caixa ultrapassar 24 horas aberto, ele aparecerá aqui e já sairá pronto para alerta Telegram."
              />
            ) : null}
            <table className="table compact">
              <thead>
                <tr>
                  <th>Filial</th>
                  <th>Caixa</th>
                  <th>Operador</th>
                  <th>Tempo aberto</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((item: any) => (
                  <tr key={`${item.id_filial}-${item.id_turno}`}>
                    <td>{formatFilialLabel(item.id_filial, item.filial_nome)}</td>
                    <td>Caixa {item.id_turno}</td>
                    <td>{item.usuario_label || item.usuario_nome || 'Operador não identificado'}</td>
                    <td>{formatHoursLabel(item.horas_aberto)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-6">
            <h2>Cancelamentos do período</h2>
            {!loading && !cancelamentos.length ? (
              <EmptyState
                title="Sem cancelamentos relevantes no histórico do período."
                detail="O ranking acompanha comprovantes vinculados a turno com CFOP acima de 5000."
              />
            ) : null}
            <table className="table compact">
              <thead>
                <tr>
                  <th>Filial</th>
                  <th>Caixa</th>
                  <th>Operador</th>
                  <th>Total cancelado</th>
                  <th>Ocorrências</th>
                </tr>
              </thead>
              <tbody>
                {cancelamentos.map((item: any) => (
                  <tr key={`${item.id_filial}-${item.id_turno}`}>
                    <td>{item.filial_label}</td>
                    <td>Caixa {item.id_turno}</td>
                    <td>{item.usuario_label}</td>
                    <td>{formatCurrency(item.total_cancelamentos)}</td>
                    <td>{Number(item.qtd_cancelamentos || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-6">
            <h2>Evolução diária do período</h2>
            {!loading && !byDay.length ? (
              <EmptyState
                title="Sem série diária para o período."
                detail="Não houve movimento histórico suficiente para construir a série diária de caixa."
              />
            ) : null}
            <table className="table compact">
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Caixas</th>
                  <th>Vendas</th>
                  <th>Pagamentos</th>
                  <th>Cancelamentos</th>
                </tr>
              </thead>
              <tbody>
                {byDay.slice(-10).reverse().map((item: any) => (
                  <tr key={item.data_key}>
                    <td>{formatDateKey(item.data_key)}</td>
                    <td>{Number(item.caixas || 0)}</td>
                    <td>{formatCurrency(item.total_vendas)}</td>
                    <td>{formatCurrency(item.total_pagamentos)}</td>
                    <td>{formatCurrency(item.total_cancelamentos)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
