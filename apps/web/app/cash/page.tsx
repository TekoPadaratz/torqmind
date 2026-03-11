'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateTime,
  formatFilialLabel,
  formatHoursLabel,
} from '../lib/format';
import { useScopeQuery } from '../lib/scope';

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

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
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

        const res = await apiGet(`/bi/cash/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar o módulo de Caixa'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.ready, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa]);

  const kpis = data?.kpis || {};
  const openBoxes = data?.open_boxes || [];
  const paymentMix = data?.payment_mix || [];
  const cancelamentos = data?.cancelamentos || [];
  const alerts = data?.alerts || [];
  const sourceStatus = String(data?.source_status || 'unavailable');

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
          <div className="muted">Painel operacional de caixa com foco em turnos abertos, fechamento pendente, vendas expostas e cancelamentos.</div>
          {!loading ? <div style={{ marginTop: 10, fontSize: 18, fontWeight: 700 }}>{data?.summary}</div> : null}
        </div>

        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3">
            <div className="label">Caixas abertos</div>
            <div className="value">{loading ? '...' : Number(kpis.caixas_abertos || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Acima de 24h</div>
            <div className="value">{loading ? '...' : Number(kpis.caixas_criticos || 0)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Vendas em caixas abertos</div>
            <div className="value">{loading ? '...' : formatCurrency(kpis.total_vendas_abertas)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Cancelamentos em caixas abertos</div>
            <div className="value">{loading ? '...' : formatCurrency(kpis.total_cancelamentos_abertos)}</div>
          </div>

          <div className="card col-8">
            <h2>Caixas abertos agora</h2>
            {!loading && sourceStatus === 'unavailable' ? (
              <EmptyState
                title="O módulo está pronto, aguardando a próxima carga operacional."
                detail="Assim que TURNOS e USUÁRIOS entrarem da Xpert, esta tela passa a mostrar abertura, operador, vendas e alertas automaticamente."
              />
            ) : null}
            {!loading && sourceStatus !== 'unavailable' && !openBoxes.length ? (
              <EmptyState
                title="Nenhum caixa aberto neste momento."
                detail="A operação está sem pendências de fechamento no recorte monitorado."
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
                        <div className="cashMetric">
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
                          <div className="label">Vendas válidas</div>
                          <div className="cashMetricValue">{Number(item.qtd_vendas || 0)}</div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>

          <div className="card col-4 chartCard">
            <h2>Formas de pagamento</h2>
            {!loading && !paymentMix.length ? (
              <EmptyState
                title="Sem pagamentos vinculados aos caixas abertos."
                detail="A distribuição por forma aparece automaticamente quando houver comprovantes ligados ao turno em aberto."
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
            <h2>Cancelamentos em caixas abertos</h2>
            {!loading && !cancelamentos.length ? (
              <EmptyState
                title="Sem cancelamentos relevantes nos caixas abertos."
                detail="O módulo acompanha apenas comprovantes válidos do caixa com CFOP acima de 5000."
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
        </div>
      </div>
    </div>
  );
}
