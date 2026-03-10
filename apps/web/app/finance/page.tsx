'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Area, AreaChart, Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatFilialLabel,
  formatTurnoLabel,
} from '../lib/format';
import { useScopeQuery } from '../lib/scope';

export default function FinancePage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

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

        const res = await apiGet(`/bi/finance/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar financeiro'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa]);

  const chartData = useMemo(
    () =>
      (data?.by_day || []).map((r: any) => ({
        data: formatDateKeyShort(r.data_key),
        aberto: Number(r.valor_aberto || 0),
        pago: Number(r.valor_pago || 0),
      })),
    [data]
  );

  const hasFinance = useMemo(() => (data?.by_day || []).length > 0, [data]);
  const paymentsByDay = useMemo(
    () =>
      (data?.payments?.by_day || []).map((r: any) => ({
        data: formatDateKeyShort(r.data_key),
        valor: Number(r.total_valor || 0),
        category: r.category,
      })),
    [data]
  );
  const paymentsByTurno = useMemo(() => data?.payments?.by_turno || [], [data]);
  const paymentsKpis = data?.payments?.kpis || {};
  const paymentsAnomalies = data?.payments?.anomalies || [];
  const openCash = data?.open_cash || {};

  return (
    <div>
      <AppNav title="Financeiro" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Fluxo, pagamentos e posição financeira.</div>
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3"><div className="label">Receber total</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.receber_total)}</div></div>
          <div className="card kpi col-3"><div className="label">Receber aberto</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.receber_aberto)}</div></div>
          <div className="card kpi col-3"><div className="label">Pagar total</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.pagar_total)}</div></div>
          <div className="card kpi col-3"><div className="label">Pagar aberto</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.pagar_aberto)}</div></div>

          <div className="card col-12 chartCard">
            <h2>Fluxo por vencimento</h2>
            {!loading && !hasFinance ? <p className="muted">Sem dados financeiros para o periodo selecionado.</p> : null}
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Area type="monotone" dataKey="pago" stackId="1" stroke="#22d3ee" fill="#22d3ee" fillOpacity={0.35} />
                  <Area type="monotone" dataKey="aberto" stackId="1" stroke="#fb7185" fill="#fb7185" fillOpacity={0.35} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card kpi col-4">
            <div className="label">Pagamentos (período)</div>
            <div className="value">{loading ? '...' : formatCurrency(paymentsKpis.total_valor)}</div>
          </div>
          <div className="card kpi col-4">
            <div className="label">Variação vs período anterior</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.delta_pct || 0).toFixed(1)}%`}</div>
          </div>
          <div className="card kpi col-4" id="payment-mapping">
            <div className="label">Pagamentos nao categorizados</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.unknown_share_pct || 0).toFixed(1)}%`}</div>
          </div>

          <div className="card col-12">
            <h2>Monitor de caixa em aberto</h2>
            {!loading ? (
              <>
                <div className="muted" style={{ marginBottom: 8 }}>{openCash.summary || 'Monitoramento operacional indisponivel.'}</div>
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
                        ? 'Dados de turno indisponiveis.'
                        : openCash.source_status === 'unmapped'
                          ? 'Fonte de turnos ainda nao mapeada.'
                          : 'Nenhum turno em aberto acima do limite esperado.'
                    }
                    detail={openCash.summary}
                  />
                )}
              </>
            ) : null}
          </div>

          <div className="card col-12 chartCard">
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

          <div className="card col-7">
            <h2>Ranking por turno (pagamentos)</h2>
            {!loading && !paymentsByTurno.length ? (
              <EmptyState title="Sem dados de turno no periodo." detail="A fonte de pagamentos por turno nao trouxe registros para o recorte selecionado." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Data</th><th>Turno</th><th>Categoria</th><th>Valor</th><th>Comprovantes</th></tr></thead>
              <tbody>
                {paymentsByTurno.slice(0, 15).map((r: any, idx: number) => (
                  <tr key={`${r.data_key}-${r.id_turno}-${r.category}-${idx}`}>
                    <td>{formatDateKey(r.data_key)}</td>
                    <td>{formatTurnoLabel(r.id_turno)}</td>
                    <td>{r.category}</td>
                    <td>{formatCurrency(r.total_valor)}</td>
                    <td>{Number(r.qtd_comprovantes || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-5">
            <h2>Anomalias de pagamento</h2>
            {!loading && !paymentsAnomalies.length ? <p className="muted">Sem anomalias relevantes no período.</p> : null}
            <table className="table compact">
              <thead><tr><th>Evento</th><th>Sev</th><th>Score</th><th>Impacto</th></tr></thead>
              <tbody>
                {paymentsAnomalies.slice(0, 10).map((a: any, idx: number) => (
                  <tr key={`${a.insight_id || a.event_type}-${idx}`}>
                    <td>{a.event_label || a.event_type}</td>
                    <td>{a.severity}</td>
                    <td>{Number(a.score || 0)}</td>
                    <td>{formatCurrency(a.impacto_estimado)}</td>
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
