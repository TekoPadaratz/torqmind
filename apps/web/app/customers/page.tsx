'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import AppNav from '../components/AppNav';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import { useScopeQuery } from '../lib/scope';

function fmtMoney(v: any) {
  const n = Number(v || 0);
  return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

export default function CustomersPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    return [claims.role, claims.id_empresa ? `E${claims.id_empresa}` : '', claims.id_filial ? `F${claims.id_filial}` : '']
      .filter(Boolean)
      .join(' · ');
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

        const res = await apiGet(`/bi/customers/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar clientes'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa]);

  const topChart = useMemo(
    () =>
      (data?.top_customers || []).slice(0, 10).map((c: any) => ({
        cliente: `${c.id_cliente}`,
        faturamento: Number(c.faturamento || 0),
      })),
    [data]
  );
  const anon = data?.anonymous_retention || {};
  const anonKpis = anon?.kpis || {};

  return (
    <div>
      <AppNav title="Analise de Clientes" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Recorrência, churn e valor da base.</div>
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3"><div className="label">Clientes identificados</div><div className="value">{loading ? '...' : data?.rfm?.clientes_identificados ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Ativos 7d</div><div className="value">{loading ? '...' : data?.rfm?.ativos_7d ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Em risco 30d</div><div className="value">{loading ? '...' : data?.rfm?.em_risco_30d ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Fat. 90d</div><div className="value">{loading ? '...' : fmtMoney(data?.rfm?.faturamento_90d)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Recorrencia anonima (trend)</div><div className="value">{loading ? '...' : `${Number(anonKpis?.trend_pct || 0).toFixed(1)}%`}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Impacto anonimo estimado (7d)</div><div className="value">{loading ? '...' : fmtMoney(anonKpis?.impact_estimated_7d)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Indice recorrencia anonima</div><div className="value">{loading ? '...' : `${Number(anonKpis?.repeat_proxy_idx || 0).toFixed(1)}%`}</div></div>

          <div className="card col-12">
            <h2>Risco de churn (top 10)</h2>
            {!loading && !(data?.churn_top || []).length ? <p className="muted">Sem clientes com risco alto no período.</p> : null}
            <table className="table compact">
              <thead>
                <tr>
                  <th>Cliente</th>
                  <th>Score churn</th>
                  <th>Última compra</th>
                  <th>Compras 30d</th>
                  <th>Compras 60-30d</th>
                  <th>Fat. 30d</th>
                  <th>Fat. 60-30d</th>
                </tr>
              </thead>
              <tbody>
                {(data?.churn_top || []).map((c: any) => (
                  <tr key={c.id_cliente}>
                    <td>{c.cliente_nome}</td>
                    <td>
                      <span className={`badge ${Number(c.churn_score || 0) >= 80 ? 'warn' : 'ok'}`}>{c.churn_score}</span>
                    </td>
                    <td>{c.last_purchase ? String(c.last_purchase).slice(0, 10) : '-'}</td>
                    <td>{c.compras_30d}</td>
                    <td>{c.compras_60_30}</td>
                    <td>{fmtMoney(c.faturamento_30d)}</td>
                    <td>{fmtMoney(c.faturamento_60_30)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-7 chartCard">
            <h2>Top clientes por faturamento</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topChart}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="cliente" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="faturamento" fill="#818cf8" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-5">
            <h2>Top clientes</h2>
            <table className="table compact">
              <thead><tr><th>Cliente</th><th>Compras</th><th>Ticket</th></tr></thead>
              <tbody>
                {(data?.top_customers || []).slice(0, 10).map((c: any) => (
                  <tr key={c.id_cliente}><td>{c.cliente_nome}</td><td>{c.compras}</td><td>{fmtMoney(c.ticket_medio)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Radar de recorrencia anonima (coortes operacionais)</h2>
            <div className="muted" style={{ marginBottom: 8 }}>
              {loading ? '...' : anonKpis?.recommendation || 'Sem recomendacao para o periodo.'}
            </div>
            <table className="table compact">
              <thead><tr><th>Dia Semana</th><th>Atual</th><th>Periodo anterior</th><th>Tendencia</th></tr></thead>
              <tbody>
                {(anon?.breakdown_dow || []).map((r: any) => (
                  <tr key={r.dow}>
                    <td>{r.dow}</td>
                    <td>{fmtMoney(r.anon_current)}</td>
                    <td>{fmtMoney(r.anon_prev)}</td>
                    <td>{Number(r.trend_pct || 0).toFixed(1)}%</td>
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
