'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis, Line, LineChart } from 'recharts';

import AppNav from '../components/AppNav';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import { useScopeQuery } from '../lib/scope';

function fmtMoney(v: any) {
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

export default function FraudPage() {
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

        const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const res = await apiGet(`/bi/fraud/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar fraude'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa]);

  const byDay = useMemo(
    () => (data?.by_day || []).map((r: any) => ({ ...r, data: shortDateKey(r.data_key), cancelamentos: Number(r.cancelamentos || 0) })),
    [data]
  );
  const riskByDay = useMemo(
    () =>
      (data?.risk_by_day || []).map((r: any) => ({
        ...r,
        data: shortDateKey(r.data_key),
        eventos_alto_risco: Number(r.eventos_alto_risco || 0),
        impacto_estimado_total: Number(r.impacto_estimado_total || 0),
      })),
    [data]
  );
  const maxRiskDate = dataKeyToISO(data?.risk_window?.max_data_key);
  const scopeOutdatedForRisk =
    !!maxRiskDate &&
    !!scope.dt_fim &&
    scope.dt_fim < maxRiskDate &&
    Number(data?.risk_kpis?.total_eventos || 0) === 0;

  return (
    <div>
      <AppNav title="Sistema Anti-Fraude" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card" style={{ marginTop: 12, borderColor: '#f59e0b' }}>
            <strong>Escopo fora da janela de risco.</strong> O antifraude tem dados ate <strong>{maxRiskDate}</strong>. Ajuste em{' '}
            <Link href="/scope">Definir Escopo</Link>.
          </div>
        ) : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-6"><div className="label">Cancelamentos</div><div className="value">{loading ? '...' : Number(data?.kpis?.cancelamentos || 0)}</div></div>
          <div className="card kpi col-6"><div className="label">Valor cancelado</div><div className="value">{loading ? '...' : fmtMoney(data?.kpis?.valor_cancelado)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Impacto de risco (R$)</div><div className="value">{loading ? '...' : fmtMoney(data?.risk_kpis?.impacto_total)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Eventos alto risco</div><div className="value">{loading ? '...' : Number(data?.risk_kpis?.eventos_alto_risco || 0)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Score medio</div><div className="value">{loading ? '...' : Number(data?.risk_kpis?.score_medio || 0).toFixed(1)}</div></div>

          <div className="card col-8 chartCard">
            <h2>Cancelamentos por dia</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={byDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="cancelamentos" fill="#f97316" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-4">
            <h2>Top funcionarios por risco</h2>
            <table className="table compact">
              <thead><tr><th>Funcionario</th><th>Score</th><th>Impacto</th></tr></thead>
              <tbody>
                {(data?.risk_top_employees || []).slice(0, 10).map((u: any) => (
                  <tr key={u.id_funcionario}>
                    <td>{u.funcionario_nome || u.id_funcionario}</td>
                    <td>{Number(u.score_medio || 0).toFixed(1)}</td>
                    <td>{fmtMoney(u.impacto_estimado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Risco por turno e local</h2>
            <table className="table compact">
              <thead><tr><th>Turno</th><th>Local</th><th>Eventos</th><th>Alto risco</th><th>Impacto</th><th>Score medio</th></tr></thead>
              <tbody>
                {(data?.risk_by_turn_local || []).slice(0, 10).map((r: any, idx: number) => (
                  <tr key={`${r.id_turno}-${r.id_local_venda}-${idx}`}>
                    <td>{r.id_turno ?? '-'}</td>
                    <td>{r.id_local_venda ?? '-'}</td>
                    <td>{r.eventos}</td>
                    <td>{r.alto_risco}</td>
                    <td>{fmtMoney(r.impacto_estimado)}</td>
                    <td>{Number(r.score_medio || 0).toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Ultimos eventos de risco</h2>
            <table className="table compact">
              <thead>
                <tr><th>Data</th><th>Filial</th><th>Evento</th><th>Funcionario</th><th>Score</th><th>Valor</th><th>Impacto</th><th>Reasons</th></tr>
              </thead>
              <tbody>
                {(data?.risk_last_events || []).slice(0, 20).map((e: any) => (
                  <tr key={e.id}>
                    <td>{e.data || '-'}</td>
                    <td>{e.id_filial}</td>
                    <td>{e.event_type}</td>
                    <td>{e.funcionario_nome || e.id_funcionario || '-'}</td>
                    <td>{e.score_risco}</td>
                    <td>{fmtMoney(e.valor_total)}</td>
                    <td>{fmtMoney(e.impacto_estimado)}</td>
                    <td>{Object.keys(e.reasons || {}).slice(0, 3).join(', ') || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Anomalias de pagamento (top score)</h2>
            {!loading && !(data?.payments_risk || []).length ? (
              <p className="muted">Sem anomalias de pagamento no periodo.</p>
            ) : null}
            <table className="table compact">
              <thead>
                <tr><th>Data</th><th>Filial</th><th>Turno</th><th>Evento</th><th>Severidade</th><th>Score</th><th>Impacto</th></tr>
              </thead>
              <tbody>
                {(data?.payments_risk || []).slice(0, 12).map((e: any, idx: number) => (
                  <tr key={`${e.insight_id || e.event_type}-${idx}`}>
                    <td>{shortDateKey(e.data_key)}</td>
                    <td>{e.id_filial}</td>
                    <td>{e.id_turno ?? '-'}</td>
                    <td>{e.event_type}</td>
                    <td>{e.severity}</td>
                    <td>{Number(e.score || 0)}</td>
                    <td>{fmtMoney(e.impacto_estimado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12 chartCard">
            <h2>Serie temporal de alto risco</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={riskByDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Line type="monotone" dataKey="eventos_alto_risco" stroke="#ef4444" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="impacto_estimado_total" stroke="#f59e0b" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
