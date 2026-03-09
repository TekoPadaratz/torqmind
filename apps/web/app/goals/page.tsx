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

export default function GoalsPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [riskData, setRiskData] = useState<any>(null);
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

        const res = await apiGet(`/bi/goals/overview?${qs.toString()}`);
        const risk = await apiGet(`/bi/risk/overview?${qs.toString()}`);
        setData(res);
        setRiskData(risk);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar metas'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa]);

  const topLeaderboard = useMemo(
    () =>
      (data?.leaderboard || []).slice(0, 10).map((r: any) => ({
        funcionario: String(r.id_funcionario),
        faturamento: Number(r.faturamento || 0),
      })),
    [data]
  );

  return (
    <div>
      <AppNav title="Metas e Equipe" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Metas, ranking e resultado da equipe.</div>
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card col-7 chartCard">
            <h2>Ranking de faturamento por funcionario</h2>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topLeaderboard}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="funcionario" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="faturamento" fill="#10b981" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-5">
            <h2>Metas do dia</h2>
            {loading ? <div className="skeleton" /> : null}
            {!loading && !(data?.goals_today || []).length ? (
              <p className="muted">Nenhuma meta configurada para a data final do escopo.</p>
            ) : null}
            {(data?.goals_today || []).length ? (
              <table className="table compact">
                <thead><tr><th>Tipo</th><th>Alvo</th></tr></thead>
                <tbody>
                  {(data?.goals_today || []).map((g: any) => (
                    <tr key={g.goal_type}><td>{g.goal_type}</td><td>{fmtMoney(g.target_value)}</td></tr>
                  ))}
                </tbody>
              </table>
            ) : null}
          </div>

          <div className="card col-12">
            <h2>Leaderboard detalhado</h2>
            <table className="table compact">
              <thead><tr><th>Funcionario</th><th>Vendas</th><th>Faturamento</th><th>Margem</th><th>Risco medio</th><th>Status risco</th></tr></thead>
              <tbody>
                {(data?.leaderboard || []).map((r: any) => (
                  <tr key={r.id_funcionario}>
                    <td>{r.funcionario_nome}</td>
                    <td>{r.vendas}</td>
                    <td>{fmtMoney(r.faturamento)}</td>
                    <td>{fmtMoney(r.margem)}</td>
                    <td>{Number((riskData?.top_employees || []).find((x: any) => x.id_funcionario === r.id_funcionario)?.score_medio || 0).toFixed(1)}</td>
                    <td>
                      {Number((riskData?.top_employees || []).find((x: any) => x.id_funcionario === r.id_funcionario)?.score_medio || 0) >= 80 ? (
                        <span className="badge critical">ALTO</span>
                      ) : Number((riskData?.top_employees || []).find((x: any) => x.id_funcionario === r.id_funcionario)?.score_medio || 0) >= 60 ? (
                        <span className="badge warn">SUSPEITO</span>
                      ) : (
                        <span className="badge info">OK</span>
                      )}
                    </td>
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
