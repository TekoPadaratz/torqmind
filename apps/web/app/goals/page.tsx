'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

import AppNav from '../components/AppNav';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';

function useScope() {
  const params = useSearchParams();
  const dt_ini = params.get('dt_ini') || '';
  const dt_fim = params.get('dt_fim') || '';
  const id_filial = params.get('id_filial');
  const id_empresa = params.get('id_empresa');
  return { dt_ini, dt_fim, id_filial, id_empresa };
}

function fmtMoney(v: any) {
  const n = Number(v || 0);
  return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

export default function GoalsPage() {
  const router = useRouter();
  const scope = useScope();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    return [claims.role, claims.id_empresa ? `E${claims.id_empresa}` : '', claims.id_filial ? `F${claims.id_filial}` : '']
      .filter(Boolean)
      .join(' · ');
  }, [claims]);

  const totals = useMemo(() => {
    const lb = data?.leaderboard || [];
    const fat = lb.reduce((acc: number, r: any) => acc + Number(r.faturamento || 0), 0);
    const mar = lb.reduce((acc: number, r: any) => acc + Number(r.margem || 0), 0);
    const ven = lb.reduce((acc: number, r: any) => acc + Number(r.vendas || 0), 0);
    return { fat, mar, ven };
  }, [data]);

  useEffect(() => {
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
      try {
        const me = await apiGet('/auth/me');
        setClaims(me);

        const qs = new URLSearchParams({ dt_ini: scope.dt_ini, dt_fim: scope.dt_fim });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const res = await apiGet(`/bi/goals/overview?${qs.toString()}`);
        setData(res);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa]);

  return (
    <div>
      <AppNav title="Metas & Equipe" userLabel={userLabel} />

      <div className="container">
        <div className="card">
          <h2>Escopo</h2>
          <div className="row">
            <div className="kpi">
              <div className="label">Período</div>
              <div className="value">
                {scope.dt_ini} → {scope.dt_fim}
              </div>
            </div>
            <div className="kpi">
              <div className="label">Filial</div>
              <div className="value">{scope.id_filial || 'Todas'}</div>
            </div>
          </div>
        </div>

        <div className="card">
          <h2>Resumo do período</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Faturamento</div>
                <div className="value">{fmtMoney(totals.fat)}</div>
              </div>
              <div className="kpi">
                <div className="label">Margem</div>
                <div className="value">{fmtMoney(totals.mar)}</div>
              </div>
              <div className="kpi">
                <div className="label">Vendas</div>
                <div className="value">{totals.ven}</div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h2>Metas do dia (dt_fim)</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (data?.goals_today || []).length ? (
            <table className="table">
              <thead>
                <tr>
                  <th>Tipo</th>
                  <th>Alvo</th>
                  <th>Atual (estimado)</th>
                </tr>
              </thead>
              <tbody>
                {(data.goals_today || []).map((g: any) => {
                  let current: any = '-';
                  if (g.goal_type === 'FATURAMENTO_DIA') current = fmtMoney(totals.fat);
                  if (g.goal_type === 'MARGEM_DIA') current = fmtMoney(totals.mar);
                  if (g.goal_type === 'VENDAS_DIA') current = totals.ven;
                  return (
                    <tr key={g.goal_type}>
                      <td>{g.goal_type}</td>
                      <td>{fmtMoney(g.target_value)}</td>
                      <td>{current}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <p className="muted">
              Nenhuma meta configurada. Você pode inserir em <code>app.goals</code> (ex: FATURAMENTO_DIA, MARGEM_DIA).
            </p>
          )}
        </div>

        <div className="card">
          <h2>Leaderboard (funcionários)</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Funcionário</th>
                  <th>Vendas</th>
                  <th>Faturamento</th>
                  <th>Margem</th>
                </tr>
              </thead>
              <tbody>
                {(data?.leaderboard || []).map((r: any) => (
                  <tr key={r.id_funcionario}>
                    <td>
                      #{r.id_funcionario} — {r.funcionario_nome}
                    </td>
                    <td>{r.vendas}</td>
                    <td>{fmtMoney(r.faturamento)}</td>
                    <td>{fmtMoney(r.margem)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
