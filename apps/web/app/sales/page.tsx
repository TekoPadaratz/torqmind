'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import { buildUserLabel, formatCurrency } from '../lib/format';
import { useScopeQuery } from '../lib/scope';

export const dynamic = 'force-dynamic';

export default function SalesPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState<boolean>(true);
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

        const res = await apiGet(`/bi/sales/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar vendas'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_filial, scope.id_empresa]);

  const hourAgg = useMemo(() => {
    const rows = new Array(24).fill(0).map((_, hora) => ({ hora: `${hora.toString().padStart(2, '0')}:00`, faturamento: 0 }));
    for (const r of data?.by_hour || []) {
      const h = Number(r.hora || 0);
      if (h >= 0 && h < 24) rows[h].faturamento += Number(r.faturamento || 0);
    }
    return rows;
  }, [data]);

  return (
    <div>
      <AppNav title="Vendas" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Faturamento, margem e desempenho comercial.</div>
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3"><div className="label">Faturamento</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.faturamento)}</div></div>
          <div className="card kpi col-3"><div className="label">Margem</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.margem)}</div></div>
          <div className="card kpi col-3"><div className="label">Ticket médio</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.ticket_medio)}</div></div>
          <div className="card kpi col-3"><div className="label">Itens</div><div className="value">{loading ? '...' : Number(data?.kpis?.itens || 0)}</div></div>

          <div className="card col-8 chartCard">
            <h2>Faturamento por hora</h2>
            {!loading && !hourAgg.some((row) => Number(row.faturamento || 0) > 0) ? (
              <EmptyState title="Sem vendas por hora no período." detail="Não houve movimento comercial suficiente para distribuir a curva horária." />
            ) : null}
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={hourAgg}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="hora" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="faturamento" fill="#22d3ee" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-4">
            <h2>Top vendedores</h2>
            {!loading && !(data?.top_employees || []).length ? (
              <EmptyState title="Sem ranking de vendedores." detail="Nenhum funcionário apareceu com vendas no período selecionado." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Funcionário</th><th>Fat.</th></tr></thead>
              <tbody>
                {(data?.top_employees || []).slice(0, 8).map((f: any) => (
                  <tr key={f.id_funcionario}>
                    <td>{f.funcionario_nome}</td>
                    <td>{formatCurrency(f.faturamento)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-6">
            <h2>Top produtos</h2>
            {!loading && !(data?.top_products || []).length ? (
              <EmptyState title="Sem produtos ranqueados." detail="A fonte de itens vendidos não retornou registros para este recorte." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Produto</th><th>Fat.</th><th>Margem</th></tr></thead>
              <tbody>
                {(data?.top_products || []).slice(0, 10).map((p: any) => (
                  <tr key={p.id_produto}><td>{p.produto_nome}</td><td>{formatCurrency(p.faturamento)}</td><td>{formatCurrency(p.margem)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-6">
            <h2>Top grupos</h2>
            {!loading && !(data?.top_groups || []).length ? (
              <EmptyState title="Sem grupos ranqueados." detail="A agregação por grupo não trouxe dados para o período." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Grupo</th><th>Fat.</th><th>Margem</th></tr></thead>
              <tbody>
                {(data?.top_groups || []).slice(0, 10).map((g: any) => (
                  <tr key={`${g.id_grupo_produto}-${g.grupo_nome}`}><td>{g.grupo_nome}</td><td>{formatCurrency(g.faturamento)}</td><td>{formatCurrency(g.margem)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
