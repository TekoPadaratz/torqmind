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

export default function SalesPage() {
  const router = useRouter();
  const scope = useScope();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState<boolean>(true);

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    const role = claims.role;
    const emp = claims.id_empresa ? `E${claims.id_empresa}` : '';
    const fil = claims.id_filial ? `F${claims.id_filial}` : '';
    return [role, emp, fil].filter(Boolean).join(' · ');
  }, [claims]);

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

        const res = await apiGet(`/bi/sales/overview?${qs.toString()}`);
        setData(res);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa]);

  const hourAgg = useMemo(() => {
    const arr = new Array(24).fill(0);
    const vendas = new Array(24).fill(0);
    if (!data?.by_hour) return { arr, vendas };
    for (const r of data.by_hour) {
      const h = Number(r.hora);
      arr[h] += Number(r.faturamento || 0);
      vendas[h] += Number(r.vendas || 0);
    }
    return { arr, vendas };
  }, [data]);

  return (
    <div>
      <AppNav title="Vendas & Stores" userLabel={userLabel} />

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
          <h2>KPIs de vendas</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Faturamento</div>
                <div className="value">{fmtMoney(data?.kpis?.faturamento)}</div>
              </div>
              <div className="kpi">
                <div className="label">Margem</div>
                <div className="value">{fmtMoney(data?.kpis?.margem)}</div>
              </div>
              <div className="kpi">
                <div className="label">Ticket médio</div>
                <div className="value">{fmtMoney(data?.kpis?.ticket_medio)}</div>
              </div>
              <div className="kpi">
                <div className="label">Itens</div>
                <div className="value">{Number(data?.kpis?.itens || 0)}</div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h2>Vendas por hora (acumulado)</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Hora</th>
                  <th>Faturamento</th>
                  <th>Vendas</th>
                </tr>
              </thead>
              <tbody>
                {hourAgg.arr.map((v: number, h: number) => (
                  <tr key={h}>
                    <td>{h.toString().padStart(2, '0')}:00</td>
                    <td>{fmtMoney(v)}</td>
                    <td>{hourAgg.vendas[h]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Top produtos</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Produto</th>
                  <th>Qtd</th>
                  <th>Faturamento</th>
                  <th>Margem</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_products || []).map((p: any) => (
                  <tr key={p.id_produto}>
                    <td>
                      #{p.id_produto} — {p.produto_nome}
                    </td>
                    <td>{Number(p.qtd || 0).toFixed(0)}</td>
                    <td>{fmtMoney(p.faturamento)}</td>
                    <td>{fmtMoney(p.margem)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Top grupos</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Grupo</th>
                  <th>Faturamento</th>
                  <th>Margem</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_groups || []).map((g: any) => (
                  <tr key={g.id_grupo_produto}>
                    <td>
                      #{g.id_grupo_produto} — {g.grupo_nome}
                    </td>
                    <td>{fmtMoney(g.faturamento)}</td>
                    <td>{fmtMoney(g.margem)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Top vendedores</h2>
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
                {(data?.top_employees || []).map((f: any) => (
                  <tr key={f.id_funcionario}>
                    <td>
                      #{f.id_funcionario} — {f.funcionario_nome}
                    </td>
                    <td>{f.vendas}</td>
                    <td>{fmtMoney(f.faturamento)}</td>
                    <td>{fmtMoney(f.margem)}</td>
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
