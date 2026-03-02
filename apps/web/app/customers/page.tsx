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

export default function CustomersPage() {
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

        const res = await apiGet(`/bi/customers/overview?${qs.toString()}`);
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
      <AppNav title="Análise de Clientes" userLabel={userLabel} />

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
          <h2>Snapshot (RFM simplificado)</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Clientes identificados</div>
                <div className="value">{data?.rfm?.clientes_identificados ?? 0}</div>
              </div>
              <div className="kpi">
                <div className="label">Ativos (7d)</div>
                <div className="value">{data?.rfm?.ativos_7d ?? 0}</div>
              </div>
              <div className="kpi">
                <div className="label">Em risco (30d)</div>
                <div className="value">{data?.rfm?.em_risco_30d ?? 0}</div>
              </div>
              <div className="kpi">
                <div className="label">Faturamento (90d)</div>
                <div className="value">{fmtMoney(data?.rfm?.faturamento_90d)}</div>
              </div>
            </div>
          )}
          <div className="muted">
            Este RFM é rule-based (sem ML). A base DW já está pronta para evoluir para cohort, churn e LTV.
          </div>
        </div>

        <div className="card">
          <h2>Top clientes (por faturamento)</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Cliente</th>
                  <th>Compras</th>
                  <th>Última compra</th>
                  <th>Ticket médio</th>
                  <th>Faturamento</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_customers || []).map((c: any) => (
                  <tr key={c.id_cliente}>
                    <td>
                      #{c.id_cliente} — {c.cliente_nome}
                    </td>
                    <td>{c.compras}</td>
                    <td>{c.ultima_compra || '-'}</td>
                    <td>{fmtMoney(c.ticket_medio)}</td>
                    <td>{fmtMoney(c.faturamento)}</td>
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
