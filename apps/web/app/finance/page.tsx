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

export default function FinancePage() {
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

        const res = await apiGet(`/bi/finance/overview?${qs.toString()}`);
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
      <AppNav title="Financeiro" userLabel={userLabel} />

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
          <h2>Contas a receber</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Total</div>
                <div className="value">{fmtMoney(data?.kpis?.receber_total)}</div>
              </div>
              <div className="kpi">
                <div className="label">Pago</div>
                <div className="value">{fmtMoney(data?.kpis?.receber_pago)}</div>
              </div>
              <div className="kpi">
                <div className="label">Aberto</div>
                <div className="value">{fmtMoney(data?.kpis?.receber_aberto)}</div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h2>Contas a pagar</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Total</div>
                <div className="value">{fmtMoney(data?.kpis?.pagar_total)}</div>
              </div>
              <div className="kpi">
                <div className="label">Pago</div>
                <div className="value">{fmtMoney(data?.kpis?.pagar_pago)}</div>
              </div>
              <div className="kpi">
                <div className="label">Aberto</div>
                <div className="value">{fmtMoney(data?.kpis?.pagar_aberto)}</div>
              </div>
            </div>
          )}
          <div className="muted">
            A série abaixo é por *vencimento* (data_key) e já está pronta para um gráfico de fluxo de caixa.
          </div>
        </div>

        <div className="card">
          <h2>Vencimentos por dia</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Tipo</th>
                  <th>Total</th>
                  <th>Pago</th>
                  <th>Aberto</th>
                </tr>
              </thead>
              <tbody>
                {(data?.by_day || []).map((r: any) => (
                  <tr key={`${r.data_key}-${r.tipo_titulo}-${r.id_filial}`}
                    >
                    <td>{String(r.data_key)}</td>
                    <td>{Number(r.tipo_titulo) === 1 ? 'Receber' : 'Pagar'}</td>
                    <td>{fmtMoney(r.valor_total)}</td>
                    <td>{fmtMoney(r.valor_pago)}</td>
                    <td>{fmtMoney(r.valor_aberto)}</td>
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
