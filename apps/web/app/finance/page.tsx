'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

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

export default function FinancePage() {
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

        const res = await apiGet(`/bi/finance/overview?${qs.toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar financeiro'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa]);

  const chartData = useMemo(
    () =>
      (data?.by_day || []).map((r: any) => ({
        data: shortDateKey(r.data_key),
        aberto: Number(r.valor_aberto || 0),
        pago: Number(r.valor_pago || 0),
      })),
    [data]
  );

  const hasFinance = useMemo(() => (data?.by_day || []).length > 0, [data]);

  return (
    <div>
      <AppNav title="Financeiro" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3"><div className="label">Receber total</div><div className="value">{loading ? '...' : fmtMoney(data?.kpis?.receber_total)}</div></div>
          <div className="card kpi col-3"><div className="label">Receber aberto</div><div className="value">{loading ? '...' : fmtMoney(data?.kpis?.receber_aberto)}</div></div>
          <div className="card kpi col-3"><div className="label">Pagar total</div><div className="value">{loading ? '...' : fmtMoney(data?.kpis?.pagar_total)}</div></div>
          <div className="card kpi col-3"><div className="label">Pagar aberto</div><div className="value">{loading ? '...' : fmtMoney(data?.kpis?.pagar_aberto)}</div></div>

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
        </div>
      </div>
    </div>
  );
}
