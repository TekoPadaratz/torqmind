'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildUserLabel, formatCurrency, formatDateOnly } from '../lib/format';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { loadSession, readCachedSession } from '../lib/session';

export const dynamic = 'force-dynamic';

function buildChurnSignal(customer: any) {
  const reasons = customer?.reasons || {};
  const recencyDays = Number(reasons.recency_days || 0);
  const expectedCycleDays = Number(reasons.expected_cycle_days || 0);
  const frequencyDrop = Number(reasons.frequency_drop || 0);
  const monetaryDrop = Number(reasons.monetary_drop || 0);
  const compras30 = Number(customer?.compras_30d || 0);
  const comprasPrev = Number(customer?.compras_60_30 || 0);
  const faturamento30 = Number(customer?.faturamento_30d || 0);
  const faturamentoPrev = Number(customer?.faturamento_60_30 || 0);

  if (expectedCycleDays > 0 && recencyDays > expectedCycleDays * 2) {
    return 'Não voltou no intervalo esperado para a rotina do posto.';
  }
  if (comprasPrev > 0 && compras30 === 0) {
    return 'Deixou de retornar no ciclo recente e pede reativação comercial.';
  }
  if (frequencyDrop >= 15) {
    return 'Reduziu a frequência de visitas nas últimas semanas.';
  }
  if (comprasPrev > compras30 && compras30 > 0) {
    return 'Perdeu ritmo de compra em relação ao padrão anterior.';
  }
  if (monetaryDrop >= 20) {
    return 'Perdeu força de ticket médio e merece reativação comercial.';
  }
  if (faturamentoPrev > faturamento30 && faturamento30 > 0) {
    return 'Reduziu gasto no posto e merece abordagem personalizada.';
  }
  return customer?.recommendation || 'Vale retomar contato e monitorar a próxima visita.';
}

function churnCoverageLabel(snapshot: any) {
  const status = String(snapshot?.snapshot_status || 'missing');
  const effectiveDate = snapshot?.effective_dt_ref || snapshot?.requested_dt_ref;

  if (status === 'exact') {
    return `Snapshot diário exato em ${formatDateOnly(effectiveDate)}.`;
  }
  if (status === 'best_effort') {
    return `Snapshot diário mais recente até a data-base, efetivo em ${formatDateOnly(effectiveDate)}.`;
  }
  if (status === 'operational_current') {
    return `Fallback operacional corrente do churn em ${formatDateOnly(effectiveDate)}.`;
  }
  return 'Sem snapshot diário nem fallback operacional confiável para a data-base atual.';
}

export default function CustomersPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

  useEffect(() => {
    if (!scope.ready) return;
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const me = await loadSession(router, 'product');
        if (!me) return;
        setClaims(me);
        if (!scope.dt_ini || !scope.dt_fim) {
          router.replace(me?.home_path || '/dashboard');
          return;
        }

        const res = await apiGet(`/bi/customers/overview?${buildScopeParams(scope).toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar clientes'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filiais_key, scope.id_empresa, scope.ready]);

  const topChart = useMemo(
    () =>
      (data?.top_customers || []).slice(0, 10).map((c: any) => ({
        cliente: c.cliente_nome || `#ID ${c.id_cliente}`,
        faturamento: Number(c.faturamento || 0),
      })),
    [data]
  );
  const anon = data?.anonymous_retention || {};
  const anonKpis = anon?.kpis || {};
  const churnSnapshot = data?.churn_snapshot || {};

  return (
    <div>
      <AppNav title="Análise de Clientes" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Recorrência, churn e oportunidades de reativação da base, com leitura própria de comportamento do cliente e sem misturar sinais de caixa ou cancelamento operacional.</div>
          {!loading ? <div style={{ marginTop: 10, fontWeight: 700 }}>{churnCoverageLabel(churnSnapshot)}</div> : null}
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3"><div className="label">Clientes identificados</div><div className="value">{loading ? '...' : data?.rfm?.clientes_identificados ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Ativos 7d</div><div className="value">{loading ? '...' : data?.rfm?.ativos_7d ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Em risco 30d</div><div className="value">{loading ? '...' : data?.rfm?.em_risco_30d ?? 0}</div></div>
          <div className="card kpi col-3"><div className="label">Fat. 90d</div><div className="value">{loading ? '...' : formatCurrency(data?.rfm?.faturamento_90d)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Recorrência anônima</div><div className="value">{loading ? '...' : `${Number(anonKpis?.trend_pct || 0).toFixed(1)}%`}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Impacto estimado (7d)</div><div className="value">{loading ? '...' : formatCurrency(anonKpis?.impact_estimated_7d)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Índice de recorrência anônima</div><div className="value">{loading ? '...' : `${Number(anonKpis?.repeat_proxy_idx || 0).toFixed(1)}%`}</div></div>

          <div className="card col-12">
            <h2>Risco de churn (top 10)</h2>
            {!loading ? (
              <div className="muted" style={{ marginTop: 8 }}>
                Data-base solicitada: {formatDateOnly(churnSnapshot?.requested_dt_ref || claims?.server_today)}.
                {' '}Referência efetiva: {formatDateOnly(churnSnapshot?.effective_dt_ref || churnSnapshot?.requested_dt_ref || claims?.server_today)}.
              </div>
            ) : null}
            {!loading && !(data?.churn_top || []).length ? (
              <EmptyState title="Nenhum cliente em risco relevante." detail="A base identificada não trouxe sinais fortes de churn para este recorte." />
            ) : null}
            <table className="table compact">
              <thead>
                <tr>
                  <th>Cliente</th>
                  <th>Score</th>
                  <th>Última compra</th>
                  <th>Sinal principal</th>
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
                    <td>{formatDateOnly(c.last_purchase)}</td>
                    <td>{buildChurnSignal(c)}</td>
                    <td>{c.compras_30d}</td>
                    <td>{c.compras_60_30}</td>
                    <td>{formatCurrency(c.faturamento_30d)}</td>
                    <td>{formatCurrency(c.faturamento_60_30)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-7 chartCard">
            <h2>Top clientes por faturamento</h2>
            {!loading && !topChart.length ? (
              <EmptyState title="Sem clientes identificados com faturamento." detail="A filial não trouxe clientes nomeados para este recorte." />
            ) : null}
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topChart}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="cliente" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" tickFormatter={formatCurrency} width={112} />
                  <Tooltip formatter={(value: any) => formatCurrency(value)} />
                  <Bar dataKey="faturamento" fill="#818cf8" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-5">
            <h2>Top clientes</h2>
            {!loading && !(data?.top_customers || []).length ? (
              <EmptyState title="Sem top clientes no período." detail="Não houve base identificada suficiente para ranqueamento." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Cliente</th><th>Compras</th><th>Ticket</th></tr></thead>
              <tbody>
                {(data?.top_customers || []).slice(0, 10).map((c: any) => (
                  <tr key={c.id_cliente}><td>{c.cliente_nome}</td><td>{c.compras}</td><td>{formatCurrency(c.ticket_medio)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Radar de recorrência anônima</h2>
            <div className="muted" style={{ marginBottom: 8 }}>
              {loading ? '...' : anonKpis?.recommendation || 'Sem leitura adicional para o período.'}
            </div>
            {!loading && !(anon?.breakdown_dow || []).length ? (
              <EmptyState
                title="Sem leitura anônima suficiente neste recorte."
                detail="A integração ainda não trouxe volume confiável para comparar recorrência sem identificação nominal."
              />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Dia da semana</th><th>Atual</th><th>Período anterior</th><th>Tendência</th></tr></thead>
              <tbody>
                {(anon?.breakdown_dow || []).map((r: any) => (
                  <tr key={r.dow}>
                    <td>{r.dow}</td>
                    <td>{formatCurrency(r.anon_current)}</td>
                    <td>{formatCurrency(r.anon_prev)}</td>
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
