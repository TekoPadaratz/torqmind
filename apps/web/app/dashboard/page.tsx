'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

import { apiGet, apiPost } from '../lib/api';
import { requireAuth } from '../lib/auth';
import AppNav from '../components/AppNav';

function useScope() {
  const params = useSearchParams();
  const dt_ini = params.get('dt_ini') || '';
  const dt_fim = params.get('dt_fim') || '';
  const id_filial = params.get('id_filial');
  const id_empresa = params.get('id_empresa');
  return { dt_ini, dt_fim, id_filial, id_empresa };
}

export default function Dashboard() {
  const router = useRouter();
  const scope = useScope();

  const [claims, setClaims] = useState<any>(null);
  const [kpis, setKpis] = useState<any>(null);
  const [series, setSeries] = useState<any[]>([]);
  const [insights, setInsights] = useState<any[]>([]);
  const [briefing, setBriefing] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [etlMsg, setEtlMsg] = useState<string>('');

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    const role = claims.role;
    const emp = claims.id_empresa ? `E${claims.id_empresa}` : '';
    const fil = claims.id_filial ? `F${claims.id_filial}` : '';
    const parts = [role, emp, fil].filter(Boolean);
    return parts.join(' · ');
  }, [claims]);

  useEffect(() => {
    if (!requireAuth()) {
      router.push('/');
      return;
    }

    // Guard scope
    if (!scope.dt_ini || !scope.dt_fim) {
      router.push('/scope');
      return;
    }

    const load = async () => {
      setLoading(true);
      try {
        const me = await apiGet('/auth/me');
        setClaims(me);

        const qs = new URLSearchParams({
          dt_ini: scope.dt_ini,
          dt_fim: scope.dt_fim,
        });
        if (scope.id_filial) qs.set('id_filial', scope.id_filial);
        if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);

        const [k, s, ins, br] = await Promise.all([
          apiGet(`/dashboard/kpis?${qs.toString()}`),
          apiGet(`/dashboard/series?${qs.toString()}`),
          apiGet(`/dashboard/insights?${qs.toString()}`),
          apiGet(`/bi/jarvis/briefing?${new URLSearchParams({
            dt_ref: scope.dt_fim,
            ...(scope.id_filial ? { id_filial: scope.id_filial } : {}),
            ...(scope.id_empresa ? { id_empresa: scope.id_empresa } : {}),
          }).toString()}`),
        ]);

        setKpis(k);
        setSeries(s.points || []);
        setInsights(ins.points || []);
        setBriefing(br);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa]);

  const runEtl = async () => {
    try {
      setEtlMsg('Rodando ETL...');
      const qs = new URLSearchParams();
      qs.set('refresh_mart', 'true');
      if (scope.id_empresa) qs.set('id_empresa', scope.id_empresa);
      const res = await apiPost(`/etl/run?${qs.toString()}`, {});
      setEtlMsg(`ETL OK: ${JSON.stringify(res.meta || {})}`);
      // Reload after
      setTimeout(() => window.location.reload(), 700);
    } catch (e: any) {
      setEtlMsg(`ETL falhou: ${e?.message || e}`);
    }
  };

  const lastInsight = insights.length ? insights[insights.length - 1] : null;

  return (
    <div>
      <AppNav title="Dashboard Geral" userLabel={userLabel} />

      <div className="container">
        <div className="card">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
            <h2>Escopo</h2>
            <button className="btn" onClick={runEtl}>
              Atualizar dados (ETL)
            </button>
          </div>
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
            <div className="kpi">
              <div className="label">Empresa</div>
              <div className="value">{scope.id_empresa || claims?.id_empresa || '1'}</div>
            </div>
          </div>
          {etlMsg ? <p style={{ marginTop: 10, opacity: 0.9 }}>{etlMsg}</p> : null}
        </div>

        <div className="card">
          <h2>KPIs</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Faturamento</div>
                <div className="value">R$ {Number(kpis?.faturamento || 0).toFixed(2)}</div>
              </div>
              <div className="kpi">
                <div className="label">Margem</div>
                <div className="value">R$ {Number(kpis?.margem || 0).toFixed(2)}</div>
              </div>
              <div className="kpi">
                <div className="label">Ticket médio</div>
                <div className="value">R$ {Number(kpis?.ticket_medio || 0).toFixed(2)}</div>
              </div>
              <div className="kpi">
                <div className="label">Itens</div>
                <div className="value">{Number(kpis?.itens || 0)}</div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h2>Jarvis (briefing executivo)</h2>
          {loading ? (
            <p>Carregando briefing...</p>
          ) : briefing ? (
            <div>
              <div className="row">
                <div className="kpi">
                  <div className="label">Data ref.</div>
                  <div className="value">{briefing.data_ref}</div>
                </div>
                <div className="kpi">
                  <div className="label">Cancelamentos</div>
                  <div className="value">{briefing.kpis?.cancelamentos ?? 0}</div>
                </div>
              </div>
              <ul>
                {(briefing.bullets || []).map((b: string, idx: number) => (
                  <li key={idx} style={{ marginBottom: 8 }}>
                    {b}
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <p>Sem briefing para o período.</p>
          )}
        </div>

        <div className="card">
          <h2>Série diária</h2>
          <p style={{ opacity: 0.85 }}>
            (Esta versão do front ainda não desenha gráfico. Você já tem a série em JSON pronta para
            plugar em Recharts/ECharts.)
          </p>
          <div className="mono" style={{ maxHeight: 260, overflow: 'auto' }}>
            {JSON.stringify(series.slice(-30), null, 2)}
          </div>
        </div>

        <div className="card">
          <h2>Jarvis base consolidada</h2>
          {lastInsight ? (
            <div className="mono">{JSON.stringify(lastInsight, null, 2)}</div>
          ) : (
            <p>Sem insights no período.</p>
          )}
        </div>
      </div>
    </div>
  );
}
