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

export default function FraudPage() {
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

        const res = await apiGet(`/bi/fraud/overview?${qs.toString()}`);
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
      <AppNav title="Sistema Anti-Fraude" userLabel={userLabel} />

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
          <h2>KPIs de cancelamentos</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <div className="row">
              <div className="kpi">
                <div className="label">Cancelamentos</div>
                <div className="value">{Number(data?.kpis?.cancelamentos || 0)}</div>
              </div>
              <div className="kpi">
                <div className="label">Valor cancelado</div>
                <div className="value">{fmtMoney(data?.kpis?.valor_cancelado)}</div>
              </div>
            </div>
          )}
          <div className="muted">
            Dica: configure o Telegram para receber alerta em tempo real (tabela <code>app.user_notification_settings</code>).
          </div>
        </div>

        <div className="card">
          <h2>Cancelamentos por dia</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Filial</th>
                  <th>Cancelamentos</th>
                  <th>Valor</th>
                </tr>
              </thead>
              <tbody>
                {(data?.by_day || []).map((r: any) => (
                  <tr key={`${r.data_key}-${r.id_filial}`}
                    >
                    <td>{String(r.data_key)}</td>
                    <td>{r.id_filial}</td>
                    <td>{r.cancelamentos}</td>
                    <td>{fmtMoney(r.valor_cancelado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Top usuários por cancelamento</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Usuário</th>
                  <th>Cancelamentos</th>
                  <th>Valor cancelado</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_users || []).map((u: any) => (
                  <tr key={u.id_usuario}>
                    <td>{u.id_usuario ?? '(Sem usuário)'}</td>
                    <td>{u.cancelamentos}</td>
                    <td>{fmtMoney(u.valor_cancelado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Últimos eventos cancelados</h2>
          {loading ? (
            <p>Carregando...</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Data/hora</th>
                  <th>Filial</th>
                  <th>DB</th>
                  <th>Comprovante</th>
                  <th>Usuário</th>
                  <th>Turno</th>
                  <th>Valor</th>
                </tr>
              </thead>
              <tbody>
                {(data?.last_events || []).map((e: any) => (
                  <tr key={`${e.id_db}-${e.id_comprovante}`}
                    >
                    <td>{e.data || '(sem data)'}</td>
                    <td>{e.id_filial}</td>
                    <td>{e.id_db}</td>
                    <td>{e.id_comprovante}</td>
                    <td>{e.id_usuario ?? '?'}</td>
                    <td>{e.id_turno ?? '?'}</td>
                    <td>{fmtMoney(e.valor_total)}</td>
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
