'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Area, AreaChart, Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatDateOnly,
  formatTurnoLabel,
} from '../lib/format';
import { buildScopeParams, useScopeQuery } from '../lib/scope';

export const dynamic = 'force-dynamic';

function financeCoverageLabel(aging: any) {
  const status = String(aging?.snapshot_status || 'missing');
  const effectiveDate = aging?.effective_dt_ref || aging?.dt_ref || aging?.requested_dt_ref;

  if (status === 'exact') {
    return `Snapshot executivo exato em ${formatDateOnly(effectiveDate)}.`;
  }
  if (status === 'best_effort') {
    return `Snapshot executivo best-effort em ${formatDateOnly(effectiveDate)} para a data-base solicitada.`;
  }
  if (status === 'operational') {
    return `Leitura operacional calculada em ${formatDateOnly(effectiveDate)} a partir de títulos abertos do DW.`;
  }
  return `Sem snapshot executivo para ${formatDateOnly(aging?.requested_dt_ref)} e sem fallback operacional confiável.`;
}

export default function FinancePage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
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
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const me = await apiGet('/auth/me');
        setClaims(me);
        if (!scope.dt_ini || !scope.dt_fim) {
          router.replace(me?.home_path || '/dashboard');
          return;
        }

        const qs = buildScopeParams(scope).toString();
        const res = await apiGet(`/bi/finance/overview?${qs}&include_operational=false`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar financeiro'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filial, scope.id_empresa, scope.ready]);

  const chartData = useMemo(
    () =>
      (data?.by_day || []).map((r: any) => ({
        data: formatDateKeyShort(r.data_key),
        aberto: Number(r.valor_aberto || 0),
        pago: Number(r.valor_pago || 0),
      })),
    [data]
  );

  const hasFinance = useMemo(() => (data?.by_day || []).length > 0, [data]);
  const paymentsByDay = useMemo(
    () =>
      (data?.payments?.by_day || [])
        .filter((r: any) => Number(r.total_valor || 0) > 0)
        .map((r: any) => ({
        data: formatDateKeyShort(r.data_key),
        valor: Number(r.total_valor || 0),
        category: r.category,
      })),
    [data]
  );
  const paymentsByTurno = useMemo(
    () => (data?.payments?.by_turno || []).filter((r: any) => Number(r.total_valor || 0) > 0),
    [data]
  );
  const paymentsKpis = data?.payments?.kpis || {};
  const paymentsAnomalies = data?.payments?.anomalies || [];
  const aging = data?.aging || {};
  const receberVencido = Number(aging.receber_total_vencido || 0);
  const pagarVencido = Number(aging.pagar_total_vencido || 0);
  const cashPressure = receberVencido + pagarVencido;
  const snapshotStatus = String(aging.snapshot_status || 'missing');
  const top5Concentration = Number(aging.top5_concentration_pct || 0);
  const actionHeadline =
    receberVencido >= pagarVencido && receberVencido > 0
      ? 'Cobrar os recebíveis vencidos mais concentrados.'
      : pagarVencido > 0
        ? 'Renegociar compromissos vencidos para proteger o caixa.'
        : top5Concentration >= 45
          ? 'Desconcentrar a carteira antes que o risco aumente.'
          : 'Manter disciplina de cobrança e acompanhamento diário.';
  const actionDetail =
    receberVencido >= pagarVencido && receberVencido > 0
      ? 'Priorize os maiores títulos vencidos e a filial com mais caixa travado.'
      : pagarVencido > 0
        ? 'Reordene pagamentos, preserve caixa operacional e trate os maiores vencidos primeiro.'
        : top5Concentration >= 45
          ? 'Os maiores títulos já pesam demais na exposição atual e pedem ação preventiva.'
          : 'Sem ruptura material no recorte, mas o cockpit continua focado em pressão, atraso e concentração.';
  const paymentMixPreview = (paymentsKpis?.mix || [])
    .slice(0, 3)
    .map((item: any) => `${item.category_label || item.label || item.category}: ${formatCurrency(item.total_valor)}`)
    .join(' · ');
  const paymentsStatus = String(paymentsKpis?.source_status || 'unavailable');

  return (
    <div>
      <AppNav title="Financeiro" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Cockpit financeiro para decidir cobrança, renegociação, concentração e pressão imediata de caixa sem ruído operacional.</div>
          {!loading ? <div style={{ marginTop: 10, fontWeight: 700 }}>{financeCoverageLabel(aging)}</div> : null}
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card col-6">
            <h2>Ação prioritária</h2>
            <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>{loading ? '...' : actionHeadline}</div>
            {!loading ? <div className="muted" style={{ marginTop: 8 }}>{actionDetail}</div> : null}
          </div>

          <div className="card kpi col-3"><div className="label">Receber em aberto</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.receber_aberto)}</div></div>
          <div className="card kpi col-3 riskCard"><div className="label">Receber vencido</div><div className="value">{loading ? '...' : formatCurrency(receberVencido)}</div></div>
          <div className="card kpi col-3"><div className="label">Pagar em aberto</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.pagar_aberto)}</div></div>
          <div className="card kpi col-3 riskCard"><div className="label">Pagar vencido</div><div className="value">{loading ? '...' : formatCurrency(pagarVencido)}</div></div>

          <div className="card col-4">
            <h2>Pressão imediata de caixa</h2>
            <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>{loading ? '...' : formatCurrency(cashPressure)}</div>
            {!loading ? (
              <div className="muted" style={{ marginTop: 8 }}>
                {cashPressure > 0
                  ? 'Vencidos que já exigem cobrança, renegociação ou reordenação de pagamentos.'
                  : aging?.data_gaps
                    ? 'A leitura mostrada não encontrou exposição aberta relevante na referência efetiva.'
                    : 'Sem concentração crítica de vencidos no recorte analisado.'}
              </div>
            ) : null}
          </div>

          <div className="card col-4">
            <h2>Concentração da carteira</h2>
            <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>
              {loading ? '...' : `${top5Concentration.toFixed(1)}%`}
            </div>
            {!loading ? (
              <div className="muted" style={{ marginTop: 8 }}>
                {top5Concentration > 0
                  ? 'Os 5 maiores títulos concentram esse percentual da exposição atual.'
                  : 'A carteira segue distribuída, sem concentração material no topo.'}
              </div>
            ) : null}
          </div>

          <div className="card col-4">
            <h2>Leitura dos pagamentos</h2>
            <div style={{ marginTop: 12, fontSize: 28, fontWeight: 800 }}>{loading ? '...' : formatCurrency(paymentsKpis.total_valor)}</div>
            {!loading ? <div className="muted" style={{ marginTop: 8 }}>{paymentsKpis.summary || paymentMixPreview || 'Sem movimento financeiro conciliado no recorte.'}</div> : null}
          </div>

          <div className="card col-12 chartCard">
            <h2>Fluxo por vencimento</h2>
            {!loading && !hasFinance ? <p className="muted">Sem dados financeiros para o período selecionado.</p> : null}
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

          <div className="card kpi col-4">
            <div className="label">Variação dos pagamentos</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.delta_pct || 0).toFixed(1)}%`}</div>
            {!loading ? <div className="muted">Comparação com o período imediatamente anterior.</div> : null}
          </div>
          <div className="card kpi col-4" id="payment-mapping">
            <div className="label">Pagamentos não identificados</div>
            <div className="value">{loading ? '...' : `${Number(paymentsKpis.unknown_share_pct || 0).toFixed(1)}%`}</div>
            {!loading ? <div className="muted">Parcela ainda sem correspondência oficial de meio de pagamento no payload recebido.</div> : null}
          </div>

          <div className="card col-12 chartCard">
            <h2>Meios de pagamento por dia</h2>
            {!loading && paymentsStatus === 'value_gap' ? (
              <EmptyState
                title="Valores de pagamentos ainda em validação da carga."
                detail="Os registros da operação chegaram, mas a leitura monetária ainda não está estável o bastante para decisão."
              />
            ) : null}
            {!loading && paymentsStatus !== 'value_gap' && !paymentsByDay.length ? (
              <EmptyState title="Sem pagamentos recebidos no período." detail="A consolidação diária de pagamentos ainda não trouxe movimento para este recorte." />
            ) : null}
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={paymentsByDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="valor" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-7">
            <h2>Distribuição por forma e turno</h2>
            {!loading && paymentsStatus === 'value_gap' ? (
              <EmptyState
                title="A leitura por turno ainda não está confiável."
                detail="Os vínculos chegaram da operação, mas o valor monetário ainda está em correção no pipeline."
              />
            ) : null}
            {!loading && paymentsStatus !== 'value_gap' && !paymentsByTurno.length ? (
              <EmptyState title="Sem leitura por turno no período." detail="A fonte de pagamentos por turno ainda não retornou registros para o recorte selecionado." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Data</th><th>Turno</th><th>Forma</th><th>Valor</th><th>Comprovantes</th></tr></thead>
              <tbody>
                {paymentsByTurno.slice(0, 15).map((r: any, idx: number) => (
                  <tr key={`${r.data_key}-${r.id_turno}-${r.category}-${idx}`}>
                    <td>{formatDateKey(r.data_key)}</td>
                    <td>{r.turno_label || formatTurnoLabel(r.id_turno)}</td>
                    <td>{r.category_label || r.label || r.category}</td>
                    <td>{formatCurrency(r.total_valor)}</td>
                    <td>{Number(r.qtd_comprovantes || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-5">
            <h2>Sinais de pagamento fora do padrão</h2>
            {!loading && paymentsStatus === 'value_gap' ? (
              <EmptyState
                title="Motor de anomalias aguardando valor monetário confiável."
                detail="Enquanto a carga monetária de pagamentos estiver em correção, esta leitura fica em monitoramento técnico."
              />
            ) : null}
            {!loading && paymentsStatus !== 'value_gap' && !paymentsAnomalies.length ? (
              <EmptyState title="Sem anomalias relevantes no período." detail="A leitura de pagamentos seguiu estável neste recorte." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Evento</th><th>Severidade</th><th>Score</th><th>Impacto</th></tr></thead>
              <tbody>
                {paymentsAnomalies.slice(0, 10).map((a: any, idx: number) => (
                  <tr key={`${a.insight_id || a.event_type}-${idx}`}>
                    <td>{a.event_label || a.event_type}</td>
                    <td>{a.severity}</td>
                    <td>{Number(a.score || 0)}</td>
                    <td>{formatCurrency(a.impacto_estimado)}</td>
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
