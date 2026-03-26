'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis, Line, LineChart } from 'recharts';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateKeyShort,
  formatDateTime,
  formatFilialLabel,
  formatHoursLabel,
  formatTurnoLabel,
} from '../lib/format';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { loadSession, readCachedSession } from '../lib/session';

export const dynamic = 'force-dynamic';

function operationalSourceLabel(source: string) {
  const normalized = String(source || '').toLowerCase();
  if (normalized === 'turno') return 'Resolvido pelo turno';
  if (normalized === 'comprovante') return 'Fallback do comprovante';
  return 'Sem resolução';
}

export default function FraudPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

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

        const res = await apiGet(`/bi/fraud/overview?${buildScopeParams(scope).toString()}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar fraude'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filiais_key, scope.id_empresa, scope.ready]);

  const byDay = useMemo(
    () => (data?.by_day || []).map((r: any) => ({ ...r, data: formatDateKeyShort(r.data_key), cancelamentos: Number(r.cancelamentos || 0) })),
    [data]
  );
  const riskByDay = useMemo(
    () =>
      (data?.risk_by_day || []).map((r: any) => ({
        ...r,
        data: formatDateKeyShort(r.data_key),
        eventos_alto_risco: Number(r.eventos_alto_risco || 0),
        impacto_estimado_total: Number(r.impacto_estimado_total || 0),
      })),
    [data]
  );

  const definitions = data?.definitions || {};
  const maxRiskDateKey = Number(data?.risk_window?.max_data_key || 0);
  const maxRiskDate = formatDateKey(maxRiskDateKey);
  const scopeEndKey = Number(String(scope.dt_fim || '').replaceAll('-', ''));
  const scopeOutdatedForRisk =
    maxRiskDateKey > 0 &&
    scopeEndKey > 0 &&
    scopeEndKey < maxRiskDateKey &&
    Number(data?.risk_kpis?.total_eventos || 0) === 0;

  const openCash = data?.open_cash || {};
  const topOperationalUser = (data?.top_users || [])[0];
  const latestOperationalEvent = (data?.last_events || [])[0];
  const topEmployee = (data?.risk_top_employees || [])[0];
  const topTurn = (data?.risk_by_turn_local || [])[0];
  const topModeledEvent = (data?.risk_last_events || [])[0];

  return (
    <div>
      <AppNav title="Sistema Anti-Fraude" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">
            Central antifraude com duas leituras complementares: cancelamentos operacionais reais por operador de caixa e risco modelado para priorização investigativa.
          </div>
          {!loading ? (
            <div className="muted" style={{ marginTop: 10, display: 'grid', gap: 8 }}>
              <div><strong>Cancelamentos operacionais:</strong> {definitions.operational_cancelamentos || 'Comprovantes cancelados com CFOP acima de 5000 vinculados ao turno.'}</div>
              <div><strong>Operador exibido:</strong> {definitions.cashier_operator || 'O operador é resolvido pelo turno do caixa e não pelo frentista da venda.'}</div>
              <div><strong>Eventos de alto risco:</strong> {definitions.high_risk_events || 'Camada modelada que aponta padrões fora da curva.'}</div>
              <div><strong>Impacto estimado:</strong> {definitions.estimated_impact || 'Estimativa financeira para priorização, não perda confirmada.'}</div>
            </div>
          ) : null}
        </div>

        {error ? <div className="card errorCard">{error}</div> : null}
        {scopeOutdatedForRisk ? (
          <div className="card" style={{ marginTop: 12, borderColor: '#f59e0b' }}>
            <strong>Período fora da janela de risco modelado.</strong> O antifraude modelado tem dados até <strong>{maxRiskDate}</strong>. Os cancelamentos operacionais continuam válidos no período selecionado.
          </div>
        ) : null}

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-4"><div className="label">Cancelamentos operacionais</div><div className="value">{loading ? '...' : Number(data?.kpis?.cancelamentos || 0)}</div></div>
          <div className="card kpi col-4"><div className="label">Valor cancelado</div><div className="value">{loading ? '...' : formatCurrency(data?.kpis?.valor_cancelado)}</div></div>
          <div className="card kpi col-4"><div className="label">Caixas abertos agora</div><div className="value">{loading ? '...' : Number(openCash?.total_open || 0)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Impacto estimado</div><div className="value">{loading ? '...' : formatCurrency(data?.risk_kpis?.impacto_total)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Eventos alto risco</div><div className="value">{loading ? '...' : Number(data?.risk_kpis?.eventos_alto_risco || 0)}</div></div>
          <div className="card kpi col-4 riskCard"><div className="label">Score médio</div><div className="value">{loading ? '...' : Number(data?.risk_kpis?.score_medio || 0).toFixed(1)}</div></div>

          <div className="card col-4">
            <h2>Operador de caixa mais exposto</h2>
            {topOperationalUser ? (
              <>
                <div style={{ fontSize: 20, fontWeight: 800, marginTop: 8 }}>{topOperationalUser.usuario_label}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  {topOperationalUser.cancelamentos} cancelamento(s), sendo {topOperationalUser.resolvidos_por_turno} resolvido(s) diretamente pelo turno.
                </div>
                <div style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}>{formatCurrency(topOperationalUser.valor_cancelado)}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  {topOperationalUser.fallback_comprovante > 0
                    ? `${topOperationalUser.fallback_comprovante} evento(s) ainda dependem do fallback do comprovante.`
                    : 'Todos os eventos deste operador foram resolvidos pela verdade do turno.'}
                </div>
              </>
            ) : (
              <EmptyState title="Sem operador destacado." detail="O período não trouxe cancelamentos operacionais com concentração material." />
            )}
          </div>

          <div className="card col-4">
            <h2>Último cancelamento operacional</h2>
            {latestOperationalEvent ? (
              <>
                <div style={{ fontSize: 18, fontWeight: 800, marginTop: 8 }}>{latestOperationalEvent.usuario_label}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  {formatFilialLabel(latestOperationalEvent.id_filial, latestOperationalEvent.filial_nome)} · {formatDateTime(latestOperationalEvent.data)}
                </div>
                <div style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}>{formatCurrency(latestOperationalEvent.valor_total)}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  {operationalSourceLabel(latestOperationalEvent.usuario_source)} · {formatTurnoLabel(latestOperationalEvent.id_turno)}
                </div>
              </>
            ) : (
              <EmptyState title="Sem cancelamento recente." detail="Não houve cancelamento operacional no recorte atual." />
            )}
          </div>

          <div className="card col-4">
            <h2>Como ler o risco modelado</h2>
            <div style={{ marginTop: 12, fontSize: 22, fontWeight: 800 }}>{loading ? '...' : `${Number(data?.risk_kpis?.eventos_alto_risco || 0)} eventos`}</div>
            {!loading ? (
              <div className="muted" style={{ marginTop: 8 }}>
                Eventos de alto risco sinalizam padrões fora do comportamento normal. Impacto estimado serve para priorizar investigação e não substitui a validação operacional do cliente.
              </div>
            ) : null}
          </div>

          <div className="card col-12">
            <h2>Monitor de turnos</h2>
            {loading ? null : (
              <>
                <div className="muted" style={{ marginBottom: 8 }}>{openCash.summary || 'Monitoramento operacional indisponível.'}</div>
                {openCash.source_status === 'ok' && openCash.items?.length ? (
                  <table className="table compact">
                    <thead><tr><th>Filial</th><th>Turno</th><th>Operador</th><th>Horas aberto</th><th>Severidade</th></tr></thead>
                    <tbody>
                      {openCash.items.map((item: any) => (
                        <tr key={`${item.id_filial}-${item.id_turno}`}>
                          <td>{formatFilialLabel(item.id_filial, item.filial_nome)}</td>
                          <td>{formatTurnoLabel(item.id_turno)}</td>
                          <td>{item.usuario_label || item.usuario_nome || 'Operador não identificado'}</td>
                          <td>{formatHoursLabel(item.horas_aberto)}</td>
                          <td>{item.status_label || item.severity}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState
                    title={
                      openCash.source_status === 'unavailable'
                        ? 'Monitor de turnos em integração.'
                        : openCash.source_status === 'unmapped'
                          ? 'Fonte operacional ainda não mapeada.'
                          : 'Nenhuma ocorrência relevante.'
                    }
                    detail={openCash.summary || 'Assim que a base operacional estiver pronta, esta leitura passa a destacar turnos abertos e antigos automaticamente.'}
                  />
                )}
              </>
            )}
          </div>

          <div className="card col-6 chartCard">
            <h2>Cancelamentos por dia</h2>
            <div className="muted" style={{ marginBottom: 8 }}>
              Série operacional de cancelamentos reconciliados por turno, usando a mesma base semântica do Caixa.
            </div>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={byDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip />
                  <Bar dataKey="cancelamentos" fill="#f97316" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-6">
            <h2>Operadores de caixa com mais cancelamentos</h2>
            {!loading && !(data?.top_users || []).length ? (
              <EmptyState title="Sem operadores destacados." detail="Não houve concentração operacional relevante por operador de caixa." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Operador</th><th>Cancelamentos</th><th>Valor</th><th>Fallback</th></tr></thead>
              <tbody>
                {(data?.top_users || []).slice(0, 10).map((u: any) => (
                  <tr key={`${u.id_usuario}-${u.usuario_label}`}>
                    <td>{u.usuario_label}</td>
                    <td>{Number(u.cancelamentos || 0)}</td>
                    <td>{formatCurrency(u.valor_cancelado)}</td>
                    <td>{Number(u.fallback_comprovante || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Últimos cancelamentos operacionais</h2>
            {!loading && !(data?.last_events || []).length ? (
              <EmptyState title="Sem eventos operacionais recentes." detail="Não houve cancelamentos reconciliados por turno no recorte analisado." />
            ) : null}
            <table className="table compact">
              <thead>
                <tr><th>Data</th><th>Filial</th><th>Turno</th><th>Operador</th><th>Origem da resolução</th><th>Valor</th></tr>
              </thead>
              <tbody>
                {(data?.last_events || []).slice(0, 20).map((e: any) => (
                  <tr key={`${e.id_filial}-${e.id_db}-${e.id_comprovante}`}>
                    <td>{formatDateTime(e.data)}</td>
                    <td>{e.filial_label || formatFilialLabel(e.id_filial, e.filial_nome)}</td>
                    <td>{formatTurnoLabel(e.id_turno)}</td>
                    <td>{e.usuario_label}</td>
                    <td>{operationalSourceLabel(e.usuario_source)}</td>
                    <td>{formatCurrency(e.valor_total)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-4">
            <h2>Maior foco do risco modelado</h2>
            {topTurn ? (
              <>
                <div style={{ fontSize: 20, fontWeight: 800, marginTop: 8 }}>{topTurn.filial_label}</div>
                <div className="muted" style={{ marginTop: 6 }}>{topTurn.turno_label} · {topTurn.local_label}</div>
                <div style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}>{formatCurrency(topTurn.impacto_estimado)}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  {topTurn.eventos} evento(s), sendo {topTurn.alto_risco} de alto risco.
                </div>
              </>
            ) : (
              <EmptyState title="Sem foco crítico destacado." detail="O período não trouxe concentração relevante por turno e canal no modelo de risco." />
            )}
          </div>

          <div className="card col-4">
            <h2>Colaborador mais exposto no modelo</h2>
            {topEmployee ? (
              <>
                <div style={{ fontSize: 20, fontWeight: 800, marginTop: 8 }}>{topEmployee.funcionario_nome}</div>
                <div className="muted" style={{ marginTop: 6 }}>{topEmployee.eventos} evento(s) modelados</div>
                <div style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}>{formatCurrency(topEmployee.impacto_estimado)}</div>
                <div className="muted" style={{ marginTop: 6 }}>
                  Score médio de {Number(topEmployee.score_medio || 0).toFixed(1)} no período.
                </div>
              </>
            ) : (
              <EmptyState title="Sem colaborador exposto." detail="Nenhum colaborador ultrapassou o limiar do modelo neste recorte." />
            )}
          </div>

          <div className="card col-4">
            <h2>Ponto crítico mais recente</h2>
            {topModeledEvent ? (
              <>
                <div style={{ fontSize: 18, fontWeight: 800, marginTop: 8 }}>{topModeledEvent.event_label}</div>
                <div className="muted" style={{ marginTop: 6 }}>{topModeledEvent.filial_label} · {formatDateTime(topModeledEvent.data)}</div>
                <div style={{ marginTop: 12, fontSize: 24, fontWeight: 900 }}>{formatCurrency(topModeledEvent.impacto_estimado)}</div>
                <div className="muted" style={{ marginTop: 6 }}>{topModeledEvent.reason_summary}</div>
              </>
            ) : (
              <EmptyState title="Sem ponto crítico recente." detail="Não houve evento de risco modelado com destaque imediato neste recorte." />
            )}
          </div>

          <div className="card col-12">
            <h2>Risco por turno e canal</h2>
            {!loading && !(data?.risk_by_turn_local || []).length ? (
              <EmptyState title="Sem risco concentrado por turno e canal." detail="Nenhum agrupamento relevante foi encontrado no período." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Filial</th><th>Turno</th><th>Canal</th><th>Eventos</th><th>Alto risco</th><th>Impacto</th><th>Score médio</th></tr></thead>
              <tbody>
                {(data?.risk_by_turn_local || []).slice(0, 10).map((r: any, idx: number) => (
                  <tr key={`${r.id_turno}-${r.id_local_venda}-${idx}`}>
                    <td>{r.filial_label || formatFilialLabel(r.id_filial, r.filial_nome)}</td>
                    <td>{r.turno_label || formatTurnoLabel(r.id_turno)}</td>
                    <td>{r.local_label || 'Canal não informado'}</td>
                    <td>{r.eventos}</td>
                    <td>{r.alto_risco}</td>
                    <td>{formatCurrency(r.impacto_estimado)}</td>
                    <td>{Number(r.score_medio || 0).toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-4">
            <h2>Colaboradores com maior exposição</h2>
            {!loading && !(data?.risk_top_employees || []).length ? (
              <EmptyState title="Sem colaboradores com risco relevante." detail="Nenhum colaborador ultrapassou o limiar no período." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Colaborador</th><th>Score</th><th>Impacto</th></tr></thead>
              <tbody>
                {(data?.risk_top_employees || []).slice(0, 10).map((u: any) => (
                  <tr key={u.id_funcionario}>
                    <td>{u.funcionario_nome}</td>
                    <td>{Number(u.score_medio || 0).toFixed(1)}</td>
                    <td>{formatCurrency(u.impacto_estimado)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-8 chartCard">
            <h2>Série temporal de alto risco</h2>
            <div className="muted" style={{ marginBottom: 8 }}>
              O gráfico abaixo mostra a camada modelada. Use junto com os cancelamentos operacionais acima, nunca como substituto da verdade do caixa.
            </div>
            <div className="chartWrap">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={riskByDay}>
                  <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                  <XAxis dataKey="data" stroke="#9fb0d0" />
                  <YAxis stroke="#9fb0d0" />
                  <Tooltip
                    formatter={(value: any, _name: any, item: any) =>
                      item?.dataKey === 'impacto_estimado_total'
                        ? formatCurrency(value)
                        : Number(value || 0)
                    }
                  />
                  <Line type="monotone" dataKey="eventos_alto_risco" stroke="#ef4444" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="impacto_estimado_total" stroke="#f59e0b" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="card col-12">
            <h2>Últimos eventos de risco modelado</h2>
            {!loading && !(data?.risk_last_events || []).length ? (
              <EmptyState title="Sem eventos recentes de risco." detail="Não houve ocorrências registradas na janela analisada." />
            ) : null}
            <table className="table compact">
              <thead>
                <tr><th>Data</th><th>Filial</th><th>Evento</th><th>Responsável</th><th>Score</th><th>Valor</th><th>Impacto</th><th>Leitura executiva</th></tr>
              </thead>
              <tbody>
                {(data?.risk_last_events || []).slice(0, 20).map((e: any) => (
                  <tr key={e.id}>
                    <td>{formatDateTime(e.data)}</td>
                    <td>{e.filial_label || formatFilialLabel(e.id_filial, e.filial_nome)}</td>
                    <td>{e.event_label || e.event_type}</td>
                    <td>{e.responsavel_label || e.operador_caixa_label || e.funcionario_label || '-'}</td>
                    <td>{e.score_risco}</td>
                    <td>{formatCurrency(e.valor_total)}</td>
                    <td>{formatCurrency(e.impacto_estimado)}</td>
                    <td>{e.reason_summary || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="card col-12">
            <h2>Anomalias de pagamento (top score)</h2>
            {!loading && !(data?.payments_risk || []).length ? (
              <p className="muted">Sem anomalias de pagamento no período.</p>
            ) : null}
            <table className="table compact">
              <thead>
                <tr><th>Data</th><th>Filial</th><th>Turno</th><th>Evento</th><th>Severidade</th><th>Score</th><th>Impacto</th></tr>
              </thead>
              <tbody>
                {(data?.payments_risk || []).slice(0, 12).map((e: any, idx: number) => (
                  <tr key={`${e.insight_id || e.event_type}-${idx}`}>
                    <td>{formatDateKey(e.data_key)}</td>
                    <td>{e.filial_label || formatFilialLabel(e.id_filial, e.filial_nome)}</td>
                    <td>{formatTurnoLabel(e.id_turno)}</td>
                    <td>{e.event_label || e.event_type}</td>
                    <td>{e.severity}</td>
                    <td>{Number(e.score || 0)}</td>
                    <td>{formatCurrency(e.impacto_estimado)}</td>
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
