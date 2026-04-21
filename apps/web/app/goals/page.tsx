'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import ScopeTransitionState from '../components/ui/ScopeTransitionState';
import { buildUserLabel, formatCurrency } from '../lib/format';
import { buildGoalsMotivation, getSellerBadge } from '../lib/goals-motivation';
import { buildModuleLoadingCopy, buildModuleUnavailableCopy } from '../lib/reading-state.mjs';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { useBiScopeData } from '../lib/use-bi-scope-data';
import { apiPost } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildProductHref, createScopeEpoch } from '../lib/product-scope.mjs';
import { formatGoalTargetInputFromNumber, normalizeGoalTargetInput, parseGoalTargetInput } from '../lib/goal-target-input.mjs';
import { startScopeTransition } from '../lib/scope-runtime';

export const dynamic = 'force-dynamic';

function buildRiskStatus(score: number) {
  if (score >= 80) return { label: 'Atenção operacional', className: 'warn' };
  if (score >= 60) return { label: 'Monitorar rotina', className: 'info' };
  return { label: 'Operação estável', className: 'ok' };
}

export default function GoalsPage() {
  const scope = useScopeQuery();
  const { claims, data, error, loading, pendingUnavailable } = useBiScopeData<any>({
    moduleKey: 'goals_overview',
    scope,
    errorMessage: 'Falha ao carregar metas',
    buildRequestUrl: (currentScope) => `/bi/goals/overview?${buildScopeParams(currentScope).toString()}`,
  });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy('metas e equipe')
    : buildModuleLoadingCopy('metas e equipe');
  const [showMargin, setShowMargin] = useState(false);
  const router = useRouter();
  const [metaDraft, setMetaDraft] = useState('');
  const [metaSaving, setMetaSaving] = useState(false);
  const [metaMessage, setMetaMessage] = useState('');
  const [metaError, setMetaError] = useState('');
  const singleBranchId = scope.id_filial || (scope.id_filiais.length === 1 ? scope.id_filiais[0] : null);
  const metaEditable = Boolean(singleBranchId);

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);

  const leaderboard = useMemo(
    () =>
      (data?.leaderboard || [])
        .filter((r: any) => String(r?.funcionario_nome || '').trim() && String(r?.funcionario_nome || '').toLowerCase() !== 'sem funcionário')
        .map((r: any, index: number) => ({
          ...r,
          rank: index + 1,
          faturamento: Number(r.faturamento || 0),
          margem: Number(r.margem || 0),
          vendas: Number(r.vendas || 0),
          scoreRisco: Number((data?.risk_top_employees || []).find((x: any) => x.id_funcionario === r.id_funcionario)?.score_medio || 0),
        })),
    [data]
  );
  const podium = useMemo(() => leaderboard.slice(0, 5), [leaderboard]);
  const detailedLeaderboard = useMemo(() => leaderboard.slice(0, 15), [leaderboard]);
  const motivation = useMemo(() => buildGoalsMotivation(detailedLeaderboard), [detailedLeaderboard]);
  const projection = data?.monthly_projection || {};
  const projectionSummary = projection.summary || {};
  const projectionGoal = projection.goal || {};
  const projectionHistory = projection.history || {};
  const projectionForecast = projection.forecast || {};
  const teamGoals = data?.goals_today || [];
  const totalTeamGoalValue = teamGoals.reduce((value, goal) => value + Number(goal.target_value || 0), 0);
  const metaGoalMonth = projectionGoal.goal_month || projection.month_ref || null;
  const businessClock = data?.business_clock || {};
  const branchLabel = scope.id_filial
    ? `Filial ${scope.id_filial}`
    : scope.id_filiais.length
      ? `${scope.id_filiais.length} filiais selecionadas`
      : 'Todas as filiais';
  const projectionStatusLabel = useMemo(() => {
    const status = String(projection.status || '');
    if (status === 'above_goal') return 'Acima da meta';
    if (status === 'below_goal') return 'Abaixo da meta';
    if (status === 'above_history') return 'Acima da média recente';
    return 'Em acompanhamento';
  }, [projection.status]);

  useEffect(() => {
    if (Number(projectionGoal.target_value || 0) > 0) {
      setMetaDraft(formatGoalTargetInputFromNumber(projectionGoal.target_value));
    } else {
      setMetaDraft('');
    }
  }, [projectionGoal.target_value]);

  const handleMetaSave = async () => {
    if (!metaEditable || !singleBranchId) {
      setMetaError('Selecione apenas uma filial exclusiva para editar a meta.');
      return;
    }
    const parsedValue = parseGoalTargetInput(metaDraft);
    if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
      setMetaError('Informe um valor válido acima de zero.');
      return;
    }
    setMetaSaving(true);
    setMetaError('');
    setMetaMessage('');
    try {
      const params = new URLSearchParams();
      params.set('id_filial', singleBranchId);
      if (scope.id_empresa) {
        params.set('id_empresa', scope.id_empresa);
      }
      await apiPost(`/bi/goals/target?${params.toString()}`, {
        target_value: parsedValue,
        goal_month: metaGoalMonth || undefined,
        goal_type: 'FATURAMENTO',
      });
      setMetaMessage('Meta salva com sucesso. A projeção será atualizada.');
      const nextScope = { ...scope, scope_epoch: createScopeEpoch() };
      startScopeTransition(nextScope, 'goals_overview');
      router.replace(buildProductHref('/goals', nextScope));
    } catch (err: any) {
      setMetaError(extractApiError(err, 'Falha ao salvar meta'));
    } finally {
      setMetaSaving(false);
    }
  };

  return (
    <div>
      <AppNav title="Metas e Equipe" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? 'unavailable' : 'loading'}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={5}
              panels={3}
            />
          </div>
        ) : (
          <div className="bi-grid" style={{ marginTop: 12 }}>
            <div
              className="card col-12"
              style={{
                background:
                  'linear-gradient(135deg, rgba(10,18,35,0.98), rgba(8,43,52,0.98) 46%, rgba(17,82,61,0.98))',
                borderColor: 'rgba(110,231,255,0.18)',
              }}
            >
              <div className="panelHead">
                <div>
                  <h2 style={{ marginBottom: 4 }}>Top 5 Vendedores</h2>
                  <div className="muted">Ranking por vendas brutas, com leitura competitiva e margem protegida por padrão.</div>
                </div>
                <button className="btn" onClick={() => setShowMargin((current) => !current)}>
                  {showMargin ? 'Ocultar margem' : 'Mostrar margem'}
                </button>
              </div>

              {!loading && !podium.length ? (
                <EmptyState title="Sem vendedores ranqueados." detail="Não houve base identificada suficiente para montar o pódio da equipe." />
              ) : null}

              {podium.length ? (
                <>
                  <div
                    style={{
                      display: 'grid',
                      gridTemplateColumns: '1.5fr 1fr',
                      gap: 16,
                      marginTop: 18,
                      alignItems: 'stretch',
                    }}
                  >
                    <div
                      style={{
                        borderRadius: 24,
                        padding: '22px 24px',
                        background: 'linear-gradient(180deg, rgba(255,255,255,0.10), rgba(255,255,255,0.04))',
                        border: '1px solid rgba(255,255,255,0.10)',
                      }}
                    >
                      <div style={{ fontSize: 28, fontWeight: 900, lineHeight: 1.05, maxWidth: 780 }}>{motivation.headline}</div>
                      <div className="muted" style={{ marginTop: 10, fontSize: 15, maxWidth: 760 }}>
                        {motivation.subheadline}
                      </div>
                    </div>

                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
                        gap: 10,
                      }}
                    >
                      {motivation.indicators.map((item) => (
                        <div
                          key={item.label}
                          style={{
                            borderRadius: 18,
                            padding: '14px 16px',
                            background: 'rgba(7,18,31,0.42)',
                            border: '1px solid rgba(255,255,255,0.08)',
                          }}
                        >
                          <div className="muted" style={{ fontSize: 12 }}>{item.label}</div>
                          <div style={{ fontSize: 22, fontWeight: 800, marginTop: 4 }}>{item.value}</div>
                          <div className="muted" style={{ marginTop: 4, fontSize: 12 }}>{item.detail}</div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div
                    style={{
                      display: 'grid',
                      gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                      gap: 14,
                      marginTop: 18,
                      alignItems: 'end',
                    }}
                  >
                    {podium.map((row: any) => {
                      const heights: Record<number, number> = { 1: 220, 2: 190, 3: 176, 4: 150, 5: 140 };
                      const accent: Record<number, string> = {
                        1: '#fbbf24',
                        2: '#cbd5e1',
                        3: '#f59e0b',
                        4: '#67e8f9',
                        5: '#86efac',
                      };
                      const badge = getSellerBadge(row, podium);
                      return (
                        <div
                          key={row.id_funcionario}
                          style={{
                            minHeight: heights[row.rank] || 140,
                            borderRadius: 24,
                            padding: '18px 18px 16px',
                            background: `linear-gradient(180deg, ${accent[row.rank]}22, rgba(255,255,255,0.04))`,
                            border: `1px solid ${accent[row.rank]}55`,
                            boxShadow: row.rank === 1 ? '0 18px 50px rgba(251,191,36,0.16)' : '0 12px 32px rgba(15,23,42,0.24)',
                            display: 'flex',
                            flexDirection: 'column',
                            justifyContent: 'space-between',
                            gap: 10,
                          }}
                        >
                          <div>
                            <div
                              style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                width: 36,
                                height: 36,
                                borderRadius: 999,
                                background: accent[row.rank],
                                color: '#07121f',
                                fontWeight: 800,
                                marginBottom: 14,
                              }}
                            >
                              {row.rank}
                            </div>
                            <div style={{ fontSize: row.rank === 1 ? 22 : 18, fontWeight: 800, lineHeight: 1.1 }}>{row.funcionario_nome}</div>
                            <div className="muted" style={{ marginTop: 6 }}>
                              {row.vendas} venda(s) fechadas
                            </div>
                            <div
                              style={{
                                marginTop: 12,
                                display: 'inline-flex',
                                alignItems: 'center',
                                borderRadius: 999,
                                padding: '6px 10px',
                                fontSize: 12,
                                fontWeight: 700,
                                color: '#07121f',
                                background: badge.tone,
                              }}
                            >
                              {badge.label}
                            </div>
                          </div>
                          <div>
                            <div style={{ fontSize: row.rank === 1 ? 28 : 22, fontWeight: 900 }}>{formatCurrency(row.faturamento)}</div>
                            <div className="muted" style={{ marginTop: 6 }}>
                              {showMargin ? `Margem ${formatCurrency(row.margem)}` : 'Margem protegida'}
                            </div>
                            <div style={{ marginTop: 10 }}>
                              {(() => {
                                const riskStatus = buildRiskStatus(Number(row.scoreRisco || 0));
                                return <span className={`badge ${riskStatus.className}`}>{riskStatus.label}</span>;
                              })()}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </>
              ) : null}
            </div>
          <div className="card col-12">
            <div className="panelHead">
              <h2>Leaderboard detalhado</h2>
              <span className="muted">Até 15 nomes válidos para acompanhar a disputa completa da equipe.</span>
            </div>
            {!loading && !detailedLeaderboard.length ? (
              <EmptyState title="Sem leaderboard detalhado." detail="A fonte de desempenho por funcionário não retornou registros no período." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Pos.</th><th>Funcionário</th><th>Destaque</th><th>Vendas</th><th>Faturamento</th><th>Margem</th><th>Leitura operacional</th><th>Status</th></tr></thead>
              <tbody>
                {detailedLeaderboard.map((r: any) => {
                  const badge = getSellerBadge(r, detailedLeaderboard);
                  const riskStatus = buildRiskStatus(Number(r.scoreRisco || 0));
                  return (
                    <tr key={r.id_funcionario} style={r.rank <= 5 ? { background: 'rgba(251,191,36,0.06)' } : undefined}>
                      <td><strong>{r.rank}º</strong></td>
                      <td>{r.funcionario_nome}</td>
                      <td>
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            borderRadius: 999,
                            padding: '4px 10px',
                            fontSize: 12,
                            fontWeight: 700,
                            background: `${badge.tone}22`,
                            color: badge.tone,
                            border: `1px solid ${badge.tone}55`,
                          }}
                        >
                          {badge.label}
                        </span>
                      </td>
                      <td>{r.vendas}</td>
                      <td>{formatCurrency(r.faturamento)}</td>
                      <td>{showMargin ? formatCurrency(r.margem) : 'Oculta'}</td>
                      <td>{Number(r.scoreRisco || 0).toFixed(1)}</td>
                      <td>
                        <span className={`badge ${riskStatus.className}`}>{riskStatus.label}</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {!loading && detailedLeaderboard.length < 15 ? (
              <div className="muted" style={{ marginTop: 10 }}>
                Exibindo {detailedLeaderboard.length} vendedor(es) válidos neste recorte.
              </div>
            ) : null}
          </div>

          <div className="card col-12 teamIndicatorsCard">
            <div className="panelHead">
              <div>
                <h2>Indicadores da equipe</h2>
                <div className="muted">Foco em metas válidas para o recorte atual e insights por filial.</div>
              </div>
              <span className="badge bronze">{branchLabel}</span>
            </div>
            <div className="teamGoalsGrid">
              {teamGoals.length ? (
                teamGoals.map((goal: any) => (
                  <div key={`${goal.goal_type}-${goal.goal_date || goal.goal_month || goal.branch_goal_count}`} className="teamGoalsTile">
                    <div className="muted" style={{ fontSize: 12 }}>{goal.goal_type}</div>
                    <div className="value">{formatCurrency(goal.target_value)}</div>
                    <div className="muted" style={{ fontSize: 12 }}>{goal.branch_goal_count || 0} vínculo(s)</div>
                  </div>
                ))
              ) : (
                <div className="muted">Nenhum objetivo diário registrado para este escopo.</div>
              )}
            </div>
            <div className="muted" style={{ marginTop: 12 }}>
              Meta total configurada no período: {formatCurrency(totalTeamGoalValue)}
            </div>
          </div>

          <div className="card kpi col-3">
            <div className="label">Realizado MTD</div>
            <div className="value">{formatCurrency(projectionSummary.mtd_actual)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Projeção ajustada</div>
            <div className="value">{formatCurrency(projectionSummary.projection_adjusted)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Meta do mês</div>
            <div className="value">{projectionGoal.configured ? formatCurrency(projectionGoal.target_value) : 'Sem meta'}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Ritmo necessário por dia</div>
            <div className="value">{projectionGoal.configured ? formatCurrency(projectionGoal.required_daily_to_goal) : '-'}</div>
          </div>
          <div className="card col-12">
            <div className="panelHead">
              <div>
                <h2>Projeção de fechamento do mês</h2>
                <div className="muted">Modelo simples e auditável: realizado MTD, projeção linear e ajuste por dia da semana quando houver histórico suficiente.</div>
              </div>
              <span className={`badge ${projectionGoal.configured ? (projection.status === 'below_goal' ? 'warn' : 'ok') : 'info'}`}>{projectionStatusLabel}</span>
            </div>
            <div style={{ marginTop: 14, fontSize: 26, fontWeight: 900 }}>{projection.headline || 'A projeção mensal ainda está sendo preparada.'}</div>
            {projectionForecast.confidence_label ? (
              <div className="muted" style={{ marginTop: 8 }}>
                Confiança {projectionForecast.confidence_label.toLowerCase()}: {projectionForecast.confidence_reason}
              </div>
            ) : null}
            <div className="projectionMetaForm" style={{ marginTop: 18 }}>
              <div className="projectionMetaInput">
                <input
                  className="input"
                  type="text"
                  inputMode="numeric"
                  placeholder="R$ 0,00"
                  value={metaDraft}
                  onChange={(event) => {
                    setMetaDraft(normalizeGoalTargetInput(event.target.value));
                    setMetaError('');
                    setMetaMessage('');
                  }}
                  disabled={!metaEditable || metaSaving}
                />
                <button className="btn" type="button" onClick={handleMetaSave} disabled={!metaEditable || metaSaving}>
                  {metaSaving ? 'Salvando...' : 'Salvar meta editável'}
                </button>
              </div>
              <div className="muted" style={{ marginTop: 8 }}>
                {metaEditable
                  ? `Meta aplicada na filial ${singleBranchId}.`
                  : 'Selecione exclusivamente uma filial para editar a meta.'}
              </div>
              {!loading ? (
                <div className="muted" style={{ marginTop: 6 }}>
                  Data-base do negócio: {businessClock.business_date || scope.dt_fim || '-'} {businessClock.timezone ? `(${businessClock.timezone})` : ''}
                </div>
              ) : null}
              {metaError ? (
                <div className="muted" style={{ marginTop: 6, color: '#f87171' }}>
                  {metaError}
                </div>
              ) : null}
              {metaMessage ? (
                <div className="muted" style={{ marginTop: 6, color: '#34d399' }}>
                  {metaMessage}
                </div>
              ) : null}
            </div>
          </div>

          <div className="card col-6">
            <h2>Como a projeção foi calculada</h2>
            <div className="muted" style={{ marginTop: 8 }}>
              Base: faturamento acumulado do mês até agora dividido pelos dias corridos do mês observados. A projeção ajustada só altera o restante do mês quando já existe histórico suficiente para capturar diferença por dia da semana.
            </div>
            <div style={{ marginTop: 12, display: 'grid', gap: 8 }}>
              {(projection.drivers || []).map((driver: string) => (
                <div key={driver} className="muted">- {driver}</div>
              ))}
            </div>
          </div>

          <div className="card col-6">
            <h2>Comparativos executivos</h2>
            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
              <div className="muted">
                <strong>Gap para meta:</strong>{' '}
                {projectionGoal.configured ? formatCurrency(projectionGoal.gap_to_goal) : 'Meta mensal não configurada'}
              </div>
              <div className="muted">
                <strong>Variação vs meta:</strong>{' '}
                {projectionGoal.configured ? `${Number(projectionGoal.variation_pct || 0).toFixed(1)}%` : '-'}
              </div>
              <div className="muted">
                <strong>Média últimos 3 meses:</strong> {formatCurrency(projectionHistory.average_last_3_months)}
              </div>
              <div className="muted">
                <strong>Variação vs 3 meses:</strong> {projectionHistory.variation_vs_last_3m_pct !== null && projectionHistory.variation_vs_last_3m_pct !== undefined ? `${Number(projectionHistory.variation_vs_last_3m_pct || 0).toFixed(1)}%` : '-'}
              </div>
            </div>
          </div>

          <div className="card col-12">
            <h2>Meses fechados de referência</h2>
            {!loading && !(projectionHistory.last_3_months || []).length ? (
              <EmptyState title="Sem meses fechados suficientes." detail="Assim que houver base histórica consolidada, esta comparação passa a mostrar os 3 últimos fechamentos." />
            ) : null}
            <table className="table compact">
              <thead><tr><th>Mês</th><th>Faturamento fechado</th></tr></thead>
              <tbody>
                {(projectionHistory.last_3_months || []).map((item: any) => (
                  <tr key={item.month_ref}>
                    <td>{String(item.month_ref || '').slice(5, 7)}/{String(item.month_ref || '').slice(0, 4)}</td>
                    <td>{formatCurrency(item.faturamento)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {projectionHistory.average_basis_note ? (
              <div className="muted" style={{ marginTop: 10 }}>
                {projectionHistory.average_basis_note}
              </div>
            ) : null}
          </div>

          </div>
        )}
      </div>
    </div>
  );
}
