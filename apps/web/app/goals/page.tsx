'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildUserLabel, formatCurrency } from '../lib/format';
import { buildGoalsMotivation, getSellerBadge } from '../lib/goals-motivation';
import { buildScopeParams, useScopeQuery } from '../lib/scope';
import { loadSession, readCachedSession } from '../lib/session';

export const dynamic = 'force-dynamic';

function buildRiskStatus(score: number) {
  if (score >= 80) return { label: 'Atenção operacional', className: 'warn' };
  if (score >= 60) return { label: 'Monitorar rotina', className: 'info' };
  return { label: 'Operação estável', className: 'ok' };
}

export default function GoalsPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showMargin, setShowMargin] = useState(false);

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

        const qs = buildScopeParams(scope).toString();
        const res = await apiGet(`/bi/goals/overview?${qs}`);
        setData(res);
      } catch (err: any) {
        setError(extractApiError(err, 'Falha ao carregar metas'));
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router, scope.dt_ini, scope.dt_fim, scope.id_filiais_key, scope.id_empresa, scope.ready]);

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

  return (
    <div>
      <AppNav title="Metas e Equipe" userLabel={userLabel} />
      <div className="container">
        <div className="card">
          <div className="muted">Tela de incentivo comercial com leitura segura para TV, sala de reunião e acompanhamento diário.</div>
        </div>
        {error ? <div className="card errorCard">{error}</div> : null}

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
        </div>
      </div>
    </div>
  );
}
