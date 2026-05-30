"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { apiGet } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";

// Tier styling
const TIER_STYLES: Record<string, { color: string; bg: string; icon: string }> = {
  bronze: { color: "#cd7f32", bg: "rgba(205,127,50,0.10)", icon: "🥉" },
  silver: { color: "#a0a0a0", bg: "rgba(160,160,160,0.10)", icon: "🥈" },
  gold: { color: "#d4a017", bg: "rgba(212,160,23,0.10)", icon: "🥇" },
  diamond: { color: "#4f9cf7", bg: "rgba(79,156,247,0.12)", icon: "💎" },
};

const MODE_LABELS: Record<string, string> = {
  team_total: "Geral / Equipe",
  equal_split: "Rateio igual por equipe",
  individual_sales: "Individual por vendedor",
};

interface CommissionsTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
}

export default function CommissionsTab({ idEmpresa, idFilial }: CommissionsTabProps) {
  const now = new Date();
  const [month, setMonth] = useState(now.getMonth() + 1);
  const [year, setYear] = useState(now.getFullYear());
  const [mode, setMode] = useState<string>("team_total");
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const fetchResults = useCallback(async () => {
    if (!idFilial) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      params.set("month", String(month));
      params.set("year", String(year));
      params.set("payment_mode", mode);
      const resp = await apiGet(`/bi/team/commissions/results?${params.toString()}`);
      setData(resp);
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao carregar comissões.");
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial, month, year, mode]);

  useEffect(() => {
    fetchResults();
  }, [fetchResults]);

  if (!idFilial) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState
          title="Selecione uma filial"
          detail="Escolha uma filial no painel lateral para visualizar as comissões."
        />
      </div>
    );
  }

  const tierProgress = data?.tier_progress || [];
  const currentTier = data?.nivel_atingido;
  const vendaElegivel = data?.venda_elegivel || 0;
  const maxTierAmount = tierProgress.length > 0
    ? Math.max(...tierProgress.map((t: any) => t.min_sales_amount))
    : 120000;
  const progressPercent = Math.min((vendaElegivel / (maxTierAmount * 1.1)) * 100, 100);

  const months = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
  ];

  return (
    <div style={{ marginTop: 16 }}>
      {/* Filters */}
      <div className="card" style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", padding: "12px 16px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <label style={{ fontWeight: 500, fontSize: 13 }}>Mês:</label>
          <select
            value={month}
            onChange={(e) => setMonth(Number(e.target.value))}
            style={{ padding: "4px 8px", borderRadius: 6, border: "1px solid var(--border)", background: "var(--card-bg)", color: "inherit" }}
          >
            {months.map((m, i) => (
              <option key={i} value={i + 1}>{m}</option>
            ))}
          </select>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <label style={{ fontWeight: 500, fontSize: 13 }}>Ano:</label>
          <select
            value={year}
            onChange={(e) => setYear(Number(e.target.value))}
            style={{ padding: "4px 8px", borderRadius: 6, border: "1px solid var(--border)", background: "var(--card-bg)", color: "inherit" }}
          >
            {[2024, 2025, 2026].map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <label style={{ fontWeight: 500, fontSize: 13 }}>Modo:</label>
          <select
            value={mode}
            onChange={(e) => setMode(e.target.value)}
            style={{ padding: "4px 8px", borderRadius: 6, border: "1px solid var(--border)", background: "var(--card-bg)", color: "inherit" }}
          >
            <option value="team_total">Geral / Equipe</option>
            <option value="equal_split">Rateio igual</option>
            <option value="individual_sales">Individual</option>
          </select>
        </div>
      </div>

      {error && <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div>}

      {loading ? (
        <div className="card" style={{ marginTop: 12, textAlign: "center", padding: 32 }}>
          <div className="muted">Calculando comissões...</div>
        </div>
      ) : data ? (
        <>
          {data.message && (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState title="Sem dados" detail={data.message} />
            </div>
          )}

          {!data.message && (
            <>
              {/* KPI Cards */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginTop: 12 }}>
                <div className="card" style={{ textAlign: "center", padding: "16px 12px" }}>
                  <div className="muted" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>Venda elegível</div>
                  <div style={{ fontSize: 22, fontWeight: 700, marginTop: 4 }}>{formatCurrency(vendaElegivel)}</div>
                </div>
                <div className="card" style={{ textAlign: "center", padding: "16px 12px" }}>
                  <div className="muted" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>Nível atingido</div>
                  <div style={{ fontSize: 22, fontWeight: 700, marginTop: 4, color: currentTier ? TIER_STYLES[currentTier.tier_key]?.color : undefined }}>
                    {currentTier ? `${TIER_STYLES[currentTier.tier_key]?.icon || ""} ${currentTier.tier_name}` : "Sem nível"}
                  </div>
                </div>
                <div className="card" style={{ textAlign: "center", padding: "16px 12px" }}>
                  <div className="muted" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>Comissão estimada</div>
                  <div style={{ fontSize: 22, fontWeight: 700, marginTop: 4, color: "#22c55e" }}>{formatCurrency(data.comissao_total)}</div>
                </div>
                <div className="card" style={{ textAlign: "center", padding: "16px 12px" }}>
                  <div className="muted" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>Percentual aplicado</div>
                  <div style={{ fontSize: 22, fontWeight: 700, marginTop: 4 }}>{data.percentual_aplicado}%</div>
                </div>
                {data.proximo_nivel && (
                  <div className="card" style={{ textAlign: "center", padding: "16px 12px" }}>
                    <div className="muted" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>Próximo nível</div>
                    <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4, color: TIER_STYLES[data.proximo_nivel.tier_key]?.color }}>
                      {TIER_STYLES[data.proximo_nivel.tier_key]?.icon} {data.proximo_nivel.tier_name}
                    </div>
                    <div className="muted" style={{ fontSize: 12, marginTop: 2 }}>
                      Faltam {formatCurrency(data.proximo_nivel.falta)}
                    </div>
                  </div>
                )}
              </div>

              {/* Progress Bar */}
              {tierProgress.length > 0 && (
                <div className="card" style={{ marginTop: 12, padding: "16px 20px" }}>
                  <div style={{ marginBottom: 8, fontSize: 13, fontWeight: 600 }}>Progresso por nível</div>
                  <div style={{ position: "relative", height: 28, background: "rgba(255,255,255,0.05)", borderRadius: 14, overflow: "hidden" }}>
                    <div
                      style={{
                        position: "absolute",
                        left: 0,
                        top: 0,
                        bottom: 0,
                        width: `${progressPercent}%`,
                        background: currentTier
                          ? `linear-gradient(90deg, ${TIER_STYLES[currentTier.tier_key]?.color}40, ${TIER_STYLES[currentTier.tier_key]?.color}90)`
                          : "rgba(100,100,100,0.3)",
                        borderRadius: 14,
                        transition: "width 0.5s ease",
                      }}
                    />
                    {tierProgress.map((tier: any) => {
                      const pos = (tier.min_sales_amount / (maxTierAmount * 1.1)) * 100;
                      return (
                        <div
                          key={tier.tier_key}
                          style={{
                            position: "absolute",
                            left: `${pos}%`,
                            top: 0,
                            bottom: 0,
                            width: 2,
                            background: tier.achieved ? TIER_STYLES[tier.tier_key]?.color : "rgba(255,255,255,0.2)",
                          }}
                          title={`${tier.tier_name}: ${formatCurrency(tier.min_sales_amount)}`}
                        />
                      );
                    })}
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 6 }}>
                    {tierProgress.map((tier: any) => (
                      <div key={tier.tier_key} style={{ fontSize: 10, color: tier.achieved ? TIER_STYLES[tier.tier_key]?.color : "var(--text-muted)", textAlign: "center" }}>
                        <div>{tier.tier_name}</div>
                        <div>{formatCurrency(tier.min_sales_amount)}</div>
                      </div>
                    ))}
                  </div>
                  {/* Motivational text */}
                  <div style={{ marginTop: 10, fontSize: 13, fontStyle: "italic", color: "var(--text-muted)" }}>
                    {!currentTier && data.proximo_nivel
                      ? `Faltam ${formatCurrency(data.proximo_nivel.falta)} para iniciar a comissão da equipe.`
                      : currentTier && data.proximo_nivel
                        ? `A equipe já atingiu ${currentTier.tier_name}. Faltam ${formatCurrency(data.proximo_nivel.falta)} para chegar em ${data.proximo_nivel.tier_name}.`
                        : currentTier && !data.proximo_nivel
                          ? `${currentTier.tier_name} atingido! A filial chegou ao maior nível configurado.`
                          : ""}
                  </div>
                </div>
              )}

              {/* Employee Grid */}
              <div className="card" style={{ marginTop: 12 }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                  <h2 style={{ margin: 0, fontSize: 15 }}>Vendedores</h2>
                  <span className="muted" style={{ fontSize: 12 }}>
                    Modo: {MODE_LABELS[mode] || mode} • {data.vendedores_elegiveis} elegíveis
                  </span>
                </div>

                {mode === "team_total" && data.comissao_total > 0 && (
                  <div style={{ padding: "8px 12px", background: "rgba(34,197,94,0.08)", borderRadius: 8, marginBottom: 10, fontSize: 13 }}>
                    Comissão total da equipe: <strong>{formatCurrency(data.comissao_total)}</strong>
                  </div>
                )}
                {mode === "equal_split" && (
                  <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
                    Rateio igual considera vendedores com venda elegível no mês.
                  </div>
                )}

                {(data.vendedores || []).length === 0 ? (
                  <EmptyState title="Sem vendedores" detail="Não há vendedores identificados nas vendas elegíveis deste mês." />
                ) : (
                  <div style={{ overflowX: "auto" }}>
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Vendedor</th>
                          <th>Venda elegível</th>
                          <th>Participação</th>
                          <th>Vendas</th>
                          {mode !== "team_total" && <th>Comissão estimada</th>}
                          <th>Obs.</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(data.vendedores || []).map((emp: any) => (
                          <tr key={emp.id_funcionario}>
                            <td style={{ fontWeight: 500 }}>
                              {emp.id_funcionario === -1 ? (
                                <span style={{ color: "var(--text-muted)", fontStyle: "italic" }}>Sem vendedor identificado</span>
                              ) : emp.nome_vendedor}
                            </td>
                            <td>{formatCurrency(emp.venda_elegivel)}</td>
                            <td>{emp.participacao}%</td>
                            <td>{emp.quantidade_vendas}</td>
                            {mode !== "team_total" && (
                              <td style={{ fontWeight: 600, color: "#22c55e" }}>
                                {emp.comissao_estimada != null ? formatCurrency(emp.comissao_estimada) : "—"}
                              </td>
                            )}
                            <td className="muted" style={{ fontSize: 11 }}>
                              {emp.id_funcionario === -1 ? "Venda sem vendedor" : ""}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {/* Groups breakdown */}
              {(data.grupos_configurados || []).length > 0 && (
                <div className="card" style={{ marginTop: 12 }}>
                  <h2 style={{ fontSize: 14, marginBottom: 8 }}>Grupos participantes</h2>
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Grupo</th>
                        <th>Venda no mês</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data.grupos_configurados || []).map((g: any) => (
                        <tr key={g.id_grupo_produto}>
                          <td>{g.nome}</td>
                          <td>{formatCurrency(g.venda_total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </>
      ) : null}
    </div>
  );
}
