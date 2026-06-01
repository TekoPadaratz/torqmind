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

function parseBrCurrency(input: string): number {
  const normalized = input.replace(/\./g, "").replace(",", ".").replace(/[^\d.]/g, "");
  const value = Number(normalized);
  return Number.isFinite(value) ? value : 0;
}

function formatBrCurrencyInput(raw: string): string {
  const digits = raw.replace(/\D/g, "");
  const cents = Number(digits || "0");
  return (cents / 100).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

interface CommissionsTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  referenceDate?: string | null;
}

export default function CommissionsTab({ idEmpresa, idFilial, referenceDate }: CommissionsTabProps) {
  const today = useMemo(() => new Date(), []);
  const parsedRef = useMemo(() => {
    if (!referenceDate) return today;
    const date = new Date(`${referenceDate}T00:00:00`);
    return Number.isNaN(date.getTime()) ? today : date;
  }, [referenceDate, today]);

  const [selectedMonth, setSelectedMonth] = useState(parsedRef.getMonth() + 1);
  const [selectedYear, setSelectedYear] = useState(parsedRef.getFullYear());

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [lossInput, setLossInput] = useState("0,00");

  useEffect(() => {
    setSelectedMonth(parsedRef.getMonth() + 1);
    setSelectedYear(parsedRef.getFullYear());
  }, [parsedRef]);

  const fetchResults = useCallback(async () => {
    if (!idFilial) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      params.set("month", String(selectedMonth));
      params.set("year", String(selectedYear));
      const resp = await apiGet(`/bi/team/commissions/results?${params.toString()}`);
      setData(resp);
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao carregar comissões.");
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial, selectedMonth, selectedYear]);

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

  const manager = data?.gerente || {};
  const managerGross = Number(manager?.comissao_bruta || 0);
  const managerLoss = parseBrCurrency(lossInput);
  const managerNet = Math.max(managerGross - managerLoss, 0);

  return (
    <div style={{ marginTop: 16 }}>
      <div className="card" style={{ padding: "12px 16px", fontSize: 13 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
          <div>
            Relatório individual de comissão ({String(selectedMonth).padStart(2, "0")}/{selectedYear})
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(Number(e.target.value))}
              style={{
                padding: "6px 10px",
                borderRadius: 6,
                border: "1px solid var(--border)",
                background: "var(--card-bg)",
                color: "inherit",
                fontSize: 12,
              }}
            >
              {Array.from({ length: 12 }, (_, i) => i + 1).map((m) => (
                <option key={m} value={m}>
                  {String(m).padStart(2, "0")}
                </option>
              ))}
            </select>
            <select
              value={selectedYear}
              onChange={(e) => setSelectedYear(Number(e.target.value))}
              style={{
                padding: "6px 10px",
                borderRadius: 6,
                border: "1px solid var(--border)",
                background: "var(--card-bg)",
                color: "inherit",
                fontSize: 12,
              }}
            >
              {Array.from({ length: 6 }, (_, i) => parsedRef.getFullYear() - 3 + i).map((y) => (
                <option key={y} value={y}>
                  {y}
                </option>
              ))}
            </select>
          </div>
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
              <div className="card" style={{ marginTop: 12 }}>
                <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 10 }}>Comissão de gerente</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10 }}>
                  <div>
                    <div className="muted" style={{ fontSize: 11 }}>Venda total (sem combustíveis)</div>
                    <div style={{ fontWeight: 700, fontSize: 18 }}>{formatCurrency(manager.venda_total_sem_combustiveis || 0)}</div>
                  </div>
                  <div>
                    <div className="muted" style={{ fontSize: 11 }}>Perdas no mês (informado pelo gerente)</div>
                    <input
                      type="text"
                      inputMode="numeric"
                      value={lossInput}
                      onChange={(e) => setLossInput(formatBrCurrencyInput(e.target.value))}
                      style={{
                        marginTop: 4,
                        width: "100%",
                        maxWidth: 200,
                        padding: "6px 8px",
                        borderRadius: 6,
                        border: "1px solid var(--border)",
                        background: "var(--card-bg)",
                        color: "inherit",
                      }}
                    />
                  </div>
                  <div>
                    <div className="muted" style={{ fontSize: 11 }}>Comissão líquida gerente</div>
                    <div style={{ fontWeight: 800, fontSize: 20, color: "#22c55e" }}>{formatCurrency(managerNet)}</div>
                    <div className="muted" style={{ fontSize: 11 }}>
                      Bruta {formatCurrency(managerGross)} {manager?.percentual_aplicado ? `(${manager.percentual_aplicado}%)` : ""}
                    </div>
                </div>
                </div>
              </div>

              {/* Employee Grid */}
              <div className="card" style={{ marginTop: 12 }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                  <h2 style={{ margin: 0, fontSize: 15 }}>Vendedores</h2>
                  <span className="muted" style={{ fontSize: 12 }}>
                    {data.vendedores_elegiveis} elegíveis
                  </span>
                </div>

                <div style={{ padding: "8px 12px", background: "rgba(34,197,94,0.08)", borderRadius: 8, marginBottom: 10, fontSize: 13 }}>
                  Comissão total de vendedores: <strong>{formatCurrency(data.comissao_total || 0)}</strong>
                </div>

                {(data.vendedores || []).length === 0 ? (
                  <EmptyState title="Sem vendedores" detail="Não há vendedores com identificação válida para o mês selecionado." />
                ) : (
                  <div style={{ overflowX: "auto" }}>
                    <table className="table compact">
                      <thead>
                        <tr>
                          <th>Funcionário</th>
                          <th>Venda</th>
                          <th>Nível</th>
                          <th>%</th>
                          <th>Comissão</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(data.vendedores || []).map((emp: any) => (
                          <tr key={emp.id_funcionario}>
                            <td style={{ fontWeight: 500 }}>{emp.nome_vendedor}</td>
                            <td>{formatCurrency(emp.venda_elegivel)}</td>
                            <td>
                              {emp.nivel_atingido ? (
                                <span style={{ color: TIER_STYLES[emp.nivel_atingido.tier_key]?.color }}>
                                  {TIER_STYLES[emp.nivel_atingido.tier_key]?.icon} {emp.nivel_atingido.tier_name}
                                </span>
                              ) : (
                                <span className="muted">Sem nível</span>
                              )}
                            </td>
                            <td>{Number(emp.percentual_aplicado || 0).toFixed(2)}%</td>
                            <td style={{ fontWeight: 700, color: "#22c55e" }}>{formatCurrency(emp.comissao_estimada || 0)}</td>
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
