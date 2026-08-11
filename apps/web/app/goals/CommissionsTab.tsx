"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { apiGet } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";
import { sortGridRows } from "../lib/grid-sort";
import ManagerCommissionGrid from "./ManagerCommissionGrid";

// Tier styling
const TIER_STYLES: Record<string, { color: string; bg: string; icon: string }> = {
  bronze: { color: "#cd7f32", bg: "rgba(205,127,50,0.10)", icon: "🥉" },
  silver: { color: "#a0a0a0", bg: "rgba(160,160,160,0.10)", icon: "🥈" },
  gold: { color: "#d4a017", bg: "rgba(212,160,23,0.10)", icon: "🥇" },
  diamond: { color: "#4f9cf7", bg: "rgba(79,156,247,0.12)", icon: "💎" },
};

// Commission payment modes (kept in sync with the API contract).
const PAYMENT_MODE_LABELS: Record<string, string> = {
  team_total: "Equipe (comissão total)",
  equal_split: "Divisão igual entre vendedores",
  individual_sales: "Individual por vendas",
  per_branch: "Padrão de cada filial",
};

interface CommissionsTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  idFiliais?: string[];
  referenceDate?: string | null;
}

export default function CommissionsTab({ idEmpresa, idFilial, idFiliais, referenceDate }: CommissionsTabProps) {
  const today = useMemo(() => new Date(), []);
  const parsedRef = useMemo(() => {
    if (!referenceDate) return today;
    const date = new Date(`${referenceDate}T00:00:00`);
    return Number.isNaN(date.getTime()) ? today : date;
  }, [referenceDate, today]);

  const [selectedMonth, setSelectedMonth] = useState(parsedRef.getMonth() + 1);
  const [selectedYear, setSelectedYear] = useState(parsedRef.getFullYear());
  const [paymentMode, setPaymentMode] = useState<string>("");

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const multiFiliais = useMemo(
    () => (idFiliais || []).map(String).filter((v) => v && v !== "0"),
    [idFiliais],
  );
  const isMulti = multiFiliais.length > 1 || (!idFilial && multiFiliais.length > 0);
  const hasScope = Boolean(idFilial) || multiFiliais.length > 0;

  useEffect(() => {
    setSelectedMonth(parsedRef.getMonth() + 1);
    setSelectedYear(parsedRef.getFullYear());
  }, [parsedRef]);

  const fetchResults = useCallback(async () => {
    if (!idFilial && multiFiliais.length === 0) {
      setData(null);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      params.set("month", String(selectedMonth));
      params.set("year", String(selectedYear));
      if (isMulti || (!idFilial && multiFiliais.length > 0)) {
        for (const f of multiFiliais) params.append("id_filiais", String(f));
      } else if (idFilial) {
        params.set("id_filial", String(idFilial));
      }
      // Modo de pagamento só aplica override quando há 1 filial selecionada
      if (idFilial && paymentMode) params.set("payment_mode", paymentMode);
      const resp = await apiGet(`/bi/team/commissions/results?${params.toString()}`);
      setData(resp);
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao carregar comissões.");
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial, multiFiliais, isMulti, selectedMonth, selectedYear, paymentMode]);

  useEffect(() => {
    fetchResults();
  }, [fetchResults]);

  const sellersSorted = useMemo(() => {
    const rows = (data?.vendedores || []) as Record<string, unknown>[];
    return sortGridRows(rows, (row) => ({
      filial: String(row.filial_label || row.id_filial || ""),
      nome: String(row.nome_vendedor || ""),
    }));
  }, [data?.vendedores]);

  const groupsSorted = useMemo(() => {
    const rows = (data?.grupos_configurados || []) as Record<string, unknown>[];
    return sortGridRows(rows, (row) => ({
      filial: String(row.filial_label || row.id_filial || ""),
      nome: String(row.nome || ""),
    }));
  }, [data?.grupos_configurados]);

  const { query: sellersQ, setQuery: setSellersQ, filteredRows: filteredSellers } = useGridSearch(
    sellersSorted,
    { excludeKeys: /^id_/i },
  );
  const { query: groupsQ, setQuery: setGroupsQ, filteredRows: filteredGroups } = useGridSearch(
    groupsSorted,
    { excludeKeys: /^id_/i },
  );

  // Filial: exibe quando há mais de uma no escopo (contrato grids BI)
  const showFilialCol = Boolean(data?.multi_filial) || multiFiliais.length > 1;
  const numCell = { textAlign: "right" as const, fontVariantNumeric: "tabular-nums" as const };

  if (!hasScope) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState
          title="Selecione o escopo"
          detail="Escolha uma ou mais filiais (ou Todas) no painel lateral para visualizar as comissões."
        />
      </div>
    );
  }

  return (
    <div style={{ marginTop: 16 }}>
      <div className="card" style={{ padding: "12px 16px", fontSize: 13 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
          <div>
            Relatório de comissão ({String(selectedMonth).padStart(2, "0")}/{selectedYear})
            {data?.payment_mode ? (
              <span className="muted" style={{ marginLeft: 8, fontSize: 12 }}>
                · {PAYMENT_MODE_LABELS[data.payment_mode] || data.payment_mode}
              </span>
            ) : null}
            {showFilialCol ? (
              <span className="muted" style={{ marginLeft: 8, fontSize: 12 }}>
                · {multiFiliais.length || data?.id_filiais?.length || 0} filiais
              </span>
            ) : null}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {idFilial ? (
              <select
                value={paymentMode}
                onChange={(e) => setPaymentMode(e.target.value)}
                title="Modo de cálculo da comissão"
                style={{
                  padding: "6px 10px",
                  borderRadius: 6,
                  border: "1px solid var(--border)",
                  background: "var(--card-bg)",
                  color: "inherit",
                  fontSize: 12,
                }}
              >
                <option value="">Modo: padrão da config</option>
                <option value="team_total">Equipe (comissão total)</option>
                <option value="equal_split">Divisão igual</option>
                <option value="individual_sales">Individual por vendas</option>
              </select>
            ) : null}
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

      <ManagerCommissionGrid
        idEmpresa={idEmpresa}
        idFilial={idFilial}
        idFiliais={idFiliais}
        month={selectedMonth}
        year={selectedYear}
      />

      {loading ? (
        <div className="card" style={{ marginTop: 12, textAlign: "center", padding: 32 }}>
          <div className="muted">Calculando comissões...</div>
        </div>
      ) : data ? (
        <>
          {data.message && (data.vendedores || []).length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState title="Sem dados" detail={data.message} />
            </div>
          ) : (
            <>
              <div className="card" style={{ marginTop: 12 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 10 }}>
                  <GridSearchInput value={sellersQ} onChange={setSellersQ} />
                  <div style={{ marginLeft: "auto", textAlign: "right" }}>
                    <h2 style={{ margin: 0, fontSize: 15 }}>Vendedores</h2>
                    <span className="muted" style={{ fontSize: 12 }}>
                      {data.vendedores_elegiveis} elegíveis · total {formatCurrency(data.comissao_total || 0)}
                    </span>
                  </div>
                </div>

                {(data.vendedores || []).length === 0 ? (
                  <EmptyState title="Sem vendedores" detail="Não há vendedores com identificação válida para o período selecionado." />
                ) : (
                  <div className="tableScroll">
                    <table className="table compact" style={{ width: "100%", minWidth: showFilialCol ? 720 : 560 }}>
                      <thead>
                        <tr>
                          {showFilialCol ? <th style={{ textAlign: "left" }}>Filial</th> : null}
                          <th style={{ textAlign: "left" }}>Funcionário</th>
                          <th style={{ textAlign: "right" }}>Venda</th>
                          <th style={{ textAlign: "left" }}>Nível</th>
                          <th style={{ textAlign: "right" }}>%</th>
                          <th style={{ textAlign: "right" }}>Comissão</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredSellers.map((emp: any) => (
                          <tr key={`${emp.id_filial || "x"}-${emp.id_funcionario}`}>
                            {showFilialCol ? (
                              <td style={{ fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>
                                {emp.filial_label || `Filial ${emp.id_filial || "—"}`}
                              </td>
                            ) : null}
                            <td style={{ fontWeight: 500, textAlign: "left" }}>{emp.nome_vendedor}</td>
                            <td style={numCell}>{formatCurrency(emp.venda_elegivel)}</td>
                            <td style={{ textAlign: "left" }}>
                              {emp.nivel_atingido ? (
                                <span style={{ color: TIER_STYLES[emp.nivel_atingido.tier_key]?.color }}>
                                  {TIER_STYLES[emp.nivel_atingido.tier_key]?.icon} {emp.nivel_atingido.tier_name}
                                </span>
                              ) : (
                                <span className="muted">Sem nível</span>
                              )}
                            </td>
                            <td style={numCell}>{Number(emp.percentual_aplicado || 0).toFixed(2)}%</td>
                            <td style={{ ...numCell, fontWeight: 700, color: "#22c55e" }}>
                              {formatCurrency(emp.comissao_estimada || 0)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {(data.grupos_configurados || []).length > 0 && (
                <div className="card" style={{ marginTop: 12 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 8 }}>
                    <GridSearchInput value={groupsQ} onChange={setGroupsQ} />
                    <h2 style={{ margin: 0, marginLeft: "auto", fontSize: 14 }}>Grupos participantes</h2>
                  </div>
                  <table className="table compact">
                    <thead>
                      <tr>
                        {showFilialCol ? <th style={{ textAlign: "left" }}>Filial</th> : null}
                        <th style={{ textAlign: "left" }}>Grupo</th>
                        <th style={{ textAlign: "right" }}>Venda no mês</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredGroups.map((g: any) => (
                        <tr key={`${g.id_filial || "x"}-${g.id_grupo_produto}`}>
                          {showFilialCol ? (
                            <td style={{ fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>
                              {g.filial_label || `Filial ${g.id_filial || "—"}`}
                            </td>
                          ) : null}
                          <td style={{ textAlign: "left" }}>{g.nome}</td>
                          <td style={numCell}>{formatCurrency(g.venda_total)}</td>
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
