"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet, isRequestCanceled } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";
import { sortGridRows } from "../lib/grid-sort";
import { splitAnoMes } from "../lib/month-year.mjs";

const TIER_STYLES: Record<string, { color: string; bg: string; icon: string }> = {
  bronze: { color: "#cd7f32", bg: "rgba(205,127,50,0.10)", icon: "🥉" },
  silver: { color: "#a0a0a0", bg: "rgba(160,160,160,0.10)", icon: "🥈" },
  gold: { color: "#d4a017", bg: "rgba(212,160,23,0.10)", icon: "🥇" },
  diamond: { color: "#4f9cf7", bg: "rgba(79,156,247,0.12)", icon: "💎" },
};

/** Ranking do grid: Diamante → Ouro → Prata → Bronze → sem nível (exceção ao contrato Filial→Nome). */
const TIER_RANK: Record<string, number> = {
  diamond: 4,
  gold: 3,
  silver: 2,
  bronze: 1,
};

function sellerTierRank(nivel: { tier_key?: string } | null | undefined): number {
  const key = String(nivel?.tier_key || "").trim().toLowerCase();
  return TIER_RANK[key] ?? 0;
}

interface CommissionsTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  idFiliais?: string[];
  anoMes: number;
}

export default function CommissionsTab({
  idEmpresa,
  idFilial,
  idFiliais,
  anoMes,
}: CommissionsTabProps) {
  const { year: selectedYear, month: selectedMonth } = splitAnoMes(anoMes);
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
  // Chave estável para deps (evita refetch por nova referência de array).
  const multiFiliaisKey = multiFiliais.join(",");

  useEffect(() => {
    if (!idFilial && multiFiliais.length === 0) {
      setData(null);
      setError("");
      setLoading(false);
      return;
    }

    const ac = new AbortController();
    setLoading(true);
    setError("");
    setData(null);

    const params = new URLSearchParams();
    if (idEmpresa) params.set("id_empresa", String(idEmpresa));
    params.set("month", String(selectedMonth));
    params.set("year", String(selectedYear));
    if (isMulti || (!idFilial && multiFiliais.length > 0)) {
      for (const f of multiFiliais) params.append("id_filiais", String(f));
    } else if (idFilial) {
      params.set("id_filial", String(idFilial));
    }
    if (idFilial && paymentMode) params.set("payment_mode", paymentMode);

    (async () => {
      try {
        // Timeout maior que o default (30s): multi-filial histórico pode ser mais pesado.
        const resp = await apiGet(`/bi/team/commissions/results?${params.toString()}`, {
          signal: ac.signal,
          timeout: 90000,
        });
        if (ac.signal.aborted) return;
        setData(resp);
        setError("");
      } catch (err: any) {
        if (ac.signal.aborted || isRequestCanceled(err)) return;
        setData(null);
        setError(err?.response?.data?.detail || "Falha ao carregar comissões.");
      } finally {
        if (!ac.signal.aborted) setLoading(false);
      }
    })();

    return () => ac.abort();
    // multiFiliaisKey cobre o array; multiFiliais lido do closure estável pelo key.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [idEmpresa, idFilial, multiFiliaisKey, isMulti, selectedMonth, selectedYear, paymentMode]);

  // Exceção de ranking: nível DESC; dentro do nível, Filial ASC → Nome ASC (sort estável).
  const sellersSorted = useMemo(() => {
    const rows = (data?.vendedores || []) as Record<string, unknown>[];
    const byFilialNome = sortGridRows(rows, (row) => ({
      filial: String(row.filial_label || row.id_filial || ""),
      nome: String(row.nome_vendedor || ""),
    }));
    return [...byFilialNome].sort(
      (a, b) =>
        sellerTierRank((b as { nivel_atingido?: { tier_key?: string } }).nivel_atingido) -
        sellerTierRank((a as { nivel_atingido?: { tier_key?: string } }).nivel_atingido),
    );
  }, [data?.vendedores]);

  const { query: sellersQ, setQuery: setSellersQ, filteredRows: filteredSellers } = useGridSearch(
    sellersSorted,
    { excludeKeys: /^id_/i },
  );

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
      {error ? <div className="card errorCard" style={{ marginBottom: 12 }}>{error}</div> : null}

      {loading ? (
        <div className="card" style={{ textAlign: "center", padding: 32 }}>
          <div className="muted">Calculando comissões...</div>
        </div>
      ) : data ? (
        <div className="card">
          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 10 }}>
            <GridSearchInput value={sellersQ} onChange={setSellersQ} />
            {idFilial ? (
              <label className="profitScopeMonth" title="Modo de cálculo da comissão">
                <span className="profitScopeMonthLabel">Modo</span>
                <select
                  className="profitScopeMonthSelect"
                  value={paymentMode}
                  onChange={(e) => setPaymentMode(e.target.value)}
                  aria-label="Modo de cálculo da comissão"
                >
                  <option value="">Padrão da config</option>
                  <option value="individual_sales">Individual por quantidade</option>
                  <option value="team_total">Equipe (quantidade total)</option>
                  <option value="equal_split">Divisão igual</option>
                </select>
              </label>
            ) : null}
            <div style={{ marginLeft: "auto", textAlign: "right" }}>
              <span className="muted" style={{ fontSize: 12 }}>
                {data.vendedores_elegiveis} elegíveis · total {formatCurrency(data.comissao_total || 0)}
              </span>
            </div>
          </div>

          {data.message && (data.vendedores || []).length === 0 ? (
            <EmptyState title="Sem dados" detail={data.message} />
          ) : (data.vendedores || []).length === 0 ? (
            <EmptyState title="Sem vendedores" detail="Não há vendedores com identificação válida para o período selecionado." />
          ) : (
            <div className="tableScroll">
              <table className="table compact" style={{ width: "100%", minWidth: showFilialCol ? 720 : 560 }}>
                <thead>
                  <tr>
                    {showFilialCol ? <th style={{ textAlign: "left" }}>Filial</th> : null}
                    <th style={{ textAlign: "left" }}>Funcionário</th>
                    <th style={{ textAlign: "right" }}>Quantidade</th>
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
                      <td style={numCell}>
                        {Number(emp.quantidade_vendas || 0).toLocaleString("pt-BR", {
                          maximumFractionDigits: 0,
                        })}
                      </td>
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
                      <td style={{ ...numCell, fontWeight: 700, color: "var(--color-positive)" }}>
                        {formatCurrency(emp.comissao_estimada || 0)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
