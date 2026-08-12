"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet, isRequestCanceled } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";
import { splitAnoMes } from "../lib/month-year.mjs";

const TIER_STYLES: Record<
  string,
  { color: string; bg: string; bgActive: string; label: string }
> = {
  diamond: {
    color: "#5ba4ff",
    bg: "rgba(79,156,247,0.10)",
    bgActive: "rgba(79,156,247,0.22)",
    label: "Diamante",
  },
  gold: {
    color: "#e0b12a",
    bg: "rgba(212,160,23,0.12)",
    bgActive: "rgba(212,160,23,0.26)",
    label: "Ouro",
  },
  silver: {
    color: "#b8c0cc",
    bg: "rgba(160,160,160,0.12)",
    bgActive: "rgba(160,160,160,0.26)",
    label: "Prata",
  },
  bronze: {
    color: "#d08a4a",
    bg: "rgba(205,127,50,0.12)",
    bgActive: "rgba(205,127,50,0.26)",
    label: "Bronze",
  },
  none: {
    color: "var(--muted)",
    bg: "var(--surface-faint)",
    bgActive: "var(--surface-soft)",
    label: "Sem nível",
  },
};

const TIER_FILTER_ORDER = ["diamond", "gold", "silver", "bronze", "none"] as const;

type SellerRow = {
  id_filial?: number | null;
  filial_label?: string | null;
  id_funcionario?: number | null;
  nome_vendedor?: string | null;
  quantidade_vendas?: number | null;
  venda_elegivel?: number | null;
  percentual_aplicado?: number | null;
  comissao_estimada?: number | null;
  nivel_atingido?: { tier_key?: string; tier_name?: string } | null;
  [key: string]: unknown;
};

function sellerTierKey(emp: SellerRow): string {
  const key = String(emp?.nivel_atingido?.tier_key || "")
    .trim()
    .toLowerCase();
  return key && TIER_STYLES[key] ? key : "none";
}

function sellerTierLabel(emp: SellerRow): string {
  const key = sellerTierKey(emp);
  if (key === "none") return TIER_STYLES.none.label;
  return String(emp?.nivel_atingido?.tier_name || TIER_STYLES[key]?.label || key);
}

function sortSellersByCommission(rows: SellerRow[]): SellerRow[] {
  return [...rows].sort((a, b) => {
    const ca = Number(a.comissao_estimada || 0);
    const cb = Number(b.comissao_estimada || 0);
    if (cb !== ca) return cb - ca;
    const na = String(a.nome_vendedor || "").localeCompare(
      String(b.nome_vendedor || ""),
      "pt-BR",
      { sensitivity: "base" },
    );
    if (na !== 0) return na;
    return Number(a.id_funcionario || 0) - Number(b.id_funcionario || 0);
  });
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
  /** Vazio = todos os níveis (mesmo padrão de Prioridades de cobrança). */
  const [selectedTiers, setSelectedTiers] = useState<Set<string>>(new Set());

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const multiFiliais = useMemo(
    () => (idFiliais || []).map(String).filter((v) => v && v !== "0"),
    [idFiliais],
  );
  const isMulti = multiFiliais.length > 1 || (!idFilial && multiFiliais.length > 0);
  const hasScope = Boolean(idFilial) || multiFiliais.length > 0;
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [idEmpresa, idFilial, multiFiliaisKey, isMulti, selectedMonth, selectedYear, paymentMode]);

  const sellers = useMemo(
    () => ((data?.vendedores || []) as SellerRow[]),
    [data?.vendedores],
  );

  const tierStats = useMemo(() => {
    const map = new Map<string, { key: string; label: string; count: number; total: number }>();
    for (const emp of sellers) {
      const key = sellerTierKey(emp);
      const cur = map.get(key) || {
        key,
        label: key === "none" ? TIER_STYLES.none.label : sellerTierLabel(emp),
        count: 0,
        total: 0,
      };
      cur.count += 1;
      cur.total += Number(emp.comissao_estimada || 0);
      if (key !== "none" && emp.nivel_atingido?.tier_name) {
        cur.label = String(emp.nivel_atingido.tier_name);
      }
      map.set(key, cur);
    }
    return TIER_FILTER_ORDER.map((key) => map.get(key)).filter(Boolean) as Array<{
      key: string;
      label: string;
      count: number;
      total: number;
    }>;
  }, [sellers]);

  const tierFiltered = useMemo(() => {
    if (selectedTiers.size === 0) return sellers;
    return sellers.filter((emp) => selectedTiers.has(sellerTierKey(emp)));
  }, [sellers, selectedTiers]);

  const { query: sellersQ, setQuery: setSellersQ, filteredRows: searchedSellers } = useGridSearch(
    tierFiltered,
    { excludeKeys: /^id_/i },
  );

  const filialGroups = useMemo(() => {
    const byFilial = new Map<
      string,
      { id_filial: number; label: string; sellers: SellerRow[]; total: number }
    >();
    for (const emp of searchedSellers as SellerRow[]) {
      const id = Number(emp.id_filial || 0);
      const label = String(emp.filial_label || (id ? `Filial ${id}` : "Sem filial"));
      const key = `${id}:${label}`;
      const cur = byFilial.get(key) || { id_filial: id, label, sellers: [], total: 0 };
      cur.sellers.push(emp);
      cur.total += Number(emp.comissao_estimada || 0);
      byFilial.set(key, cur);
    }
    return Array.from(byFilial.values())
      .map((g) => ({
        ...g,
        sellers: sortSellersByCommission(g.sellers),
      }))
      .sort((a, b) =>
        a.label.localeCompare(b.label, "pt-BR", { numeric: true, sensitivity: "base" }),
      );
  }, [searchedSellers]);

  const filteredTotal = useMemo(
    () => filialGroups.reduce((acc, g) => acc + g.total, 0),
    [filialGroups],
  );
  const filteredCount = useMemo(
    () => filialGroups.reduce((acc, g) => acc + g.sellers.length, 0),
    [filialGroups],
  );

  const numCell = { textAlign: "right" as const, fontVariantNumeric: "tabular-nums" as const };

  const toggleTier = (key: string) => {
    setSelectedTiers((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

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
        <>
          <div className="card commissionToolbar">
            <div className="commissionToolbarRow">
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
              <div className="commissionToolbarMeta">
                <span className="muted" style={{ fontSize: 12 }}>
                  {filteredCount} de {data.vendedores_elegiveis || sellers.length} ·{" "}
                  {formatCurrency(filteredTotal || data.comissao_total || 0)}
                </span>
              </div>
            </div>

            {tierStats.length > 0 ? (
              <div className="commissionTierFilters" role="group" aria-label="Filtrar por nível">
                <button
                  type="button"
                  className={`commissionTierChip${selectedTiers.size === 0 ? " commissionTierChip--active" : ""}`}
                  aria-pressed={selectedTiers.size === 0}
                  onClick={() => setSelectedTiers(new Set())}
                >
                  <span className="commissionTierChipName">Todos</span>
                  <span className="commissionTierChipMeta">
                    {sellers.length} · {formatCurrency(data.comissao_total || 0)}
                  </span>
                </button>
                {tierStats.map((tier) => {
                  const style = TIER_STYLES[tier.key] || TIER_STYLES.none;
                  const active = selectedTiers.has(tier.key);
                  return (
                    <button
                      key={tier.key}
                      type="button"
                      className={`commissionTierChip${active ? " commissionTierChip--active" : ""}`}
                      aria-pressed={active}
                      onClick={() => toggleTier(tier.key)}
                      style={{
                        color: style.color,
                        borderColor: active ? style.color : `${style.color}55`,
                        background: active ? style.bgActive : style.bg,
                        boxShadow: active ? `0 0 0 1px ${style.color}` : undefined,
                      }}
                    >
                      <span className="commissionTierChipName">{tier.label}</span>
                      <span className="commissionTierChipMeta">
                        {tier.count} · {formatCurrency(tier.total)}
                      </span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {data.message && sellers.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState title="Sem dados" detail={data.message} />
            </div>
          ) : sellers.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState
                title="Sem vendedores"
                detail="Não há vendedores com identificação válida para o período selecionado."
              />
            </div>
          ) : filialGroups.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState
                title="Nenhum resultado"
                detail={
                  sellersQ
                    ? `Nada encontrado para “${sellersQ}” com os filtros atuais.`
                    : "Nenhum vendedor nos níveis selecionados."
                }
              />
            </div>
          ) : (
            filialGroups.map((group) => (
              <div
                key={`${group.id_filial}:${group.label}`}
                className="solvenciaFilialCard commissionFilialCard"
                style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
              >
                <div className="commissionFilialHead">
                  <div>
                    <div className="sectionEyebrow">Filial</div>
                    <h2 className="commissionFilialTitle">{group.label}</h2>
                  </div>
                  <div className="commissionFilialSummary">
                    <span>
                      {group.sellers.length}{" "}
                      {group.sellers.length === 1 ? "vendedor" : "vendedores"}
                    </span>
                    <strong>{formatCurrency(group.total)}</strong>
                  </div>
                </div>

                <div className="tableScroll">
                  <table className="table compact" style={{ width: "100%", minWidth: 560 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left" }}>Funcionário</th>
                        <th style={{ textAlign: "right" }}>Quantidade</th>
                        <th style={{ textAlign: "right" }}>Venda</th>
                        <th style={{ textAlign: "left" }}>Nível</th>
                        <th style={{ textAlign: "right" }}>%</th>
                        <th style={{ textAlign: "right" }}>Comissão</th>
                      </tr>
                    </thead>
                    <tbody>
                      {group.sellers.map((emp) => {
                        const tierKey = sellerTierKey(emp);
                        const style = TIER_STYLES[tierKey] || TIER_STYLES.none;
                        return (
                          <tr key={`${emp.id_filial || "x"}-${emp.id_funcionario}`}>
                            <td style={{ fontWeight: 500, textAlign: "left" }}>
                              {emp.nome_vendedor}
                            </td>
                            <td style={numCell}>
                              {Number(emp.quantidade_vendas || 0).toLocaleString("pt-BR", {
                                maximumFractionDigits: 0,
                              })}
                            </td>
                            <td style={numCell}>{formatCurrency(emp.venda_elegivel)}</td>
                            <td style={{ textAlign: "left" }}>
                              {tierKey === "none" ? (
                                <span className="muted">Sem nível</span>
                              ) : (
                                <span style={{ color: style.color, fontWeight: 650 }}>
                                  {sellerTierLabel(emp)}
                                </span>
                              )}
                            </td>
                            <td style={numCell}>
                              {Number(emp.percentual_aplicado || 0).toFixed(2)}%
                            </td>
                            <td
                              style={{
                                ...numCell,
                                fontWeight: 700,
                                color: "var(--color-positive)",
                              }}
                            >
                              {formatCurrency(emp.comissao_estimada || 0)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            ))
          )}
        </>
      ) : null}
    </div>
  );
}
