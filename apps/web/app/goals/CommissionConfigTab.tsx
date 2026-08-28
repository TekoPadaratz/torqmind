"use client";

import { useCallback, useEffect, useState, type CSSProperties } from "react";
import { apiGet, apiPut } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";

/** Quantidade inteira (ex.: 160) — níveis de premiação não são em R$. */
function formatQty(value: number): string {
  return Number(value || 0).toLocaleString("pt-BR", { maximumFractionDigits: 0 });
}

function parseQty(text: string): number {
  const clean = text.replace(/[^\d]/g, "");
  return clean ? parseInt(clean, 10) : 0;
}

function matchesProductSearch(nome: string, idProduto: number, query: string): boolean {
  const q = String(query || "")
    .trim()
    .toLocaleLowerCase("pt-BR");
  if (!q) return true;
  const hay = `${nome} ${idProduto}`.toLocaleLowerCase("pt-BR");
  return hay.includes(q);
}

const TIER_STYLES: Record<string, { color: string; bg: string; icon: string }> = {
  bronze: { color: "#cd7f32", bg: "rgba(205,127,50,0.08)", icon: "🥉" },
  silver: { color: "#a0a0a0", bg: "rgba(160,160,160,0.08)", icon: "🥈" },
  gold: { color: "#d4a017", bg: "rgba(212,160,23,0.08)", icon: "🥇" },
  diamond: { color: "#4f9cf7", bg: "rgba(79,156,247,0.10)", icon: "💎" },
};

interface ConfigTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  onSaved?: () => void;
}

interface TierDraft {
  tier_key: string;
  tier_name: string;
  min_sales_amount: number;
  commission_percent: number;
  sort_order: number;
  is_active: boolean;
}

type ProductRow = {
  id_produto: number;
  nome: string;
  selected: boolean;
};

type GroupRow = {
  id_grupo_produto: number;
  nome: string;
  selected: boolean;
  faturamento_30d?: number;
  expanded?: boolean;
  productsLoaded?: boolean;
  productsLoading?: boolean;
  products?: ProductRow[];
  /** Busca local dos produtos ao expandir o grupo. */
  productQuery?: string;
};

type EmployeeRow = {
  id_funcionario: number;
  nome: string;
  funcao: string;
  include_in_commission: boolean;
};

export default function CommissionConfigTab({ idEmpresa, idFilial, onSaved }: ConfigTabProps) {
  const [groups, setGroups] = useState<GroupRow[]>([]);
  const [employees, setEmployees] = useState<EmployeeRow[]>([]);
  const [employeeQuery, setEmployeeQuery] = useState("");
  const [tiers, setTiers] = useState<TierDraft[]>([]);
  const [paymentMode, setPaymentMode] = useState("individual_sales");
  const [excludedIds, setExcludedIds] = useState<Set<number>>(new Set());
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const fetchConfig = useCallback(async () => {
    if (!idFilial) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      const resp = await apiGet(`/bi/team/commissions/config?${params.toString()}`);
      const excluded = new Set<number>(
        (resp.excluded_products || []).map((p: any) => Number(p.id_produto)).filter((n: number) => n > 0),
      );
      setExcludedIds(excluded);
      setGroups(
        (resp.groups || []).map((g: any) => ({
          id_grupo_produto: Number(g.id_grupo_produto),
          nome: String(g.nome || `Grupo ${g.id_grupo_produto}`),
          selected: !!g.selected,
          faturamento_30d: Number(g.faturamento_30d || 0),
          expanded: false,
          productsLoaded: false,
          productsLoading: false,
          products: [],
        })),
      );
      setTiers(resp.tiers || []);
      setPaymentMode(resp.config?.default_payment_mode || "individual_sales");
      setEmployees(
        (resp.employees || []).map((e: any) => ({
          id_funcionario: Number(e.id_funcionario),
          nome: String(e.nome || ""),
          funcao: String(e.funcao || ""),
          include_in_commission: !!e.include_in_commission,
        })),
      );
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao carregar configuração."));
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const loadProducts = async (idGrupo: number) => {
    if (!idFilial) return;
    setGroups((prev) =>
      prev.map((g) =>
        g.id_grupo_produto === idGrupo ? { ...g, productsLoading: true, expanded: true } : g,
      ),
    );
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      params.set("id_grupo_produto", String(idGrupo));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      const resp = await apiGet(`/bi/team/commissions/config/products?${params.toString()}`);
      setGroups((prev) =>
        prev.map((g) => {
          if (g.id_grupo_produto !== idGrupo) return g;
          const products: ProductRow[] = (resp.products || []).map((p: any) => {
            const id = Number(p.id_produto);
            return {
              id_produto: id,
              nome: String(p.nome || `Produto ${id}`),
              selected: g.selected ? !excludedIds.has(id) : false,
            };
          });
          return {
            ...g,
            products,
            productsLoaded: true,
            productsLoading: false,
            expanded: true,
          };
        }),
      );
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao carregar produtos do grupo."));
      setGroups((prev) =>
        prev.map((g) =>
          g.id_grupo_produto === idGrupo ? { ...g, productsLoading: false } : g,
        ),
      );
    }
  };

  const toggleExpand = (idGrupo: number) => {
    const group = groups.find((g) => g.id_grupo_produto === idGrupo);
    if (!group) return;
    if (group.expanded) {
      setGroups((prev) =>
        prev.map((g) => (g.id_grupo_produto === idGrupo ? { ...g, expanded: false } : g)),
      );
      return;
    }
    if (group.productsLoaded) {
      setGroups((prev) =>
        prev.map((g) => (g.id_grupo_produto === idGrupo ? { ...g, expanded: true } : g)),
      );
      return;
    }
    void loadProducts(idGrupo);
  };

  const toggleGroup = (idGrupo: number) => {
    const group = groups.find((g) => g.id_grupo_produto === idGrupo);
    if (!group) return;
    const nextSelected = !group.selected;
    const productIds = (group.products || []).map((p) => p.id_produto);
    setExcludedIds((ex) => {
      const next = new Set(ex);
      for (const id of productIds) next.delete(id);
      return next;
    });
    setGroups((prev) =>
      prev.map((g) => {
        if (g.id_grupo_produto !== idGrupo) return g;
        const products = (g.products || []).map((p) => ({ ...p, selected: nextSelected }));
        return { ...g, selected: nextSelected, products };
      }),
    );
  };

  const toggleProduct = (idGrupo: number, idProduto: number) => {
    const group = groups.find((g) => g.id_grupo_produto === idGrupo);
    if (!group) return;
    const products = (group.products || []).map((p) =>
      p.id_produto === idProduto ? { ...p, selected: !p.selected } : p,
    );
    const anyOn = products.some((p) => p.selected);
    setExcludedIds((ex) => {
      const next = new Set(ex);
      for (const p of products) {
        if (p.selected) next.delete(p.id_produto);
        else next.add(p.id_produto);
      }
      return next;
    });
    setGroups((prev) =>
      prev.map((g) =>
        g.id_grupo_produto === idGrupo ? { ...g, selected: anyOn, products } : g,
      ),
    );
  };

  const setGroupProductQuery = (idGrupo: number, query: string) => {
    setGroups((prev) =>
      prev.map((g) =>
        g.id_grupo_produto === idGrupo ? { ...g, productQuery: query } : g,
      ),
    );
  };

  const setAllGroupsSelected = (nextSelected: boolean) => {
    setExcludedIds((ex) => {
      const next = new Set(ex);
      for (const g of groups) {
        for (const p of g.products || []) next.delete(p.id_produto);
      }
      return next;
    });
    setGroups((prev) =>
      prev.map((g) => ({
        ...g,
        selected: nextSelected,
        products: (g.products || []).map((p) => ({ ...p, selected: nextSelected })),
      })),
    );
  };

  /** Marca/desmarca produtos do grupo (se houver busca, só os visíveis). */
  const setGroupProductsSelected = (idGrupo: number, nextSelected: boolean) => {
    const group = groups.find((g) => g.id_grupo_produto === idGrupo);
    if (!group) return;
    const query = group.productQuery || "";
    const targetIds = new Set(
      (group.products || [])
        .filter((p) => matchesProductSearch(p.nome, p.id_produto, query))
        .map((p) => p.id_produto),
    );
    if (targetIds.size === 0) return;

    const products = (group.products || []).map((p) =>
      targetIds.has(p.id_produto) ? { ...p, selected: nextSelected } : p,
    );
    const anyOn = products.some((p) => p.selected);
    setExcludedIds((ex) => {
      const next = new Set(ex);
      for (const p of products) {
        if (p.selected) next.delete(p.id_produto);
        else next.add(p.id_produto);
      }
      return next;
    });
    setGroups((prev) =>
      prev.map((g) =>
        g.id_grupo_produto === idGrupo ? { ...g, selected: anyOn, products } : g,
      ),
    );
  };

  const updateTier = (index: number, field: keyof TierDraft, value: any) => {
    setTiers((prev) => prev.map((t, i) => (i === index ? { ...t, [field]: value } : t)));
  };

  const handleSave = async () => {
    if (!idFilial) return;
    setError("");
    setMessage("");

    const activeTiers = tiers.filter((t) => t.is_active);
    for (let i = 1; i < activeTiers.length; i++) {
      if (activeTiers[i].min_sales_amount <= activeTiers[i - 1].min_sales_amount) {
        setError(`A quantidade mínima de "${activeTiers[i].tier_name}" deve ser maior que "${activeTiers[i - 1].tier_name}" (${formatQty(activeTiers[i - 1].min_sales_amount)}).`);
        return;
      }
      if (activeTiers[i].commission_percent <= activeTiers[i - 1].commission_percent) {
        setError(`O percentual de "${activeTiers[i].tier_name}" (${activeTiers[i].commission_percent}%) deve ser maior que "${activeTiers[i - 1].tier_name}" (${activeTiers[i - 1].commission_percent}%).`);
        return;
      }
    }
    for (const t of tiers) {
      if (t.is_active && t.min_sales_amount <= 0) {
        setError(`A quantidade mínima de "${t.tier_name}" deve ser maior que zero.`);
        return;
      }
      if (t.is_active && t.commission_percent <= 0) {
        setError(`O percentual de "${t.tier_name}" deve ser maior que zero.`);
        return;
      }
    }

    setSaving(true);
    try {
      const selectedGroups = groups
        .filter((g) => g.selected)
        .map((g) => ({ id_grupo_produto: g.id_grupo_produto, nome: g.nome }));

      const excluded_products: { id_produto: number; nome: string }[] = [];
      const seen = new Set<number>();
      for (const g of groups) {
        for (const p of g.products || []) {
          if (!p.selected && !seen.has(p.id_produto)) {
            seen.add(p.id_produto);
            excluded_products.push({ id_produto: p.id_produto, nome: p.nome });
          }
        }
      }
      // Keep excludes for products not yet loaded in drill-down
      Array.from(excludedIds).forEach((id) => {
        if (!seen.has(id)) {
          seen.add(id);
          excluded_products.push({ id_produto: id, nome: `Produto ${id}` });
        }
      });

      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      await apiPut(`/bi/team/commissions/config?${params.toString()}`, {
        groups: selectedGroups,
        tiers,
        default_payment_mode: paymentMode,
        excluded_products,
        employees: employees.map((e) => ({
          id_funcionario: e.id_funcionario,
          nome: e.nome,
          funcao: e.funcao,
          include_in_commission: e.include_in_commission,
        })),
      });
      setMessage("Configuração salva com sucesso!");
      if (onSaved) onSaved();
      await fetchConfig();
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao salvar configuração."));
    } finally {
      setSaving(false);
    }
  };

  if (!idFilial) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState title="Selecione uma filial" detail="Escolha uma filial no painel lateral para configurar as comissões." />
      </div>
    );
  }

  if (loading) {
    return (
      <div className="card" style={{ marginTop: 16, padding: 32, textAlign: "center" }}>
        <div className="muted">Carregando configuração...</div>
      </div>
    );
  }

  const selectedCount = groups.filter((g) => g.selected).length;
  const allGroupsSelected = groups.length > 0 && selectedCount === groups.length;

  const employeeQueryNorm = employeeQuery.trim().toLocaleLowerCase("pt-BR");
  const filteredEmployees = employees.filter((e) => {
    if (!employeeQueryNorm) return true;
    const hay = `${e.nome} ${e.funcao} ${e.id_funcionario}`.toLocaleLowerCase("pt-BR");
    return hay.includes(employeeQueryNorm);
  });
  const includedEmployees = employees.filter((e) => e.include_in_commission).length;

  const bulkBtnStyle: CSSProperties = {
    fontSize: 12,
    padding: "4px 10px",
    borderRadius: 6,
    border: "1px solid var(--border)",
    background: "var(--card-bg)",
    color: "inherit",
    cursor: "pointer",
    whiteSpace: "nowrap",
  };

  return (
    <div style={{ marginTop: 16 }}>
      <div className="card" style={{ padding: "14px 18px", borderLeft: "3px solid var(--color-accent, #3b82f6)" }}>
        <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>Configuração da Comissão — Funcionários</div>
        <div className="muted" style={{ fontSize: 13 }}>
          Grupos da base do posto. Expanda um grupo para incluir/excluir produtos.
        </div>
      </div>

      {error && <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div>}
      {message && <div className="card" style={{ marginTop: 12, color: "#22c55e", fontWeight: 500, padding: "10px 14px" }}>{message}</div>}

      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", marginBottom: 10 }}>
          <div style={{ fontWeight: 600, fontSize: 13 }}>Funcionários — considerar no relatório</div>
          <GridSearchInput value={employeeQuery} onChange={setEmployeeQuery} />
          <span className="muted" style={{ fontSize: 12 }}>
            {includedEmployees} de {employees.length} incluídos
          </span>
        </div>
        <div className="muted" style={{ fontSize: 12, marginBottom: 10 }}>
          Função vinda do Xpert. Desmarque quem não é vendedor (gerente, administrativo, etc.).
        </div>
        {employees.length === 0 ? (
          <EmptyState title="Sem funcionários" detail="Nenhum funcionário ativo encontrado na filial." />
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table className="dataTable" style={{ fontSize: 13 }}>
              <thead>
                <tr>
                  <th>Funcionário</th>
                  <th>Função (Xpert)</th>
                  <th style={{ textAlign: "center" }}>Vendedor</th>
                </tr>
              </thead>
              <tbody>
                {filteredEmployees.map((emp) => (
                  <tr key={emp.id_funcionario}>
                    <td>{emp.nome || `Funcionário ${emp.id_funcionario}`}</td>
                    <td>{emp.funcao || "—"}</td>
                    <td style={{ textAlign: "center" }}>
                      <input
                        type="checkbox"
                        checked={emp.include_in_commission}
                        onChange={(e) => {
                          const checked = e.target.checked;
                          setEmployees((prev) =>
                            prev.map((row) =>
                              row.id_funcionario === emp.id_funcionario
                                ? { ...row, include_in_commission: checked }
                                : row,
                            ),
                          );
                        }}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>Modo de pagamento padrão</div>
        <select
          value={paymentMode}
          onChange={(e) => setPaymentMode(e.target.value)}
          style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid var(--border)", background: "var(--card-bg)", color: "inherit", width: "100%", maxWidth: 320 }}
        >
          <option value="individual_sales">Individual por quantidade</option>
          <option value="team_total">Equipe (quantidade total)</option>
          <option value="equal_split">Rateio igual por equipe</option>
        </select>
        <div className="muted" style={{ fontSize: 11, marginTop: 4 }}>Define o modo padrão ao abrir a aba Comissões.</div>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 10 }}>
          <div style={{ fontWeight: 600, fontSize: 13 }}>Grupos e produtos participantes</div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <button
              type="button"
              className="btn"
              style={bulkBtnStyle}
              onClick={() => setAllGroupsSelected(!allGroupsSelected)}
              disabled={groups.length === 0}
            >
              {allGroupsSelected ? "Desmarcar todos os grupos" : "Marcar todos os grupos"}
            </button>
            <span className="muted" style={{ fontSize: 12 }}>
              {selectedCount} grupo(s) · {excludedIds.size} produto(s) excluído(s)
            </span>
          </div>
        </div>
        {selectedCount === 0 && (
          <div style={{ padding: "8px 12px", background: "rgba(234,179,8,0.08)", borderRadius: 6, fontSize: 12, marginBottom: 8, color: "#ca8a04" }}>
            Selecione os grupos que devem participar da comissão.
          </div>
        )}
        <div style={{ maxHeight: 420, overflowY: "auto", border: "1px solid var(--border)", borderRadius: 8 }}>
          {groups.map((g) => (
            <div key={g.id_grupo_produto} style={{ borderBottom: "1px solid var(--table-row-border)" }}>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "28px 18px 1fr auto",
                  alignItems: "center",
                  gap: 8,
                  padding: "8px 12px",
                }}
              >
                <button
                  type="button"
                  onClick={() => toggleExpand(g.id_grupo_produto)}
                  aria-label={g.expanded ? "Recolher produtos" : "Expandir produtos"}
                  style={{
                    width: 28,
                    height: 28,
                    borderRadius: 6,
                    border: "1px solid var(--border)",
                    background: "var(--card-bg)",
                    color: "inherit",
                    cursor: "pointer",
                    fontSize: 12,
                  }}
                >
                  {g.expanded ? "▾" : "▸"}
                </button>
                <input
                  type="checkbox"
                  checked={!!g.selected}
                  onChange={() => toggleGroup(g.id_grupo_produto)}
                  style={{ width: 16, height: 16, flexShrink: 0 }}
                />
                <span style={{ fontSize: 13, minWidth: 0 }}>{g.nome}</span>
                <span className="muted" style={{ fontSize: 11, whiteSpace: "nowrap" }}>
                  {(g.faturamento_30d || 0) > 0 ? `${formatCurrency(g.faturamento_30d || 0)} /30d` : "—"}
                </span>
              </div>
              {g.expanded ? (
                <div style={{ padding: "0 12px 10px 48px", background: "var(--surface-faint, rgba(0,0,0,0.03))" }}>
                  {g.productsLoading ? (
                    <div className="muted" style={{ fontSize: 12, padding: "6px 0" }}>Carregando produtos…</div>
                  ) : (g.products || []).length === 0 ? (
                    <div className="muted" style={{ fontSize: 12, padding: "6px 0" }}>Nenhum produto neste grupo.</div>
                  ) : (
                    <>
                      <div
                        style={{
                          padding: "6px 0 10px",
                          display: "flex",
                          alignItems: "center",
                          gap: 10,
                          flexWrap: "wrap",
                          justifyContent: "flex-start",
                        }}
                      >
                        <GridSearchInput
                          value={g.productQuery || ""}
                          onChange={(value) => setGroupProductQuery(g.id_grupo_produto, value)}
                          placeholder="Pesquisar produto…"
                          aria-label={`Pesquisar produtos de ${g.nome}`}
                        />
                        {(() => {
                          const visible = (g.products || []).filter((p) =>
                            matchesProductSearch(p.nome, p.id_produto, g.productQuery || ""),
                          );
                          const allVisibleOn =
                            visible.length > 0 && visible.every((p) => p.selected);
                          return (
                            <button
                              type="button"
                              className="btn"
                              style={bulkBtnStyle}
                              disabled={visible.length === 0}
                              onClick={() =>
                                setGroupProductsSelected(g.id_grupo_produto, !allVisibleOn)
                              }
                            >
                              {allVisibleOn ? "Desmarcar todos" : "Marcar todos"}
                            </button>
                          );
                        })()}
                      </div>
                      {(() => {
                        const visible = (g.products || []).filter((p) =>
                          matchesProductSearch(p.nome, p.id_produto, g.productQuery || ""),
                        );
                        if (visible.length === 0) {
                          return (
                            <div className="muted" style={{ fontSize: 12, padding: "4px 0 6px" }}>
                              Nenhum produto para “{g.productQuery}”.
                            </div>
                          );
                        }
                        return (
                          <div
                            style={{
                              display: "grid",
                              gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
                              gap: 6,
                              alignItems: "start",
                            }}
                          >
                            {visible.map((p) => (
                              <label
                                key={p.id_produto}
                                style={{
                                  display: "flex",
                                  alignItems: "flex-start",
                                  gap: 8,
                                  fontSize: 12,
                                  cursor: "pointer",
                                  minWidth: 0,
                                  padding: "2px 0",
                                }}
                              >
                                <input
                                  type="checkbox"
                                  checked={!!p.selected}
                                  onChange={() => toggleProduct(g.id_grupo_produto, p.id_produto)}
                                  style={{ width: 14, height: 14, flexShrink: 0, marginTop: 2 }}
                                />
                                <span style={{ minWidth: 0, lineHeight: 1.35, wordBreak: "break-word" }}>
                                  {p.nome}
                                </span>
                              </label>
                            ))}
                          </div>
                        );
                      })()}
                    </>
                  )}
                </div>
              ) : null}
            </div>
          ))}
        </div>
      </div>

      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 12 }}>Níveis de premiação</div>
        <div style={{ display: "grid", gap: 10 }}>
          {tiers.map((tier, i) => {
            const style = TIER_STYLES[tier.tier_key] || TIER_STYLES.bronze;
            return (
              <div
                key={tier.tier_key}
                style={{
                  display: "grid",
                  gridTemplateColumns: "auto 1fr 140px 100px auto",
                  gap: 10,
                  alignItems: "center",
                  padding: "10px 14px",
                  borderRadius: 10,
                  background: tier.is_active ? style.bg : "var(--surface-faint)",
                  border: `1px solid ${tier.is_active ? style.color + "30" : "var(--border)"}`,
                  opacity: tier.is_active ? 1 : 0.5,
                }}
              >
                <span style={{ fontSize: 18 }}>{style.icon}</span>
                <div>
                  <input
                    type="text"
                    value={tier.tier_name}
                    onChange={(e) => updateTier(i, "tier_name", e.target.value)}
                    style={{
                      background: "transparent",
                      border: "none",
                      borderBottom: "1px solid var(--border)",
                      color: style.color,
                      fontWeight: 700,
                      fontSize: 14,
                      width: "100%",
                      maxWidth: 140,
                    }}
                  />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>Qtd</span>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={formatQty(tier.min_sales_amount)}
                    onChange={(e) => updateTier(i, "min_sales_amount", parseQty(e.target.value))}
                    style={{
                      width: 100,
                      padding: "4px 6px",
                      borderRadius: 4,
                      border: "1px solid var(--border)",
                      background: "var(--card-bg)",
                      color: "inherit",
                      fontSize: 13,
                    }}
                  />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    step={0.1}
                    value={tier.commission_percent}
                    onChange={(e) => updateTier(i, "commission_percent", Number(e.target.value))}
                    style={{
                      width: 60,
                      padding: "4px 6px",
                      borderRadius: 4,
                      border: "1px solid var(--border)",
                      background: "var(--card-bg)",
                      color: "inherit",
                      fontSize: 13,
                    }}
                  />
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>%</span>
                </div>
                <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 11, cursor: "pointer" }}>
                  <input
                    type="checkbox"
                    checked={tier.is_active}
                    onChange={(e) => updateTier(i, "is_active", e.target.checked)}
                  />
                  Ativo
                </label>
              </div>
            );
          })}
        </div>
        <div className="muted" style={{ fontSize: 11, marginTop: 8 }}>
          Venda elegível = grupos marcados − produtos desmarcados.
        </div>
      </div>

      <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
        <button
          className="btn"
          onClick={handleSave}
          disabled={saving}
          style={{ padding: "10px 24px", fontWeight: 600 }}
        >
          {saving ? "Salvando..." : "Salvar configuração"}
        </button>
      </div>
    </div>
  );
}
