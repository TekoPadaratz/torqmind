"use client";

import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
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

type EmployeeRow = {
  id_funcionario: number;
  nome: string;
  funcao: string;
  include_in_commission: boolean;
};

const FUNCAO_SEM_CADASTRO = "Sem função cadastrada";

function funcaoLabel(raw: string): string {
  const text = String(raw || "").trim();
  return text || FUNCAO_SEM_CADASTRO;
}

function funcaoKeyFromLabel(label: string): string {
  return label.toLocaleLowerCase("pt-BR");
}

function employeeDisplayName(emp: EmployeeRow): string {
  const nome = String(emp.nome || "").trim();
  if (!nome) return "Nome não cadastrado";
  if (nome === String(emp.id_funcionario)) return "Nome não cadastrado";
  if (/^\d+$/.test(nome) && Number(nome) === emp.id_funcionario) return "Nome não cadastrado";
  return nome;
}

function matchesEmployeeSearch(emp: EmployeeRow, query: string): boolean {
  const q = String(query || "").trim().toLocaleLowerCase("pt-BR");
  if (!q) return true;
  const hay = `${employeeDisplayName(emp)} ${emp.funcao} ${emp.id_funcionario}`
    .toLocaleLowerCase("pt-BR");
  return hay.includes(q);
}

type FuncaoUiState = { expanded?: boolean; employeeQuery?: string };

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

export default function CommissionConfigTab({ idEmpresa, idFilial, onSaved }: ConfigTabProps) {
  const [groups, setGroups] = useState<GroupRow[]>([]);
  const [employees, setEmployees] = useState<EmployeeRow[]>([]);
  const [funcaoSearch, setFuncaoSearch] = useState("");
  const [funcaoUi, setFuncaoUi] = useState<Record<string, FuncaoUiState>>({});
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
          nome: String(e.nome || "").trim(),
          funcao: String(e.funcao || "").trim(),
          include_in_commission: !!e.include_in_commission,
        })),
      );
      setFuncaoUi({});
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao carregar configuração."));
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const funcaoGroups = useMemo(() => {
    const map = new Map<string, { label: string; employees: EmployeeRow[] }>();
    for (const emp of employees) {
      const label = funcaoLabel(emp.funcao);
      const key = funcaoKeyFromLabel(label);
      if (!map.has(key)) map.set(key, { label, employees: [] });
      map.get(key)!.employees.push(emp);
    }
    return Array.from(map.entries())
      .map(([funcaoKey, g]) => {
        const sorted = [...g.employees].sort((a, b) =>
          employeeDisplayName(a).localeCompare(employeeDisplayName(b), "pt-BR"),
        );
        return {
          funcaoKey,
          label: g.label,
          employees: sorted,
          selected: sorted.some((e) => e.include_in_commission),
          expanded: funcaoUi[funcaoKey]?.expanded ?? false,
          employeeQuery: funcaoUi[funcaoKey]?.employeeQuery ?? "",
        };
      })
      .sort((a, b) => a.label.localeCompare(b.label, "pt-BR"));
  }, [employees, funcaoUi]);

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

  const toggleFuncaoExpand = (funcaoKey: string) => {
    setFuncaoUi((prev) => ({
      ...prev,
      [funcaoKey]: { ...prev[funcaoKey], expanded: !prev[funcaoKey]?.expanded },
    }));
  };

  const toggleFuncao = (funcaoKey: string) => {
    const group = funcaoGroups.find((g) => g.funcaoKey === funcaoKey);
    if (!group) return;
    const nextSelected = !group.selected;
    const ids = new Set(group.employees.map((e) => e.id_funcionario));
    setEmployees((prev) =>
      prev.map((e) =>
        ids.has(e.id_funcionario) ? { ...e, include_in_commission: nextSelected } : e,
      ),
    );
  };

  const toggleEmployee = (idFuncionario: number) => {
    setEmployees((prev) =>
      prev.map((e) =>
        e.id_funcionario === idFuncionario
          ? { ...e, include_in_commission: !e.include_in_commission }
          : e,
      ),
    );
  };

  const setFuncaoEmployeeQuery = (funcaoKey: string, query: string) => {
    setFuncaoUi((prev) => ({
      ...prev,
      [funcaoKey]: { ...prev[funcaoKey], employeeQuery: query },
    }));
  };

  const setFuncaoEmployeesSelected = (funcaoKey: string, nextSelected: boolean) => {
    const group = funcaoGroups.find((g) => g.funcaoKey === funcaoKey);
    if (!group) return;
    const query = group.employeeQuery || "";
    const targetIds = new Set(
      group.employees
        .filter((e) => matchesEmployeeSearch(e, query))
        .map((e) => e.id_funcionario),
    );
    if (targetIds.size === 0) return;
    setEmployees((prev) =>
      prev.map((e) =>
        targetIds.has(e.id_funcionario) ? { ...e, include_in_commission: nextSelected } : e,
      ),
    );
  };

  const setAllEmployeesIncluded = (nextSelected: boolean) => {
    setEmployees((prev) => prev.map((e) => ({ ...e, include_in_commission: nextSelected })));
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

  const funcaoSearchNorm = funcaoSearch.trim().toLocaleLowerCase("pt-BR");
  const filteredFuncaoGroups = funcaoGroups.filter((fg) => {
    if (!funcaoSearchNorm) return true;
    if (fg.label.toLocaleLowerCase("pt-BR").includes(funcaoSearchNorm)) return true;
    return fg.employees.some((e) => matchesEmployeeSearch(e, funcaoSearch));
  });
  const includedEmployees = employees.filter((e) => e.include_in_commission).length;
  const allFuncoesSelected =
    funcaoGroups.length > 0 && funcaoGroups.every((fg) => fg.selected);

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
      {error && <div className="card errorCard" style={{ marginBottom: 12 }}>{error}</div>}
      {message && (
        <div className="card" style={{ marginBottom: 12, color: "#22c55e", fontWeight: 500, padding: "10px 14px" }}>
          {message}
        </div>
      )}

      <section
        className="solvenciaFilialCard commissionFilialCard commissionConfigSection"
        style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
      >
        <div className="commissionFilialHead commissionConfigSectionHead">
          <div>
            <div className="sectionEyebrow">Equipe</div>
            <h2 className="commissionFilialTitle">Funcionários no relatório</h2>
          </div>
          <div className="commissionFilialSummary">
            <span className="muted">Incluídos</span>
            <strong>{includedEmployees}</strong>
            <span className="muted">de {employees.length}</span>
          </div>
        </div>
        <div className="muted" style={{ fontSize: 12, marginBottom: 10 }}>
          Agrupado por função do Xpert. Marque a função para incluir todos; expanda para ajustar individualmente.
        </div>
        <div className="commissionConfigToolbar">
          <GridSearchInput value={funcaoSearch} onChange={setFuncaoSearch} aria-label="Pesquisar função ou funcionário" />
          <button
            type="button"
            className="btn"
            style={bulkBtnStyle}
            onClick={() => setAllEmployeesIncluded(!allFuncoesSelected)}
            disabled={employees.length === 0}
          >
            {allFuncoesSelected ? "Desmarcar todas as funções" : "Marcar todas as funções"}
          </button>
        </div>
        {employees.length === 0 ? (
          <EmptyState title="Sem funcionários" detail="Nenhum funcionário ativo encontrado na filial." />
        ) : filteredFuncaoGroups.length === 0 ? (
          <EmptyState title="Sem resultados" detail={`Nenhuma função ou funcionário para “${funcaoSearch.trim()}”.`} />
        ) : (
          <div className="commissionConfigTree">
            {filteredFuncaoGroups.map((fg) => (
              <div key={fg.funcaoKey}>
                <div className="commissionConfigTreeRow">
                  <button
                    type="button"
                    className="commissionConfigExpandBtn"
                    onClick={() => toggleFuncaoExpand(fg.funcaoKey)}
                    aria-label={fg.expanded ? "Recolher funcionários" : "Expandir funcionários"}
                  >
                    {fg.expanded ? "▾" : "▸"}
                  </button>
                  <input
                    type="checkbox"
                    checked={fg.selected}
                    onChange={() => toggleFuncao(fg.funcaoKey)}
                    style={{ width: 16, height: 16, flexShrink: 0 }}
                  />
                  <span style={{ fontSize: 13, minWidth: 0, fontWeight: 600 }}>{fg.label}</span>
                  <span className="commissionConfigTreeMeta">
                    {fg.employees.filter((e) => e.include_in_commission).length}/{fg.employees.length}
                  </span>
                </div>
                {fg.expanded ? (
                  <div className="commissionConfigTreeExpand">
                    <div className="commissionConfigToolbar" style={{ paddingTop: 8 }}>
                      <GridSearchInput
                        value={fg.employeeQuery}
                        onChange={(value) => setFuncaoEmployeeQuery(fg.funcaoKey, value)}
                        placeholder="Pesquisar funcionário…"
                        aria-label={`Pesquisar funcionários de ${fg.label}`}
                      />
                      {(() => {
                        const visible = fg.employees.filter((e) =>
                          matchesEmployeeSearch(e, fg.employeeQuery),
                        );
                        const allVisibleOn =
                          visible.length > 0 && visible.every((e) => e.include_in_commission);
                        return (
                          <button
                            type="button"
                            className="btn"
                            style={bulkBtnStyle}
                            disabled={visible.length === 0}
                            onClick={() => setFuncaoEmployeesSelected(fg.funcaoKey, !allVisibleOn)}
                          >
                            {allVisibleOn ? "Desmarcar todos" : "Marcar todos"}
                          </button>
                        );
                      })()}
                    </div>
                    {(() => {
                      const visible = fg.employees.filter((e) =>
                        matchesEmployeeSearch(e, fg.employeeQuery),
                      );
                      if (visible.length === 0) {
                        return (
                          <div className="muted" style={{ fontSize: 12, padding: "4px 0 6px" }}>
                            Nenhum funcionário para “{fg.employeeQuery}”.
                          </div>
                        );
                      }
                      return (
                        <div className="commissionConfigEmployeeGrid">
                          {visible.map((emp) => (
                            <label key={emp.id_funcionario} className="commissionConfigEmployeeLabel">
                              <input
                                type="checkbox"
                                checked={emp.include_in_commission}
                                onChange={() => toggleEmployee(emp.id_funcionario)}
                                style={{ width: 14, height: 14, flexShrink: 0, marginTop: 2 }}
                              />
                              <span style={{ minWidth: 0, wordBreak: "break-word" }}>
                                {employeeDisplayName(emp)}
                              </span>
                            </label>
                          ))}
                        </div>
                      );
                    })()}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        )}
      </section>

      <section
        className="solvenciaFilialCard commissionFilialCard commissionConfigSection"
        style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
      >
        <div className="commissionConfigSectionHead">
          <div className="sectionEyebrow">Pagamento</div>
          <h2 className="commissionFilialTitle">Modo padrão</h2>
        </div>
        <select
          value={paymentMode}
          onChange={(e) => setPaymentMode(e.target.value)}
          style={{
            padding: "8px 10px",
            borderRadius: 8,
            border: "1px solid var(--border)",
            background: "var(--card-bg)",
            color: "inherit",
            width: "100%",
            maxWidth: 360,
          }}
        >
          <option value="individual_sales">Individual por quantidade</option>
          <option value="team_total">Equipe (quantidade total)</option>
          <option value="equal_split">Rateio igual por equipe</option>
        </select>
        <div className="muted" style={{ fontSize: 11, marginTop: 6 }}>
          Define o modo padrão ao abrir a aba Comissões.
        </div>
      </section>

      <section
        className="solvenciaFilialCard commissionFilialCard commissionConfigSection"
        style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
      >
        <div className="commissionFilialHead commissionConfigSectionHead">
          <div>
            <div className="sectionEyebrow">Produtos</div>
            <h2 className="commissionFilialTitle">Grupos participantes</h2>
          </div>
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
          <div className="commissionConfigHint">
            Selecione os grupos que devem participar da comissão.
          </div>
        )}
        <div className="commissionConfigTree">
          {groups.map((g) => (
            <div key={g.id_grupo_produto}>
              <div className="commissionConfigTreeRow">
                <button
                  type="button"
                  className="commissionConfigExpandBtn"
                  onClick={() => toggleExpand(g.id_grupo_produto)}
                  aria-label={g.expanded ? "Recolher produtos" : "Expandir produtos"}
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
                <span className="commissionConfigTreeMeta">
                  {(g.faturamento_30d || 0) > 0 ? `${formatCurrency(g.faturamento_30d || 0)} /30d` : "—"}
                </span>
              </div>
              {g.expanded ? (
                <div className="commissionConfigTreeExpand">
                  {g.productsLoading ? (
                    <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>Carregando produtos…</div>
                  ) : (g.products || []).length === 0 ? (
                    <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>Nenhum produto neste grupo.</div>
                  ) : (
                    <>
                      <div className="commissionConfigToolbar" style={{ paddingTop: 8 }}>
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
                          <div className="commissionConfigEmployeeGrid">
                            {visible.map((p) => (
                              <label key={p.id_produto} className="commissionConfigEmployeeLabel">
                                <input
                                  type="checkbox"
                                  checked={!!p.selected}
                                  onChange={() => toggleProduct(g.id_grupo_produto, p.id_produto)}
                                  style={{ width: 14, height: 14, flexShrink: 0, marginTop: 2 }}
                                />
                                <span style={{ minWidth: 0, wordBreak: "break-word" }}>{p.nome}</span>
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
      </section>

      <section
        className="solvenciaFilialCard commissionFilialCard commissionConfigSection"
        style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
      >
        <div className="commissionConfigSectionHead">
          <div className="sectionEyebrow">Premiação</div>
          <h2 className="commissionFilialTitle">Níveis de comissão</h2>
        </div>
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
      </section>

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
