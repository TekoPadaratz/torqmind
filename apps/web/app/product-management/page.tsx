"use client";

import { Fragment, useCallback, useMemo, useState } from "react";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { apiGet } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { buildUserLabel, formatCurrency } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";
import { canAccessScreenKey, readCachedSession } from "../lib/session";

export const dynamic = "force-dynamic";

const SCREEN_TITLE = "Gestão de Produtos";
const NEVER_SOLD_DAYS = 9999;

type ProductRow = {
  id_filial: number;
  filial_label?: string;
  id_produto: number;
  nome_produto: string;
  setor: string;
  setor_label: string;
  qtd_estoque: number;
  dias_sem_venda: number;
  last_sale_date?: string | null;
  custo_medio: number;
  custo_medio_total: number;
  preco_venda: number;
  receita_total: number;
};

type PurchaseRow = {
  rank: number;
  numero_documento: string;
  data_compra: string;
  qtd: number;
  valor_unitario: number;
  valor_total: number;
};

type Payload = {
  min_dias_sem_venda?: number;
  total?: number;
  produtos?: ProductRow[];
  setores?: { key: string; label: string }[];
};

type SortKey =
  | "nome_produto"
  | "setor_label"
  | "last_sale_date"
  | "dias_sem_venda"
  | "qtd_estoque"
  | "custo_medio"
  | "custo_medio_total"
  | "preco_venda"
  | "receita_total";

type SortDir = "asc" | "desc";

function fmtQty(value: number): string {
  return Number(value || 0).toLocaleString("pt-BR", { maximumFractionDigits: 3 });
}

function hasBranchScope(scope: {
  id_filial?: string | null;
  id_filiais?: string[];
  branch_scope?: string | null;
}): boolean {
  if (scope.branch_scope === "all") return true;
  if (scope.id_filial) return true;
  return (scope.id_filiais?.length || 0) > 0;
}

function diasSortValue(dias: number): number {
  return dias >= NEVER_SOLD_DAYS ? NEVER_SOLD_DAYS : dias;
}

function fmtDateBr(iso: string | null | undefined): string {
  if (!iso) return "—";
  const raw = String(iso).split("T")[0];
  const [y, m, d] = raw.split("-");
  if (!y || !m || !d) return raw;
  return `${d}/${m}/${y}`;
}

function compareProductRows(a: ProductRow, b: ProductRow, key: SortKey, dir: SortDir): number {
  const sign = dir === "asc" ? 1 : -1;

  if (key === "last_sale_date") {
    const av = a.last_sale_date || "";
    const bv = b.last_sale_date || "";
    if (!av && !bv) return a.nome_produto.localeCompare(b.nome_produto, "pt-BR");
    if (!av) return 1;
    if (!bv) return -1;
    const cmp = av.localeCompare(bv);
    if (cmp !== 0) return sign * cmp;
    return a.nome_produto.localeCompare(b.nome_produto, "pt-BR");
  }

  if (key === "dias_sem_venda") {
    const tierA = a.dias_sem_venda >= NEVER_SOLD_DAYS ? 1 : 0;
    const tierB = b.dias_sem_venda >= NEVER_SOLD_DAYS ? 1 : 0;
    if (tierA !== tierB) return sign * (tierA - tierB);
    const cmp = diasSortValue(a.dias_sem_venda) - diasSortValue(b.dias_sem_venda);
    if (cmp !== 0) return sign * cmp;
    return a.nome_produto.localeCompare(b.nome_produto, "pt-BR");
  }

  const numericKeys: SortKey[] = [
    "qtd_estoque",
    "custo_medio",
    "custo_medio_total",
    "preco_venda",
    "receita_total",
  ];
  if (numericKeys.includes(key)) {
    const cmp = Number(a[key] || 0) - Number(b[key] || 0);
    if (cmp !== 0) return sign * cmp;
    return a.nome_produto.localeCompare(b.nome_produto, "pt-BR");
  }

  const av = String(a[key] || "");
  const bv = String(b[key] || "");
  const cmp = av.localeCompare(bv, "pt-BR");
  if (cmp !== 0) return sign * cmp;
  return a.id_produto - b.id_produto;
}

function buildPurchaseScopeParams(
  scope: ReturnType<typeof useScopeQuery>,
  row: ProductRow,
): URLSearchParams {
  const p = new URLSearchParams();
  if (scope.id_empresa) p.set("id_empresa", String(scope.id_empresa));
  p.set("id_filial", String(row.id_filial));
  p.set("id_produto", String(row.id_produto));
  return p;
}

export default function ProductManagementPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const session = readCachedSession();
  const allowed = canAccessScreenKey(session, "product_management");
  const branchReady = scope.ready && hasBranchScope(scope);

  const [diasSemVenda, setDiasSemVenda] = useState(7);
  const [setorFilter, setSetorFilter] = useState("");
  const [reloadNonce, setReloadNonce] = useState(0);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [purchaseCache, setPurchaseCache] = useState<Record<string, PurchaseRow[]>>({});
  const [purchaseLoading, setPurchaseLoading] = useState<string | null>(null);
  const [purchaseError, setPurchaseError] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("dias_sem_venda");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const { claims, data, error, loading, pendingUnavailable } = useBiScopeData<Payload>({
    moduleKey: `product_stock_idle:${diasSemVenda}:${setorFilter}:${reloadNonce}`,
    scope,
    keepPreviousData: true,
    requestTimeoutMs: 30_000,
    errorMessage: "Falha ao carregar gestão de produtos",
    buildRequestUrl: (currentScope) => {
      if (!allowed || !hasBranchScope(currentScope)) return null;
      const p = buildScopeParams(currentScope);
      p.set("dias_sem_venda", String(diasSemVenda));
      if (setorFilter) p.set("setor", setorFilter);
      p.set("limit", "5000");
      return `/bi/operations/product-stock-idle?${p.toString()}`;
    },
  });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("gestão de produtos")
    : buildModuleLoadingCopy("gestão de produtos");

  const rows = data?.produtos || [];

  const { query, setQuery, filteredRows: searchedRows } = useGridSearch(rows, {
    excludeKeys: /^id_/,
  });

  const sortedRows = useMemo(() => {
    const copy = [...searchedRows];
    copy.sort((a, b) => compareProductRows(a, b, sortKey, sortDir));
    return copy;
  }, [searchedRows, sortKey, sortDir]);

  const filialGroups = useMemo(() => {
    const byFilial = new Map<number, { id_filial: number; label: string; products: ProductRow[] }>();
    for (const row of sortedRows) {
      const id = row.id_filial;
      const cur = byFilial.get(id) || {
        id_filial: id,
        label: row.filial_label || `Filial ${id}`,
        products: [],
      };
      cur.products.push(row);
      byFilial.set(id, cur);
    }
    return Array.from(byFilial.values()).sort((a, b) =>
      a.label.localeCompare(b.label, "pt-BR", { numeric: true, sensitivity: "base" }),
    );
  }, [sortedRows]);

  const setorOptions = useMemo(() => {
    const fromApi = data?.setores || [];
    if (fromApi.length > 0) return fromApi;
    const keys = new Set(rows.map((r) => r.setor));
    return Array.from(keys).map((k) => ({ key: k, label: k }));
  }, [data?.setores, rows]);

  const onSortColumn = useCallback((key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      return;
    }
    setSortKey(key);
    setSortDir(
      key === "dias_sem_venda" || key === "nome_produto" || key === "setor_label"
        ? "asc"
        : "desc",
    );
  }, [sortKey]);

  const sortIndicator = useCallback(
    (key: SortKey) => {
      if (sortKey !== key) return "";
      return sortDir === "asc" ? " ▲" : " ▼";
    },
    [sortDir, sortKey],
  );

  const toggleExpand = useCallback(
    async (row: ProductRow) => {
      const key = `${row.id_filial}:${row.id_produto}`;
      if (expandedKey === key) {
        setExpandedKey(null);
        return;
      }
      setExpandedKey(key);
      setPurchaseError("");
      if (purchaseCache[key]) return;
      setPurchaseLoading(key);
      try {
        const p = buildPurchaseScopeParams(scope, row);
        const resp = await apiGet(`/bi/operations/product-stock-idle/purchases?${p.toString()}`, {
          timeout: 20_000,
        });
        setPurchaseCache((prev) => ({
          ...prev,
          [key]: (resp?.compras || []) as PurchaseRow[],
        }));
      } catch (err: unknown) {
        setPurchaseError(extractApiError(err, "Falha ao carregar compras."));
        setExpandedKey(null);
      } finally {
        setPurchaseLoading(null);
      }
    },
    [expandedKey, scope, purchaseCache],
  );

  const truncated = (data?.total || 0) > sortedRows.length;

  if (!allowed && session) {
    return (
      <>
        <AppNav title={SCREEN_TITLE} userLabel={userLabel} />
        <div className="container" style={{ marginTop: 24 }}>
          <EmptyState title="Sem permissão" detail="Você não tem acesso à Gestão de Produtos." />
        </div>
      </>
    );
  }

  return (
    <>
      <AppNav title={SCREEN_TITLE} userLabel={userLabel} />
      <div className="container" style={{ marginTop: 16 }}>
        <section
          className="solvenciaFilialCard commissionFilialCard"
          style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
        >
          <div className="commissionFilialHead">
            <div>
              <div className="sectionEyebrow">Operação</div>
              <h1 className="commissionFilialTitle">{SCREEN_TITLE}</h1>
            </div>
            <div className="commissionFilialSummary">
              <span className="muted">Parados ≥</span>
              <strong>{diasSemVenda}</strong>
              <span className="muted">dias · {data?.total ?? sortedRows.length} produto(s)</span>
            </div>
          </div>
          <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>
            Produtos com estoque e sem venda há pelo menos o número de dias abaixo (com histórico de venda).
            Clique na linha para ver as últimas 3 notas de compra.
          </p>

          {!branchReady ? (
            <ScopeTransitionState
              mode={scope.ready ? "unavailable" : "loading"}
              headline={scope.ready ? "Selecione o escopo de filiais" : transitionCopy.headline}
              detail={
                scope.ready
                  ? "Escolha uma filial, várias filiais ou Todas no painel lateral para carregar os produtos parados."
                  : transitionCopy.detail
              }
            />
          ) : (loading || pendingUnavailable) && !data ? (
            <ScopeTransitionState mode="loading" headline={transitionCopy.headline} detail={transitionCopy.detail} />
          ) : error ? (
            <EmptyState title="Falha ao carregar" detail={String(error)} />
          ) : (
            <>
              <div className="commissionConfigToolbar" style={{ marginBottom: 12 }}>
                <GridSearchInput value={query} onChange={setQuery} />
                <label className="muted" style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
                  Dias sem venda ≥
                  <input
                    type="number"
                    min={0}
                    max={3650}
                    value={diasSemVenda}
                    onChange={(e) => setDiasSemVenda(Math.max(0, Number(e.target.value) || 0))}
                    className="input"
                    style={{ width: 72, padding: "6px 8px" }}
                  />
                </label>
                <select
                  value={setorFilter}
                  onChange={(e) => setSetorFilter(e.target.value)}
                  className="input"
                  style={{ minWidth: 160, padding: "6px 10px" }}
                >
                  <option value="">Todos os setores</option>
                  {setorOptions.map((s) => (
                    <option key={s.key} value={s.key}>{s.label}</option>
                  ))}
                </select>
                <button type="button" className="btn" onClick={() => setReloadNonce((n) => n + 1)} style={{ fontSize: 12 }}>
                  Atualizar
                </button>
              </div>

              {truncated ? (
                <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
                  Exibindo {sortedRows.length} de {data?.total} produtos. Refine o setor ou a filial para ver todos.
                </div>
              ) : null}

              {purchaseError ? (
                <div className="card errorCard" style={{ marginBottom: 12 }}>{purchaseError}</div>
              ) : null}

              {sortedRows.length === 0 ? (
                <EmptyState
                  title="Nenhum produto parado"
                  detail={`Não há produtos com estoque e ≥ ${diasSemVenda} dias sem venda no escopo selecionado.`}
                />
              ) : (
                filialGroups.map((group) => (
                  <section
                    key={`${group.id_filial}:${group.label}`}
                    className="solvenciaFilialCard commissionFilialCard"
                    style={{ marginTop: 12, borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
                  >
                    <div className="commissionFilialHead">
                      <div>
                        <div className="sectionEyebrow">Filial</div>
                        <h2 className="commissionFilialTitle">{group.label}</h2>
                      </div>
                      <div className="commissionFilialSummary">
                        <span className="muted">{group.products.length} produto(s)</span>
                      </div>
                    </div>
                    <div className="tableScroll">
                      <table className="table compact" style={{ fontSize: 13 }}>
                        <thead>
                          <tr>
                            <th style={{ textAlign: "left", cursor: "pointer" }} onClick={() => onSortColumn("nome_produto")}>
                              Produto{sortIndicator("nome_produto")}
                            </th>
                            <th style={{ textAlign: "left", cursor: "pointer" }} onClick={() => onSortColumn("setor_label")}>
                              Setor{sortIndicator("setor_label")}
                            </th>
                            <th style={{ textAlign: "left", cursor: "pointer" }} onClick={() => onSortColumn("last_sale_date")}>
                              Última venda{sortIndicator("last_sale_date")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("dias_sem_venda")}>
                              Dias s/ venda{sortIndicator("dias_sem_venda")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("qtd_estoque")}>
                              Qtd{sortIndicator("qtd_estoque")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("custo_medio")}>
                              Custo unitário{sortIndicator("custo_medio")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("custo_medio_total")}>
                              Custo total{sortIndicator("custo_medio_total")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("preco_venda")}>
                              Vlr venda{sortIndicator("preco_venda")}
                            </th>
                            <th style={{ textAlign: "right", cursor: "pointer" }} onClick={() => onSortColumn("receita_total")}>
                              Receita total{sortIndicator("receita_total")}
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {group.products.map((row) => {
                            const key = `${row.id_filial}:${row.id_produto}`;
                            const expanded = expandedKey === key;
                            const purchases = purchaseCache[key] || [];
                            return (
                              <Fragment key={key}>
                                <tr
                                  onClick={() => void toggleExpand(row)}
                                  style={{
                                    cursor: "pointer",
                                    borderBottom: "1px solid var(--table-row-border)",
                                    background: expanded ? "var(--surface-faint)" : undefined,
                                  }}
                                >
                                  <td style={{ padding: "8px 6px" }}>
                                    <span style={{ marginRight: 6, opacity: 0.7 }}>{expanded ? "▾" : "▸"}</span>
                                    {row.nome_produto}
                                  </td>
                                  <td style={{ padding: "8px 6px", textTransform: "capitalize" }}>
                                    {row.setor_label || row.setor}
                                  </td>
                                  <td style={{ padding: "8px 6px", fontVariantNumeric: "tabular-nums" }}>
                                    {fmtDateBr(row.last_sale_date)}
                                  </td>
                                  <td style={{ padding: "8px 6px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                                    {row.dias_sem_venda >= NEVER_SOLD_DAYS ? "—" : row.dias_sem_venda}
                                  </td>
                                  <td style={{ padding: "8px 6px", textAlign: "right" }}>{fmtQty(row.qtd_estoque)}</td>
                                  <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.custo_medio)}</td>
                                  <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.custo_medio_total)}</td>
                                  <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.preco_venda)}</td>
                                  <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.receita_total)}</td>
                                </tr>
                                {expanded ? (
                                  <tr>
                                    <td colSpan={9} className="commissionConfigTreeExpand">
                                      {purchaseLoading === key ? (
                                        <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>Carregando compras…</div>
                                      ) : purchases.length === 0 ? (
                                        <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>
                                          Sem notas de compra recentes para este produto.
                                        </div>
                                      ) : (
                                        <table className="table compact" style={{ fontSize: 12, marginTop: 8, width: "100%" }}>
                                          <thead>
                                            <tr>
                                              <th style={{ textAlign: "left" }}>Nota Fiscal</th>
                                              <th style={{ textAlign: "left" }}>Data</th>
                                              <th style={{ textAlign: "right" }}>Qtd</th>
                                              <th style={{ textAlign: "right" }}>Custo unitário</th>
                                              <th style={{ textAlign: "right" }}>Valor total</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {purchases.map((c) => {
                                              const qtd = Number(c.qtd || 0);
                                              const unit =
                                                Number(c.valor_unitario || 0) > 0
                                                  ? Number(c.valor_unitario)
                                                  : qtd > 0
                                                    ? Number(c.valor_total || 0) / qtd
                                                    : 0;
                                              return (
                                              <tr key={c.rank}>
                                                <td>{c.numero_documento || "—"}</td>
                                                <td>{fmtDateBr(c.data_compra)}</td>
                                                <td style={{ textAlign: "right" }}>{fmtQty(qtd)}</td>
                                                <td style={{ textAlign: "right" }}>{formatCurrency(unit)}</td>
                                                <td style={{ textAlign: "right" }}>{formatCurrency(c.valor_total)}</td>
                                              </tr>
                                              );
                                            })}
                                          </tbody>
                                        </table>
                                      )}
                                    </td>
                                  </tr>
                                ) : null}
                              </Fragment>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </section>
                ))
              )}
            </>
          )}
        </section>
      </div>
    </>
  );
}
