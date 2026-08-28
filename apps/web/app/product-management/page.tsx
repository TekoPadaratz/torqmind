"use client";

import { Fragment, useCallback, useMemo, useState } from "react";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { apiGet } from "../lib/api";
import { buildUserLabel, formatCurrency } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { rowMatchesGridSearch, useGridSearch } from "../lib/use-grid-search";
import { canAccessScreenKey, readCachedSession } from "../lib/session";

export const dynamic = "force-dynamic";

const SCREEN_TITLE = "Gestão de Produtos";

type ProductRow = {
  id_filial: number;
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

function fmtQty(value: number): string {
  return Number(value || 0).toLocaleString("pt-BR", { maximumFractionDigits: 3 });
}

export default function ProductManagementPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const session = readCachedSession();
  const allowed = canAccessScreenKey(session, "product_management");

  const [diasSemVenda, setDiasSemVenda] = useState(7);
  const [setorFilter, setSetorFilter] = useState("");
  const [reloadNonce, setReloadNonce] = useState(0);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [purchaseCache, setPurchaseCache] = useState<Record<string, PurchaseRow[]>>({});
  const [purchaseLoading, setPurchaseLoading] = useState<string | null>(null);

  const { claims, data, error, loading, pendingUnavailable } = useBiScopeData<Payload>({
    moduleKey: `product_stock_idle:${diasSemVenda}:${setorFilter}:${reloadNonce}`,
    scope,
    errorMessage: "Falha ao carregar gestão de produtos",
    buildRequestUrl: (currentScope) => {
      if (!allowed || !currentScope.id_filial) return null;
      const p = buildScopeParams(currentScope);
      p.set("dias_sem_venda", String(diasSemVenda));
      if (setorFilter) p.set("setor", setorFilter);
      p.set("limit", "3000");
      return `/bi/operations/product-stock-idle?${p.toString()}`;
    },
  });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("gestão de produtos")
    : buildModuleLoadingCopy("gestão de produtos");

  const rows = data?.produtos || [];
  const { query, setQuery, filteredRows } = useGridSearch(rows, {
    excludeKeys: /^id_/,
  });

  const setorOptions = useMemo(() => {
    const fromApi = data?.setores || [];
    if (fromApi.length > 0) return fromApi;
    const keys = new Set(rows.map((r) => r.setor));
    return Array.from(keys).map((k) => ({ key: k, label: k }));
  }, [data?.setores, rows]);

  const toggleExpand = useCallback(
    async (row: ProductRow) => {
      const key = `${row.id_filial}:${row.id_produto}`;
      if (expandedKey === key) {
        setExpandedKey(null);
        return;
      }
      setExpandedKey(key);
      if (purchaseCache[key]) return;
      setPurchaseLoading(key);
      try {
        const p = buildScopeParams({ ...scope, id_filial: String(row.id_filial) });
        p.set("id_produto", String(row.id_produto));
        const resp = await apiGet(`/bi/operations/product-stock-idle/purchases?${p.toString()}`);
        setPurchaseCache((prev) => ({
          ...prev,
          [key]: (resp?.compras || []) as PurchaseRow[],
        }));
      } finally {
        setPurchaseLoading(null);
      }
    },
    [expandedKey, scope, purchaseCache],
  );

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
              <span className="muted">dias · {data?.total ?? rows.length} produto(s)</span>
            </div>
          </div>
          <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>
            Produtos com estoque positivo e sem venda há pelo menos o número de dias abaixo. Clique na linha para ver as
            últimas 3 notas de compra.
          </p>

          {!scope.ready || !scope.id_filial ? (
            <ScopeTransitionState headline={transitionCopy.headline} detail={transitionCopy.detail} />
          ) : (loading || pendingUnavailable) && !data ? (
            <div className="muted" style={{ padding: 24 }}>{transitionCopy.detail}</div>
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

              {filteredRows.length === 0 ? (
                <EmptyState
                  title="Nenhum produto parado"
                  detail={`Não há produtos com estoque e ≥ ${diasSemVenda} dias sem venda nesta filial.`}
                />
              ) : (
                <div className="tableScroll">
                  <table className="table compact" style={{ fontSize: 13 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left" }}>Produto</th>
                        <th style={{ textAlign: "left" }}>Setor</th>
                        <th style={{ textAlign: "right" }}>Dias s/ venda</th>
                        <th style={{ textAlign: "right" }}>Qtd</th>
                        <th style={{ textAlign: "right" }}>Custo médio</th>
                        <th style={{ textAlign: "right" }}>Custo total</th>
                        <th style={{ textAlign: "right" }}>Vlr venda</th>
                        <th style={{ textAlign: "right" }}>Receita total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((row) => {
                        const key = `${row.id_filial}:${row.id_produto}`;
                        const expanded = expandedKey === key;
                        const purchases = purchaseCache[key] || [];
                        return (
                          <Fragment key={key}>
                            <tr
                              onClick={() => toggleExpand(row)}
                              style={{ cursor: "pointer", borderBottom: "1px solid var(--table-row-border)" }}
                              className={expanded ? "commissionConfigTreeRow" : undefined}
                            >
                              <td style={{ padding: "8px 6px" }}>
                                <span style={{ marginRight: 6, opacity: 0.7 }}>{expanded ? "▾" : "▸"}</span>
                                {row.nome_produto}
                              </td>
                              <td style={{ padding: "8px 6px", textTransform: "capitalize" }}>
                                {row.setor_label || row.setor}
                              </td>
                              <td style={{ padding: "8px 6px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                                {row.dias_sem_venda >= 9999 ? "—" : row.dias_sem_venda}
                              </td>
                              <td style={{ padding: "8px 6px", textAlign: "right" }}>{fmtQty(row.qtd_estoque)}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.custo_medio)}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.custo_medio_total)}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.preco_venda)}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right" }}>{formatCurrency(row.receita_total)}</td>
                            </tr>
                            {expanded ? (
                              <tr key={`${key}-detail`}>
                                <td colSpan={8} style={{ padding: "0 12px 12px 32px", background: "var(--surface-faint)" }}>
                                  {purchaseLoading === key ? (
                                    <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>Carregando compras…</div>
                                  ) : purchases.length === 0 ? (
                                    <div className="muted" style={{ fontSize: 12, padding: "8px 0" }}>
                                      Sem notas de compra recentes para este produto.
                                    </div>
                                  ) : (
                                    <table className="table compact" style={{ fontSize: 12, marginTop: 8 }}>
                                      <thead>
                                        <tr>
                                          <th style={{ textAlign: "left" }}>Documento</th>
                                          <th style={{ textAlign: "left" }}>Data</th>
                                          <th style={{ textAlign: "right" }}>Qtd</th>
                                          <th style={{ textAlign: "right" }}>Valor</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {purchases.map((c) => (
                                          <tr key={c.rank}>
                                            <td>{c.numero_documento || "—"}</td>
                                            <td>{c.data_compra || "—"}</td>
                                            <td style={{ textAlign: "right" }}>{fmtQty(c.qtd)}</td>
                                            <td style={{ textAlign: "right" }}>{formatCurrency(c.valor_total)}</td>
                                          </tr>
                                        ))}
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
              )}
            </>
          )}
        </section>
      </div>
    </>
  );
}
