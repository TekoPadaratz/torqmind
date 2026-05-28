"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel, formatCurrency } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

export const dynamic = "force-dynamic";

/* eslint-disable @typescript-eslint/no-explicit-any */

const STATUS_LABELS: Record<string, string> = {
  abaixo_minimo: "Abaixo Mínimo",
  abaixo_ideal: "Abaixo Ideal",
  saudavel: "Saudável",
  acima_meta: "Acima Meta",
  sem_custo: "Sem Custo",
};

const STATUS_COLORS: Record<string, string> = {
  abaixo_minimo: "#ef4444",
  abaixo_ideal: "#f59e0b",
  saudavel: "#10b981",
  acima_meta: "#3b82f6",
  sem_custo: "#6b7280",
};

const CLASSIFICATION_LABELS: Record<string, string> = {
  pessoal: "Pessoal",
  comercial: "Comercial",
  administrativo: "Administrativo",
  financeiro: "Financeiro",
  tributos: "Tributos",
  perdas: "Perdas",
  nao_classificado: "Não Classif.",
};

const CLASSIFICATION_COLORS: Record<string, string> = {
  pessoal: "#6366f1",
  comercial: "#06b6d4",
  administrativo: "#8b5cf6",
  financeiro: "#f43f5e",
  tributos: "#eab308",
  perdas: "#64748b",
  nao_classificado: "#94a3b8",
};

function fmtPct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

export default function ProfitManagementPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();

  const [activeTab, setActiveTab] = useState<"overview" | "products" | "repricing">("overview");
  const [sectorFilter, setSectorFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [searchTerm, setSearchTerm] = useState("");

  const { claims, data: overviewData, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "profit_overview",
      scope,
      errorMessage: "Falha ao carregar Gestão de Lucro",
      buildRequestUrl: (currentScope) =>
        `/bi/profit-management/overview?${buildScopeParams(currentScope).toString()}`,
    });

  const { data: dreData } = useBiScopeData<any>({
    moduleKey: "profit_dre",
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) =>
      `/bi/profit-management/dre?${buildScopeParams(currentScope).toString()}`,
  });

  const { data: expensesData } = useBiScopeData<any>({
    moduleKey: "profit_expenses",
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) =>
      `/bi/profit-management/expenses?${buildScopeParams(currentScope).toString()}`,
  });

  const { data: productsData } = useBiScopeData<any>({
    moduleKey: "profit_products",
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) => {
      let url = `/bi/profit-management/products?${buildScopeParams(currentScope).toString()}`;
      if (sectorFilter) url += `&setor=${encodeURIComponent(sectorFilter)}`;
      if (statusFilter) url += `&status=${encodeURIComponent(statusFilter)}`;
      return url;
    },
  });

  const { data: repricingData } = useBiScopeData<any>({
    moduleKey: "profit_repricing",
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) =>
      `/bi/profit-management/repricing?${buildScopeParams(currentScope).toString()}`,
  });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("gestão de lucro")
    : buildModuleLoadingCopy("gestão de lucro");

  const overview = overviewData?.data;
  const dre = dreData?.data;
  const expenses = expensesData?.data;
  const products = productsData?.data;
  const repricing = repricingData?.data;

  const filteredProducts = useMemo(() => {
    if (!products?.produtos) return [];
    let filtered = products.produtos;
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      filtered = filtered.filter(
        (p: any) =>
          p.nome_produto?.toLowerCase().includes(term) ||
          p.grupo?.toLowerCase().includes(term),
      );
    }
    return filtered;
  }, [products, searchTerm]);

  const expenseChartData = useMemo(() => {
    if (!expenses?.categorias) return [];
    return expenses.categorias
      .filter((c: any) => c.valor > 0)
      .map((c: any) => ({
        name: CLASSIFICATION_LABELS[c.classificacao] || c.classificacao,
        value: c.valor,
        fill: CLASSIFICATION_COLORS[c.classificacao] || "#94a3b8",
      }));
  }, [expenses]);

  if (loading || pendingUnavailable) {
    return (
      <div>
        <AppNav title="Gestão de Lucro" userLabel={userLabel} />
        <div className="container">
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={5}
              panels={3}
            />
          </div>
        </div>
      </div>
    );
  }

  if (error || !overview) {
    return (
      <div>
        <AppNav title="Gestão de Lucro" userLabel={userLabel} />
        <div className="container" style={{ marginTop: 12 }}>
          <EmptyState
            title="Gestão de Lucro"
            detail={
              overviewData?.message ||
              "Ainda não há dados suficientes para calcular o Lucro Gerencial Estimado desta filial."
            }
          />
        </div>
      </div>
    );
  }

  const kpis = overview.kpis || {};

  return (
    <div>
      <AppNav title="Gestão de Lucro" userLabel={userLabel} />
      <div className="container">
        {/* Period indicator */}
        <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
          Base: {overview.periodo_base} · Estimativa gerencial calculada automaticamente
        </div>

        {/* KPI Cards */}
        <div className="bi-grid" style={{ marginTop: 16 }}>
          <div className="card kpi col-2">
            <div className="label">Lucro Gerencial Estimado</div>
            <div className="value" style={{ color: kpis.lucro_gerencial_estimado >= 0 ? "#10b981" : "#ef4444" }}>
              {formatCurrency(kpis.lucro_gerencial_estimado)}
            </div>
          </div>
          <div className="card kpi col-2">
            <div className="label">Margem Gerencial</div>
            <div className="value" style={{ color: kpis.margem_gerencial_pct >= 0.05 ? "#10b981" : "#f59e0b" }}>
              {fmtPct(kpis.margem_gerencial_pct)}
            </div>
          </div>
          <div className="card kpi col-2">
            <div className="label">Despesa / Receita</div>
            <div className="value" style={{ color: kpis.desp_sobre_receita_pct < 0.15 ? "#10b981" : "#f59e0b" }}>
              {fmtPct(kpis.desp_sobre_receita_pct)}
            </div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Impacto Potencial 60d</div>
            <div className="value" style={{ color: "#3b82f6" }}>
              {formatCurrency(kpis.impacto_positivo_60d)}
            </div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Produtos c/ Reajuste</div>
            <div className="value" style={{ color: kpis.produtos_com_reajuste > 0 ? "#f59e0b" : "#10b981" }}>
              {kpis.produtos_com_reajuste || 0}
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div style={{ marginTop: 24, display: "flex", gap: 16, borderBottom: "1px solid var(--border)" }}>
          {(["overview", "products", "repricing"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              style={{
                padding: "8px 4px",
                border: "none",
                background: "none",
                cursor: "pointer",
                fontWeight: activeTab === tab ? 600 : 400,
                borderBottom: activeTab === tab ? "2px solid #6366f1" : "2px solid transparent",
                color: activeTab === tab ? "#6366f1" : "var(--text-muted)",
              }}
            >
              {tab === "overview" ? "Visão Geral" : tab === "products" ? "Produtos" : "Oportunidades"}
            </button>
          ))}
        </div>

        {/* TAB: Overview */}
        {activeTab === "overview" && (
          <div style={{ marginTop: 16 }}>
            {/* DRE */}
            {dre?.linhas && (
              <div className="card" style={{ marginTop: 12 }}>
                <div className="sectionEyebrow">DRE Gerencial Resumida</div>
                <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 8 }}>
                  <tbody>
                    {dre.linhas.map((l: any, i: number) => (
                      <tr
                        key={i}
                        style={{
                          borderBottom: "1px solid var(--border)",
                          fontWeight: l.tipo === "resultado" || l.tipo === "subtotal" ? 600 : 400,
                          background: l.tipo === "resultado" ? "rgba(99,102,241,0.06)" : "transparent",
                        }}
                      >
                        <td style={{ padding: "6px 8px", fontSize: 13 }}>{l.label}</td>
                        <td
                          style={{
                            padding: "6px 8px",
                            textAlign: "right",
                            fontFamily: "monospace",
                            fontSize: 13,
                            color: l.valor < 0 ? "#ef4444" : l.tipo === "resultado" && l.valor >= 0 ? "#10b981" : "inherit",
                          }}
                        >
                          {formatCurrency(l.valor)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {dre.disclaimer && (
                  <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>{dre.disclaimer}</div>
                )}
              </div>
            )}

            {/* Expenses Chart */}
            {expenseChartData.length > 0 && (
              <div className="card" style={{ marginTop: 16 }}>
                <div className="sectionEyebrow">Peso das Despesas por Classificação</div>
                <div style={{ height: 240, marginTop: 8 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={expenseChartData} layout="vertical" margin={{ left: 90 }}>
                      <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                      <XAxis type="number" tickFormatter={(v) => formatCurrency(v)} />
                      <YAxis type="category" dataKey="name" width={90} tick={{ fontSize: 11 }} />
                      <Tooltip formatter={(v: number) => formatCurrency(v)} />
                      <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                        {expenseChartData.map((entry: any, idx: number) => (
                          <Cell key={idx} fill={entry.fill} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>
                  Despesas usam vencimento como competência. Pagamento/baixa não define o mês.
                </div>
              </div>
            )}
          </div>
        )}

        {/* TAB: Products */}
        {activeTab === "products" && (
          <div style={{ marginTop: 16 }}>
            {/* Filters */}
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
              <input
                type="text"
                placeholder="Buscar produto..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                style={{ padding: "6px 10px", fontSize: 13, border: "1px solid var(--border)", borderRadius: 6, background: "var(--bg-card)" }}
              />
              <select
                value={sectorFilter}
                onChange={(e) => setSectorFilter(e.target.value)}
                style={{ padding: "6px 10px", fontSize: 13, border: "1px solid var(--border)", borderRadius: 6, background: "var(--bg-card)" }}
              >
                <option value="">Todos os setores</option>
                <option value="conveniencia">Conveniência</option>
                <option value="combustivel">Combustível</option>
                <option value="automotivo">Automotivo</option>
                <option value="cigarro">Cigarro</option>
                <option value="servico">Serviço</option>
              </select>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                style={{ padding: "6px 10px", fontSize: 13, border: "1px solid var(--border)", borderRadius: 6, background: "var(--bg-card)" }}
              >
                <option value="">Todos os status</option>
                <option value="abaixo_minimo">Abaixo do Mínimo</option>
                <option value="abaixo_ideal">Abaixo do Ideal</option>
                <option value="saudavel">Saudável</option>
                <option value="acima_meta">Acima da Meta</option>
              </select>
            </div>

            {/* Products table */}
            <div className="card" style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr style={{ borderBottom: "2px solid var(--border)" }}>
                    <th style={{ textAlign: "left", padding: "8px 6px" }}>Produto</th>
                    <th style={{ textAlign: "left", padding: "8px 4px" }}>Setor</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Qtd</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Receita</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Preço</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Custo</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Margem</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>P.Ideal</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Reajuste</th>
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Impacto 60d</th>
                    <th style={{ textAlign: "center", padding: "8px 4px" }}>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredProducts.map((p: any) => (
                    <tr key={p.id_produto} style={{ borderBottom: "1px solid var(--border)" }}>
                      <td style={{ padding: "6px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.nome_produto}</td>
                      <td style={{ padding: "6px 4px", textTransform: "capitalize" }}>{p.setor}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{Number(p.qtd_vendida).toFixed(0)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.receita)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.preco_atual)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.custo_unitario)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right", color: p.margem_bruta_pct >= 0.2 ? "#10b981" : p.margem_bruta_pct >= 0.1 ? "#f59e0b" : "#ef4444" }}>
                        {fmtPct(p.margem_bruta_pct)}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.preco_ideal)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right", color: p.reajuste_pct > 0 ? "#f59e0b" : "#10b981" }}>
                        {p.reajuste_pct > 0 ? `+${fmtPct(p.reajuste_pct)}` : "—"}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "right", fontWeight: 500, color: p.impacto_60d > 0 ? "#3b82f6" : "#6b7280" }}>
                        {p.impacto_60d > 0 ? formatCurrency(p.impacto_60d) : "—"}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "center" }}>
                        <span
                          style={{
                            display: "inline-block",
                            padding: "2px 8px",
                            borderRadius: 12,
                            fontSize: 11,
                            fontWeight: 500,
                            backgroundColor: `${STATUS_COLORS[p.status] || "#d1d5db"}18`,
                            color: STATUS_COLORS[p.status] || "#6b7280",
                          }}
                        >
                          {STATUS_LABELS[p.status] || p.status}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredProducts.length === 0 && (
                <div className="muted" style={{ padding: 16, textAlign: "center" }}>
                  Nenhum produto encontrado com os filtros selecionados.
                </div>
              )}
            </div>
            {products?.disclaimer && (
              <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>{products.disclaimer}</div>
            )}
          </div>
        )}

        {/* TAB: Repricing */}
        {activeTab === "repricing" && (
          <div style={{ marginTop: 16 }}>
            {repricing?.oportunidades?.length > 0 ? (
              <>
                <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
                  <div className="card kpi col-4">
                    <div className="label">Impacto Total 60d</div>
                    <div className="value" style={{ color: "#3b82f6" }}>
                      {formatCurrency(repricing.impacto_total_60d)}
                    </div>
                  </div>
                  <div className="card kpi col-4">
                    <div className="label">Oportunidades</div>
                    <div className="value" style={{ color: "#f59e0b" }}>
                      {repricing.total_oportunidades}
                    </div>
                  </div>
                </div>

                <div className="card" style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid var(--border)" }}>
                        <th style={{ textAlign: "left", padding: "8px 6px" }}>Produto</th>
                        <th style={{ textAlign: "left", padding: "8px 4px" }}>Setor</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Preço Atual</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Preço Ideal</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Reajuste</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Qtd/mês</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Impacto 60d</th>
                      </tr>
                    </thead>
                    <tbody>
                      {repricing.oportunidades.map((op: any) => (
                        <tr key={op.id_produto} style={{ borderBottom: "1px solid var(--border)" }}>
                          <td style={{ padding: "6px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{op.nome_produto}</td>
                          <td style={{ padding: "6px 4px", textTransform: "capitalize" }}>{op.setor}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(op.preco_atual)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", color: "#6366f1", fontWeight: 500 }}>{formatCurrency(op.preco_ideal)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", color: "#f59e0b", fontWeight: 500 }}>+{fmtPct(op.reajuste_pct)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right" }}>{Number(op.qtd_mes_anterior).toFixed(0)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", fontWeight: 600, color: "#3b82f6" }}>{formatCurrency(op.impacto_60d)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <EmptyState
                title="Nenhuma oportunidade identificada"
                detail="Todos os produtos analisados estão com preço saudável ou acima da meta."
              />
            )}
            <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>
              {repricing?.disclaimer || "Estimativa baseada no volume vendido. Assume manutenção do volume. Não considera elasticidade de preço."}
            </div>
          </div>
        )}

        {/* Explanations */}
        <div className="card" style={{ marginTop: 24, padding: 16 }}>
          <div className="sectionEyebrow">Entenda os cálculos</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginTop: 8, fontSize: 11 }} className="muted">
            <div>
              <p><strong>Lucro Gerencial Estimado:</strong> Receita líquida − CMV − despesas operacionais rateadas.</p>
              <p><strong>CMV:</strong> Custo da Mercadoria Vendida = qtd × custo unitário no momento da venda.</p>
              <p><strong>Margem Bruta:</strong> Receita − CMV (antes das despesas).</p>
              <p><strong>Desp. Rateada:</strong> Proporcional à receita de cada setor/produto.</p>
            </div>
            <div>
              <p><strong>Preço Mínimo:</strong> Custo unitário + despesa por unidade (breakeven).</p>
              <p><strong>Preço Ideal:</strong> Preço mínimo ÷ (1 − margem desejada). Padrão: 30% conveniência.</p>
              <p><strong>Impacto 60d:</strong> (Preço ideal − preço atual) × qtd mês anterior × 2.</p>
              <p><strong>Atenção:</strong> Não é lucro contábil/fiscal. É gerencial para decisão de preço.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
