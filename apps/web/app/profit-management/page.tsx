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
    buildRequestUrl: (currentScope) =>
      `/bi/profit-management/products?${buildScopeParams(currentScope).toString()}&limit=1000`,
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
    if (sectorFilter) {
      filtered = filtered.filter(
        (p: any) => p.setor === sectorFilter,
      );
    }
    if (statusFilter) {
      filtered = filtered.filter(
        (p: any) => p.status === statusFilter,
      );
    }
    return filtered;
  }, [products, searchTerm, sectorFilter, statusFilter]);

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

  const lucroColor = kpis.lucro_gerencial_estimado >= 0 ? "var(--color-positive)" : "var(--color-negative)";
  const margemColor = kpis.margem_gerencial_pct >= 0.05 ? "var(--color-positive)" : "var(--color-warning)";
  const despReceitaColor = kpis.desp_sobre_receita_pct < 0.15 ? "var(--color-positive)" : "var(--color-warning)";

  return (
    <div>
      <AppNav title="Gestão de Lucro" userLabel={userLabel} />
      <div className="container">
        {/* Period indicator */}
        <div className="muted" style={{ marginTop: 8, fontSize: 12 }}>
          Base: {overview.periodo_base} · Estimativa gerencial calculada automaticamente
        </div>

        {/* KPI Cards — responsive strip */}
        <div className="profitKpiStrip">
          <div className="profitKpiCard" style={{ "--kpi-accent": lucroColor } as React.CSSProperties}>
            <div className="profitKpiLabel">Lucro Gerencial Estimado</div>
            <div className="profitKpiValue" style={{ color: lucroColor }}>
              {formatCurrency(kpis.lucro_gerencial_estimado)}
            </div>
            <div className="profitKpiContext">
              {kpis.lucro_gerencial_estimado >= 0 ? "Resultado positivo" : "Atenção: resultado negativo"}
            </div>
          </div>
          <div className="profitKpiCard" style={{ "--kpi-accent": margemColor } as React.CSSProperties}>
            <div className="profitKpiLabel">Margem Gerencial</div>
            <div className="profitKpiValue" style={{ color: margemColor }}>
              {fmtPct(kpis.margem_gerencial_pct)}
            </div>
            <div className="profitKpiContext">Receita líq. − CMV − despesas</div>
          </div>
          <div className="profitKpiCard" style={{ "--kpi-accent": despReceitaColor } as React.CSSProperties}>
            <div className="profitKpiLabel">Despesa / Receita</div>
            <div className="profitKpiValue" style={{ color: despReceitaColor }}>
              {fmtPct(kpis.desp_sobre_receita_pct)}
            </div>
            <div className="profitKpiContext">Peso das despesas operacionais</div>
          </div>
          <div className="profitKpiCard" style={{ "--kpi-accent": "var(--color-info)" } as React.CSSProperties}>
            <div className="profitKpiLabel">Potencial Estimado 60d</div>
            <div className="profitKpiValue" style={{ color: "var(--color-info)" }}>
              {formatCurrency(kpis.impacto_positivo_60d)}
            </div>
            <div className="profitKpiContext">Oportunidade de repricing</div>
          </div>
          <div className="profitKpiCard" style={{ "--kpi-accent": kpis.produtos_com_reajuste > 0 ? "var(--color-warning)" : "var(--color-positive)" } as React.CSSProperties}>
            <div className="profitKpiLabel">Produtos c/ Reajuste</div>
            <div className="profitKpiValue" style={{ color: kpis.produtos_com_reajuste > 0 ? "var(--color-warning)" : "var(--color-positive)" }}>
              {kpis.produtos_com_reajuste || 0}
            </div>
            <div className="profitKpiContext">
              {kpis.produtos_abaixo_minimo > 0 ? `${kpis.produtos_abaixo_minimo} abaixo do mínimo` : "Todos acima do mínimo"}
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div className="profitTabs">
          {(["overview", "products", "repricing"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`profitTab${activeTab === tab ? " active" : ""}`}
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
                <table className="dreTable" style={{ marginTop: 8 }}>
                  <tbody>
                    {dre.linhas.map((l: any, i: number) => (
                      <tr
                        key={i}
                        className={l.tipo === "resultado" ? "dreRow-resultado" : l.tipo === "subtotal" ? "dreRow-subtotal" : ""}
                      >
                        <td>{l.label}</td>
                        <td
                          style={{
                            color: l.valor < 0 ? "var(--color-negative)" : l.tipo === "resultado" && l.valor >= 0 ? "var(--color-positive)" : "inherit",
                          }}
                        >
                          {formatCurrency(l.valor)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {dre.margem_gerencial_pct != null && (
                  <div className="calcFootnote">
                    Margem gerencial: {fmtPct(dre.margem_gerencial_pct)} · {dre.disclaimer || "Não é lucro contábil/fiscal oficial."}
                  </div>
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
                <div className="calcFootnote">
                  As despesas foram distribuídas conforme vencimento como competência. A baixa é informativa.
                </div>
              </div>
            )}
          </div>
        )}

        {/* TAB: Products */}
        {activeTab === "products" && (
          <div style={{ marginTop: 16 }}>
            {/* Filters */}
            <div className="profitFilterBar">
              <input
                type="text"
                placeholder="Buscar produto ou grupo..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
              <select
                value={sectorFilter}
                onChange={(e) => setSectorFilter(e.target.value)}
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
              >
                <option value="">Todos os status</option>
                <option value="abaixo_minimo">Abaixo do Mínimo</option>
                <option value="abaixo_ideal">Abaixo do Ideal</option>
                <option value="saudavel">Saudável</option>
                <option value="acima_meta">Acima da Meta</option>
              </select>
              <span className="profitFilterCount">
                {filteredProducts.length}{products?.total ? ` de ${products.total}` : ""} produtos
              </span>
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
                    <th style={{ textAlign: "right", padding: "8px 4px" }}>Potencial 60d</th>
                    <th style={{ textAlign: "center", padding: "8px 4px" }}>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredProducts.map((p: any, idx: number) => (
                    <tr key={`${p.id_produto}-${p.setor}-${idx}`} style={{ borderBottom: "1px solid var(--border)" }}>
                      <td style={{ padding: "6px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.nome_produto}</td>
                      <td style={{ padding: "6px 4px", textTransform: "capitalize" }}>{p.setor}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{Number(p.qtd_vendida).toFixed(0)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.receita)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.preco_atual)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.custo_unitario)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right", color: p.margem_bruta_pct >= 0.2 ? "var(--color-positive)" : p.margem_bruta_pct >= 0.1 ? "var(--color-warning)" : "var(--color-negative)" }}>
                        {fmtPct(p.margem_bruta_pct)}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(p.preco_ideal)}</td>
                      <td style={{ padding: "6px 4px", textAlign: "right", color: p.reajuste_pct > 0 ? "var(--color-warning)" : "var(--color-positive)" }}>
                        {p.reajuste_pct > 0 ? `+${fmtPct(p.reajuste_pct)}` : "—"}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "right", fontWeight: 500, color: p.impacto_60d > 0 ? "var(--color-info)" : "var(--color-neutral)" }}>
                        {p.impacto_60d > 0 ? formatCurrency(p.impacto_60d) : "—"}
                      </td>
                      <td style={{ padding: "6px 4px", textAlign: "center" }}>
                        <span
                          className="statusPill"
                          style={{
                            backgroundColor: `${STATUS_COLORS[p.status] || "#d1d5db"}18`,
                            color: STATUS_COLORS[p.status] || "var(--color-neutral)",
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
            <div className="calcFootnote">
              {products?.disclaimer || "Estimativa baseada no volume vendido no mês anterior. Assume manutenção do volume."}
            </div>
          </div>
        )}

        {/* TAB: Repricing */}
        {activeTab === "repricing" && (
          <div style={{ marginTop: 16 }}>
            {repricing?.oportunidades?.length > 0 ? (
              <>
                <div className="profitKpiStrip" style={{ gridTemplateColumns: "repeat(2, 1fr)" }}>
                  <div className="profitKpiCard" style={{ "--kpi-accent": "var(--color-info)" } as React.CSSProperties}>
                    <div className="profitKpiLabel">Potencial Total 60d</div>
                    <div className="profitKpiValue" style={{ color: "var(--color-info)" }}>
                      {formatCurrency(repricing.impacto_total_60d)}
                    </div>
                    <div className="profitKpiContext">Soma do impacto de todos os produtos listados</div>
                  </div>
                  <div className="profitKpiCard" style={{ "--kpi-accent": "var(--color-warning)" } as React.CSSProperties}>
                    <div className="profitKpiLabel">Oportunidades Identificadas</div>
                    <div className="profitKpiValue" style={{ color: "var(--color-warning)" }}>
                      {repricing.total_oportunidades}
                    </div>
                    <div className="profitKpiContext">Produtos com espaço para reajuste de preço</div>
                  </div>
                </div>

                <div className="card" style={{ overflowX: "auto", marginTop: 16 }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid var(--border)" }}>
                        <th style={{ textAlign: "left", padding: "8px 6px" }}>Produto</th>
                        <th style={{ textAlign: "left", padding: "8px 4px" }}>Setor</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Preço Atual</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Preço Ideal</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Reajuste</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Qtd/mês</th>
                        <th style={{ textAlign: "right", padding: "8px 4px" }}>Potencial 60d</th>
                      </tr>
                    </thead>
                    <tbody>
                      {repricing.oportunidades.map((op: any, idx: number) => (
                        <tr key={`${op.id_produto}-${idx}`} style={{ borderBottom: "1px solid var(--border)" }}>
                          <td style={{ padding: "6px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{op.nome_produto}</td>
                          <td style={{ padding: "6px 4px", textTransform: "capitalize" }}>{op.setor}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right" }}>{formatCurrency(op.preco_atual)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", color: "var(--color-insight)", fontWeight: 500 }}>{formatCurrency(op.preco_ideal)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", color: "var(--color-warning)", fontWeight: 500 }}>+{fmtPct(op.reajuste_pct)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right" }}>{Number(op.qtd_mes_anterior).toFixed(0)}</td>
                          <td style={{ padding: "6px 4px", textAlign: "right", fontWeight: 600, color: "var(--color-info)" }}>{formatCurrency(op.impacto_60d)}</td>
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
            <div className="calcFootnote" style={{ marginTop: 12 }}>
              {repricing?.disclaimer || "Estimativa baseada no volume vendido. Assume manutenção do volume. Não considera elasticidade de preço."}
            </div>
          </div>
        )}

        {/* Explanations */}
        <div className="card" style={{ marginTop: 24, padding: 16 }}>
          <div className="sectionEyebrow">Entenda os cálculos</div>
          <div className="calcFootnoteGrid">
            <div>
              <p><strong>Lucro Gerencial Estimado:</strong> Receita líquida − CMV − despesas operacionais rateadas. Não é lucro contábil/fiscal.</p>
              <p><strong>CMV:</strong> Custo da mercadoria vendida — quantidade × custo unitário no momento da venda.</p>
              <p><strong>Margem Bruta:</strong> Receita − CMV, antes das despesas operacionais.</p>
              <p><strong>Despesas Rateadas:</strong> Distribuídas proporcionalmente à participação de vendas de cada setor/produto.</p>
            </div>
            <div>
              <p><strong>Preço Mínimo:</strong> Custo unitário + despesa por unidade — abaixo disso, o produto gera prejuízo.</p>
              <p><strong>Preço Ideal:</strong> Preço mínimo ÷ (1 − margem desejada). Padrão: 30% conveniência, 8% combustível.</p>
              <p><strong>Potencial 60d:</strong> (Preço ideal − preço atual) × volume mensal × 2. Estima o ganho em 60 dias.</p>
              <p><strong>Importante:</strong> Estimativa gerencial para decisão de preço. Não substitui análise contábil/fiscal.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
