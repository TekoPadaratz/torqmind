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

  const [activeTab, setActiveTab] = useState<"overview" | "products" | "repricing" | "solvencia">("overview");
  const [sectorFilter, setSectorFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [solvenciaMonth, setSolvenciaMonth] = useState<number | null>(null);

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

  const { data: solvenciaData } = useBiScopeData<any>({
    moduleKey: `profit_solvencia:${solvenciaMonth ?? "atual"}`,
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) =>
      `/bi/profit-management/solvencia?${buildScopeParams(currentScope).toString()}${
        solvenciaMonth ? `&ano_mes=${solvenciaMonth}` : ""
      }`,
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
  const solvencia = solvenciaData?.data;

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
          {(["overview", "products", "repricing", "solvencia"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`profitTab${activeTab === tab ? " active" : ""}`}
            >
              {tab === "overview" ? "Visão Geral" : tab === "products" ? "Produtos" : tab === "repricing" ? "Oportunidades" : "Solvência"}
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

        {/* TAB: Solvência / Capital de Giro */}
        {activeTab === "solvencia" && (
          <div style={{ marginTop: 16 }}>
            {solvencia ? (
              <>
                {/* Cabeçalho + filtro de mês */}
                <div
                  className="card"
                  style={{ marginTop: 12, display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}
                >
                  <div>
                    <div className="sectionEyebrow">Solvência de Curto Prazo</div>
                    <div style={{ fontSize: 13, color: "var(--color-text-secondary)" }}>
                      Meus ativos cobrem as contas a pagar do mês?
                    </div>
                  </div>
                  <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 500 }}>
                    <span>Mês de referência:</span>
                    <select
                      value={solvenciaMonth ?? solvencia.ano_mes}
                      onChange={(e) => setSolvenciaMonth(Number(e.target.value))}
                      style={{ padding: "6px 12px", borderRadius: 8, border: "1px solid var(--color-border)", background: "var(--color-surface)", color: "inherit", fontSize: 13 }}
                    >
                      {(solvencia.meses_disponiveis || []).map((m: any) => (
                        <option key={m.ano_mes} value={m.ano_mes}>{m.label}</option>
                      ))}
                    </select>
                  </label>
                </div>

                {/* Veredito */}
                {(() => {
                  const idx = solvencia.indices || {};
                  const temAtivo = !!solvencia.tem_ativo_dados;
                  const cobre = !!idx.cobre_passivo;
                  const cor = !temAtivo ? "var(--color-info)" : cobre ? "var(--color-positive)" : "var(--color-negative)";
                  const titulo = !temAtivo
                    ? "Preparando a leitura dos seus ativos"
                    : cobre
                    ? "Seus ativos cobrem o passivo deste mês"
                    : "Atenção: os ativos não cobrem o passivo deste mês";
                  const detalhe = !temAtivo
                    ? "Estamos habilitando a coleta de caixa, banco, cheques e estoque. Por enquanto, veja abaixo o total de contas a pagar do mês selecionado."
                    : cobre
                    ? "O ativo circulante é suficiente para quitar as contas a pagar que vencem no mês, sem depender de novas vendas."
                    : "O ativo circulante disponível é menor que as contas a pagar do mês. Reforce o caixa ou renegocie vencimentos.";
                  return (
                    <div className="card" style={{ marginTop: 16, padding: 20, borderLeft: `4px solid ${cor}` }}>
                      <div style={{ fontSize: 16, fontWeight: 600, color: cor }}>{titulo}</div>
                      <div style={{ fontSize: 13, color: "var(--color-text-secondary)", marginTop: 6 }}>{detalhe}</div>
                    </div>
                  );
                })()}

                {/* Ativo Circulante x Passivo */}
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 16, marginTop: 16 }}>
                  <div className="card" style={{ padding: 20 }}>
                    <div className="sectionEyebrow">Ativo Circulante</div>
                    <div style={{ fontSize: 26, fontWeight: 700, marginTop: 8, color: "var(--color-positive)" }}>
                      {solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.circulante || 0) : "—"}
                    </div>
                    <table className="dreTable" style={{ marginTop: 12, fontSize: 12 }}>
                      <tbody>
                        <tr><td>Caixa (dinheiro)</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.caixa || 0) : "—"}</td></tr>
                        <tr><td>Banco</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.banco || 0) : "—"}</td></tr>
                        <tr><td>Cartões a compensar</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.cartoes || 0) : "—"}</td></tr>
                        <tr><td>Cheques a receber</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.cheques || 0) : "—"}</td></tr>
                        <tr><td>Estoque de combustível</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.estoque_combustivel || 0) : "—"}</td></tr>
                        <tr><td>Estoque de loja</td><td>{solvencia.tem_ativo_dados ? formatCurrency(solvencia.ativo?.estoque_loja || 0) : "—"}</td></tr>
                      </tbody>
                    </table>
                    {solvencia.cobertura_estoque && solvencia.cobertura_estoque.postos_total > 0 && (
                      <div className="calcFootnote" style={{ marginTop: 8 }}>
                        Estoque de combustível medido por sensor de tanque em {solvencia.cobertura_estoque.postos_com_combustivel} de {solvencia.cobertura_estoque.postos_total} postos
                        {solvencia.cobertura_estoque.postos_com_combustivel < solvencia.cobertura_estoque.postos_total
                          ? " — nos demais o sensor não está sincronizando, então o combustível não é contabilizado."
                          : "."}
                      </div>
                    )}
                  </div>

                  <div className="card" style={{ padding: 20 }}>
                    <div className="sectionEyebrow">Passivo do Mês (Contas a Pagar)</div>
                    <div style={{ fontSize: 26, fontWeight: 700, marginTop: 8, color: "var(--color-negative)" }}>
                      {formatCurrency(solvencia.passivo?.contas_pagar || 0)}
                    </div>
                    <table className="dreTable" style={{ marginTop: 12, fontSize: 12 }}>
                      <tbody>
                        <tr><td>Títulos em aberto</td><td>{solvencia.passivo?.qtd_titulos || 0}</td></tr>
                        <tr><td>Já vencido</td><td style={{ color: (solvencia.passivo?.vencido || 0) > 0 ? "var(--color-warning)" : "inherit" }}>{formatCurrency(solvencia.passivo?.vencido || 0)}</td></tr>
                        <tr><td>A vencer</td><td>{formatCurrency((solvencia.passivo?.contas_pagar || 0) - (solvencia.passivo?.vencido || 0))}</td></tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Índices */}
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 16, marginTop: 16 }}>
                  <div className="card" style={{ padding: 20 }}>
                    <div className="sectionEyebrow">Índice de Liquidez Corrente</div>
                    <div style={{ fontSize: 26, fontWeight: 700, marginTop: 8 }}>
                      {solvencia.tem_ativo_dados ? (solvencia.indices?.liquidez_corrente || 0).toFixed(2).replace(".", ",") : "—"}
                    </div>
                    <div className="calcFootnote">Ativo circulante ÷ passivo do mês. Igual ou acima de 1,00 significa que os ativos cobrem o passivo.</div>
                  </div>
                  <div className="card" style={{ padding: 20 }}>
                    <div className="sectionEyebrow">Capital de Giro Líquido</div>
                    <div style={{ fontSize: 26, fontWeight: 700, marginTop: 8, color: solvencia.tem_ativo_dados ? ((solvencia.indices?.capital_giro_liquido || 0) >= 0 ? "var(--color-positive)" : "var(--color-negative)") : "inherit" }}>
                      {solvencia.tem_ativo_dados ? formatCurrency(solvencia.indices?.capital_giro_liquido || 0) : "—"}
                    </div>
                    <div className="calcFootnote">Ativo circulante − passivo do mês. Positivo indica folga de caixa; negativo, aperto.</div>
                  </div>
                </div>

                {/* Detalhe por filial (consolidado) */}
                {solvencia.consolidado && (solvencia.por_filial?.length || 0) > 0 && (
                  <div className="card" style={{ marginTop: 16 }}>
                    <div className="sectionEyebrow">Por posto</div>
                    <div style={{ overflowX: "auto" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, marginTop: 8 }}>
                        <thead>
                          <tr style={{ borderBottom: "1px solid var(--color-border)", textAlign: "right" }}>
                            <th style={{ padding: "6px 4px", textAlign: "left" }}>Posto</th>
                            <th style={{ padding: "6px 4px" }}>Ativo circulante</th>
                            <th style={{ padding: "6px 4px" }}>Contas a pagar</th>
                            <th style={{ padding: "6px 4px" }}>Já vencido</th>
                            <th style={{ padding: "6px 4px" }}>Liquidez</th>
                            <th style={{ padding: "6px 4px" }}>Capital de giro</th>
                          </tr>
                        </thead>
                        <tbody>
                          {solvencia.por_filial.map((f: any) => (
                            <tr key={f.id_filial} style={{ borderBottom: "1px solid var(--color-border-subtle)", textAlign: "right" }}>
                              <td style={{ padding: "6px 4px", textAlign: "left" }}>{f.filial_label}</td>
                              <td style={{ padding: "6px 4px" }}>{f.tem_ativo_dados ? formatCurrency(f.ativo_circulante) : "—"}</td>
                              <td style={{ padding: "6px 4px" }}>{formatCurrency(f.passivo_contas_pagar)}</td>
                              <td style={{ padding: "6px 4px", color: f.passivo_vencido > 0 ? "var(--color-warning)" : "inherit" }}>{formatCurrency(f.passivo_vencido)}</td>
                              <td style={{ padding: "6px 4px" }}>{f.tem_ativo_dados ? f.liquidez_corrente.toFixed(2).replace(".", ",") : "—"}</td>
                              <td style={{ padding: "6px 4px", color: f.tem_ativo_dados ? (f.capital_giro_liquido >= 0 ? "var(--color-positive)" : "var(--color-negative)") : "inherit" }}>{f.tem_ativo_dados ? formatCurrency(f.capital_giro_liquido) : "—"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                <div className="calcFootnote" style={{ marginTop: 12 }}>
                  {solvencia.disclaimer}
                </div>
              </>
            ) : (
              <EmptyState
                title="Preparando a análise de solvência"
                detail="Estamos consolidando as contas a pagar e os ativos do mês. Volte em instantes."
              />
            )}
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
