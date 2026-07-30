"use client";

import { useMemo } from "react";
import { useSearchParams } from "next/navigation";
import {
  Bar,
  BarChart,
  Cell,
  CartesianGrid,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ChartTooltip from "../components/ui/ChartTooltip";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKeyShort,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { canAccessScreenKey } from "../lib/session";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import FinanceChequesSection from "./FinanceChequesSection";
import FinanceBudgetSection from "./FinanceBudgetSection";
import FinanceTitlesSection from "./FinanceTitlesSection";

export const dynamic = "force-dynamic";

type FinanceView = "overview" | "payable" | "receivable" | "cheques" | "budget";

const VIEW_SCREEN: Record<FinanceView, string> = {
  overview: "finance.overview",
  payable: "finance.payable",
  receivable: "finance.receivable",
  cheques: "finance.cheques",
  budget: "finance.budget",
};

const VIEW_TITLE: Record<FinanceView, string> = {
  overview: "Financeiro — Geral",
  payable: "Financeiro — Contas a pagar",
  receivable: "Financeiro — Contas a receber",
  cheques: "Financeiro — Cheques",
  budget: "Financeiro — Orçamento",
};

function resolveFinanceView(raw: string | null, claims: any): FinanceView {
  const requested = (raw || "overview").toLowerCase();
  const candidate: FinanceView =
    requested === "payable" ||
    requested === "receivable" ||
    requested === "cheques" ||
    requested === "budget"
      ? (requested as FinanceView)
      : "overview";
  if (canAccessScreenKey(claims, VIEW_SCREEN[candidate])) return candidate;
  const order: FinanceView[] = ["overview", "receivable", "payable", "cheques", "budget"];
  return order.find((v) => canAccessScreenKey(claims, VIEW_SCREEN[v])) || "overview";
}

const MIX_COLORS = ["#38bdf8", "#34d399", "#f59e0b", "#818cf8", "#fb7185", "#94a3b8"];

export default function FinancePage() {
  const scope = useScopeQuery();
  const searchParams = useSearchParams();
  useEnsureScopedProductUrl();
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "finance_overview",
      scope,
      errorMessage: "Falha ao carregar financeiro",
      buildRequestUrl: (currentScope, session) => {
        const view = resolveFinanceView(searchParams.get("view"), session);
        if (view === "cheques" || view === "budget") return null;
        return `/bi/finance/overview?${buildScopeParams(currentScope).toString()}&include_operational=false`;
      },
    });
  const view = resolveFinanceView(searchParams.get("view"), claims);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("financeiro")
    : buildModuleLoadingCopy("financeiro");
  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  const aging = data?.aging || {};
  const kpis = data?.kpis || {};
  const receberAberto = Number(kpis.receber_aberto ?? aging.receber_total_aberto ?? 0);
  const pagarAberto = Number(kpis.pagar_aberto ?? aging.pagar_total_aberto ?? 0);
  const receberVencido = Number(kpis.receber_vencido ?? aging.receber_total_vencido ?? 0);
  const pagarVencido = Number(kpis.pagar_vencido ?? aging.pagar_total_vencido ?? 0);
  const receberAVencer = Number(kpis.receber_a_vencer ?? 0);
  const pagarAVencer = Number(kpis.pagar_a_vencer ?? 0);

  const paymentsByDay = useMemo(
    () =>
      (data?.receipts_by_day?.by_day || [])
        .filter((row: any) => Number(row?.valor || 0) > 0)
        .map((row: any) => ({
          data: formatDateKeyShort(String(row?.data_key || "")),
          valor: Number(row?.valor || 0),
        })),
    [data],
  );

  const paymentMixChart = useMemo(() => {
    const rows = (data?.payments?.kpis?.mix || []).filter(
      (item: any) => Number(item.total_valor || 0) > 0,
    );
    const topRows = rows.slice(0, 5).map((item: any) => ({
      label: item.category_label || item.label || item.category,
      value: Number(item.total_valor || 0),
    }));
    const othersValue = rows
      .slice(5)
      .reduce((acc: number, item: any) => acc + Number(item.total_valor || 0), 0);
    if (othersValue > 0) topRows.push({ label: "Outras formas", value: othersValue });
    return topRows;
  }, [data]);

  if (view === "cheques") {
    return (
      <div>
        <AppNav title={VIEW_TITLE.cheques} userLabel={userLabel} />
        <div className="container">
          <div className="bi-grid" style={{ marginTop: 12 }}>
            <FinanceChequesSection />
          </div>
        </div>
      </div>
    );
  }

  if (view === "budget") {
    return (
      <div>
        <AppNav title={VIEW_TITLE.budget} userLabel={userLabel} />
        <div className="container">
          <div className="bi-grid" style={{ marginTop: 12 }}>
            <FinanceBudgetSection />
          </div>
        </div>
      </div>
    );
  }

  if (view === "payable" || view === "receivable") {
    const tipo = view === "payable" ? 0 : 1;
    return (
      <div>
        <AppNav title={VIEW_TITLE[view]} userLabel={userLabel} />
        <div className="container">
          {error ? <div className="card errorCard">{error}</div> : null}
          <div className="bi-grid" style={{ marginTop: 12 }}>
            {view === "receivable" ? (
              <>
                <div className="card kpi col-4">
                  <div className="label">Receber em aberto</div>
                  <div className="value">{loading ? "..." : formatCurrency(receberAberto)}</div>
                </div>
                <div className="card kpi col-4 riskCard">
                  <div className="label">Receber vencido</div>
                  <div className="value">{loading ? "..." : formatCurrency(receberVencido)}</div>
                </div>
                <div className="card kpi col-4">
                  <div className="label">Receber a vencer</div>
                  <div className="value">{loading ? "..." : formatCurrency(receberAVencer)}</div>
                </div>
              </>
            ) : (
              <>
                <div className="card kpi col-4">
                  <div className="label">Pagar em aberto</div>
                  <div className="value">{loading ? "..." : formatCurrency(pagarAberto)}</div>
                </div>
                <div className="card kpi col-4 riskCard">
                  <div className="label">Pagar vencido</div>
                  <div className="value">{loading ? "..." : formatCurrency(pagarVencido)}</div>
                </div>
                <div className="card kpi col-4">
                  <div className="label">Pagar a vencer</div>
                  <div className="value">{loading ? "..." : formatCurrency(pagarAVencer)}</div>
                </div>
              </>
            )}
            <FinanceTitlesSection
              tipo={tipo as 0 | 1}
              scope={scope}
              entidadeLabel={tipo === 0 ? "Fornecedor" : "Cliente"}
            />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <AppNav title={VIEW_TITLE.overview} userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={4}
              panels={2}
            />
          </div>
        ) : (
          <div className="bi-grid" style={{ marginTop: 12 }}>
            <div className="card kpi col-3">
              <div className="label">Receber em aberto</div>
              <div className="value">{loading ? "..." : formatCurrency(receberAberto)}</div>
            </div>
            <div className="card kpi col-3 riskCard">
              <div className="label">Receber vencido</div>
              <div className="value">{loading ? "..." : formatCurrency(receberVencido)}</div>
            </div>
            <div className="card kpi col-3">
              <div className="label">Pagar em aberto</div>
              <div className="value">{loading ? "..." : formatCurrency(pagarAberto)}</div>
            </div>
            <div className="card kpi col-3 riskCard">
              <div className="label">Pagar vencido</div>
              <div className="value">{loading ? "..." : formatCurrency(pagarVencido)}</div>
            </div>

            <div className="card col-7 chartCard">
              <h2>Recebimentos por dia</h2>
              {!loading && !paymentsByDay.length ? (
                <EmptyState title="Sem recebimentos no período." />
              ) : null}
              <div className="chartWrap" style={{ height: 260 }}>
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={paymentsByDay}>
                    <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
                    <XAxis dataKey="data" stroke="var(--muted)" />
                    <YAxis stroke="var(--muted)" tickFormatter={formatCurrency} width={112} />
                    <Tooltip
                      content={(props) => (
                        <ChartTooltip
                          {...props}
                          valueFormatter={(v) => formatCurrency(v)}
                        />
                      )}
                    />
                    <Bar dataKey="valor" name="Recebido" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="card col-5 chartCard">
              <h2>Mix por forma</h2>
              {!loading && !paymentMixChart.length ? (
                <EmptyState title="Sem mix no período." />
              ) : null}
              <div className="chartWrap" style={{ height: 260 }}>
                <ResponsiveContainer width="100%" height={260}>
                  <PieChart>
                    <Pie
                      data={paymentMixChart}
                      dataKey="value"
                      nameKey="label"
                      innerRadius={56}
                      outerRadius={88}
                      paddingAngle={3}
                    >
                      {paymentMixChart.map((entry: any, index: number) => (
                        <Cell
                          key={`${entry.label}-${index}`}
                          fill={MIX_COLORS[index % MIX_COLORS.length]}
                        />
                      ))}
                    </Pie>
                    <Tooltip
                      content={(props) => (
                        <ChartTooltip
                          {...props}
                          valueFormatter={(v) => formatCurrency(v)}
                        />
                      )}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
