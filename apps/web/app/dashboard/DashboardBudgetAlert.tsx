"use client";

import { useMemo } from "react";
import Link from "next/link";

import { formatCurrency } from "../lib/format";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

export default function DashboardBudgetAlert() {
  const scope = useScopeQuery();
  const { data } = useBiScopeData<any>({
    moduleKey: "dashboard_budget_alerts",
    scope,
    errorMessage: "",
    buildRequestUrl: (currentScope) =>
      `/bi/budget/alerts?${buildScopeParams(currentScope).toString()}`,
  });

  const alerts = useMemo(() => (data?.alerts || []).slice(0, 5), [data]);
  if (!alerts.length) return null;

  const estourados = alerts.filter((a: any) => a.status === "estourado").length;

  return (
    <div
      className={`card homeBlock dashboardBudgetAlert${estourados > 0 ? " is-over" : ""}`}
    >
      <div className="dashboardBudgetAlertHead">
        <strong>Orçamento de despesas</strong>
        <span className="muted">
          {estourados > 0
            ? `${estourados} conta(s) já passaram do teto neste mês.`
            : "Contas de despesa chegando ao limite do teto neste mês."}
        </span>
        <Link href="/finance" className="dashboardBudgetAlertLink">
          Ver no Financeiro →
        </Link>
      </div>
      <div className="dashboardBudgetAlertList">
        {alerts.map((a: any, i: number) => (
          <div
            key={`${a.id_filial}-${a.nome_conta}-${i}`}
            className={`dashboardBudgetAlertItem${a.status === "estourado" ? " is-over" : ""}`}
          >
            <div className="dashboardBudgetAlertName">
              {a.filial_label ? `${a.filial_label} · ` : ""}
              {a.nome_conta}
            </div>
            <div className="dashboardBudgetAlertPct">
              {Number(a.consumo_pct || 0).toFixed(0)}% do teto
            </div>
            <div className="muted dashboardBudgetAlertMeta">
              {formatCurrency(a.realizado)} de {formatCurrency(a.orcado)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
