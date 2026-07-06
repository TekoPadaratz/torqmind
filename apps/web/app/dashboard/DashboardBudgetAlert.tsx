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
      className="card col-12"
      style={{
        marginTop: 12,
        borderColor: estourados > 0 ? "var(--color-negative)" : "var(--color-warning)",
        background: "rgba(184,115,51,0.06)",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        <span style={{ fontSize: 18 }}>{estourados > 0 ? "🔴" : "⚠️"}</span>
        <strong>Orçamento de despesas</strong>
        <span className="muted" style={{ fontSize: 12 }}>
          {estourados > 0
            ? `${estourados} conta(s) já passaram do teto neste mês.`
            : "Contas de despesa chegando ao limite do teto neste mês."}
        </span>
        <Link href="/finance" style={{ marginLeft: "auto", color: "var(--accent-copper)", fontSize: 12, fontWeight: 600 }}>
          Ver no Financeiro →
        </Link>
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 10 }}>
        {alerts.map((a: any, i: number) => (
          <div
            key={`${a.id_filial}-${a.nome_conta}-${i}`}
            style={{
              border: "1px solid var(--border)",
              borderRadius: 10,
              padding: "8px 12px",
              minWidth: 200,
              background: "rgba(255,255,255,0.02)",
            }}
          >
            <div style={{ fontSize: 12, fontWeight: 600 }}>
              {a.filial_label ? `${a.filial_label} · ` : ""}{a.nome_conta}
            </div>
            <div
              style={{
                fontWeight: 700,
                color: a.status === "estourado" ? "var(--color-negative)" : "var(--color-warning)",
              }}
            >
              {Number(a.consumo_pct || 0).toFixed(0)}% do teto
            </div>
            <div className="muted" style={{ fontSize: 11 }}>
              {formatCurrency(a.realizado)} de {formatCurrency(a.orcado)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
