"use client";

import { useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import MonthYearSelect from "../components/ui/MonthYearSelect";
import BudgetConfigTab from "../goals/BudgetConfigTab";
import { formatCurrency } from "../lib/format";
import { currentAnoMesSP, splitAnoMes } from "../lib/month-year.mjs";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";

const STATUS_STYLE: Record<string, { label: string; color: string }> = {
  ok: { label: "No orçamento", color: "var(--color-positive)" },
  alerta: { label: "Em alerta", color: "var(--color-warning)" },
  estourado: { label: "Estourado", color: "var(--color-negative)" },
};

type BudgetTab = "resultado" | "configurar";

export default function FinanceBudgetSection() {
  const scope = useScopeQuery();
  const [tab, setTab] = useState<BudgetTab>("resultado");
  const [anoMes, setAnoMes] = useState(() => currentAnoMesSP());
  const { year: ano, month: mes } = splitAnoMes(anoMes);

  const { data, loading } = useBiScopeData<any>({
    moduleKey: `budget_overview_${ano}_${mes}`,
    scope,
    errorMessage: "Falha ao carregar orçamento",
    buildRequestUrl: (currentScope) => {
      if (tab !== "resultado") return null;
      return `/bi/budget/overview?${buildScopeParams(currentScope).toString()}&ano=${ano}&mes=${mes}`;
    },
  });

  const contas = useMemo(() => data?.contas || [], [data]);
  const { query, setQuery, filteredRows } = useGridSearch(contas as Record<string, unknown>[]);
  const summary = data?.summary || {};
  const showFilial = true;
  const idEmpresa = scope.id_empresa != null ? Number(scope.id_empresa) : null;
  const idFilial =
    scope.id_filial != null
      ? Number(scope.id_filial)
      : Array.isArray(scope.id_filiais) && scope.id_filiais.length === 1
        ? Number(scope.id_filiais[0])
        : null;

  return (
    <div className="card col-12" style={{ marginTop: 16 }}>
      <div className="sectionEyebrow">Gestão Orçamentária</div>
      <h2 style={{ marginTop: 4 }}>Orçamento de despesas</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Acompanhe realizado × orçado e configure os tetos por conta gerencial (1 filial).
      </div>

      <div className="presetFilterChips" style={{ marginTop: 14 }} role="tablist" aria-label="Abas orçamento">
        <button
          type="button"
          className={`presetFilterChip${tab === "resultado" ? " is-active" : ""}`}
          onClick={() => setTab("resultado")}
        >
          Resultado
        </button>
        <button
          type="button"
          className={`presetFilterChip${tab === "configurar" ? " is-active" : ""}`}
          onClick={() => setTab("configurar")}
        >
          Configurar
        </button>
      </div>

      {tab === "configurar" ? (
        <div style={{ marginTop: 16 }}>
          <BudgetConfigTab idEmpresa={idEmpresa} idFilial={idFilial} />
        </div>
      ) : (
        <>
          <div style={{ display: "flex", gap: 8, marginTop: 14, alignItems: "center", flexWrap: "wrap" }}>
            <MonthYearSelect
              value={anoMes}
              onChange={setAnoMes}
              title="Mês de competência do orçamento"
              aria-label="Mês do orçamento"
            />
            {summary.contas_estouradas > 0 ? (
              <span style={{ color: "var(--color-negative)", fontSize: 12, fontWeight: 600 }}>
                {summary.contas_estouradas} conta(s) estourada(s)
              </span>
            ) : summary.contas_em_alerta > 0 ? (
              <span style={{ color: "var(--color-warning)", fontSize: 12, fontWeight: 600 }}>
                {summary.contas_em_alerta} conta(s) em alerta
              </span>
            ) : null}
            <GridSearchInput value={query} onChange={setQuery} />
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginTop: 16 }}>
            <div className="card">
              <div className="muted" style={{ fontSize: 12 }}>Orçado (mês)</div>
              <div style={{ fontSize: 20, fontWeight: 700 }}>{loading ? "..." : formatCurrency(summary.total_orcado)}</div>
            </div>
            <div className="card">
              <div className="muted" style={{ fontSize: 12 }}>Realizado (mês)</div>
              <div style={{ fontSize: 20, fontWeight: 700 }}>{loading ? "..." : formatCurrency(summary.total_realizado)}</div>
            </div>
            <div className="card">
              <div className="muted" style={{ fontSize: 12 }}>Saldo</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: Number(summary.saldo || 0) < 0 ? "var(--color-negative)" : undefined }}>
                {loading ? "..." : formatCurrency(summary.saldo)}
              </div>
            </div>
          </div>

          {!loading && !contas.length ? (
            <div style={{ marginTop: 12 }}>
              <EmptyState
                title="Nenhuma conta orçada."
                detail="Use a aba Configurar para definir tetos por conta nesta filial."
              />
            </div>
          ) : (
            <div className="tableScroll" style={{ marginTop: 12 }}>
              <table className="table compact">
                <thead>
                  <tr>
                    <th>Conta</th>
                    {showFilial ? <th>Filial</th> : null}
                    <th>Orçado</th>
                    <th>Realizado</th>
                    <th>Saldo</th>
                    <th>Consumo</th>
                    <th>Situação</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map((c: any) => {
                    const st = STATUS_STYLE[c.status] || STATUS_STYLE.ok;
                    return (
                      <tr key={`${c.id_filial}-${c.id_plano_conta}`}>
                        <td>{c.nome_conta}</td>
                        {showFilial ? <td>{c.filial_label || "—"}</td> : null}
                        <td>{formatCurrency(c.orcado)}</td>
                        <td style={{ fontWeight: 700 }}>{formatCurrency(c.realizado)}</td>
                        <td style={{ color: Number(c.saldo) < 0 ? "var(--color-negative)" : undefined }}>{formatCurrency(c.saldo)}</td>
                        <td style={{ fontWeight: 700, color: st.color }}>{Number(c.consumo_pct || 0).toFixed(1)}%</td>
                        <td><span style={{ color: st.color, fontWeight: 600 }}>{st.label}</span></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
