"use client";

import { useEffect, useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { formatCurrency } from "../lib/format";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";

const MONTHS = [
  "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
  "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
];

const STATUS_STYLE: Record<string, { label: string; color: string }> = {
  ok: { label: "No orçamento", color: "var(--color-positive)" },
  alerta: { label: "Em alerta", color: "var(--color-warning)" },
  estourado: { label: "Estourado", color: "var(--color-negative)" },
};

export default function FinanceBudgetSection() {
  const scope = useScopeQuery();
  const [ano, setAno] = useState<number | null>(null);
  const [mes, setMes] = useState<number | null>(null);

  const { data, loading } = useBiScopeData<any>({
    moduleKey: `budget_overview_${ano}_${mes}`,
    scope,
    errorMessage: "Falha ao carregar orçamento",
    buildRequestUrl: (currentScope) => {
      let url = `/bi/budget/overview?${buildScopeParams(currentScope).toString()}`;
      if (ano && mes) url += `&ano=${ano}&mes=${mes}`;
      return url;
    },
  });

  useEffect(() => {
    if (data && (ano === null || mes === null)) {
      setAno(Number(data.ano));
      setMes(Number(data.mes));
    }
  }, [data, ano, mes]);

  const contas = useMemo(() => data?.contas || [], [data]);
  const { query, setQuery, filteredRows } = useGridSearch(contas as Record<string, unknown>[]);
  const summary = data?.summary || {};
  // Sempre exibe o posto (apelido) no grid, mesmo com 1 filial no escopo.
  const showFilial = true;
  const currentYear = ano || new Date().getFullYear();
  const years = [currentYear - 2, currentYear - 1, currentYear, currentYear + 1];

  return (
    <div className="card col-12" style={{ marginTop: 16 }}>
      <div className="sectionEyebrow">Gestão Orçamentária</div>
      <h2 style={{ marginTop: 4 }}>Despesas realizadas × orçado no mês</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Compara o que já foi gasto em cada conta com o teto definido em Metas &amp; Equipe. Fica em
        alerta quando o gasto chega perto do teto e estourado quando passa. Configure os tetos em
        Metas &amp; Equipe → Gestão Orçamentária.
      </div>

      {/* Month / Year filter */}
      <div style={{ display: "flex", gap: 8, marginTop: 14, alignItems: "center", flexWrap: "wrap" }}>
        <select
          value={mes || 1}
          onChange={(e) => setMes(parseInt(e.target.value))}
          style={{ padding: "7px 10px", borderRadius: 8, border: "1px solid var(--border)", background: "var(--filter-bg)", color: "var(--text)" }}
        >
          {MONTHS.map((m, i) => (
            <option key={m} value={i + 1}>{m}</option>
          ))}
        </select>
        <select
          value={currentYear}
          onChange={(e) => setAno(parseInt(e.target.value))}
          style={{ padding: "7px 10px", borderRadius: 8, border: "1px solid var(--border)", background: "var(--filter-bg)", color: "var(--text)" }}
        >
          {years.map((y) => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
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

      {/* Summary cards */}
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

      {/* Grid */}
      {!loading && !contas.length ? (
        <div style={{ marginTop: 12 }}>
          <EmptyState
            title="Nenhuma conta orçada."
            detail="Defina os tetos de despesa em Metas & Equipe → Gestão Orçamentária para acompanhar aqui."
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
    </div>
  );
}
