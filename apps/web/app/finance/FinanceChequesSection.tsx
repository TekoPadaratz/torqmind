"use client";

import { useEffect, useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { formatCurrency, formatDateOnly } from "../lib/format";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";

type ChequeStatus = "a_compensar" | "depositado" | "devolvido" | "compensado";

const STATUS_ORDER: ChequeStatus[] = ["a_compensar", "depositado", "devolvido", "compensado"];
const STATUS_LABELS: Record<ChequeStatus, string> = {
  a_compensar: "A compensar",
  depositado: "Depositado",
  devolvido: "Devolvido",
  compensado: "Compensado",
};
const STATUS_COLORS: Record<ChequeStatus, string> = {
  a_compensar: "var(--color-warning)",
  depositado: "var(--color-info)",
  devolvido: "var(--color-negative)",
  compensado: "var(--color-positive)",
};
const DEFAULT_SELECTED: ChequeStatus[] = ["a_compensar", "depositado", "devolvido"];
const PAGE_SIZE = 20;

export default function FinanceChequesSection() {
  const scope = useScopeQuery();
  const [selected, setSelected] = useState<Set<ChequeStatus>>(new Set(DEFAULT_SELECTED));
  const [page, setPage] = useState(0);

  const statusParam = useMemo(
    () => (selected.size ? STATUS_ORDER.filter((s) => selected.has(s)).join(",") : "todos"),
    [selected],
  );

  const { data, loading } = useBiScopeData<any>({
    moduleKey: `finance_cheques_${statusParam}`,
    scope,
    errorMessage: "Falha ao carregar cheques",
    buildRequestUrl: (currentScope) =>
      `/bi/finance/cheques?status=${encodeURIComponent(statusParam)}&${buildScopeParams(currentScope).toString()}`,
  });

  const summary = data?.summary || {};
  const porStatus = summary?.por_status || {};
  const cheques = useMemo(() => data?.cheques || [], [data]);
  const { query, setQuery, filteredRows } = useGridSearch(cheques as Record<string, unknown>[]);
  const showFilial = useMemo(
    () => new Set(cheques.map((c: any) => c.id_filial)).size > 1,
    [cheques],
  );

  useEffect(() => {
    setPage(0);
  }, [statusParam, filteredRows.length]);

  const pageCount = Math.max(1, Math.ceil(filteredRows.length / PAGE_SIZE));
  const pageItems = useMemo(() => {
    const safe = Math.min(page, pageCount - 1);
    return filteredRows.slice(safe * PAGE_SIZE, safe * PAGE_SIZE + PAGE_SIZE);
  }, [filteredRows, page, pageCount]);

  const toggle = (s: ChequeStatus) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(s)) next.delete(s);
      else next.add(s);
      return next.size ? next : new Set(DEFAULT_SELECTED);
    });

  const statusPill = (s: string) => {
    const key = (s as ChequeStatus) || "a_compensar";
    return (
      <span style={{ color: STATUS_COLORS[key] || "var(--muted)", fontWeight: 600 }}>
        {STATUS_LABELS[key] || key}
      </span>
    );
  };

  return (
    <div className="card col-12" style={{ marginTop: 16 }}>
      <div className="sectionEyebrow">Controle de Cheques</div>
      <h2 style={{ marginTop: 4 }}>Cheques recebidos — à vista e a prazo</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Todos os cheques recebidos por status: a compensar, depositado, devolvido (com o motivo) e
        compensado. Por padrão os compensados ficam fora — use o filtro para incluí-los. Cheques
        vencidos (data do &ldquo;bom para&rdquo; passou) aparecem destacados.
      </div>

      {/* Summary cards por status */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: 12,
          marginTop: 16,
        }}
      >
        {STATUS_ORDER.map((s) => {
          const st = porStatus[s] || { qtd: 0, valor: 0 };
          return (
            <div key={s} className="card" style={{ borderColor: STATUS_COLORS[s] }}>
              <div className="muted" style={{ fontSize: 12 }}>{STATUS_LABELS[s]}</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: STATUS_COLORS[s] }}>
                {loading ? "..." : formatCurrency(st.valor)}
              </div>
              <div className="muted" style={{ fontSize: 11 }}>{Number(st.qtd || 0)} cheque(s)</div>
            </div>
          );
        })}
        <div className="card" style={{ borderColor: "var(--color-negative)" }}>
          <div className="muted" style={{ fontSize: 12 }}>Vencidos (não compensados)</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-negative)" }}>
            {loading ? "..." : formatCurrency(summary.vencidos_valor)}
          </div>
          <div className="muted" style={{ fontSize: 11 }}>
            {Number(summary.vencidos_qtd || 0)} cheque(s) · à vista {Number(summary.avista_qtd || 0)} / a prazo {Number(summary.aprazo_qtd || 0)}
          </div>
        </div>
      </div>

      {/* Filtro multi-status */}
      <div style={{ display: "flex", gap: 6, marginTop: 16, flexWrap: "wrap", alignItems: "center" }}>
        <span className="muted" style={{ fontSize: 12 }}>Mostrar:</span>
        {STATUS_ORDER.map((s) => {
          const on = selected.has(s);
          return (
            <button
              key={s}
              type="button"
              onClick={() => toggle(s)}
              aria-pressed={on}
              style={{
                padding: "6px 14px",
                borderRadius: 8,
                border: `1px solid ${on ? "var(--accent-copper)" : "var(--border)"}`,
                background: on ? "var(--accent-copper-soft)" : "transparent",
                color: on ? "var(--text)" : "var(--muted)",
                fontWeight: on ? 600 : 400,
                fontSize: 13,
                cursor: "pointer",
              }}
            >
              {STATUS_LABELS[s]}
            </button>
          );
        })}
        <GridSearchInput value={query} onChange={setQuery} />
      </div>

      {/* Grid */}
      {!loading && !cheques.length ? (
        <div style={{ marginTop: 12 }}>
          <EmptyState
            title="Nenhum cheque nesta condição."
            detail="Não há cheques para o filtro e escopo selecionados."
          />
        </div>
      ) : (
        <>
          <div className="tableScroll" style={{ marginTop: 12 }}>
            <table className="table compact">
              <thead>
                <tr>
                  <th>Cliente</th>
                  {showFilial ? <th>Filial</th> : null}
                  <th>Recebido</th>
                  <th>Vencimento</th>
                  <th>Prazo</th>
                  <th>Valor</th>
                  <th>Status</th>
                  <th>Motivo devolução</th>
                  <th>Nº cheque</th>
                </tr>
              </thead>
              <tbody>
                {pageItems.map((c: any) => (
                  <tr
                    key={`${c.id_filial}-${c.id_cheque}`}
                    style={c.vencido ? { background: "rgba(239,68,68,0.06)" } : undefined}
                  >
                    <td>{c.cliente_nome || "—"}</td>
                    {showFilial ? <td>{c.filial_label || "—"}</td> : null}
                    <td>{c.dt_recebido ? formatDateOnly(c.dt_recebido) : "—"}</td>
                    <td style={c.vencido ? { color: "var(--color-negative)", fontWeight: 600 } : undefined}>
                      {c.dt_vencimento ? formatDateOnly(c.dt_vencimento) : "—"}
                    </td>
                    <td>{c.avista ? "À vista" : "A prazo"}</td>
                    <td style={{ fontWeight: 700 }}>{formatCurrency(c.valor)}</td>
                    <td>{statusPill(c.status)}</td>
                    <td style={{ minWidth: 200 }}>{c.motivo_devolucao || "—"}</td>
                    <td>{c.numero || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {pageCount > 1 ? (
            <div style={{ display: "flex", gap: 10, marginTop: 12, alignItems: "center", justifyContent: "flex-end" }}>
              <button
                type="button"
                className="btn"
                disabled={page <= 0}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
              >
                ← Anterior
              </button>
              <span className="muted" style={{ fontSize: 12 }}>
                Página {Math.min(page, pageCount - 1) + 1} de {pageCount} · {filteredRows.length} cheque(s)
              </span>
              <button
                type="button"
                className="btn"
                disabled={page >= pageCount - 1}
                onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
              >
                Próxima →
              </button>
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}
