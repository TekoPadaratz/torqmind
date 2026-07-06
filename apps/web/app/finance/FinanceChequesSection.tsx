"use client";

import { useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import { formatCurrency, formatDateOnly } from "../lib/format";
import { buildScopeParams, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";

type ChequeStatus = "vencidos" | "todos" | "nao_vencidos";

const STATUS_LABELS: Record<ChequeStatus, string> = {
  vencidos: "Vencidos",
  todos: "Todos",
  nao_vencidos: "A vencer",
};

export default function FinanceChequesSection() {
  const scope = useScopeQuery();
  const [status, setStatus] = useState<ChequeStatus>("vencidos");

  const { data, loading } = useBiScopeData<any>({
    moduleKey: `finance_cheques_${status}`,
    scope,
    errorMessage: "Falha ao carregar cheques",
    buildRequestUrl: (currentScope) =>
      `/bi/finance/cheques?status=${status}&${buildScopeParams(currentScope).toString()}`,
  });

  const summary = data?.summary || {};
  const cheques = useMemo(() => data?.cheques || [], [data]);
  const showFilial = useMemo(
    () => new Set(cheques.map((c: any) => c.id_filial)).size > 1,
    [cheques],
  );

  return (
    <div className="card col-12" style={{ marginTop: 16 }}>
      <div className="sectionEyebrow">Controle de Cheques</div>
      <h2 style={{ marginTop: 4 }}>Cheques recebidos ainda não compensados</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Cheques que já entraram mas o dinheiro ainda não caiu (não compensados). O card mostra o
        total já <strong>vencido</strong> (data do &ldquo;bom para&rdquo; passou). Um cheque pode
        estar não compensado e ainda a vencer — por isso o filtro abaixo.
      </div>

      {/* Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: 12,
          marginTop: 16,
        }}
      >
        <div className="card" style={{ borderColor: "var(--color-negative)" }}>
          <div className="muted" style={{ fontSize: 12 }}>Vencidos (não compensados)</div>
          <div style={{ fontSize: 22, fontWeight: 700, color: "var(--color-negative)" }}>
            {loading ? "..." : formatCurrency(summary.vencidos_valor)}
          </div>
          <div className="muted" style={{ fontSize: 12 }}>
            {Number(summary.vencidos_qtd || 0)} cheque(s)
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>A vencer (não compensados)</div>
          <div style={{ fontSize: 22, fontWeight: 700, color: "var(--color-warning)" }}>
            {loading ? "..." : formatCurrency(summary.a_vencer_valor)}
          </div>
          <div className="muted" style={{ fontSize: 12 }}>
            {Number(summary.a_vencer_qtd || 0)} cheque(s)
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Total não compensado</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>
            {loading ? "..." : formatCurrency(summary.total_valor)}
          </div>
          <div className="muted" style={{ fontSize: 12 }}>
            {Number(summary.total_qtd || 0)} cheque(s)
          </div>
        </div>
      </div>

      {/* Filter */}
      <div style={{ display: "flex", gap: 6, marginTop: 16 }}>
        {(["vencidos", "todos", "nao_vencidos"] as ChequeStatus[]).map((opt) => (
          <button
            key={opt}
            type="button"
            onClick={() => setStatus(opt)}
            style={{
              padding: "6px 14px",
              borderRadius: 8,
              border: `1px solid ${status === opt ? "var(--accent-copper)" : "var(--border)"}`,
              background: status === opt ? "rgba(184,115,51,0.16)" : "transparent",
              color: status === opt ? "var(--text)" : "var(--muted)",
              fontWeight: status === opt ? 600 : 400,
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            {STATUS_LABELS[opt]}
          </button>
        ))}
      </div>

      {/* Grid */}
      {!loading && !cheques.length ? (
        <div style={{ marginTop: 12 }}>
          <EmptyState
            title="Nenhum cheque nesta condição."
            detail="Não há cheques não compensados para o filtro e escopo selecionados."
          />
        </div>
      ) : (
        <div className="tableScroll" style={{ marginTop: 12 }}>
          <table className="table compact">
            <thead>
              <tr>
                <th>Cliente</th>
                {showFilial ? <th>Filial</th> : null}
                <th>Recebido</th>
                <th>Vencimento</th>
                <th>Valor</th>
                <th>Banco</th>
                <th>Agência</th>
                <th>Conta</th>
                <th>Nº cheque</th>
                <th>Situação</th>
              </tr>
            </thead>
            <tbody>
              {cheques.map((c: any) => (
                <tr key={`${c.id_filial}-${c.id_cheque}`}>
                  <td>{c.cliente_nome || "—"}</td>
                  {showFilial ? <td>{c.filial_label || "—"}</td> : null}
                  <td>{c.dt_recebido ? formatDateOnly(c.dt_recebido) : "—"}</td>
                  <td>{c.dt_vencimento ? formatDateOnly(c.dt_vencimento) : "—"}</td>
                  <td style={{ fontWeight: 700 }}>{formatCurrency(c.valor)}</td>
                  <td>{c.banco || "—"}</td>
                  <td>{c.agencia || "—"}</td>
                  <td>{c.nroconta || "—"}</td>
                  <td>{c.numero || "—"}</td>
                  <td>
                    <span
                      className={`badge ${c.vencido ? "warn" : "ok"}`}
                      style={c.vencido ? { color: "var(--color-negative)" } : undefined}
                    >
                      {c.vencido ? "Vencido" : "A vencer"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
