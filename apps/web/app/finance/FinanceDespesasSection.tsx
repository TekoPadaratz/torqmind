"use client";

import { useEffect, useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridPager from "../components/ui/GridPager";
import GridSearchInput from "../components/ui/GridSearchInput";
import PresetFilterChips from "../components/ui/PresetFilterChips";
import { formatCurrency, formatDateOnly } from "../lib/format";
import { apiGet } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { buildScopeParams, type ScopeQuery } from "../lib/scope";

const PAGE_SIZE = 50;

const MONTHS = [
  "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
  "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
];

const STATUS_PRESETS = [
  { id: "aberto", label: "Aberto" },
  { id: "pago", label: "Pago" },
  { id: "vencido", label: "Vencido" },
];

type SummaryRow = {
  id_planodecontas: number;
  codigo_plano?: string;
  nome_plano?: string;
  classificacao_gerencial?: string;
  valor?: number;
  qtd?: number;
};

type DetailRow = {
  id_filial?: number;
  filial_nome?: string;
  nome_plano?: string;
  dt_vencimento?: string | null;
  valor?: number;
  dt_pagamento?: string | null;
  status?: string;
  status_label?: string;
  historico?: string;
};

type Props = { scope: ScopeQuery };

export default function FinanceDespesasSection({ scope }: Props) {
  const now = new Date();
  const [ano, setAno] = useState(now.getFullYear());
  const [mes, setMes] = useState(now.getMonth() + 1);
  const [q, setQ] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [selectedConta, setSelectedConta] = useState<SummaryRow | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [summary, setSummary] = useState<any>(null);
  const [detail, setDetail] = useState<any>(null);

  const years = useMemo(
    () => [ano - 2, ano - 1, ano, ano + 1].filter((y, i, arr) => arr.indexOf(y) === i),
    [ano],
  );

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedQ(q.trim()), 250);
    return () => window.clearTimeout(t);
  }, [q]);

  useEffect(() => {
    setPage(1);
  }, [debouncedQ, status, ano, mes, scope.scope_key, selectedConta?.id_planodecontas]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const params = buildScopeParams(scope);
        params.set("ano", String(ano));
        params.set("mes", String(mes));
        if (debouncedQ) params.set("q", debouncedQ);
        if (status) params.set("status", status);
        if (selectedConta) {
          params.set("id_planodecontas", String(selectedConta.id_planodecontas));
          params.set("page", String(page));
          params.set("page_size", String(PAGE_SIZE));
        }
        const payload = await apiGet(`/bi/finance/despesas?${params.toString()}`, {
          signal: controller.signal,
        });
        if (selectedConta) setDetail(payload);
        else {
          setSummary(payload);
          setDetail(null);
        }
      } catch (err: any) {
        if (err?.name === "AbortError" || err?.code === "ERR_CANCELED") return;
        setError(extractApiError(err, "Falha ao carregar despesas"));
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [ano, mes, debouncedQ, status, scope.scope_key, selectedConta, page, scope]);

  const items: SummaryRow[] = summary?.items || [];
  const totals = selectedConta ? detail?.totals || {} : summary?.totals || {};
  const detailItems: DetailRow[] = detail?.items || [];

  return (
    <div className="card col-12" style={{ marginTop: 12 }}>
      <div className="sectionEyebrow">Financeiro</div>
      <h2 style={{ marginTop: 4 }}>Despesas</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Plano de contas do mês (contas a pagar × natureza DRE). Clique na conta para ver o grão
        (filial, vencimento, pagamento e status).
      </div>

      <div style={{ display: "flex", gap: 8, marginTop: 14, alignItems: "center", flexWrap: "wrap" }}>
        <select
          value={mes}
          onChange={(e) => {
            setSelectedConta(null);
            setMes(parseInt(e.target.value, 10));
          }}
          style={{
            padding: "7px 10px",
            borderRadius: 8,
            border: "1px solid var(--border)",
            background: "var(--filter-bg)",
            color: "var(--text)",
          }}
        >
          {MONTHS.map((m, i) => (
            <option key={m} value={i + 1}>{m}</option>
          ))}
        </select>
        <select
          value={ano}
          onChange={(e) => {
            setSelectedConta(null);
            setAno(parseInt(e.target.value, 10));
          }}
          style={{
            padding: "7px 10px",
            borderRadius: 8,
            border: "1px solid var(--border)",
            background: "var(--filter-bg)",
            color: "var(--text)",
          }}
        >
          {years.map((y) => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
        <PresetFilterChips
          options={STATUS_PRESETS}
          value={status}
          onChange={(id) => {
            setSelectedConta(null);
            setStatus(id);
          }}
          clearLabel="Todos"
        />
        <GridSearchInput value={q} onChange={setQ} placeholder="Buscar despesa, histórico, filial…" />
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 12,
          marginTop: 16,
        }}
      >
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Total</div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>
            {loading ? "…" : formatCurrency(totals.valor)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Pago</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-positive)" }}>
            {loading ? "…" : formatCurrency(totals.pago)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Aberto</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-warning)" }}>
            {loading ? "…" : formatCurrency(totals.aberto)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Vencido</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-negative)" }}>
            {loading ? "…" : formatCurrency(totals.vencido)}
          </div>
        </div>
      </div>

      {error ? <div className="errorCard" style={{ marginTop: 12 }}>{error}</div> : null}

      {selectedConta ? (
        <div style={{ marginTop: 16 }}>
          <button
            type="button"
            className="btn"
            onClick={() => setSelectedConta(null)}
            style={{ marginBottom: 12 }}
          >
            ← Voltar ao plano de contas
          </button>
          <h3 style={{ margin: "0 0 8px" }}>
            {selectedConta.nome_plano || "Despesa"}{" "}
            <span className="muted" style={{ fontSize: 13, fontWeight: 500 }}>
              {selectedConta.codigo_plano}
            </span>
          </h3>
          {!loading && detailItems.length === 0 ? (
            <EmptyState title="Sem lançamentos nesta conta." detail="Ajuste mês, status ou busca." />
          ) : (
            <div className="tableScroll">
              <table className="table">
                <thead>
                  <tr>
                    <th>Filial</th>
                    <th>Despesa</th>
                    <th>Vencimento</th>
                    <th>Valor</th>
                    <th>Pagamento</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {detailItems.map((row, idx) => (
                    <tr key={`${row.id_filial}-${row.dt_vencimento}-${idx}`}>
                      <td>{row.filial_nome || "—"}</td>
                      <td>{row.nome_plano || "—"}</td>
                      <td>{formatDateOnly(row.dt_vencimento) || "—"}</td>
                      <td>{formatCurrency(row.valor)}</td>
                      <td>{formatDateOnly(row.dt_pagamento) || "—"}</td>
                      <td>{row.status_label || row.status || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <GridPager
            page={page}
            pageSize={PAGE_SIZE}
            total={Number(detail?.total || 0)}
            totalPages={Math.max(1, Math.ceil(Number(detail?.total || 0) / PAGE_SIZE) || 1)}
            onPrev={() => setPage((p) => Math.max(1, p - 1))}
            onNext={() =>
              setPage((p) =>
                Math.min(
                  Math.max(1, Math.ceil(Number(detail?.total || 0) / PAGE_SIZE) || 1),
                  p + 1,
                ),
              )
            }
          />
        </div>
      ) : (
        <div style={{ marginTop: 16 }}>
          {!loading && items.length === 0 ? (
            <EmptyState
              title="Sem despesas no período."
              detail="Publique a mart de despesas ou escolha outro mês."
            />
          ) : (
            <div className="tableScroll">
              <table className="table">
                <thead>
                  <tr>
                    <th>Despesa</th>
                    <th>Valor</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((row) => (
                    <tr
                      key={row.id_planodecontas}
                      style={{ cursor: "pointer" }}
                      onClick={() => setSelectedConta(row)}
                    >
                      <td>
                        <div style={{ fontWeight: 600 }}>{row.nome_plano || "—"}</div>
                        <div className="muted" style={{ fontSize: 12 }}>
                          {row.codigo_plano || ""}
                          {row.classificacao_gerencial
                            ? ` · ${row.classificacao_gerencial}`
                            : ""}
                          {row.qtd ? ` · ${row.qtd} título(s)` : ""}
                        </div>
                      </td>
                      <td style={{ fontWeight: 700 }}>{formatCurrency(row.valor)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
