"use client";

import { Fragment, useEffect, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridPager from "../components/ui/GridPager";
import GridSearchInput from "../components/ui/GridSearchInput";
import PresetFilterChips from "../components/ui/PresetFilterChips";
import { formatCurrency, formatDateOnly } from "../lib/format";
import { apiGet } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { buildScopeParams, type ScopeQuery } from "../lib/scope";
import MonthYearSelect from "../components/ui/MonthYearSelect";
import { currentAnoMesSP } from "../lib/month-year.mjs";

const DETAIL_PAGE_SIZE = 30;

/** Entrada/Saída = débito/crédito do Razão Xpert (MOVLCTOS), não baixa de CAP. */
const STATUS_PRESETS = [
  { id: "entrada", label: "Entradas" },
  { id: "saida", label: "Saídas" },
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
  historico?: string;
  documento?: string;
  dt_vencimento?: string | null;
  data_competencia?: string | null;
  valor?: number;
  status?: string;
  status_label?: string;
};

type DetailPayload = {
  items?: DetailRow[];
  total?: number;
  totals?: {
    valor?: number;
    entradas?: number;
    saidas?: number;
    pago?: number;
    aberto?: number;
  };
};

type Props = { scope: ScopeQuery };

export default function FinanceDespesasSection({ scope }: Props) {
  const [anoMes, setAnoMes] = useState<number>(() => currentAnoMesSP());
  const [q, setQ] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [detailPage, setDetailPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const [summary, setSummary] = useState<any>(null);
  const [detailCache, setDetailCache] = useState<Record<number, DetailPayload>>({});

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedQ(q.trim()), 250);
    return () => window.clearTimeout(t);
  }, [q]);

  useEffect(() => {
    setExpandedId(null);
    setDetailCache({});
    setDetailPage(1);
  }, [debouncedQ, status, anoMes, scope.scope_key]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const params = buildScopeParams(scope);
        params.set("ano_mes", String(anoMes));
        if (debouncedQ) params.set("q", debouncedQ);
        if (status) params.set("status", status);
        const payload = await apiGet(`/bi/finance/despesas?${params.toString()}`, {
          signal: controller.signal,
        });
        setSummary(payload);
      } catch (err: any) {
        if (err?.name === "AbortError" || err?.code === "ERR_CANCELED") return;
        setError(extractApiError(err, "Falha ao carregar despesas"));
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [anoMes, debouncedQ, status, scope.scope_key, scope]);

  useEffect(() => {
    if (expandedId == null) return;
    const controller = new AbortController();
    const loadDetail = async () => {
      setDetailLoading(true);
      setError("");
      try {
        const params = buildScopeParams(scope);
        params.set("ano_mes", String(anoMes));
        params.set("id_planodecontas", String(expandedId));
        params.set("page", String(detailPage));
        params.set("page_size", String(DETAIL_PAGE_SIZE));
        if (debouncedQ) params.set("q", debouncedQ);
        if (status) params.set("status", status);
        const payload = await apiGet(`/bi/finance/despesas?${params.toString()}`, {
          signal: controller.signal,
        });
        setDetailCache((prev) => ({ ...prev, [expandedId]: payload }));
      } catch (err: any) {
        if (err?.name === "AbortError" || err?.code === "ERR_CANCELED") return;
        setError(extractApiError(err, "Falha ao carregar detalhe da despesa"));
      } finally {
        setDetailLoading(false);
      }
    };
    loadDetail();
    return () => controller.abort();
  }, [expandedId, detailPage, anoMes, debouncedQ, status, scope]);

  const items: SummaryRow[] = summary?.items || [];
  const totals = summary?.totals || {};
  const expandedDetail = expandedId != null ? detailCache[expandedId] : null;
  const detailItems: DetailRow[] = expandedDetail?.items || [];
  const detailTotals = expandedDetail?.totals || {};
  const detailTotal = Number(expandedDetail?.total || 0);
  const detailTotalPages = Math.max(1, Math.ceil(detailTotal / DETAIL_PAGE_SIZE) || 1);
  const kpiEntradas = totals.entradas ?? totals.pago;
  const kpiSaidas = totals.saidas ?? totals.aberto;

  const toggleExpand = (id: number) => {
    setExpandedId((prev) => {
      if (prev === id) return null;
      setDetailPage(1);
      return id;
    });
  };

  return (
    <div className="card col-12" style={{ marginTop: 12 }}>
      <div className="sectionEyebrow">Financeiro</div>
      <h2 style={{ marginTop: 4 }}>Despesas</h2>

      <div style={{ display: "flex", gap: 8, marginTop: 12, alignItems: "center", flexWrap: "wrap" }}>
        <MonthYearSelect
          value={anoMes}
          onChange={setAnoMes}
          title="Mês de referência das despesas"
          aria-label="Mês das despesas"
        />
        <PresetFilterChips
          options={STATUS_PRESETS}
          value={status}
          onChange={setStatus}
          clearLabel="Todos"
        />
        <GridSearchInput value={q} onChange={setQ} />
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
          <div className="muted" style={{ fontSize: 12 }}>Entradas</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-positive)" }}>
            {loading ? "…" : formatCurrency(kpiEntradas)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Saídas</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "var(--color-warning)" }}>
            {loading ? "…" : formatCurrency(kpiSaidas)}
          </div>
        </div>
      </div>

      {error ? <div className="errorCard" style={{ marginTop: 12 }}>{error}</div> : null}

      <div className="tableScroll tableScroll--compact" style={{ marginTop: 10 }}>
        {!loading && items.length === 0 ? (
          <EmptyState
            title="Sem despesas no período."
            detail="Escolha outro mês ou ajuste o filtro de status/busca."
          />
        ) : (
          <table className="table compact">
            <thead>
              <tr>
                <th style={{ width: 28 }} />
                <th>Despesa</th>
                <th style={{ textAlign: "right" }}>Valor</th>
              </tr>
            </thead>
            <tbody>
              {items.map((row) => {
                const expanded = expandedId === row.id_planodecontas;
                return (
                  <Fragment key={row.id_planodecontas}>
                    <tr
                      onClick={() => toggleExpand(row.id_planodecontas)}
                      style={{ cursor: "pointer" }}
                    >
                      <td style={{ width: 28 }}>{expanded ? "▾" : "▸"}</td>
                      <td>
                        <strong>{row.nome_plano || "—"}</strong>
                        <span className="muted" style={{ fontSize: 12, marginLeft: 8 }}>
                          {[row.codigo_plano, row.classificacao_gerencial, row.qtd ? `${row.qtd} lançamento(s)` : ""]
                            .filter(Boolean)
                            .join(" · ")}
                        </span>
                      </td>
                      <td style={{ textAlign: "right", fontWeight: 700 }}>
                        {formatCurrency(row.valor)}
                      </td>
                    </tr>
                    {expanded ? (
                      <tr>
                        <td
                          colSpan={3}
                          style={{ padding: "8px 12px 12px", background: "var(--surface-faint)" }}
                        >
                          <div
                            className="muted"
                            style={{
                              display: "flex",
                              flexWrap: "wrap",
                              gap: "6px 14px",
                              marginBottom: 8,
                              fontSize: 12,
                            }}
                          >
                            <span>
                              Entradas{" "}
                              <strong style={{ color: "var(--color-positive)" }}>
                                {formatCurrency(detailTotals.entradas ?? detailTotals.pago)}
                              </strong>
                            </span>
                            <span>
                              Saídas{" "}
                              <strong style={{ color: "var(--color-warning)" }}>
                                {formatCurrency(detailTotals.saidas ?? detailTotals.aberto)}
                              </strong>
                            </span>
                          </div>
                          {detailLoading && !expandedDetail ? (
                            <div className="muted" style={{ fontSize: 12 }}>Carregando lançamentos…</div>
                          ) : !detailItems.length ? (
                            <div className="muted" style={{ fontSize: 12 }}>
                              Sem lançamentos nesta conta para o filtro atual.
                            </div>
                          ) : (
                            <>
                              <table className="table compact" style={{ margin: 0, minWidth: 0 }}>
                                <thead>
                                  <tr>
                                    <th>Filial</th>
                                    <th>Data</th>
                                    <th>Documento</th>
                                    <th style={{ textAlign: "right" }}>Valor</th>
                                    <th>Tipo</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {detailItems.map((d, idx) => (
                                    <tr key={`${d.id_filial}-${d.dt_vencimento}-${d.documento}-${idx}`}>
                                      <td>{d.filial_nome || "—"}</td>
                                      <td style={{ whiteSpace: "nowrap" }}>
                                        {formatDateOnly(d.data_competencia || d.dt_vencimento) || "—"}
                                      </td>
                                      <td>{d.documento || d.historico || "—"}</td>
                                      <td style={{ textAlign: "right" }}>{formatCurrency(d.valor)}</td>
                                      <td>{d.status_label || d.status || "—"}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                              <GridPager
                                page={detailPage}
                                pageSize={DETAIL_PAGE_SIZE}
                                total={detailTotal}
                                totalPages={detailTotalPages}
                                onPrev={() => setDetailPage((p) => Math.max(1, p - 1))}
                                onNext={() =>
                                  setDetailPage((p) => Math.min(detailTotalPages, p + 1))
                                }
                              />
                            </>
                          )}
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
