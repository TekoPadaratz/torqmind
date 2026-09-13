"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import { apiGet, apiPut, isRequestCanceled } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridChrome from "../components/ui/GridChrome";
import GridSearchInput from "../components/ui/GridSearchInput";
import SortableTh from "../components/ui/SortableTh";
import { useGridSearch } from "../lib/use-grid-search";
import { useRecordGrid } from "../lib/use-record-grid";
import { extractApiError } from "../lib/errors";
import { compareGridRows, sortGridRows } from "../lib/grid-sort";
import ManagerCommissionDrilldown, {
  type DrilldownPayload,
} from "./ManagerCommissionDrilldown";
function parseBrCurrency(input: string): number {
  const normalized = input.replace(/\./g, "").replace(",", ".").replace(/[^\d.-]/g, "");
  const value = Number(normalized);
  return Number.isFinite(value) ? value : 0;
}

function formatBrCurrencyInput(raw: string): string {
  const neg = raw.trim().startsWith("-");
  const digits = raw.replace(/\D/g, "");
  const cents = Number(digits || "0");
  const value = (cents / 100) * (neg ? -1 : 1);
  return value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPctInput(raw: string): string {
  return raw.replace(",", ".").replace(/[^\d.]/g, "");
}

function calcLiquida(row: {
  comissao_bruta: number;
  perdas_estoque: number;
  sobras_estoque: number;
  furos_caixa: number;
  sobras_caixa: number;
}): number {
  return Math.round(
    (row.comissao_bruta - row.perdas_estoque + row.sobras_estoque - row.furos_caixa + row.sobras_caixa) * 100,
  ) / 100;
}

function mapApiRow(r: any): RowState {
  const rate = Number(r.rate_pct || 0);
  const venda = Number(r.venda_bruta_total || 0);
  const bruta = Number(r.comissao_bruta ?? (venda * rate) / 100);
  const perdas = Number(r.perdas_estoque || 0);
  const sobrasEst = Number(r.sobras_estoque || 0);
  const sobrasCx = Number(r.sobras_caixa || 0);
  const furos = Number(r.furos_caixa || 0);
  const base = {
    id_empresa: Number(r.id_empresa),
    id_filial: Number(r.id_filial),
    filial_label: String(r.filial_label || `Filial ${r.id_filial}`),
    venda_bruta_total: venda,
    rate_pct: rate,
    comissao_bruta: bruta,
    perdas_estoque: perdas,
    sobras_estoque: sobrasEst,
    sobras_caixa: sobrasCx,
    furos_caixa: furos,
    comissao_liquida: 0,
    rate_text: String(rate),
    perdas_text: perdas.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    sobras_est_text: sobrasEst.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    sobras_cx_text: sobrasCx.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    furos_text: furos.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
  };
  return { ...base, comissao_liquida: calcLiquida(base) };
}

type RowState = {
  id_empresa: number;
  id_filial: number;
  filial_label: string;
  venda_bruta_total: number;
  rate_pct: number;
  comissao_bruta: number;
  perdas_estoque: number;
  sobras_estoque: number;
  sobras_caixa: number;
  furos_caixa: number;
  comissao_liquida: number;
  rate_text: string;
  perdas_text: string;
  sobras_est_text: string;
  sobras_cx_text: string;
  furos_text: string;
};

interface Props {
  idEmpresa: number | null;
  idFilial: number | null;
  idFiliais?: string[];
  dtIni: string;
  dtFim: string;
}

export default function ManagerCommissionGrid({
  idEmpresa,
  idFilial,
  idFiliais,
  dtIni,
  dtFim,
}: Props) {
  const [rows, setRows] = useState<RowState[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [savingKey, setSavingKey] = useState<string | null>(null);
  const [expandedFilial, setExpandedFilial] = useState<number | null>(null);
  const [drilldownByFilial, setDrilldownByFilial] = useState<Record<number, DrilldownPayload>>({});
  const [drilldownLoading, setDrilldownLoading] = useState<number | null>(null);
  const multiKey = useMemo(
    () => (idFiliais || []).map(String).filter(Boolean).join(","),
    [idFiliais],
  );

  const appendCentralMirrorParam = (params: URLSearchParams) => {
    params.set("include_central_mirror", "true");
  };

  useEffect(() => {
    const multi = multiKey ? multiKey.split(",") : [];
    if (!idFilial && multi.length === 0) {
      setRows([]);
      setError("");
      setLoading(false);
      return;
    }
    const ac = new AbortController();
    setLoading(true);
    setError("");
    setRows([]);
    (async () => {
      try {
        const params = new URLSearchParams();
        params.set("dt_ini", dtIni);
        params.set("dt_fim", dtFim);
        if (idEmpresa) params.set("id_empresa", String(idEmpresa));
        if (!idFilial && multi.length > 0) {
          for (const f of multi) params.append("id_filiais", String(f));
        } else if (multi.length > 1) {
          for (const f of multi) params.append("id_filiais", String(f));
        } else if (idFilial) {
          params.set("id_filial", String(idFilial));
        } else if (multi[0]) {
          params.set("id_filial", String(multi[0]));
        }
        appendCentralMirrorParam(params);
        const resp = await apiGet(`/bi/team/manager-commissions/calc?${params.toString()}`, {
          signal: ac.signal,
          timeout: 60000,
        });
        if (ac.signal.aborted) return;
        const mapped = (resp?.rows || []).map(mapApiRow) as RowState[];
        setRows(
          sortGridRows(mapped, (row) => ({
            filial: row.filial_label || String(row.id_filial),
            nome: row.filial_label || String(row.id_filial),
          })),
        );
        setError("");
        setExpandedFilial(null);
        setDrilldownByFilial({});
      } catch (err: any) {
        if (ac.signal.aborted || isRequestCanceled(err)) return;
        setError(extractApiError(err, "Falha ao calcular comissão de gerentes."));
        setRows([]);
        setExpandedFilial(null);
        setDrilldownByFilial({});
      } finally {
        if (!ac.signal.aborted) setLoading(false);
      }
    })();
    return () => ac.abort();
  }, [idEmpresa, idFilial, multiKey, dtIni, dtFim]);

  const sortedRows = useMemo(
    () =>
      sortGridRows(rows, (row) => ({
        filial: row.filial_label || String(row.id_filial),
        nome: row.filial_label || String(row.id_filial),
      })),
    [rows],
  );

  const { query, setQuery, filteredRows } = useGridSearch(
    sortedRows as unknown as Record<string, unknown>[],
    { excludeKeys: /^id_/i },
  );
  const managerGrid = useRecordGrid<RowState>({
    rows: filteredRows as unknown as RowState[],
    resetKey: query,
    getTieId: (row) => row.id_filial,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label || String(a.id_filial), nome: a.filial_label },
        { filial: b.filial_label || String(b.id_filial), nome: b.filial_label },
      ),
    summableKeys: [
      "venda_bruta_total",
      "comissao_bruta",
      "perdas_estoque",
      "sobras_estoque",
      "sobras_caixa",
      "furos_caixa",
      "comissao_liquida",
    ],
    columns: {
      filial_label: { type: "text" },
      venda_bruta_total: { type: "number" },
      rate_pct: { type: "number" },
      comissao_bruta: { type: "number" },
      perdas_estoque: { type: "number" },
      sobras_estoque: { type: "number" },
      sobras_caixa: { type: "number" },
      furos_caixa: { type: "number" },
      comissao_liquida: { type: "number" },
    },
  });
  const pageRatePct =
    Number(managerGrid.pageTotals.venda_bruta_total || 0) > 0
      ? Math.round(
          ((Number(managerGrid.pageTotals.comissao_bruta || 0) /
            Number(managerGrid.pageTotals.venda_bruta_total || 0)) *
            100) *
            100,
        ) / 100
      : 0;

  const updateLocal = (targetFilial: number, patch: Partial<RowState>) => {
    setRows((prev) =>
      prev.map((row) => {
        if (row.id_filial !== targetFilial) return row;
        const next = { ...row, ...patch };
        const rate = Number(next.rate_pct) || 0;
        next.comissao_bruta = Math.round(((next.venda_bruta_total * rate) / 100) * 100) / 100;
        next.comissao_liquida = calcLiquida(next);
        return next;
      }),
    );
  };

  const toggleExpand = async (row: RowState) => {
    const fid = row.id_filial;
    if (expandedFilial === fid) {
      setExpandedFilial(null);
      return;
    }
    setExpandedFilial(fid);
    if (drilldownByFilial[fid]) return;
    setDrilldownLoading(fid);
    try {
      const params = new URLSearchParams();
      params.set("dt_ini", dtIni);
      params.set("dt_fim", dtFim);
      params.set("id_filial", String(fid));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      appendCentralMirrorParam(params);
      const resp = await apiGet(`/bi/team/manager-commissions/drilldown?${params.toString()}`, {
        timeout: 60000,
      });
      setDrilldownByFilial((prev) => ({ ...prev, [fid]: resp }));
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao carregar o detalhe."));
      setExpandedFilial(null);
    } finally {
      setDrilldownLoading(null);
    }
  };

  const persist = async (row: RowState) => {
    const key = `${row.id_filial}`;
    setSavingKey(key);
    setError("");
    try {
      const resp = await apiPut("/bi/team/manager-commissions/overrides", {
        id_empresa: row.id_empresa,
        id_filial: row.id_filial,
        dt_ini: dtIni,
        dt_fim: dtFim,
        fields: {
          rate_pct: row.rate_pct,
          perdas_estoque: row.perdas_estoque,
          sobras_estoque: row.sobras_estoque,
          sobras_caixa: row.sobras_caixa,
          furos_caixa: row.furos_caixa,
        },
      });
      // Recalc only this row from API response
      if (resp?.row) {
        const mapped = mapApiRow({
          ...resp.row,
          filial_label: resp.row.filial_label || row.filial_label,
        });
        setRows((prev) => {
          const next = prev.map((r) => (r.id_filial === mapped.id_filial ? mapped : r));
          return sortGridRows(next, (r) => ({
            filial: r.filial_label || String(r.id_filial),
            nome: r.filial_label || String(r.id_filial),
          }));
        });
        setDrilldownByFilial((prev) => {
          const next = { ...prev };
          delete next[mapped.id_filial];
          return next;
        });
        if (expandedFilial === mapped.id_filial) {
          void (async () => {
            setDrilldownLoading(mapped.id_filial);
            try {
              const params = new URLSearchParams();
              params.set("dt_ini", dtIni);
              params.set("dt_fim", dtFim);
              params.set("id_filial", String(mapped.id_filial));
              if (idEmpresa) params.set("id_empresa", String(idEmpresa));
              appendCentralMirrorParam(params);
              const detail = await apiGet(
                `/bi/team/manager-commissions/drilldown?${params.toString()}`,
                { timeout: 60000 },
              );
              setDrilldownByFilial((prev) => ({ ...prev, [mapped.id_filial]: detail }));
            } catch {
              /* detalhe volta no próximo clique */
            } finally {
              setDrilldownLoading(null);
            }
          })();
        }
      }
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao gravar ajuste."));
    } finally {
      setSavingKey(null);
    }
  };

  const inputStyle = {
    width: "100%",
    minWidth: 88,
    padding: "6px 8px",
    borderRadius: 8,
    border: "1px solid var(--border)",
    background: "var(--surface-1, var(--card-bg))",
    color: "inherit",
    fontSize: 12,
    textAlign: "right" as const,
    fontVariantNumeric: "tabular-nums" as const,
  };
  const moneyWrap = { display: "flex", alignItems: "center", gap: 4 };

  const multi = (idFiliais || []).filter(Boolean);
  if (!idFilial && multi.length === 0) {
    return (
      <div className="card" style={{ marginTop: 12 }}>
        <EmptyState
          title="Selecione o escopo"
          detail="Escolha uma ou mais filiais (ou Todas) no painel lateral para o cálculo gerencial."
        />
      </div>
    );
  }

  return (
    <div
      className="card"
      style={{
        border: "1px solid var(--border)",
        borderRadius: 14,
        overflow: "hidden",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          flexWrap: "wrap",
          marginBottom: 12,
          paddingBottom: 4,
        }}
      >
        <GridSearchInput value={query} onChange={setQuery} />
        <div className="muted" style={{ marginLeft: "auto", fontSize: 12, textAlign: "right" }}>
          Edite a linha e saia do campo para gravar.
        </div>
      </div>

      {error ? <div className="errorCard" style={{ marginBottom: 8 }}>{String(error)}</div> : null}

      {loading ? (
        <div className="muted" style={{ padding: 16, textAlign: "center" }}>Calculando…</div>
      ) : filteredRows.length === 0 ? (
        <EmptyState title="Sem linhas" detail="Nenhum cálculo para o período/filial." />
      ) : (
        <div className="tableScroll">
          <table className="table compact" style={{ width: "100%", minWidth: 980 }}>
            <thead>
              <tr>
                <th style={{ width: 28 }} />
                <SortableTh label="Filial" sortKey="filial_label" ariaSort={managerGrid.ariaSort("filial_label")} onToggle={managerGrid.toggleSort} />
                <SortableTh label="Venda bruta" sortKey="venda_bruta_total" ariaSort={managerGrid.ariaSort("venda_bruta_total")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Taxa %" sortKey="rate_pct" ariaSort={managerGrid.ariaSort("rate_pct")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Comissão bruta" sortKey="comissao_bruta" ariaSort={managerGrid.ariaSort("comissao_bruta")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Perdas estoque" sortKey="perdas_estoque" ariaSort={managerGrid.ariaSort("perdas_estoque")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Sobras estoque" sortKey="sobras_estoque" ariaSort={managerGrid.ariaSort("sobras_estoque")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Sobras caixa" sortKey="sobras_caixa" ariaSort={managerGrid.ariaSort("sobras_caixa")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Furos caixa" sortKey="furos_caixa" ariaSort={managerGrid.ariaSort("furos_caixa")} onToggle={managerGrid.toggleSort} align="right" />
                <SortableTh label="Comissão líquida" sortKey="comissao_liquida" ariaSort={managerGrid.ariaSort("comissao_liquida")} onToggle={managerGrid.toggleSort} align="right" />
              </tr>
            </thead>
            <tbody>
              {managerGrid.slice.map((row) => {
                const expanded = expandedFilial === row.id_filial;
                return (
                  <Fragment key={row.id_filial}>
                    <tr
                      onClick={() => void toggleExpand(row)}
                      style={{ cursor: "pointer" }}
                    >
                      <td style={{ width: 28 }}>{expanded ? "▾" : "▸"}</td>
                      <td style={{ fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>
                        {row.filial_label}
                      </td>
                      <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                        {formatCurrency(row.venda_bruta_total)}
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <input
                          style={inputStyle}
                          value={row.rate_text}
                          onChange={(e) => {
                            const text = formatPctInput(e.target.value);
                            const rate = Number(text.replace(",", ".")) || 0;
                            updateLocal(row.id_filial, { rate_text: text, rate_pct: rate });
                          }}
                          onBlur={() => {
                            const current = rows.find((r) => r.id_filial === row.id_filial);
                            if (current) void persist(current);
                          }}
                        />
                      </td>
                      <td style={{ textAlign: "right", fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
                        {formatCurrency(row.comissao_bruta)}
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <div style={moneyWrap}>
                          <span className="muted" style={{ fontSize: 11 }}>R$</span>
                          <input
                            style={inputStyle}
                            inputMode="decimal"
                            value={row.perdas_text}
                            onChange={(e) => {
                              const text = formatBrCurrencyInput(e.target.value);
                              updateLocal(row.id_filial, {
                                perdas_text: text,
                                perdas_estoque: parseBrCurrency(text),
                              });
                            }}
                            onBlur={() => {
                              const current = rows.find((r) => r.id_filial === row.id_filial);
                              if (current) void persist(current);
                            }}
                          />
                        </div>
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <div style={moneyWrap}>
                          <span className="muted" style={{ fontSize: 11 }}>R$</span>
                          <input
                            style={inputStyle}
                            inputMode="decimal"
                            value={row.sobras_est_text}
                            onChange={(e) => {
                              const text = formatBrCurrencyInput(e.target.value);
                              updateLocal(row.id_filial, {
                                sobras_est_text: text,
                                sobras_estoque: parseBrCurrency(text),
                              });
                            }}
                            onBlur={() => {
                              const current = rows.find((r) => r.id_filial === row.id_filial);
                              if (current) void persist(current);
                            }}
                          />
                        </div>
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <div style={moneyWrap}>
                          <span className="muted" style={{ fontSize: 11 }}>R$</span>
                          <input
                            style={inputStyle}
                            inputMode="decimal"
                            value={row.sobras_cx_text}
                            onChange={(e) => {
                              const text = formatBrCurrencyInput(e.target.value);
                              updateLocal(row.id_filial, {
                                sobras_cx_text: text,
                                sobras_caixa: parseBrCurrency(text),
                              });
                            }}
                            onBlur={() => {
                              const current = rows.find((r) => r.id_filial === row.id_filial);
                              if (current) void persist(current);
                            }}
                          />
                        </div>
                      </td>
                      <td onClick={(e) => e.stopPropagation()}>
                        <div style={moneyWrap}>
                          <span className="muted" style={{ fontSize: 11 }}>R$</span>
                          <input
                            style={inputStyle}
                            inputMode="decimal"
                            value={row.furos_text}
                            onChange={(e) => {
                              const text = formatBrCurrencyInput(e.target.value);
                              updateLocal(row.id_filial, {
                                furos_text: text,
                                furos_caixa: parseBrCurrency(text),
                              });
                            }}
                            onBlur={() => {
                              const current = rows.find((r) => r.id_filial === row.id_filial);
                              if (current) void persist(current);
                            }}
                          />
                        </div>
                      </td>
                      <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                        {formatCurrency(row.comissao_liquida)}
                        {savingKey === String(row.id_filial) ? (
                          <span className="muted" style={{ marginLeft: 6, fontWeight: 400 }}>…</span>
                        ) : null}
                      </td>
                    </tr>
                    {expanded ? (
                      <tr>
                        <td
                          colSpan={10}
                          style={{ padding: "10px 12px 14px", background: "var(--surface-faint, var(--surface-1))" }}
                          onClick={(e) => e.stopPropagation()}
                        >
                          <ManagerCommissionDrilldown
                            payload={drilldownByFilial[row.id_filial] || null}
                            loading={drilldownLoading === row.id_filial}
                          />
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                );
              })}
            </tbody>
            <tfoot className="commissionGridFoot">
              <tr>
                <td />
                <td style={{ textAlign: "left", fontWeight: 700 }}>Total da página ({managerGrid.slice.length})</td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.venda_bruta_total)}
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {pageRatePct.toFixed(2)}%
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.comissao_bruta)}
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.perdas_estoque)}
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.sobras_estoque)}
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.sobras_caixa)}
                </td>
                <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                  {formatCurrency(managerGrid.pageTotals.furos_caixa)}
                </td>
                <td
                  style={{
                    textAlign: "right",
                    fontWeight: 780,
                    fontVariantNumeric: "tabular-nums",
                    color: "var(--color-positive)",
                  }}
                >
                  {formatCurrency(managerGrid.pageTotals.comissao_liquida)}
                </td>
              </tr>
            </tfoot>
          </table>
          <GridChrome
            page={managerGrid.page}
            totalPages={managerGrid.totalPages}
            total={managerGrid.total}
            from={managerGrid.range.from}
            to={managerGrid.range.to}
            onPrev={managerGrid.onPrev}
            onNext={managerGrid.onNext}
            onResetOrder={managerGrid.resetOrder}
            isDefaultOrder={managerGrid.isDefaultOrder}
          />
        </div>
      )}
    </div>
  );
}
