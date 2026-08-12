"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { apiGet, apiPut } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";
import { sortGridRows } from "../lib/grid-sort";

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
  month: number;
  year: number;
}

export default function ManagerCommissionGrid({
  idEmpresa,
  idFilial,
  idFiliais,
  month,
  year,
}: Props) {
  const [rows, setRows] = useState<RowState[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [savingKey, setSavingKey] = useState<string | null>(null);

  const fetchCalc = useCallback(async () => {
    const multi = (idFiliais || []).map(String).filter(Boolean);
    if (!idFilial && multi.length === 0) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("month", String(month));
      params.set("year", String(year));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      // Multi (ou "todas"): manda id_filiais[]; single: id_filial
      if (!idFilial && multi.length > 0) {
        for (const f of multi) params.append("id_filiais", String(f));
      } else if (multi.length > 1) {
        for (const f of multi) params.append("id_filiais", String(f));
      } else if (idFilial) {
        params.set("id_filial", String(idFilial));
      } else if (multi[0]) {
        params.set("id_filial", String(multi[0]));
      }
      const resp = await apiGet(`/bi/team/manager-commissions/calc?${params.toString()}`);
      const mapped = (resp?.rows || []).map(mapApiRow) as RowState[];
      // Contrato grid: Filial ASC (rótulo operacional)
      setRows(
        sortGridRows(mapped, (row) => ({
          filial: row.filial_label || String(row.id_filial),
          nome: row.filial_label || String(row.id_filial),
        })),
      );
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao calcular comissão de gerentes.");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial, idFiliais, month, year]);

  useEffect(() => {
    fetchCalc();
  }, [fetchCalc]);

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

  const persist = async (row: RowState) => {
    const key = `${row.id_filial}`;
    setSavingKey(key);
    setError("");
    try {
      const resp = await apiPut("/bi/team/manager-commissions/overrides", {
        id_empresa: row.id_empresa,
        id_filial: row.id_filial,
        year,
        month,
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
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao gravar ajuste.");
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
                <th style={{ textAlign: "left" }}>Filial</th>
                <th style={{ textAlign: "right" }}>Venda bruta</th>
                <th style={{ textAlign: "right" }}>Taxa %</th>
                <th style={{ textAlign: "right" }}>Comissão bruta</th>
                <th style={{ textAlign: "right" }}>Perdas estoque</th>
                <th style={{ textAlign: "right" }}>Sobras estoque</th>
                <th style={{ textAlign: "right" }}>Sobras caixa</th>
                <th style={{ textAlign: "right" }}>Furos caixa</th>
                <th style={{ textAlign: "right" }}>Comissão líquida</th>
              </tr>
            </thead>
            <tbody>
              {(filteredRows as unknown as RowState[]).map((row) => (
                <tr key={row.id_filial}>
                  <td style={{ fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>
                    {row.filial_label}
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                    {formatCurrency(row.venda_bruta_total)}
                  </td>
                  <td>
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
                  <td>
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
                  <td>
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
                  <td>
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
                  <td>
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
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
