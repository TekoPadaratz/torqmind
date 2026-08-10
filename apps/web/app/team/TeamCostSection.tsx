"use client";

import { useEffect, useMemo, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import GridPager from "../components/ui/GridPager";
import GridSearchInput from "../components/ui/GridSearchInput";
import { formatCurrency } from "../lib/format";
import { apiGet, apiPut } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { buildScopeParams, useScopeQuery } from "../lib/scope";

const PAGE_SIZE = 50;

function currentAnoMesSP(): number {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
  });
  const parts = fmt.formatToParts(new Date());
  const y = Number(parts.find((p) => p.type === "year")?.value || 0);
  const m = Number(parts.find((p) => p.type === "month")?.value || 0);
  return y * 100 + m;
}

function fmtAnoMes(ym: number): string {
  const y = Math.floor(ym / 100);
  const m = ym % 100;
  const nomes = [
    "", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
  ];
  return `${nomes[m] || m}/${y}`;
}

function buildMesesDisponiveis(selected: number, monthsBack = 18): number[] {
  const set = new Set<number>([selected]);
  let cursor = currentAnoMesSP();
  for (let i = 0; i < monthsBack; i += 1) {
    set.add(cursor);
    const y = Math.floor(cursor / 100);
    const m = cursor % 100;
    cursor = m <= 1 ? (y - 1) * 100 + 12 : y * 100 + (m - 1);
  }
  return Array.from(set).sort((a, b) => b - a);
}

function parseMoneyInput(raw: string): number {
  const cleaned = String(raw || "")
    .replace(/\s/g, "")
    .replace(/R\$/gi, "")
    .replace(/\./g, "")
    .replace(",", ".");
  const n = Number(cleaned);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.round(n * 100) / 100;
}

function moneyDraft(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  return value.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

type EmployeeRow = {
  id_filial?: number;
  id_funcionario?: number;
  filial_nome?: string;
  nome?: string;
  funcao?: string;
  salario?: number;
  vales?: number;
  horas_extras?: number;
  custo_direto?: number;
  rateio_overhead?: number;
  custo_total?: number;
  vendas?: number;
  vales_manual?: boolean;
  horas_extras_manual?: boolean;
};

type DraftPair = { vales: string; horas_extras: string };

function rowKey(row: EmployeeRow): string {
  return `${Number(row.id_filial || 0)}:${Number(row.id_funcionario || 0)}`;
}

export default function TeamCostSection() {
  const scope = useScopeQuery();
  const [anoMes, setAnoMes] = useState<number>(() => currentAnoMesSP());
  const [q, setQ] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [saveError, setSaveError] = useState("");
  const [data, setData] = useState<any>(null);
  const [drafts, setDrafts] = useState<Record<string, DraftPair>>({});
  const [savingKey, setSavingKey] = useState<string | null>(null);

  const mesesDisponiveis = useMemo(() => buildMesesDisponiveis(anoMes), [anoMes]);

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedQ(q.trim()), 250);
    return () => window.clearTimeout(t);
  }, [q]);

  useEffect(() => {
    setPage(1);
  }, [debouncedQ, anoMes, scope.scope_key]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      setSaveError("");
      try {
        const params = buildScopeParams(scope);
        params.set("ano_mes", String(anoMes));
        params.set("page", String(page));
        params.set("page_size", String(PAGE_SIZE));
        if (debouncedQ) params.set("q", debouncedQ);
        const payload = await apiGet(`/bi/team/employee-cost?${params.toString()}`, {
          signal: controller.signal,
        });
        setData(payload);
        const next: Record<string, DraftPair> = {};
        for (const row of (payload?.items || []) as EmployeeRow[]) {
          next[rowKey(row)] = {
            vales: moneyDraft(row.vales),
            horas_extras: moneyDraft(row.horas_extras),
          };
        }
        setDrafts(next);
      } catch (err: any) {
        if (err?.name === "AbortError" || err?.code === "ERR_CANCELED") return;
        setError(extractApiError(err, "Falha ao carregar custo da equipe"));
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [anoMes, debouncedQ, page, scope]);

  const summary = data?.summary || {};
  const items: EmployeeRow[] = data?.items || [];
  const totalPages = Math.max(1, Math.ceil(Number(data?.total || 0) / PAGE_SIZE) || 1);
  const canEditMoney = items.some((r) => typeof r.vales === "number");

  const updateDraft = (key: string, field: keyof DraftPair, raw: string) => {
    setDrafts((prev) => ({
      ...prev,
      [key]: {
        vales: prev[key]?.vales ?? "",
        horas_extras: prev[key]?.horas_extras ?? "",
        [field]: raw,
      },
    }));
  };

  const saveRow = async (row: EmployeeRow) => {
    const key = rowKey(row);
    const draft = drafts[key];
    if (!draft || !row.id_filial || !row.id_funcionario) return;
    if (typeof row.vales !== "number" || typeof row.horas_extras !== "number") return;

    const vales = parseMoneyInput(draft.vales);
    const horas_extras = parseMoneyInput(draft.horas_extras);
    const unchanged =
      Math.abs(vales - Number(row.vales || 0)) < 0.005 &&
      Math.abs(horas_extras - Number(row.horas_extras || 0)) < 0.005;
    if (unchanged) {
      setDrafts((prev) => ({
        ...prev,
        [key]: { vales: moneyDraft(vales), horas_extras: moneyDraft(horas_extras) },
      }));
      return;
    }

    setSavingKey(key);
    setSaveError("");
    try {
      const params = buildScopeParams(scope);
      await apiPut(`/bi/team/employee-cost?${params.toString()}`, {
        id_filial: Number(row.id_filial),
        id_funcionario: Number(row.id_funcionario),
        ano_mes: anoMes,
        vales,
        horas_extras,
      });
      const salario = Number(row.salario || 0);
      const rateio = Number(row.rateio_overhead || 0);
      const direto = Math.round((salario + vales + horas_extras) * 100) / 100;
      setData((prev: any) => {
        if (!prev?.items) return prev;
        return {
          ...prev,
          items: prev.items.map((it: EmployeeRow) =>
            rowKey(it) === key
              ? {
                  ...it,
                  vales,
                  horas_extras,
                  custo_direto: direto,
                  custo_total: Math.round((direto + rateio) * 100) / 100,
                  vales_manual: true,
                  horas_extras_manual: true,
                }
              : it
          ),
        };
      });
      setDrafts((prev) => ({
        ...prev,
        [key]: { vales: moneyDraft(vales), horas_extras: moneyDraft(horas_extras) },
      }));
    } catch (err: any) {
      setSaveError(extractApiError(err, "Falha ao salvar Vale/Hora extra"));
      setDrafts((prev) => ({
        ...prev,
        [key]: {
          vales: moneyDraft(row.vales),
          horas_extras: moneyDraft(row.horas_extras),
        },
      }));
    } finally {
      setSavingKey(null);
    }
  };

  return (
    <div className="card col-12" style={{ marginTop: 12 }}>
      <div className="sectionEyebrow">Equipe</div>
      <h2 style={{ marginTop: 4 }}>Custo do funcionário</h2>

      <div style={{ display: "flex", gap: 8, marginTop: 14, alignItems: "center", flexWrap: "wrap" }}>
        <label className="profitScopeMonth" title="Mês de referência do custo da equipe">
          <span className="profitScopeMonthLabel">Mês</span>
          <select
            className="profitScopeMonthSelect"
            value={anoMes}
            onChange={(e) => setAnoMes(Number(e.target.value))}
            aria-label="Mês do custo da equipe"
          >
            {mesesDisponiveis.map((m) => (
              <option key={m} value={m}>
                {fmtAnoMes(m)}
              </option>
            ))}
          </select>
        </label>
        <GridSearchInput value={q} onChange={setQ} />
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: 12,
          marginTop: 16,
        }}
      >
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Funcionários ativos</div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>
            {loading ? "…" : Number(summary.qtd_funcionarios || 0).toLocaleString("pt-BR")}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Pessoal (mês)</div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>
            {loading ? "…" : formatCurrency(summary.total_pessoal_mes)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Despesas rateáveis (mês)</div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>
            {loading ? "…" : formatCurrency(summary.total_overhead_mes)}
          </div>
        </div>
        <div className="card">
          <div className="muted" style={{ fontSize: 12 }}>Rateio por funcionário</div>
          <div style={{ fontSize: 20, fontWeight: 700 }}>
            {loading ? "…" : formatCurrency(summary.rateio_overhead_cabeca)}
          </div>
        </div>
      </div>

      {error ? <div className="errorCard" style={{ marginTop: 12 }}>{error}</div> : null}
      {saveError ? <div className="errorCard" style={{ marginTop: 12 }}>{saveError}</div> : null}

      <div style={{ marginTop: 16 }}>
        {!loading && items.length === 0 ? (
          <EmptyState
            title="Sem funcionários ativos no escopo."
            detail="Ajuste a filial ou a busca e tente de novo."
          />
        ) : (
          <div className="tableScroll tableScroll--compact">
            <table className="table compact">
              <thead>
                <tr>
                  <th>Filial</th>
                  <th>Funcionário</th>
                  <th>Função</th>
                  <th>Salário</th>
                  <th>Vale</th>
                  <th>Hora extra</th>
                  <th>Custo direto</th>
                  <th>Rateio posto</th>
                  <th>Custo total</th>
                  <th>Vendas</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row, idx) => {
                  const key = rowKey(row);
                  const draft = drafts[key];
                  const editable =
                    canEditMoney &&
                    typeof row.vales === "number" &&
                    typeof row.horas_extras === "number" &&
                    Boolean(row.id_filial) &&
                    Boolean(row.id_funcionario);
                  const busy = savingKey === key;
                  return (
                    <tr key={`${key}-${idx}`}>
                      <td>{row.filial_nome || "—"}</td>
                      <td style={{ fontWeight: 600 }}>{row.nome || "—"}</td>
                      <td>{row.funcao || "—"}</td>
                      <td>{formatCurrency(row.salario)}</td>
                      <td>
                        {editable ? (
                          <input
                            className="input"
                            type="text"
                            inputMode="decimal"
                            aria-label={`Vale de ${row.nome || "funcionário"}`}
                            value={draft?.vales ?? ""}
                            disabled={busy}
                            onChange={(e) => updateDraft(key, "vales", e.target.value)}
                            onBlur={() => void saveRow(row)}
                            style={{ width: 110, textAlign: "right" }}
                          />
                        ) : (
                          formatCurrency(row.vales)
                        )}
                      </td>
                      <td>
                        {editable ? (
                          <input
                            className="input"
                            type="text"
                            inputMode="decimal"
                            aria-label={`Hora extra de ${row.nome || "funcionário"}`}
                            value={draft?.horas_extras ?? ""}
                            disabled={busy}
                            onChange={(e) => updateDraft(key, "horas_extras", e.target.value)}
                            onBlur={() => void saveRow(row)}
                            style={{ width: 110, textAlign: "right" }}
                          />
                        ) : (
                          formatCurrency(row.horas_extras)
                        )}
                      </td>
                      <td>{formatCurrency(row.custo_direto)}</td>
                      <td>{formatCurrency(row.rateio_overhead)}</td>
                      <td style={{ fontWeight: 700 }}>{formatCurrency(row.custo_total)}</td>
                      <td>{formatCurrency(row.vendas)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <GridPager
          page={page}
          pageSize={PAGE_SIZE}
          total={Number(data?.total || 0)}
          totalPages={totalPages}
          onPrev={() => setPage((p) => Math.max(1, p - 1))}
          onNext={() => setPage((p) => Math.min(totalPages, p + 1))}
        />
      </div>
    </div>
  );
}
