"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { formatCurrency } from "../lib/format";
import { apiPut } from "../lib/api";
import { buildScopeParams } from "../lib/scope";
import PortalDropdown from "../components/ui/PortalDropdown";

type Contadores = {
  OK?: number;
  ALERTA?: number;
  RISCO_ABUSIVO?: number;
  SEM_LASTRO?: number;
};

type Evento = {
  id_filial: number;
  nome_resumido?: string;
  nome_produto: string;
  data_alteracao?: string;
  dt_alteracao_preco?: string;
  preco_venda_anterior: number;
  preco_venda_novo: number;
  custo_nfe_anterior: number;
  custo_nfe_novo: number;
  margem_anterior: number;
  margem_nova: number;
  variacao_margem_pct: number | null;
  status: string;
  chave_nfe_nova?: string;
  numero_nota_nova?: string;
  cnpj_emitente_nova?: string;
};

type AnpData = {
  total_eventos: number;
  contadores: Contadores;
  config: {
    limite_alerta_amarelo_perc: number;
    limite_abusivo_anp_perc: number;
    id_filial_config?: number;
  };
  eventos: Evento[];
  periodo?: { dt_ini: string; dt_fim: string };
};

const STATUS_STYLE: Record<string, { label: string; color: string; bg: string }> = {
  OK: { label: "OK", color: "var(--color-positive)", bg: "rgba(34,197,94,0.12)" },
  ALERTA: { label: "Alerta", color: "var(--color-warning)", bg: "rgba(245,158,11,0.14)" },
  RISCO_ABUSIVO: {
    label: "Risco abusivo",
    color: "var(--color-negative)",
    bg: "rgba(239,68,68,0.14)",
  },
  SEM_LASTRO: { label: "Sem lastro", color: "#64748b", bg: "rgba(100,116,139,0.12)" },
};

function fmtPct(v: number | null | undefined): string {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `${Number(v).toFixed(1)}%`;
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return formatCurrency(Number(v));
}

function fmtDateBr(v: string | null | undefined): string {
  if (!v) return "—";
  const iso = String(v).slice(0, 10);
  const [y, m, d] = iso.split("-");
  if (!y || !m || !d) return iso;
  return `${d}/${m}/${y}`;
}

function eventDateIso(e: Evento): string {
  return String(e.data_alteracao || e.dt_alteracao_preco || "").slice(0, 10);
}

export function AnpCompliancePanel({
  data,
  loading,
  scope,
  dtIni,
  dtFim,
  onPeriodChange,
  onConfigSaved,
}: {
  data: AnpData | null | undefined;
  loading?: boolean;
  scope: Parameters<typeof buildScopeParams>[0];
  dtIni: string;
  dtFim: string;
  onPeriodChange: (dtIni: string, dtFim: string) => void;
  onConfigSaved?: () => void;
}) {
  const [statusFilter, setStatusFilter] = useState("");
  const [selectedProducts, setSelectedProducts] = useState<string[]>([]);
  const [productMenuOpen, setProductMenuOpen] = useState(false);
  const productBtnRef = useRef<HTMLButtonElement | null>(null);
  const [alerta, setAlerta] = useState(50);
  const [abusivo, setAbusivo] = useState(70);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState("");
  const [draftIni, setDraftIni] = useState(dtIni);
  const [draftFim, setDraftFim] = useState(dtFim);

  useEffect(() => {
    setDraftIni(dtIni);
    setDraftFim(dtFim);
  }, [dtIni, dtFim]);

  useEffect(() => {
    if (!data?.config) return;
    setAlerta(Number(data.config.limite_alerta_amarelo_perc ?? 50));
    setAbusivo(Number(data.config.limite_abusivo_anp_perc ?? 70));
  }, [data?.config?.limite_alerta_amarelo_perc, data?.config?.limite_abusivo_anp_perc]);

  // Ao trocar período/payload, limpa seleção de produtos inexistentes no novo conjunto.
  useEffect(() => {
    const names = new Set((data?.eventos || []).map((e) => e.nome_produto).filter(Boolean));
    setSelectedProducts((prev) => prev.filter((p) => names.has(p)));
  }, [data?.eventos]);

  const availableProducts = useMemo(() => {
    const set = new Map<string, number>();
    for (const e of data?.eventos || []) {
      const name = (e.nome_produto || "").trim();
      if (!name) continue;
      set.set(name, (set.get(name) || 0) + 1);
    }
    return Array.from(set.entries())
      .map(([nome, qtd]) => ({ nome, qtd }))
      .sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR"));
  }, [data?.eventos]);

  const eventosByProduct = useMemo(() => {
    const list = data?.eventos || [];
    if (!selectedProducts.length) return list;
    const selected = new Set(selectedProducts);
    return list.filter((e) => selected.has(e.nome_produto));
  }, [data?.eventos, selectedProducts]);

  const eventos = useMemo(() => {
    if (!statusFilter) return eventosByProduct;
    return eventosByProduct.filter((e) => e.status === statusFilter);
  }, [eventosByProduct, statusFilter]);

  const counts = useMemo(() => {
    const c: Contadores = { OK: 0, ALERTA: 0, RISCO_ABUSIVO: 0, SEM_LASTRO: 0 };
    for (const e of eventosByProduct) {
      const k = e.status as keyof Contadores;
      if (k in c) c[k] = (c[k] || 0) + 1;
    }
    return c;
  }, [eventosByProduct]);

  function applyPeriod(nextIni: string, nextFim: string) {
    let ini = nextIni;
    let fim = nextFim;
    if (ini && fim && fim < ini) {
      [ini, fim] = [fim, ini];
    }
    setDraftIni(ini);
    setDraftFim(fim);
    if (ini && fim && (ini !== dtIni || fim !== dtFim)) {
      onPeriodChange(ini, fim);
    }
  }

  function toggleProduct(nome: string) {
    setSelectedProducts((prev) =>
      prev.includes(nome) ? prev.filter((x) => x !== nome) : [...prev, nome],
    );
  }

  async function saveConfig() {
    setSaving(true);
    setSaveMsg("");
    try {
      const params = buildScopeParams(scope);
      const res = await apiPut(
        `/bi/profit-management/anp-compliance/config?${params.toString()}`,
        {
          id_filial: 0,
          limite_alerta_amarelo_perc: Number(alerta),
          limite_abusivo_anp_perc: Number(abusivo),
        },
      );
      if (res?.ok === false) {
        setSaveMsg(res.message || "Falha ao salvar.");
      } else {
        setSaveMsg("Limites salvos.");
        onConfigSaved?.();
      }
    } catch (err: unknown) {
      setSaveMsg(err instanceof Error ? err.message : "Falha ao salvar.");
    } finally {
      setSaving(false);
    }
  }

  function printPdf() {
    document.body.classList.add("anp-printing");
    const cleanup = () => {
      document.body.classList.remove("anp-printing");
      window.removeEventListener("afterprint", cleanup);
    };
    window.addEventListener("afterprint", cleanup);
    window.setTimeout(() => window.print(), 50);
  }

  const periodoLabel =
    (data?.periodo?.dt_ini && data?.periodo?.dt_fim
      ? `${fmtDateBr(data.periodo.dt_ini)} a ${fmtDateBr(data.periodo.dt_fim)}`
      : null) ||
    (draftIni && draftFim ? `${fmtDateBr(draftIni)} a ${fmtDateBr(draftFim)}` : "");

  const alertaPerc = data?.config?.limite_alerta_amarelo_perc ?? alerta;
  const abusivoPerc = data?.config?.limite_abusivo_anp_perc ?? abusivo;

  const produtosFiltroLabel =
    selectedProducts.length === 0
      ? "Todos os produtos"
      : selectedProducts.length <= 3
        ? selectedProducts.join(", ")
        : `${selectedProducts.length} produto(s) selecionado(s)`;

  const statusFiltroLabel = statusFilter
    ? STATUS_STYLE[statusFilter]?.label || statusFilter
    : "Todos os status";

  const printedAt = new Date().toLocaleString("pt-BR", { timeZone: "America/Sao_Paulo" });

  return (
    <div className="anpPrintRoot" style={{ marginTop: 16 }}>
      <div className="profitKpiStrip anpNoPrint">
        {(
          [
            ["RISCO_ABUSIVO", counts.RISCO_ABUSIVO || 0],
            ["ALERTA", counts.ALERTA || 0],
            ["OK", counts.OK || 0],
            ["SEM_LASTRO", counts.SEM_LASTRO || 0],
          ] as const
        ).map(([key, value]) => {
          const st = STATUS_STYLE[key];
          return (
            <div
              key={key}
              className="profitKpiCard"
              style={{ "--kpi-accent": st.color } as React.CSSProperties}
            >
              <div className="profitKpiLabel">{st.label}</div>
              <div className="profitKpiValue" style={{ color: st.color }}>
                {loading && !data ? "…" : value}
              </div>
              <div className="profitKpiContext">
                {key === "RISCO_ABUSIVO"
                  ? `≥ ${abusivoPerc}%`
                  : key === "ALERTA"
                    ? `≥ ${alertaPerc}%`
                    : "eventos"}
              </div>
            </div>
          );
        })}
      </div>

      <div className="anpFilterRow anpNoPrint" style={{ marginTop: 16 }}>
        <div className="anpFilterLeft">
          <input
            type="date"
            value={draftIni}
            onChange={(e) => {
              const v = e.target.value;
              setDraftIni(v);
              if (v && draftFim) applyPeriod(v, draftFim);
            }}
            title="Data inicial (filtro ANP)"
            aria-label="Data inicial do Compliance ANP"
          />
          <input
            type="date"
            value={draftFim}
            onChange={(e) => {
              const v = e.target.value;
              setDraftFim(v);
              if (draftIni && v) applyPeriod(draftIni, v);
            }}
            title="Data final (filtro ANP)"
            aria-label="Data final do Compliance ANP"
          />

          <button
            ref={productBtnRef}
            type="button"
            className="anpProductFilterBtn"
            onClick={() => setProductMenuOpen((o) => !o)}
            aria-haspopup="listbox"
            aria-expanded={productMenuOpen}
          >
            {selectedProducts.length === 0
              ? "Produtos: todos"
              : `Produtos: ${selectedProducts.length} selecionado(s)`}
            <span aria-hidden>▾</span>
          </button>
          <PortalDropdown
            open={productMenuOpen}
            onClose={() => setProductMenuOpen(false)}
            anchorRef={productBtnRef}
            minWidth={300}
          >
            <div className="anpProductMenu">
              <div className="anpProductMenuTop">
                <button type="button" onClick={() => setSelectedProducts([])}>
                  Todos os produtos
                </button>
                <button type="button" onClick={() => setProductMenuOpen(false)}>
                  Fechar
                </button>
              </div>
              {availableProducts.length === 0 ? (
                <div className="muted" style={{ padding: 8, fontSize: 12 }}>
                  Nenhum produto no período.
                </div>
              ) : (
                availableProducts.map((p) => (
                  <label key={p.nome} className="anpProductMenuItem">
                    <input
                      type="checkbox"
                      checked={selectedProducts.includes(p.nome)}
                      onChange={() => toggleProduct(p.nome)}
                    />
                    <span className="anpProductMenuName">{p.nome}</span>
                    <span className="muted" style={{ fontSize: 11 }}>
                      {p.qtd}
                    </span>
                  </label>
                ))
              )}
            </div>
          </PortalDropdown>

          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            aria-label="Filtrar status"
          >
            <option value="">Todos os status</option>
            <option value="RISCO_ABUSIVO">Risco abusivo</option>
            <option value="ALERTA">Alerta</option>
            <option value="OK">OK</option>
            <option value="SEM_LASTRO">Sem lastro</option>
          </select>

          <button
            type="button"
            className="btn"
            onClick={printPdf}
            disabled={!data || loading || eventos.length === 0}
          >
            Imprimir / PDF
          </button>

          <span className="profitFilterCount">
            {loading && !data
              ? "Carregando…"
              : `${eventos.length}${data?.total_eventos ? ` de ${data.total_eventos}` : ""} eventos`}
          </span>
        </div>

        <div className="anpFilterRight" title="Limites de classificação (salvos na empresa)">
          <span className="anpConfigLabel">Limites %</span>
          <input
            type="number"
            min={0}
            step={1}
            value={alerta}
            onChange={(e) => setAlerta(Number(e.target.value))}
            title="Limite alerta (%)"
            aria-label="Limite alerta (%)"
            placeholder="Alerta %"
          />
          <input
            type="number"
            min={0}
            step={1}
            value={abusivo}
            onChange={(e) => setAbusivo(Number(e.target.value))}
            title="Limite abusivo ANP (%)"
            aria-label="Limite abusivo ANP (%)"
            placeholder="Abusivo %"
          />
          <button type="button" className="btn" disabled={saving} onClick={saveConfig}>
            {saving ? "Salvando…" : "Salvar limites"}
          </button>
        </div>
      </div>
      {saveMsg ? (
        <div className="muted anpNoPrint" style={{ fontSize: 12, marginBottom: 8 }}>
          {saveMsg}
        </div>
      ) : null}

      {/* Tela */}
      <div className="card anpScreenArea anpNoPrint">
        <div className="anpPrintHeader">
          <div className="sectionEyebrow">Compliance ANP — variação de margem (combustíveis)</div>
          {periodoLabel ? (
            <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
              Período: {periodoLabel} · Produtos: {produtosFiltroLabel} · Status: {statusFiltroLabel} ·
              Limites: alerta {alertaPerc}% / abusivo {abusivoPerc}% ·{" "}
              {loading && !data ? "…" : `${eventos.length} evento(s)`}
            </div>
          ) : null}
        </div>
        {loading && !data ? (
          <div className="muted" style={{ marginTop: 12 }}>
            Carregando eventos do período selecionado…
          </div>
        ) : !data ? (
          <div className="muted" style={{ marginTop: 12 }}>
            Sem eventos de Compliance ANP para o período/filiais.
          </div>
        ) : eventos.length === 0 ? (
          <div className="muted" style={{ marginTop: 12 }}>
            Nenhum reajuste de preço no período/filiais/produtos selecionados.
          </div>
        ) : (
          <div className="anpTableScroll" role="region" aria-label="Tabela Compliance ANP" tabIndex={0}>
            <table className="anpTable">
              <thead>
                <tr>
                  <th>Filial</th>
                  <th>Data</th>
                  <th>Produto</th>
                  <th className="num">Preço ant.</th>
                  <th className="num">Preço novo</th>
                  <th className="num">Custo ant.</th>
                  <th className="num">Custo novo</th>
                  <th className="num">Margem ant.</th>
                  <th className="num">Margem nova</th>
                  <th className="num">Variação</th>
                  <th className="anpColStatus">Status</th>
                  <th>Doc. entrada</th>
                  <th className="anpColChave">Chave NFe</th>
                </tr>
              </thead>
              <tbody>
                {eventos.map((e, i) => {
                  const st = STATUS_STYLE[e.status] || STATUS_STYLE.OK;
                  const dataAlt = eventDateIso(e);
                  const chave = (e.chave_nfe_nova || "").trim();
                  return (
                    <tr key={`${e.id_filial}-${e.nome_produto}-${dataAlt}-${i}`}>
                      <td>{e.nome_resumido || e.id_filial}</td>
                      <td>{fmtDateBr(dataAlt)}</td>
                      <td>{e.nome_produto}</td>
                      <td className="num">{fmtMoney(e.preco_venda_anterior)}</td>
                      <td className="num">{fmtMoney(e.preco_venda_novo)}</td>
                      <td className="num">{fmtMoney(e.custo_nfe_anterior)}</td>
                      <td className="num">{fmtMoney(e.custo_nfe_novo)}</td>
                      <td className="num">{fmtMoney(e.margem_anterior)}</td>
                      <td className="num">{fmtMoney(e.margem_nova)}</td>
                      <td className="num" style={{ color: st.color, fontWeight: 600 }}>
                        {fmtPct(e.variacao_margem_pct)}
                      </td>
                      <td className="anpColStatus">
                        <span
                          style={{
                            display: "inline-block",
                            padding: "2px 8px",
                            borderRadius: 4,
                            background: st.bg,
                            color: st.color,
                            fontSize: 12,
                            fontWeight: 600,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {st.label}
                        </span>
                      </td>
                      <td>{e.numero_nota_nova || "—"}</td>
                      <td className="anpColChave" title={chave || undefined}>
                        {chave || "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Relatório de impressão (só no PDF) */}
      <div className="anpPrintReport" aria-hidden>
        <header className="anpReportHeader">
          <div className="anpReportBrand">TorqMind</div>
          <h1 className="anpReportTitle">Compliance ANP — variação de margem (combustíveis)</h1>
          <div className="anpReportMeta">
            <div>
              <strong>Período:</strong> {periodoLabel || "—"}
            </div>
            <div>
              <strong>Produtos:</strong> {produtosFiltroLabel}
            </div>
            <div>
              <strong>Status:</strong> {statusFiltroLabel}
            </div>
            <div>
              <strong>Limites:</strong> alerta {alertaPerc}% · abusivo {abusivoPerc}%
            </div>
            <div>
              <strong>Eventos:</strong> {eventos.length}
              {data?.total_eventos ? ` (de ${data.total_eventos} no período)` : ""}
            </div>
            <div>
              <strong>Gerado em:</strong> {printedAt}
            </div>
          </div>
        </header>

        {eventos.length === 0 ? (
          <p>Nenhum evento para o filtro atual.</p>
        ) : (
          <table className="anpReportTable">
            <thead>
              <tr>
                <th>Filial</th>
                <th>Data</th>
                <th>Produto</th>
                <th className="num">Preço ant.</th>
                <th className="num">Preço novo</th>
                <th className="num">Custo ant.</th>
                <th className="num">Custo novo</th>
                <th className="num">Margem ant.</th>
                <th className="num">Margem nova</th>
                <th className="num">Variação</th>
                <th>Status</th>
                <th>Doc. entrada</th>
                <th>Chave NFe</th>
              </tr>
            </thead>
            <tbody>
              {eventos.map((e, i) => {
                const st = STATUS_STYLE[e.status] || STATUS_STYLE.OK;
                const dataAlt = eventDateIso(e);
                const chave = (e.chave_nfe_nova || "").trim();
                return (
                  <tr key={`print-${e.id_filial}-${e.nome_produto}-${dataAlt}-${i}`}>
                    <td>{e.nome_resumido || e.id_filial}</td>
                    <td>{fmtDateBr(dataAlt)}</td>
                    <td>{e.nome_produto}</td>
                    <td className="num">{fmtMoney(e.preco_venda_anterior)}</td>
                    <td className="num">{fmtMoney(e.preco_venda_novo)}</td>
                    <td className="num">{fmtMoney(e.custo_nfe_anterior)}</td>
                    <td className="num">{fmtMoney(e.custo_nfe_novo)}</td>
                    <td className="num">{fmtMoney(e.margem_anterior)}</td>
                    <td className="num">{fmtMoney(e.margem_nova)}</td>
                    <td className="num">{fmtPct(e.variacao_margem_pct)}</td>
                    <td>{st.label}</td>
                    <td>{e.numero_nota_nova || "—"}</td>
                    <td className="anpReportChave">{chave || "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
