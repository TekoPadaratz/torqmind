"use client";

import { useEffect, useMemo, useState } from "react";
import { apiGet, isRequestCanceled } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";
import { coerceDisplayMessage, extractApiError } from "../lib/errors";
import { formatCommissionPeriodLabel } from "../lib/commission-period.mjs";
import CommissionCentralMirrorToggle from "./CommissionCentralMirrorToggle";

const TIER_STYLES: Record<
  string,
  { color: string; bg: string; bgActive: string; label: string }
> = {
  diamond: {
    color: "#5ba4ff",
    bg: "rgba(79,156,247,0.10)",
    bgActive: "rgba(79,156,247,0.22)",
    label: "Diamante",
  },
  gold: {
    color: "#e0b12a",
    bg: "rgba(212,160,23,0.12)",
    bgActive: "rgba(212,160,23,0.26)",
    label: "Ouro",
  },
  silver: {
    color: "#b8c0cc",
    bg: "rgba(160,160,160,0.12)",
    bgActive: "rgba(160,160,160,0.26)",
    label: "Prata",
  },
  bronze: {
    color: "#d08a4a",
    bg: "rgba(205,127,50,0.12)",
    bgActive: "rgba(205,127,50,0.26)",
    label: "Bronze",
  },
  none: {
    color: "var(--muted)",
    bg: "var(--surface-faint)",
    bgActive: "var(--surface-soft)",
    label: "Sem nível",
  },
};

const TIER_FILTER_ORDER = ["diamond", "gold", "silver", "bronze", "none"] as const;

type SellerRow = {
  id_filial?: number | null;
  filial_label?: string | null;
  id_funcionario?: number | null;
  nome_vendedor?: string | null;
  quantidade_vendas?: number | null;
  venda_elegivel?: number | null;
  percentual_aplicado?: number | null;
  comissao_estimada?: number | null;
  nivel_atingido?: { tier_key?: string; tier_name?: string } | null;
  [key: string]: unknown;
};

function sellerTierKey(emp: SellerRow): string {
  const key = String(emp?.nivel_atingido?.tier_key || "")
    .trim()
    .toLowerCase();
  return key && TIER_STYLES[key] ? key : "none";
}

function sellerTierLabel(emp: SellerRow): string {
  const key = sellerTierKey(emp);
  if (key === "none") return TIER_STYLES.none.label;
  return String(emp?.nivel_atingido?.tier_name || TIER_STYLES[key]?.label || key);
}

function sortSellersByCommission(rows: SellerRow[]): SellerRow[] {
  return [...rows].sort((a, b) => {
    const ca = Number(a.comissao_estimada || 0);
    const cb = Number(b.comissao_estimada || 0);
    if (cb !== ca) return cb - ca;
    const qa = Number(a.quantidade_vendas || 0);
    const qb = Number(b.quantidade_vendas || 0);
    if (qb !== qa) return qb - qa;
    const va = Number(a.venda_elegivel || 0);
    const vb = Number(b.venda_elegivel || 0);
    if (vb !== va) return vb - va;
    const na = String(a.nome_vendedor || "").localeCompare(
      String(b.nome_vendedor || ""),
      "pt-BR",
      { sensitivity: "base" },
    );
    if (na !== 0) return na;
    return Number(a.id_funcionario || 0) - Number(b.id_funcionario || 0);
  });
}

interface CommissionsTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  idFiliais?: string[];
  dtIni: string;
  dtFim: string;
}

function escapeHtml(value: string | number | null | undefined): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function buildCommissionsReportHtml(opts: {
  empresaLabel: string;
  filiaisLabel: string;
  periodoLabel: string;
  modoLabel: string;
  niveisLabel: string;
  includeValues: boolean;
  groups: Array<{
    label: string;
    sellers: SellerRow[];
    total: number;
  }>;
  totalGeral: number;
}): string {
  const printedAt = new Date().toLocaleString("pt-BR", { timeZone: "America/Sao_Paulo" });
  const includeValues = opts.includeValues;
  const sections = opts.groups
    .map((g) => {
      const rows = g.sellers
        .map((emp) => {
          const qty = Number(emp.quantidade_vendas || 0).toLocaleString("pt-BR", {
            maximumFractionDigits: 0,
          });
          if (!includeValues) {
            return `<tr>
            <td>${escapeHtml(emp.nome_vendedor || "—")}</td>
            <td>${escapeHtml(sellerTierLabel(emp))}</td>
            <td class="num">${escapeHtml(qty)}</td>
          </tr>`;
          }
          return `<tr>
            <td>${escapeHtml(emp.nome_vendedor || "—")}</td>
            <td>${escapeHtml(sellerTierLabel(emp))}</td>
            <td class="num">${escapeHtml(qty)}</td>
            <td class="num">${escapeHtml(formatCurrency(emp.venda_elegivel))}</td>
            <td class="num">${escapeHtml(Number(emp.percentual_aplicado || 0).toFixed(2))}%</td>
            <td class="num">${escapeHtml(formatCurrency(emp.comissao_estimada || 0))}</td>
          </tr>`;
        })
        .join("\n");
      const qtyTot = g.sellers
        .reduce((acc, emp) => acc + Number(emp.quantidade_vendas || 0), 0)
        .toLocaleString("pt-BR", { maximumFractionDigits: 0 });
      const vendaTot = formatCurrency(
        g.sellers.reduce((acc, emp) => acc + Number(emp.venda_elegivel || 0), 0),
      );
      const head = includeValues
        ? `<tr>
              <th>Vendedor</th>
              <th>Nível</th>
              <th class="num">Quantidade</th>
              <th class="num">Venda elegível</th>
              <th class="num">Percentual</th>
              <th class="num">Comissão</th>
            </tr>`
        : `<tr>
              <th>Vendedor</th>
              <th>Nível</th>
              <th class="num">Quantidade</th>
            </tr>`;
      const foot = includeValues
        ? `<tfoot>
            <tr>
              <td colspan="2"><strong>Total filial</strong></td>
              <td class="num"><strong>${escapeHtml(qtyTot)}</strong></td>
              <td class="num"><strong>${escapeHtml(vendaTot)}</strong></td>
              <td class="num">—</td>
              <td class="num"><strong>${escapeHtml(formatCurrency(g.total))}</strong></td>
            </tr>
          </tfoot>`
        : "";
      return `<section class="filial">
        <h2>${escapeHtml(g.label)}</h2>
        <table>
          <thead>${head}</thead>
          <tbody>${rows}</tbody>
          ${foot}
        </table>
      </section>`;
    })
    .join("\n");
  const totalBlock = includeValues
    ? `<div class="total-geral">Total geral: ${escapeHtml(formatCurrency(opts.totalGeral))}</div>`
    : "";

  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>Comissões — TorqMind</title>
  <style>
    @page { size: A4 landscape; margin: 8mm 10mm; }
    * { box-sizing: border-box; }
    html, body {
      margin: 0; padding: 0; background: #fff; color: #111;
      font-family: "Segoe UI", system-ui, sans-serif;
      -webkit-print-color-adjust: exact; print-color-adjust: exact;
    }
    body { padding: 8px 10px 16px; }
    .brand { font-size: 11px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: #444; }
    h1 { margin: 2px 0 8px; font-size: 16px; }
    .meta {
      display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 3px 16px;
      font-size: 10px; margin-bottom: 10px; padding-bottom: 8px; border-bottom: 2px solid #111;
    }
    /* Cabeçalho + 1ª filial na mesma folha: NÃO usar page-break-inside:avoid na
       seção inteira (navegadores empurram o bloco para a página 2). */
    .filial { break-inside: auto; page-break-inside: auto; margin: 0; }
    h2 {
      margin: 10px 0 6px; font-size: 13px;
      break-after: avoid-page; page-break-after: avoid;
    }
    table { width: 100%; border-collapse: collapse; font-size: 10px; }
    thead { display: table-header-group; }
    tfoot { display: table-footer-group; }
    tr { break-inside: avoid; page-break-inside: avoid; }
    th, td { border-bottom: 1px solid #ddd; padding: 4px 6px; text-align: left; }
    th { background: #f3f3f3; font-weight: 700; }
    td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
    tfoot td { border-top: 2px solid #111; }
    .total-geral { margin-top: 12px; font-size: 12px; font-weight: 700; }
  </style>
</head>
<body>
  <div class="brand">TorqMind</div>
  <h1>Comissões de vendedores</h1>
  <div class="meta">
    <div><strong>Empresa:</strong> ${escapeHtml(opts.empresaLabel)}</div>
    <div><strong>Filiais:</strong> ${escapeHtml(opts.filiaisLabel)}</div>
    <div><strong>Competência:</strong> ${escapeHtml(opts.periodoLabel)}</div>
    <div><strong>Modo:</strong> ${escapeHtml(opts.modoLabel)}</div>
    <div><strong>Níveis:</strong> ${escapeHtml(opts.niveisLabel)}</div>
    <div><strong>Gerado em:</strong> ${escapeHtml(printedAt)}</div>
  </div>
  ${sections}
  ${totalBlock}
</body>
</html>`;
}

export default function CommissionsTab({
  idEmpresa,
  idFilial,
  idFiliais,
  dtIni,
  dtFim,
}: CommissionsTabProps) {
  const [paymentMode, setPaymentMode] = useState<string>("");
  const [printIncludeValues, setPrintIncludeValues] = useState(true);
  const includeCentralMirror = true;
  /** Vazio = todos os níveis (mesmo padrão de Prioridades de cobrança). */
  const [selectedTiers, setSelectedTiers] = useState<Set<string>>(new Set());

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [discountData, setDiscountData] = useState<any>(null);
  const [discountLoading, setDiscountLoading] = useState(false);
  const [discountError, setDiscountError] = useState("");

  const multiFiliais = useMemo(
    () => (idFiliais || []).map(String).filter((v) => v && v !== "0"),
    [idFiliais],
  );
  const isMulti = multiFiliais.length > 1 || (!idFilial && multiFiliais.length > 0);
  const hasScope = Boolean(idFilial) || multiFiliais.length > 0;
  const multiFiliaisKey = multiFiliais.join(",");

  useEffect(() => {
    if (!idFilial && multiFiliais.length === 0) {
      setData(null);
      setError("");
      setLoading(false);
      return;
    }

    const ac = new AbortController();
    setLoading(true);
    setError("");
    setData(null);

    const params = new URLSearchParams();
    if (idEmpresa) params.set("id_empresa", String(idEmpresa));
    params.set("dt_ini", dtIni);
    params.set("dt_fim", dtFim);
    if (isMulti || (!idFilial && multiFiliais.length > 0)) {
      for (const f of multiFiliais) params.append("id_filiais", String(f));
    } else if (idFilial) {
      params.set("id_filial", String(idFilial));
    }
    if (idFilial && paymentMode) params.set("payment_mode", paymentMode);
    params.set("include_central_mirror", "true");

    (async () => {
      try {
        const resp = await apiGet(`/bi/team/commissions/results?${params.toString()}`, {
          signal: ac.signal,
          timeout: 90000,
        });
        if (ac.signal.aborted) return;
        setData(resp);
        setError("");
      } catch (err: any) {
        if (ac.signal.aborted || isRequestCanceled(err)) return;
        setData(null);
        setError(extractApiError(err, "Falha ao carregar comissões."));
      } finally {
        if (!ac.signal.aborted) setLoading(false);
      }
    })();

    return () => ac.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [idEmpresa, idFilial, multiFiliaisKey, isMulti, dtIni, dtFim, paymentMode]);

  useEffect(() => {
    if (!idFilial && multiFiliais.length === 0) {
      setDiscountData(null);
      return;
    }
    const ac = new AbortController();
    setDiscountLoading(true);
    setDiscountError("");
    const params = new URLSearchParams();
    if (idEmpresa) params.set("id_empresa", String(idEmpresa));
    params.set("dt_ini", dtIni);
    params.set("dt_fim", dtFim);
    if (isMulti || (!idFilial && multiFiliais.length > 0)) {
      for (const f of multiFiliais) params.append("id_filiais", String(f));
    } else if (idFilial) {
      params.set("id_filial", String(idFilial));
    }
    (async () => {
      try {
        const resp = await apiGet(`/bi/team/commissions/discounts?${params.toString()}`, {
          signal: ac.signal,
          timeout: 60000,
        });
        if (ac.signal.aborted) return;
        setDiscountData(resp);
      } catch (err: any) {
        if (ac.signal.aborted || isRequestCanceled(err)) return;
        setDiscountData(null);
        setDiscountError(extractApiError(err, "Falha ao carregar descontos."));
      } finally {
        if (!ac.signal.aborted) setDiscountLoading(false);
      }
    })();
    return () => ac.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [idEmpresa, idFilial, multiFiliaisKey, isMulti, dtIni, dtFim]);

  const sellers = useMemo(
    () => ((data?.vendedores || []) as SellerRow[]),
    [data?.vendedores],
  );

  const tierStats = useMemo(() => {
    const map = new Map<string, { key: string; label: string; count: number; total: number }>();
    for (const emp of sellers) {
      const key = sellerTierKey(emp);
      const cur = map.get(key) || {
        key,
        label: key === "none" ? TIER_STYLES.none.label : sellerTierLabel(emp),
        count: 0,
        total: 0,
      };
      cur.count += 1;
      cur.total += Number(emp.comissao_estimada || 0);
      if (key !== "none" && emp.nivel_atingido?.tier_name) {
        cur.label = String(emp.nivel_atingido.tier_name);
      }
      map.set(key, cur);
    }
    return TIER_FILTER_ORDER.map((key) => map.get(key)).filter(Boolean) as Array<{
      key: string;
      label: string;
      count: number;
      total: number;
    }>;
  }, [sellers]);

  const tierFiltered = useMemo(() => {
    if (selectedTiers.size === 0) return sellers;
    return sellers.filter((emp) => selectedTiers.has(sellerTierKey(emp)));
  }, [sellers, selectedTiers]);

  const { query: sellersQ, setQuery: setSellersQ, filteredRows: searchedSellers } = useGridSearch(
    tierFiltered,
    { excludeKeys: /^id_/i },
  );

  const filialGroups = useMemo(() => {
    const byFilial = new Map<
      string,
      { id_filial: number; label: string; sellers: SellerRow[]; total: number }
    >();
    for (const emp of searchedSellers as SellerRow[]) {
      const id = Number(emp.id_filial || 0);
      const label = String(emp.filial_label || (id ? `Filial ${id}` : "Sem filial"));
      const key = `${id}:${label}`;
      const cur = byFilial.get(key) || { id_filial: id, label, sellers: [], total: 0 };
      cur.sellers.push(emp);
      cur.total += Number(emp.comissao_estimada || 0);
      byFilial.set(key, cur);
    }
    return Array.from(byFilial.values())
      .map((g) => ({
        ...g,
        sellers: sortSellersByCommission(g.sellers),
      }))
      .sort((a, b) =>
        a.label.localeCompare(b.label, "pt-BR", { numeric: true, sensitivity: "base" }),
      );
  }, [searchedSellers]);

  const filteredTotal = useMemo(
    () => filialGroups.reduce((acc, g) => acc + g.total, 0),
    [filialGroups],
  );
  const filteredCount = useMemo(
    () => filialGroups.reduce((acc, g) => acc + g.sellers.length, 0),
    [filialGroups],
  );

  const numCell = { textAlign: "right" as const, fontVariantNumeric: "tabular-nums" as const };

  const toggleTier = (key: string) => {
    setSelectedTiers((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const modoLabel =
    paymentMode === "individual_sales"
      ? "Individual por quantidade"
      : paymentMode === "team_total"
        ? "Equipe (quantidade total)"
        : paymentMode === "equal_split"
          ? "Divisão igual"
          : "Padrão da config";
  const niveisLabel =
    selectedTiers.size === 0
      ? "Todos"
      : Array.from(selectedTiers)
          .map((k) => TIER_STYLES[k]?.label || k)
          .join(", ");

  function printPdf() {
    if (filialGroups.length === 0) return;
    const html = buildCommissionsReportHtml({
      empresaLabel: idEmpresa ? `Empresa ${idEmpresa}` : "—",
      filiaisLabel: filialGroups.map((g) => g.label).join(", ") || "—",
      periodoLabel: formatCommissionPeriodLabel(dtIni, dtFim),
      modoLabel,
      niveisLabel,
      includeValues: printIncludeValues,
      groups: filialGroups.map((g) => ({
        label: g.label,
        sellers: g.sellers,
        total: g.total,
      })),
      totalGeral: filteredTotal,
    });
    const iframe = document.createElement("iframe");
    iframe.setAttribute("aria-hidden", "true");
    iframe.style.position = "fixed";
    iframe.style.right = "0";
    iframe.style.bottom = "0";
    iframe.style.width = "0";
    iframe.style.height = "0";
    iframe.style.border = "0";
    document.body.appendChild(iframe);
    const win = iframe.contentWindow;
    const doc = iframe.contentDocument || win?.document;
    if (!win || !doc) {
      iframe.remove();
      return;
    }
    doc.open();
    doc.write(html);
    doc.close();
    const cleanup = () => {
      try {
        iframe.remove();
      } catch {
        /* ignore */
      }
    };
    const triggerPrint = () => {
      const onAfter = () => {
        win.removeEventListener("afterprint", onAfter);
        cleanup();
      };
      win.addEventListener("afterprint", onAfter);
      window.setTimeout(cleanup, 120_000);
      try {
        win.focus();
        win.print();
      } catch {
        cleanup();
      }
    };
    window.setTimeout(triggerPrint, 300);
  }

  const discountItems = (discountData?.items || []) as Array<Record<string, unknown>>;
  const { query: discountsSearch, setQuery: setDiscountsSearch, filteredRows: filteredDiscounts } =
    useGridSearch(discountItems, { excludeKeys: /^id_/i });

  if (!hasScope) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState
          title="Selecione o escopo"
          detail="Escolha uma ou mais filiais (ou Todas) no painel lateral para visualizar as comissões."
        />
      </div>
    );
  }

  return (
    <div style={{ marginTop: 16 }}>
      {error ? <div className="card errorCard" style={{ marginBottom: 12 }}>{String(error)}</div> : null}

      {loading ? (
        <div className="card" style={{ textAlign: "center", padding: 32 }}>
          <div className="muted">Calculando comissões...</div>
        </div>
      ) : data ? (
        <>
          <div className="card commissionToolbar">
            <div className="commissionToolbarRow">
              <GridSearchInput value={sellersQ} onChange={setSellersQ} />
              {idFilial ? (
                <label className="profitScopeMonth" title="Modo de cálculo da comissão">
                  <span className="profitScopeMonthLabel">Modo</span>
                  <select
                    className="profitScopeMonthSelect"
                    value={paymentMode}
                    onChange={(e) => setPaymentMode(e.target.value)}
                    aria-label="Modo de cálculo da comissão"
                  >
                    <option value="">Padrão da config</option>
                    <option value="individual_sales">Individual por quantidade</option>
                    <option value="team_total">Equipe (quantidade total)</option>
                    <option value="equal_split">Divisão igual</option>
                  </select>
                </label>
              ) : null}
              <div className="commissionToolbarMeta">
                <CommissionCentralMirrorToggle
                  value={includeCentralMirror}
                  onChange={() => {}}
                  visible={false}
                />
                <button
                  type="button"
                  className={`profitScopeToggle${printIncludeValues ? " on" : ""}`}
                  aria-pressed={printIncludeValues}
                  onClick={() => setPrintIncludeValues((v) => !v)}
                  title="Incluir valores monetários na impressão"
                >
                  <span className="profitScopeToggleDot" aria-hidden />
                  Imprimir valores?
                </button>
                <button
                  type="button"
                  className="btn"
                  onClick={printPdf}
                  disabled={filialGroups.length === 0}
                >
                  Imprimir / PDF
                </button>
                <span className="muted" style={{ fontSize: 12 }}>
                  {filteredCount} de {data.vendedores_elegiveis || sellers.length} ·{" "}
                  {formatCurrency(filteredTotal || data.comissao_total || 0)}
                </span>
              </div>
            </div>

            {tierStats.length > 0 ? (
              <div className="commissionTierFilters" role="group" aria-label="Filtrar por nível">
                <button
                  type="button"
                  className={`commissionTierChip${selectedTiers.size === 0 ? " commissionTierChip--active" : ""}`}
                  aria-pressed={selectedTiers.size === 0}
                  onClick={() => setSelectedTiers(new Set())}
                >
                  <span className="commissionTierChipName">Todos</span>
                  <span className="commissionTierChipMeta">
                    {sellers.length} · {formatCurrency(data.comissao_total || 0)}
                  </span>
                </button>
                {tierStats.map((tier) => {
                  const style = TIER_STYLES[tier.key] || TIER_STYLES.none;
                  const active = selectedTiers.has(tier.key);
                  return (
                    <button
                      key={tier.key}
                      type="button"
                      className={`commissionTierChip${active ? " commissionTierChip--active" : ""}`}
                      aria-pressed={active}
                      onClick={() => toggleTier(tier.key)}
                      style={{
                        color: style.color,
                        borderColor: active ? style.color : `${style.color}55`,
                        background: active ? style.bgActive : style.bg,
                        boxShadow: active ? `0 0 0 1px ${style.color}` : undefined,
                      }}
                    >
                      <span className="commissionTierChipName">{tier.label}</span>
                      <span className="commissionTierChipMeta">
                        {tier.count} · {formatCurrency(tier.total)}
                      </span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {data.message && sellers.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState
                title="Sem dados"
                detail={coerceDisplayMessage(data.message, "Sem vendas elegíveis no período.")}
              />
            </div>
          ) : sellers.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState
                title="Sem vendedores"
                detail="Não há vendedores com identificação válida para o período selecionado."
              />
            </div>
          ) : filialGroups.length === 0 ? (
            <div className="card" style={{ marginTop: 12 }}>
              <EmptyState
                title="Nenhum resultado"
                detail={
                  sellersQ
                    ? `Nada encontrado para “${sellersQ}” com os filtros atuais.`
                    : "Nenhum vendedor nos níveis selecionados."
                }
              />
            </div>
          ) : (
            filialGroups.map((group) => (
              <div
                key={`${group.id_filial}:${group.label}`}
                className="solvenciaFilialCard commissionFilialCard"
                style={{ borderLeft: "4px solid var(--accent-copper, #b8722c)" }}
              >
                <div className="commissionFilialHead">
                  <div>
                    <div className="sectionEyebrow">Filial</div>
                    <h2 className="commissionFilialTitle">{group.label}</h2>
                  </div>
                  <div className="commissionFilialSummary">
                    <span>
                      {group.sellers.length}{" "}
                      {group.sellers.length === 1 ? "vendedor" : "vendedores"}
                    </span>
                    <strong>{formatCurrency(group.total)}</strong>
                  </div>
                </div>

                <div className="tableScroll">
                  <table className="table compact" style={{ width: "100%", minWidth: 560 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left" }}>Funcionário</th>
                        <th style={{ textAlign: "right" }}>Quantidade</th>
                        <th style={{ textAlign: "right" }}>Venda</th>
                        <th style={{ textAlign: "left" }}>Nível</th>
                        <th style={{ textAlign: "right" }}>%</th>
                        <th style={{ textAlign: "right" }}>Comissão</th>
                      </tr>
                    </thead>
                    <tbody>
                      {group.sellers.map((emp) => {
                        const tierKey = sellerTierKey(emp);
                        const style = TIER_STYLES[tierKey] || TIER_STYLES.none;
                        return (
                          <tr key={`${emp.id_filial || "x"}-${emp.id_funcionario}`}>
                            <td style={{ fontWeight: 500, textAlign: "left" }}>
                              {emp.nome_vendedor}
                            </td>
                            <td style={numCell}>
                              {Number(emp.quantidade_vendas || 0).toLocaleString("pt-BR", {
                                maximumFractionDigits: 0,
                              })}
                            </td>
                            <td style={numCell}>{formatCurrency(emp.venda_elegivel)}</td>
                            <td style={{ textAlign: "left" }}>
                              {tierKey === "none" ? (
                                <span className="muted">Sem nível</span>
                              ) : (
                                <span style={{ color: style.color, fontWeight: 650 }}>
                                  {sellerTierLabel(emp)}
                                </span>
                              )}
                            </td>
                            <td style={numCell}>
                              {Number(emp.percentual_aplicado || 0).toFixed(2)}%
                            </td>
                            <td
                              style={{
                                ...numCell,
                                fontWeight: 700,
                                color: "var(--color-positive)",
                              }}
                            >
                              {formatCurrency(emp.comissao_estimada || 0)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                    <tfoot className="commissionGridFoot">
                      <tr>
                        <td style={{ textAlign: "left", fontWeight: 700 }}>Total</td>
                        <td style={{ ...numCell, fontWeight: 700 }}>
                          {group.sellers
                            .reduce((acc, emp) => acc + Number(emp.quantidade_vendas || 0), 0)
                            .toLocaleString("pt-BR", { maximumFractionDigits: 0 })}
                        </td>
                        <td style={{ ...numCell, fontWeight: 700 }}>
                          {formatCurrency(
                            group.sellers.reduce(
                              (acc, emp) => acc + Number(emp.venda_elegivel || 0),
                              0,
                            ),
                          )}
                        </td>
                        <td style={{ textAlign: "left" }} className="muted">
                          —
                        </td>
                        <td style={numCell} className="muted">
                          —
                        </td>
                        <td
                          style={{
                            ...numCell,
                            fontWeight: 780,
                            color: "var(--color-positive)",
                          }}
                        >
                          {formatCurrency(group.total)}
                        </td>
                      </tr>
                    </tfoot>
                  </table>
                </div>
              </div>
            ))
          )}

          <div className="card" style={{ marginTop: 20 }}>
            <div className="sectionEyebrow">Comissões</div>
            <h2 style={{ marginTop: 4 }}>Descontos e preços negociados</h2>
            <div style={{ marginTop: 10 }}>
              <GridSearchInput value={discountsSearch} onChange={setDiscountsSearch} />
            </div>
            {discountError ? (
              <div className="errorCard" style={{ marginTop: 10 }}>{String(discountError)}</div>
            ) : null}
            {discountLoading ? (
              <div className="muted" style={{ marginTop: 12 }}>Carregando descontos…</div>
            ) : !filteredDiscounts.length ? (
              <EmptyState
                title="Sem descontos no período"
                detail="Não há desconto na venda nem preço fixo econômico neste período."
              />
            ) : (
              <div className="tableScroll" style={{ marginTop: 10 }}>
                <table className="table compact" style={{ width: "100%", minWidth: 720 }}>
                  <thead>
                    <tr>
                      <th>Filial</th>
                      <th>Data</th>
                      <th>Documento</th>
                      <th>Cliente</th>
                      <th>Vendedor</th>
                      <th>Produto</th>
                      <th style={{ textAlign: "right" }}>Preço ref.</th>
                      <th style={{ textAlign: "right" }}>Preço aplicado</th>
                      <th style={{ textAlign: "right" }}>Desconto R$</th>
                      <th style={{ textAlign: "right" }}>Desconto %</th>
                      <th>Tipo</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(filteredDiscounts as Array<Record<string, any>>).map((row, idx) => (
                      <tr key={`${row.tipo}-${row.id_filial}-${row.documento}-${idx}`}>
                        <td>{row.filial_label || "—"}</td>
                        <td style={{ whiteSpace: "nowrap" }}>
                          {String(row.dt_venda || "").slice(0, 10) || "—"}
                        </td>
                        <td>{row.documento || "—"}</td>
                        <td>{row.cliente || "—"}</td>
                        <td>{row.vendedor || "—"}</td>
                        <td>{row.produto || "—"}</td>
                        <td style={numCell}>
                          {row.preco_referencia == null
                            ? "—"
                            : formatCurrency(Number(row.preco_referencia))}
                        </td>
                        <td style={numCell}>{formatCurrency(Number(row.preco_aplicado || 0))}</td>
                        <td style={numCell}>{formatCurrency(Number(row.desconto_rs || 0))}</td>
                        <td style={numCell}>{Number(row.desconto_pct || 0).toFixed(2)}%</td>
                        <td>{row.tipo_label || row.tipo || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      ) : null}
    </div>
  );
}
