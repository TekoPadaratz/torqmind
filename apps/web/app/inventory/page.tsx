"use client";

import { useMemo, useState, type ReactNode } from "react";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel, formatCurrency, formatDateOnly } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { canAccessScreenKey, readCachedSession } from "../lib/session";

export const dynamic = "force-dynamic";

function isoTodayLocal(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDaysIso(iso: string, delta: number): string {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(y, m - 1, d);
  dt.setDate(dt.getDate() + delta);
  const yy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function clampPeriod(ini: string, fim: string, today: string): { ini: string; fim: string } {
  let nextIni = ini;
  let nextFim = fim > today ? today : fim;
  if (nextIni > nextFim) {
    nextIni = nextFim;
  }
  return { ini: nextIni, fim: nextFim };
}

type InventoryItem = {
  id_tanque?: number;
  id_produto: number;
  combustivel: string;
  tanques: number;
  capacidade_l: number;
  estoque_l: number;
  pct_ocupado: number;
  disponivel_l: number;
  pct_disponivel: number;
  media_diaria_l: number;
  dias_cobertura: number | null;
  necessidade_l: number;
  comprar_l: number;
  custo_estoque?: number | null;
  data_leitura?: string | null;
};

type FilialBlock = {
  id_filial: number;
  filial_nome: string;
  tanques: number;
  capacidade_l: number;
  estoque_l: number;
  pct_ocupado: number;
  custo_estoque?: number | null;
  data_leitura?: string | null;
  leitura_fresca?: boolean;
  itens: InventoryItem[];
};

type InventoryPayload = {
  kpis?: {
    filiais: number;
    tanques: number;
    capacidade_l: number;
    estoque_l: number;
    pct_estoque: number;
    custo_estoque?: number | null;
  };
  filiais?: FilialBlock[];
  dias_alvo?: number;
  dias_periodo?: number;
  dt_ini?: string;
  dt_fim?: string;
};

function formatLiters(value: unknown, digits = 0): string {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toLocaleString("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })} L`;
}

function formatPct(value: unknown): string {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toLocaleString("pt-BR", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })}%`;
}

function formatDays(value: unknown): string {
  if (value == null) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("pt-BR", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
}

function shortFuelLabel(name: string): string {
  const raw = String(name || "").trim();
  if (!raw) return "—";
  return raw
    .replace(/^ÓLEO\s+/i, "")
    .replace(/^OLEO\s+/i, "")
    .replace(/\s+/g, " ");
}

function TankVisual({
  pct,
  label,
  size = "md",
}: {
  pct: number;
  label: string;
  size?: "sm" | "md";
}) {
  const clamped = Math.max(0, Math.min(100, Number(pct) || 0));
  const fillColor =
    clamped >= 75
      ? "var(--accent-good, #22c55e)"
      : clamped >= 50
        ? "var(--color-info, #3b82f6)"
        : clamped >= 25
          ? "var(--color-warning, #eab308)"
          : "var(--color-negative, #ef4444)";

  return (
    <div className="tankVisualCell">
      <div
        className={`tankVisual${size === "sm" ? " is-sm" : ""}`}
        aria-label={`${label}: ${formatPct(clamped)}`}
      >
        <div
          className="tankVisualFill"
          style={{ height: `${clamped}%`, background: fillColor }}
        />
        <div className="tankVisualPct">{formatPct(clamped)}</div>
      </div>
      <div className="tankVisualMeta muted" title={label}>
        {shortFuelLabel(label)}
      </div>
    </div>
  );
}

function MediaBar({ value, max }: { value: number; max: number }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 100 }}>
      <div
        style={{
          flex: 1,
          height: 6,
          borderRadius: 999,
          background: "rgba(212,175,55,0.12)",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            background: "var(--accent-copper, #b87333)",
            borderRadius: 999,
          }}
        />
      </div>
      <span style={{ fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
        {formatLiters(value, 0)}
      </span>
    </div>
  );
}

function BadgeValue({ children, tone = "accent" }: { children: ReactNode; tone?: "accent" | "warn" }) {
  const bg =
    tone === "warn" ? "rgba(245, 158, 11, 0.16)" : "var(--accent-copper-soft)";
  const border =
    tone === "warn" ? "rgba(251, 191, 36, 0.35)" : "var(--accent-copper)";
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: 999,
        background: bg,
        border: `1px solid ${border}`,
        fontVariantNumeric: "tabular-nums",
        fontWeight: 600,
        whiteSpace: "nowrap",
      }}
    >
      {children}
    </span>
  );
}

export default function InventoryPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const todayIso = useMemo(() => isoTodayLocal(), []);
  const [diasAlvo, setDiasAlvo] = useState(7);
  const [dtIni, setDtIni] = useState(() => addDaysIso(isoTodayLocal(), -6));
  const [dtFim, setDtFim] = useState(() => isoTodayLocal());
  const session = readCachedSession();
  const allowed = canAccessScreenKey(session, "inventory");

  const periodKey = `${dtIni}:${dtFim}`;
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<InventoryPayload>({
      moduleKey: `inventory_fuel:${diasAlvo}:${periodKey}`,
      scope,
      errorMessage: "Falha ao carregar estoque de combustíveis",
      buildRequestUrl: (currentScope) => {
        if (!allowed) return null;
        const params = buildScopeParams(currentScope);
        const period = clampPeriod(dtIni, dtFim, todayIso);
        params.set("dt_ini", period.ini);
        params.set("dt_fim", period.fim);
        params.set("dias_alvo", String(diasAlvo));
        return `/bi/estoque/combustivel?${params.toString()}`;
      },
    });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("estoque")
    : buildModuleLoadingCopy("estoque");

  const kpis = data?.kpis;
  const filiais = useMemo(
    () =>
      [...(data?.filiais || [])].sort((a, b) =>
        a.filial_nome.localeCompare(b.filial_nome, "pt-BR"),
      ),
    [data?.filiais],
  );
  const canSeeCost =
    claims?.user_role === "platform_master" ||
    claims?.user_role === "owner" ||
    claims?.role === "MASTER" ||
    claims?.role === "OWNER";

  const mediaIniLabel = formatDateOnly(data?.dt_ini || dtIni);
  const mediaFimLabel = formatDateOnly(data?.dt_fim || dtFim);
  const diasPeriodo = Number(data?.dias_periodo || 0);

  const applyIni = (value: string) => {
    const period = clampPeriod(value, dtFim, todayIso);
    setDtIni(period.ini);
    setDtFim(period.fim);
  };
  const applyFim = (value: string) => {
    const period = clampPeriod(dtIni, value, todayIso);
    setDtIni(period.ini);
    setDtFim(period.fim);
  };

  if (!allowed && session) {
    return (
      <div>
        <AppNav title="Estoque" userLabel={userLabel} />
        <div className="container">
          <div className="bi-grid">
            <div className="card col-12">Sem permissão para Estoque.</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <AppNav title="Estoque" userLabel={userLabel} />
      <div className="container">
        <div className="bi-grid">
          <header className="pageHeader col-12">
            <div>
              <div className="sectionEyebrow">Comercial</div>
              <h1>Estoque de Combustíveis</h1>
              <p className="muted" style={{ marginTop: 4 }}>
                Média diária: de {mediaIniLabel} a {mediaFimLabel}
                {diasPeriodo > 0 ? ` (${diasPeriodo} dias)` : ""}.
              </p>
            </div>
            <div className="anpFilterLeft" style={{ flexWrap: "wrap", justifyContent: "flex-end" }}>
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span className="anpConfigLabel">Início</span>
                <input
                  type="date"
                  value={dtIni}
                  max={dtFim}
                  onChange={(e) => applyIni(e.target.value)}
                  aria-label="Data inicial da média"
                />
              </label>
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span className="anpConfigLabel">Fim</span>
                <input
                  type="date"
                  value={dtFim}
                  max={todayIso}
                  min={dtIni}
                  onChange={(e) => applyFim(e.target.value)}
                  aria-label="Data final da média"
                />
              </label>
              <label style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 100 }}>
                <span className="anpConfigLabel">Dias alvo</span>
                <input
                  type="number"
                  min={1}
                  max={90}
                  value={diasAlvo}
                  onChange={(e) => {
                    const n = Number(e.target.value);
                    if (Number.isFinite(n)) setDiasAlvo(Math.max(1, Math.min(90, Math.round(n))));
                  }}
                  style={{ width: 100 }}
                />
              </label>
            </div>
          </header>

          {error ? <div className="card errorCard col-12">{error}</div> : null}

          {(loading || pendingUnavailable) && !data ? (
            <div className="col-12">
              <ScopeTransitionState
                mode={pendingUnavailable ? "unavailable" : "loading"}
                headline={transitionCopy.headline}
                detail={transitionCopy.detail}
              />
            </div>
          ) : null}

          {data && kpis ? (
            <>
              <div className="card kpi col-3">
                <div className="label">Filiais</div>
                <div className="value">{kpis.filiais}</div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Tanques</div>
                <div className="value">{kpis.tanques}</div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Capacidade total</div>
                <div className="value">{formatLiters(kpis.capacidade_l)}</div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Litros estoque</div>
                <div className="value">
                  {formatLiters(kpis.estoque_l)}
                  <span className="muted" style={{ marginLeft: 8, fontSize: "0.7em" }}>
                    {formatPct(kpis.pct_estoque)}
                  </span>
                </div>
              </div>
              {canSeeCost ? (
                <div className="card kpi col-3">
                  <div className="label">Custo no tanque</div>
                  <div className="value">{formatCurrency(kpis.custo_estoque)}</div>
                </div>
              ) : null}

              {!filiais.length ? (
                <div className="col-12">
                  <EmptyState
                    title="Sem leituras de tanque"
                    detail="Não há tanques ativos com produto vinculado no escopo selecionado."
                  />
                </div>
              ) : (
                filiais.map((filial) => {
                  const maxMedia = Math.max(
                    0,
                    ...filial.itens.map((i) => Number(i.media_diaria_l) || 0),
                  );
                  return (
                    <section key={filial.id_filial} className="card col-12">
                      <div className="inventoryFilialHead">
                        <div className="sectionEyebrow">Filial</div>
                        <h2>{filial.filial_nome}</h2>
                      </div>

                      <div className="inventoryFilialBody">
                        <div className="inventoryTankGallery" aria-label={`Tanques de ${filial.filial_nome}`}>
                          {filial.itens.map((item) => (
                            <TankVisual
                              key={`tank-${filial.id_filial}-${item.id_tanque || item.id_produto}`}
                              pct={item.pct_ocupado}
                              label={item.combustivel}
                              size="md"
                            />
                          ))}
                        </div>

                        <div className="tableScroll inventoryFilialGrid">
                          <table className="table compact" style={{ minWidth: 720 }}>
                            <thead>
                              <tr>
                                <th>Tanque</th>
                                <th>Combustível</th>
                                <th>Capacidade</th>
                                <th>Estoque</th>
                                <th>Média diária</th>
                                <th>Dias cobert.</th>
                                <th>Necessidade</th>
                                <th>Comprar (L)</th>
                              </tr>
                            </thead>
                            <tbody>
                              {filial.itens.map((item) => (
                                <tr
                                  key={`${filial.id_filial}-${item.id_tanque || item.id_produto}`}
                                >
                                  <td>
                                    {item.id_tanque ? item.id_tanque : "—"}
                                  </td>
                                  <td>{item.combustivel}</td>
                                  <td>{formatLiters(item.capacidade_l)}</td>
                                  <td>{formatLiters(item.estoque_l)}</td>
                                  <td>
                                    <MediaBar value={item.media_diaria_l} max={maxMedia} />
                                  </td>
                                  <td>{formatDays(item.dias_cobertura)}</td>
                                  <td>
                                    <BadgeValue>{formatLiters(item.necessidade_l)}</BadgeValue>
                                  </td>
                                  <td>
                                    <BadgeValue tone="warn">
                                      {formatLiters(item.comprar_l)}
                                    </BadgeValue>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </section>
                  );
                })
              )}
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
}
