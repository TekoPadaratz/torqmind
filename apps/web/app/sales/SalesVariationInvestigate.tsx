"use client";

import { useEffect, useRef, useState } from "react";

import EmptyState from "../components/ui/EmptyState";
import { formatCurrency } from "../lib/format";
import { buildScopeParams, type ScopeQuery } from "../lib/scope";
import { apiGet } from "../lib/api";

type Factor = {
  kind?: string;
  dimension?: string;
  label?: string;
  summary?: string;
  delta?: number;
  share_of_total_delta_pct?: number | null;
  causality?: string;
};

type DimensionView = {
  dimension?: string;
  note?: string;
  items?: Factor[];
  truncated?: boolean;
  shown_count?: number;
  hidden_count?: number;
  residual_delta?: number;
};

type InvestigationPayload = {
  status?: string;
  headline?: string;
  message?: string;
  domain?: string;
  comparison?: {
    basis_label?: string;
    current?: { label?: string };
    prior?: { label?: string };
    period_incomplete?: boolean;
  };
  totals?: {
    current_faturamento?: number | null;
    prior_faturamento?: number | null;
    delta?: number | null;
    delta_pct?: number | null;
  } | null;
  dimension_views?: Record<string, DimensionView>;
  additive_warning?: string;
  factors?: Factor[];
  next_checks?: Array<{ title?: string; screen?: string }>;
  follow_ups?: string[];
  warnings?: string[];
  legend?: Record<string, string>;
  freshness?: { last_updated?: string | null };
};

type Props = {
  scope: ScopeQuery;
  enabled: boolean;
  onAskAssistant?: (text: string) => void;
};

export default function SalesVariationInvestigate({ scope, enabled, onAskAssistant }: Props) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<InvestigationPayload | null>(null);
  const [scopeKey, setScopeKey] = useState("");
  const requestSeq = useRef(0);
  const startedAt = useRef(0);

  useEffect(() => {
    if (!open || !enabled) return;
    const params = buildScopeParams(scope);
    const key = params.toString();
    setScopeKey(key);
    setData(null);
    setError(null);
    setLoading(true);
    startedAt.current = performance.now();
    const seq = ++requestSeq.current;
    const controller = new AbortController();

    (async () => {
      try {
        const payload = await apiGet(`/bi/sales/investigate-variation?${key}`, {
          signal: controller.signal,
        });
        if (seq !== requestSeq.current) return;
        setData(payload as InvestigationPayload);
        setError(null);
      } catch (err: any) {
        if (controller.signal.aborted || err?.name === "AbortError") return;
        if (seq !== requestSeq.current) return;
        setData(null);
        setError(err?.message || "Falha ao investigar variação de vendas");
      } finally {
        if (seq === requestSeq.current) setLoading(false);
      }
    })();

    return () => {
      controller.abort();
    };
  }, [open, enabled, scope.dt_ini, scope.dt_fim, scope.id_empresa, scope.id_filial, scope.id_filiais_key, scope.scope_key]);

  if (!enabled) return null;

  const currentKey = buildScopeParams(scope).toString();
  const stale = Boolean(data && scopeKey && scopeKey !== currentKey);

  return (
    <div className="card col-12" style={{ marginBottom: 12 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <h2 style={{ margin: 0 }}>Investigar variação de vendas</h2>
          <p className="muted" style={{ margin: "4px 0 0", fontSize: 13, maxWidth: 640 }}>
            Compara o período e as filiais selecionados com a janela anterior de mesma duração.
            Filial, grupo e hora mostram a mesma variação por ângulos diferentes — não some os três.
          </p>
        </div>
        <button type="button" className="btn" onClick={() => setOpen((v) => !v)} aria-expanded={open}>
          {open ? "Ocultar investigação" : "Investigar vendas"}
        </button>
      </div>

      {open ? (
        <div style={{ marginTop: 16 }}>
          {loading || stale ? <p className="muted">Analisando as filiais e o período selecionados…</p> : null}
          {error && !loading ? (
            <div style={{ display: "grid", gap: 8 }}>
              <EmptyState title="Não foi possível investigar." detail={error} />
              <button
                type="button"
                className="btn"
                onClick={() => {
                  setOpen(false);
                  setTimeout(() => setOpen(true), 0);
                }}
              >
                Tentar novamente
              </button>
            </div>
          ) : null}
          {!loading && !stale && !error && data ? <InvestigationBody data={data} onAsk={onAskAssistant} /> : null}
        </div>
      ) : null}
    </div>
  );
}

function InvestigationBody({
  data,
  onAsk,
}: {
  data: InvestigationPayload;
  onAsk?: (text: string) => void;
}) {
  if (
    data.status === "no_data" ||
    data.status === "unavailable" ||
    data.status === "forbidden_scope" ||
    data.status === "period_too_long"
  ) {
    return (
      <EmptyState
        title={data.message || "Investigação indisponível."}
        detail={data.comparison?.basis_label}
      />
    );
  }

  const totals = data.totals;
  const views = data.dimension_views || {};

  return (
    <div style={{ display: "grid", gap: 16 }}>
      {data.headline ? <p style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>{data.headline}</p> : null}

      <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
        <Kpi label="Período atual" value={fmtMoney(totals?.current_faturamento)} hint={data.comparison?.current?.label} />
        <Kpi label="Base de comparação" value={fmtMoney(totals?.prior_faturamento)} hint={data.comparison?.prior?.label} />
        <Kpi label="Variação" value={fmtDelta(totals?.delta, totals?.delta_pct)} hint={data.comparison?.basis_label} />
      </div>

      {data.additive_warning ? (
        <p className="muted" style={{ margin: 0, fontSize: 12 }}>
          {data.additive_warning}
        </p>
      ) : null}

      {(data.warnings || []).length ? (
        <ul className="muted" style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
          {data.warnings!.map((w) => (
            <li key={w}>{w}</li>
          ))}
        </ul>
      ) : null}

      {(["filial", "grupo", "hora"] as const).map((dim) => {
        const view = views[dim];
        if (!view?.items?.length) return null;
        return (
          <section key={dim}>
            <h3 style={{ margin: "0 0 8px", fontSize: 14 }}>
              Visão por {dim}{" "}
              <span className="muted" style={{ fontWeight: 400, fontSize: 12 }}>
                (contribuição — não causa)
              </span>
            </h3>
            {view.note ? (
              <p className="muted" style={{ margin: "0 0 8px", fontSize: 12 }}>
                {view.note}
              </p>
            ) : null}
            <DimensionTable view={view} />
          </section>
        );
      })}

      {(data.factors || []).some((f) => f.kind === "hypothesis") ? (
        <section>
          <h3 style={{ margin: "0 0 8px", fontSize: 14 }}>Hipóteses para verificar</h3>
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {(data.factors || [])
              .filter((f) => f.kind === "hypothesis")
              .map((h, idx) => (
                <li key={`${h.label}-${idx}`}>{h.summary || h.label}</li>
              ))}
          </ul>
        </section>
      ) : null}

      {(data.next_checks || []).length ? (
        <section>
          <h3 style={{ margin: "0 0 8px", fontSize: 14 }}>Próximas verificações</h3>
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {data.next_checks!.map((c, idx) => (
              <li key={`${c.title}-${idx}`}>
                {c.title}
                {c.screen ? <span className="muted"> → {c.screen}</span> : null}
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {(data.follow_ups || []).length ? (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {data.follow_ups!.map((q) => (
            <button
              key={q}
              type="button"
              className="btn"
              style={{ fontSize: 12 }}
              onClick={() => onAsk?.(q)}
            >
              {q}
            </button>
          ))}
        </div>
      ) : null}

      {data.freshness?.last_updated ? (
        <p className="muted" style={{ margin: 0, fontSize: 12 }}>
          Atualizado em {data.freshness.last_updated}
        </p>
      ) : null}
    </div>
  );
}

function DimensionTable({ view }: { view: DimensionView }) {
  return (
    <>
      <div className="tableScroll">
        <table className="table compact" style={{ width: "100%" }}>
          <thead>
            <tr>
              <th>Fator</th>
              <th>Variação</th>
              <th>% da variação</th>
            </tr>
          </thead>
          <tbody>
            {(view.items || []).map((f, idx) => (
              <tr key={`${f.label}-${idx}`}>
                <td>{f.label || f.summary}</td>
                <td>{fmtMoney(f.delta)}</td>
                <td>
                  {f.share_of_total_delta_pct == null
                    ? "—"
                    : `${f.share_of_total_delta_pct > 0 ? "+" : ""}${f.share_of_total_delta_pct}%`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {view.truncated ? (
        <p className="muted" style={{ fontSize: 12, margin: "6px 0 0" }}>
          Mostrando os primeiros {view.shown_count} resultados
          {view.hidden_count ? ` (${view.hidden_count} ficaram de fora)` : ""}
          {view.residual_delta != null
            ? ` · variação restante ${Number(view.residual_delta).toLocaleString("pt-BR", {
                style: "currency",
                currency: "BRL",
              })}`
            : ""}
          .
        </p>
      ) : null}
    </>
  );
}

function Kpi({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div style={{ minWidth: 160 }}>
      <div className="muted" style={{ fontSize: 12 }}>
        {label}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700 }}>{value}</div>
      {hint ? (
        <div className="muted" style={{ fontSize: 11 }}>
          {hint}
        </div>
      ) : null}
    </div>
  );
}

function fmtMoney(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "—";
  return formatCurrency(value);
}

function fmtDelta(delta: number | null | undefined, pct: number | null | undefined): string {
  if (delta == null) return "—";
  const sign = delta > 0 ? "+" : "";
  const base = `${sign}${formatCurrency(delta)}`;
  if (pct == null) return base;
  return `${base} (${pct > 0 ? "+" : ""}${pct.toFixed(1)}%)`;
}
