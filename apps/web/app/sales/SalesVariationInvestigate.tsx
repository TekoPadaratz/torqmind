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

type InvestigationPayload = {
  status?: string;
  headline?: string;
  message?: string;
  comparison?: {
    basis_label?: string;
    current?: { label?: string; days?: number };
    prior?: { label?: string; days?: number };
    period_incomplete?: boolean;
    compatible?: boolean;
    compatible_note?: string;
    incomplete_note?: string;
  };
  totals?: {
    current_faturamento?: number | null;
    prior_faturamento?: number | null;
    delta?: number | null;
    delta_pct?: number | null;
    current_has_data?: boolean;
    prior_has_data?: boolean;
  } | null;
  factors?: Factor[];
  next_checks?: Array<{ title?: string; screen?: string }>;
  warnings?: string[];
  legend?: Record<string, string>;
  freshness?: { last_updated?: string | null; source?: string };
};

type Props = {
  scope: ScopeQuery;
  enabled: boolean;
};

function kindLabel(kind: string | undefined): string {
  if (kind === "contribution") return "Contribuição";
  if (kind === "hypothesis") return "Hipótese";
  return kind || "Fator";
}

export default function SalesVariationInvestigate({ scope, enabled }: Props) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<InvestigationPayload | null>(null);
  const [scopeKey, setScopeKey] = useState("");
  const requestSeq = useRef(0);

  useEffect(() => {
    if (!open || !enabled) return;
    const params = buildScopeParams(scope);
    const key = params.toString();
    setScopeKey(key);
    setData(null);
    setError(null);
    setLoading(true);
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
            Mostra contribuições observadas e hipóteses — sem inventar causa.
          </p>
        </div>
        <button
          type="button"
          className="btn"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
        >
          {open ? "Ocultar investigação" : "Investigar agora"}
        </button>
      </div>

      {open ? (
        <div style={{ marginTop: 16 }}>
          {loading || stale ? (
            <p className="muted">Analisando variação no escopo atual…</p>
          ) : null}

          {error && !loading ? (
            <div style={{ display: "grid", gap: 8 }}>
              <EmptyState title="Não foi possível investigar." detail={error} />
              <button type="button" className="btn" onClick={() => setOpen(false)}>
                Fechar
              </button>
              <button
                type="button"
                className="btn"
                onClick={() => {
                  setOpen(false);
                  setTimeout(() => setOpen(true), 0);
                }}
              >
                Tentar de novo
              </button>
            </div>
          ) : null}

          {!loading && !stale && !error && data ? (
            <InvestigationBody data={data} />
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function InvestigationBody({ data }: { data: InvestigationPayload }) {
  if (data.status === "no_data" || data.status === "unavailable" || data.status === "forbidden_scope" || data.status === "period_too_long") {
    return (
      <EmptyState
        title={data.message || "Investigação indisponível."}
        detail={
          data.status === "no_data"
            ? "Ausência de publicação não é tratada como R$ 0."
            : data.comparison?.basis_label
        }
      />
    );
  }

  const totals = data.totals;
  const contributions = (data.factors || []).filter((f) => f.kind === "contribution");
  const hypotheses = (data.factors || []).filter((f) => f.kind === "hypothesis");

  return (
    <div style={{ display: "grid", gap: 16 }}>
      {data.headline ? <p style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>{data.headline}</p> : null}

      <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
        <Kpi label="Período atual" value={fmtMoney(totals?.current_faturamento)} hint={data.comparison?.current?.label} />
        <Kpi label="Base de comparação" value={fmtMoney(totals?.prior_faturamento)} hint={data.comparison?.prior?.label} />
        <Kpi
          label="Variação"
          value={fmtDelta(totals?.delta, totals?.delta_pct)}
          hint={data.comparison?.basis_label}
        />
      </div>

      {(data.warnings || []).length ? (
        <ul className="muted" style={{ margin: 0, paddingLeft: 18, fontSize: 13 }}>
          {data.warnings!.map((w) => (
            <li key={w}>{w}</li>
          ))}
        </ul>
      ) : null}

      {contributions.length ? (
        <section>
          <h3 style={{ margin: "0 0 8px", fontSize: 14 }}>Fatores observados (contribuição)</h3>
          <p className="muted" style={{ margin: "0 0 8px", fontSize: 12 }}>
            {data.legend?.contribution || "Contribuição quantitativa — não prova causa."}
          </p>
          <div className="tableScroll">
            <table className="table compact" style={{ width: "100%" }}>
              <thead>
                <tr>
                  <th>Dimensão</th>
                  <th>Fator</th>
                  <th>Delta</th>
                  <th>% da variação</th>
                </tr>
              </thead>
              <tbody>
                {contributions.map((f, idx) => (
                  <tr key={`${f.dimension}-${f.label}-${idx}`}>
                    <td>{f.dimension || "—"}</td>
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
        </section>
      ) : null}

      {hypotheses.length ? (
        <section>
          <h3 style={{ margin: "0 0 8px", fontSize: 14 }}>Hipóteses para verificar</h3>
          <p className="muted" style={{ margin: "0 0 8px", fontSize: 12 }}>
            {data.legend?.hypothesis || "Hipótese — não é causa comprovada."}
          </p>
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {hypotheses.map((h, idx) => (
              <li key={`${h.label}-${idx}`}>
                <span className="muted" style={{ fontSize: 11, marginRight: 6 }}>
                  [{kindLabel(h.kind)}]
                </span>
                {h.summary || h.label}
              </li>
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

      {data.freshness?.last_updated ? (
        <p className="muted" style={{ margin: 0, fontSize: 12 }}>
          Última publicação na mart: {data.freshness.last_updated}
        </p>
      ) : null}
    </div>
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
