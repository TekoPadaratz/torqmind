"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import EmptyState from "../components/ui/EmptyState";
import { apiGet } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { buildScopeParams, useScopeQuery } from "../lib/scope";

type RankingRow = {
  id_funcionario: number;
  nome: string;
  litros: number;
};

type FuelSlice = {
  label: string;
  litros: number;
  pct: number;
};

type FilialPayload = {
  id_filial: number;
  filial_label: string;
  ranking: RankingRow[];
  total_litros: number;
  combustiveis: FuelSlice[];
  by_employee: Record<
    string,
    { total_litros: number; combustiveis: FuelSlice[] }
  >;
};

type Payload = {
  dt_ini?: string;
  dt_fim?: string;
  filiais?: FilialPayload[];
};

const PIE_COLORS = ["#38bdf8", "#1d4ed8", "#f97316", "#a855f7", "#ec4899", "#22c55e", "#eab308"];

function fmtLitros(value: number): string {
  const n = Number(value || 0);
  if (n >= 1_000_000) return `${(n / 1_000_000).toLocaleString("pt-BR", { maximumFractionDigits: 2 })} Mi`;
  if (n >= 1_000) return `${(n / 1_000).toLocaleString("pt-BR", { maximumFractionDigits: 2 })} Mil`;
  return n.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
}

function truncateLabel(label: string, max = 22): string {
  const t = String(label || "").trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1)}…`;
}

type Props = {
  anoMes: number;
};

export default function TeamFuelDashboard({ anoMes }: Props) {
  const scope = useScopeQuery();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [data, setData] = useState<Payload | null>(null);
  const [selectedByFilial, setSelectedByFilial] = useState<Record<number, number | null>>({});

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const params = buildScopeParams(scope);
        params.set("ano_mes", String(anoMes));
        const payload = await apiGet(`/bi/team/fuel-employees?${params.toString()}`, {
          signal: controller.signal,
        });
        setData(payload as Payload);
        setSelectedByFilial({});
      } catch (err: unknown) {
        if ((err as { name?: string; code?: string })?.name === "AbortError") return;
        if ((err as { code?: string })?.code === "ERR_CANCELED") return;
        setError(extractApiError(err, "Falha ao carregar abastecimentos"));
      } finally {
        setLoading(false);
      }
    };
    void load();
    return () => controller.abort();
  }, [anoMes, scope.scope_key]);

  const periodLabel = useMemo(() => {
    if (!data?.dt_ini || !data?.dt_fim) return "";
    const fmt = (iso: string) => {
      const [y, m, d] = iso.split("-");
      return `${d}/${m}/${y}`;
    };
    return `${fmt(data.dt_ini)} — ${fmt(data.dt_fim)}`;
  }, [data?.dt_ini, data?.dt_fim]);

  if (loading && !data) {
    return (
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="muted" style={{ padding: 16 }}>Carregando abastecimentos…</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="card errorCard" style={{ marginBottom: 12 }}>{error}</div>
    );
  }

  const filiais = data?.filiais || [];
  if (filiais.length === 0) {
    return (
      <div className="card" style={{ marginBottom: 12 }}>
        <EmptyState
          title="Sem abastecimentos no período"
          detail="Não há litros de combustível vendidos por funcionário no escopo e período selecionados."
        />
      </div>
    );
  }

  return (
    <div style={{ marginBottom: 16 }}>
      <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
        Abastecimentos por funcionário (combustível) · {periodLabel}
      </div>
      {filiais.map((filial) => {
        const selectedId = selectedByFilial[filial.id_filial] ?? null;
        const empDetail = selectedId != null ? filial.by_employee?.[String(selectedId)] : null;
        const totalLitros = empDetail?.total_litros ?? filial.total_litros;
        const pieData = (empDetail?.combustiveis ?? filial.combustiveis ?? []).map((s) => ({
          name: truncateLabel(s.label),
          fullName: s.label,
          value: s.litros,
          pct: s.pct,
        }));
        const rankingChart = filial.ranking.map((r) => ({
          ...r,
          label: truncateLabel(r.nome, 18),
          selected: selectedId === r.id_funcionario,
        }));
        const chartHeight = Math.max(220, Math.min(420, rankingChart.length * 34));

        return (
          <section
            key={filial.id_filial}
            className="solvenciaFilialCard commissionFilialCard"
            style={{ marginBottom: 12, borderLeft: "4px solid #0ea5a4" }}
          >
            <div className="commissionFilialHead">
              <div>
                <div className="sectionEyebrow">Filial</div>
                <h2 className="commissionFilialTitle">{filial.filial_label}</h2>
              </div>
              <div className="commissionFilialSummary" style={{ alignItems: "center", gap: 8 }}>
                {selectedId != null ? (
                  <>
                    <span className="muted" style={{ fontSize: 12 }}>
                      {filial.ranking.find((r) => r.id_funcionario === selectedId)?.nome}
                    </span>
                    <button
                      type="button"
                      className="btn"
                      style={{ fontSize: 11, padding: "4px 10px" }}
                      onClick={() =>
                        setSelectedByFilial((prev) => ({ ...prev, [filial.id_filial]: null }))
                      }
                    >
                      Todos
                    </button>
                  </>
                ) : (
                  <span className="muted" style={{ fontSize: 12 }}>{rankingChart.length} funcionário(s)</span>
                )}
              </div>
            </div>

            <div
              style={{
                display: "grid",
                gridTemplateColumns: "minmax(240px, 1.2fr) minmax(140px, 0.55fr) minmax(200px, 0.85fr)",
                gap: 12,
                alignItems: "stretch",
              }}
            >
              <div className="card" style={{ margin: 0, padding: 12 }}>
                <h3 style={{ margin: "0 0 8px", fontSize: 13, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                  Abastecimentos por funcionário
                </h3>
                <div style={{ maxHeight: 420, overflowY: rankingChart.length > 10 ? "auto" : "visible" }}>
                  <div className="chartWrap" style={{ height: chartHeight }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={rankingChart} layout="vertical" margin={{ left: 4, right: 8 }}>
                        <CartesianGrid stroke="rgba(255,255,255,0.08)" strokeDasharray="3 3" />
                        <XAxis
                          type="number"
                          stroke="var(--muted)"
                          tickFormatter={(v) => fmtLitros(Number(v))}
                        />
                        <YAxis
                          type="category"
                          dataKey="label"
                          stroke="var(--muted)"
                          width={118}
                          tick={{ fontSize: 11 }}
                          interval={0}
                        />
                        <Tooltip
                          formatter={(value: number) => [fmtLitros(value), "Litros"]}
                          labelFormatter={(_, payload) =>
                            String((payload?.[0]?.payload as RankingRow)?.nome || "")
                          }
                        />
                        <Bar
                          dataKey="litros"
                          radius={[0, 4, 4, 0]}
                          cursor="pointer"
                          onClick={(bar) => {
                            const id = Number((bar as { payload?: RankingRow })?.payload?.id_funcionario || 0);
                            if (id > 0) {
                              setSelectedByFilial((prev) => ({ ...prev, [filial.id_filial]: id }));
                            }
                          }}
                        >
                          {rankingChart.map((entry) => (
                            <Cell
                              key={entry.id_funcionario}
                              fill={entry.selected ? "#f59e0b" : "#94a3b8"}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              <div
                className="card"
                style={{
                  margin: 0,
                  padding: 16,
                  display: "flex",
                  flexDirection: "column",
                  justifyContent: "center",
                  alignItems: "center",
                  textAlign: "center",
                  background: "linear-gradient(160deg, rgba(14,165,164,0.18), rgba(15,23,42,0.2))",
                }}
              >
                <div style={{ fontSize: 12, opacity: 0.85, marginBottom: 8 }}>Litros vendidos</div>
                <div style={{ fontSize: "clamp(1.6rem, 3vw, 2.2rem)", fontWeight: 700, lineHeight: 1.1 }}>
                  {fmtLitros(totalLitros)}
                </div>
              </div>

              <div className="card" style={{ margin: 0, padding: 12 }}>
                <h3 style={{ margin: "0 0 8px", fontSize: 13, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                  Por combustível
                </h3>
                {pieData.length === 0 ? (
                  <EmptyState title="Sem mix" detail="Sem litros no recorte selecionado." />
                ) : (
                  <div className="chartWrap" style={{ height: 240 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={pieData}
                          dataKey="value"
                          nameKey="name"
                          cx="42%"
                          cy="50%"
                          innerRadius={42}
                          outerRadius={78}
                          paddingAngle={1}
                        >
                          {pieData.map((_, idx) => (
                            <Cell key={`cell-${idx}`} fill={PIE_COLORS[idx % PIE_COLORS.length]} />
                          ))}
                        </Pie>
                        <Tooltip
                          formatter={(value: number, _name, item) => [
                            `${fmtLitros(value)} (${(item?.payload as FuelSlice)?.pct ?? 0}%)`,
                            (item?.payload as { fullName?: string })?.fullName || "",
                          ]}
                        />
                      </PieChart>
                    </ResponsiveContainer>
                    <div style={{ fontSize: 11, marginTop: 4 }}>
                      {pieData.slice(0, 5).map((s, idx) => (
                        <div key={s.fullName} style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 2 }}>
                          <span
                            style={{
                              width: 8,
                              height: 8,
                              borderRadius: 999,
                              background: PIE_COLORS[idx % PIE_COLORS.length],
                              flexShrink: 0,
                            }}
                          />
                          <span>{s.name}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </section>
        );
      })}
    </div>
  );
}
