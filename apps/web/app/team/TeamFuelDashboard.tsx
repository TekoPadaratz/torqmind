"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
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
  qtd_abastecimentos?: number;
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
  total_abastecimentos?: number;
  combustiveis: FuelSlice[];
  by_employee: Record<
    string,
    { total_litros: number; qtd_abastecimentos?: number; combustiveis: FuelSlice[] }
  >;
};

type Payload = {
  dt_ini?: string;
  dt_fim?: string;
  filiais?: FilialPayload[];
};

const BAR_FILL = "var(--accent-copper, #b87333)";
const BAR_SELECTED = "#f59e0b";
const MIX_COLORS = ["#38bdf8", "#34d399", "#f59e0b", "#818cf8", "#fb7185", "#94a3b8", "#22d3ee"];

function fmtLitros(value: number): string {
  const n = Number(value || 0);
  if (n >= 1_000_000) {
    return `${(n / 1_000_000).toLocaleString("pt-BR", { maximumFractionDigits: 2 })} Mi`;
  }
  if (n >= 1_000) {
    return `${(n / 1_000).toLocaleString("pt-BR", { maximumFractionDigits: 2 })} Mil`;
  }
  return n.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
}

function truncateLabel(label: string, max = 28): string {
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
    if (!scope.ready) return;
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        // Escopo de filial/empresa da sidebar + competência do seletor de mês.
        // Não misturar dt_ini/dt_fim do BI geral (pode ser outro recorte).
        const params = buildScopeParams(scope);
        params.delete("dt_ini");
        params.delete("dt_fim");
        params.delete("dt_ref");
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
  }, [anoMes, scope.ready, scope.scope_key, scope.id_filiais_key, scope.id_empresa]);

  const periodLabel = useMemo(() => {
    if (!data?.dt_ini || !data?.dt_fim) return "";
    const fmt = (iso: string) => {
      const [y, m, d] = iso.split("-");
      return `${d}/${m}/${y}`;
    };
    return `${fmt(data.dt_ini)} — ${fmt(data.dt_fim)}`;
  }, [data?.dt_ini, data?.dt_fim]);

  if (!scope.ready || (loading && !data)) {
    return (
      <div className="card col-12" style={{ marginBottom: 12 }}>
        <div className="muted" style={{ padding: 16 }}>
          Carregando abastecimentos…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="card col-12 errorCard" style={{ marginBottom: 12 }}>
        {error}
      </div>
    );
  }

  const filiais = data?.filiais || [];
  if (filiais.length === 0) {
    return (
      <div className="card col-12" style={{ marginBottom: 12 }}>
        <EmptyState
          title="Sem abastecimentos no período"
          detail="Não há litros de combustível vendidos por funcionário no escopo e período selecionados."
        />
      </div>
    );
  }

  return (
    <div className="col-12" style={{ marginBottom: 8 }}>
      <div className="card col-12" style={{ marginBottom: 12, paddingBottom: 8 }}>
        <div className="sectionEyebrow">Equipe</div>
        <h2 style={{ marginTop: 4 }}>Abastecimentos por funcionário</h2>
        <div className="muted" style={{ marginTop: 6 }}>
          Combustível no período {periodLabel || "selecionado"} — ranking e mix por filial do escopo.
        </div>
      </div>

      {filiais.map((filial) => {
        const selectedId = selectedByFilial[filial.id_filial] ?? null;
        const empDetail = selectedId != null ? filial.by_employee?.[String(selectedId)] : null;
        const totalLitros = empDetail?.total_litros ?? filial.total_litros;
        const totalQty =
          empDetail?.qtd_abastecimentos ??
          filial.total_abastecimentos ??
          filial.ranking.reduce((acc, r) => acc + Number(r.qtd_abastecimentos || 0), 0);
        const selectedNome =
          selectedId != null
            ? filial.ranking.find((r) => r.id_funcionario === selectedId)?.nome
            : null;
        const mix = empDetail?.combustiveis ?? filial.combustiveis ?? [];
        const rankingChart = filial.ranking.map((r) => ({
          ...r,
          label: truncateLabel(r.nome, 22),
          selected: selectedId === r.id_funcionario,
          qtd: Number(r.qtd_abastecimentos || 0),
        }));
        const chartHeight = Math.max(280, Math.min(520, rankingChart.length * 36));

        return (
          <section
            key={filial.id_filial}
            className="solvenciaFilialCard commissionFilialCard col-12"
            style={{
              marginBottom: 12,
              borderLeft: "4px solid var(--accent-copper, #b87333)",
            }}
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
                      {selectedNome}
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
                  <span className="muted" style={{ fontSize: 12 }}>
                    {rankingChart.length} funcionário(s)
                  </span>
                )}
              </div>
            </div>

            <div className="bi-grid" style={{ marginTop: 8 }}>
              <div className="card kpi col-4" style={{ margin: 0 }}>
                <div className="label">Litros</div>
                <div className="value">{fmtLitros(totalLitros)}</div>
              </div>
              <div className="card kpi col-4" style={{ margin: 0 }}>
                <div className="label">Abastecimentos</div>
                <div className="value">
                  {Number(totalQty || 0).toLocaleString("pt-BR", { maximumFractionDigits: 0 })}
                </div>
              </div>
              <div className="card kpi col-4" style={{ margin: 0 }}>
                <div className="label">Funcionários</div>
                <div className="value">
                  {selectedId != null ? 1 : rankingChart.length}
                </div>
              </div>

              <div className="card col-12 chartCard" style={{ margin: 0 }}>
                <h2 style={{ margin: 0, fontSize: 16 }}>Ranking por litros</h2>
                <div className="muted" style={{ marginTop: 6 }}>
                  Clique em um funcionário para filtrar o mix de combustíveis.
                </div>
                <div className="chartWrap" style={{ height: chartHeight }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={rankingChart}
                      layout="vertical"
                      margin={{ top: 8, right: 16, left: 8, bottom: 8 }}
                    >
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
                        width={140}
                        tick={{ fontSize: 12 }}
                        interval={0}
                      />
                      <Tooltip
                        formatter={(value: number, _name, item) => {
                          const qtd = Number((item?.payload as RankingRow)?.qtd_abastecimentos || 0);
                          return [
                            `${fmtLitros(value)} L · ${qtd.toLocaleString("pt-BR")} abast.`,
                            "Volume",
                          ];
                        }}
                        labelFormatter={(_, payload) =>
                          String((payload?.[0]?.payload as RankingRow)?.nome || "")
                        }
                      />
                      <Bar
                        dataKey="litros"
                        radius={[0, 6, 6, 0]}
                        cursor="pointer"
                        onClick={(bar) => {
                          const id = Number(
                            (bar as { payload?: RankingRow })?.payload?.id_funcionario || 0,
                          );
                          if (id > 0) {
                            setSelectedByFilial((prev) => ({
                              ...prev,
                              [filial.id_filial]:
                                prev[filial.id_filial] === id ? null : id,
                            }));
                          }
                        }}
                      >
                        {rankingChart.map((entry) => (
                          <Cell
                            key={entry.id_funcionario}
                            fill={entry.selected ? BAR_SELECTED : BAR_FILL}
                          />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="card col-12" style={{ margin: 0 }}>
                <h2 style={{ margin: 0, fontSize: 16 }}>Mix por combustível</h2>
                {mix.length === 0 ? (
                  <EmptyState title="Sem mix" detail="Sem litros no período selecionado." />
                ) : (
                  <div className="tableScroll" style={{ marginTop: 10 }}>
                    <table className="table compact" style={{ width: "100%", minWidth: 420 }}>
                      <thead>
                        <tr>
                          <th style={{ textAlign: "left" }}>Combustível</th>
                          <th style={{ textAlign: "right" }}>Litros</th>
                          <th style={{ textAlign: "right" }}>%</th>
                          <th style={{ textAlign: "left", width: "40%" }}>Participação</th>
                        </tr>
                      </thead>
                      <tbody>
                        {mix.map((s, idx) => (
                          <tr key={s.label}>
                            <td style={{ textAlign: "left" }}>
                              <span
                                style={{
                                  display: "inline-block",
                                  width: 8,
                                  height: 8,
                                  borderRadius: 999,
                                  background: MIX_COLORS[idx % MIX_COLORS.length],
                                  marginRight: 8,
                                }}
                              />
                              {s.label}
                            </td>
                            <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                              {fmtLitros(s.litros)}
                            </td>
                            <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                              {Number(s.pct || 0).toLocaleString("pt-BR", {
                                maximumFractionDigits: 1,
                              })}
                              %
                            </td>
                            <td>
                              <div
                                style={{
                                  height: 8,
                                  borderRadius: 999,
                                  background: "rgba(255,255,255,0.08)",
                                  overflow: "hidden",
                                }}
                              >
                                <div
                                  style={{
                                    width: `${Math.max(0, Math.min(100, Number(s.pct || 0)))}%`,
                                    height: "100%",
                                    background: MIX_COLORS[idx % MIX_COLORS.length],
                                  }}
                                />
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
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
