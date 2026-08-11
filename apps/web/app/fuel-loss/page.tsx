"use client";

import { useMemo, useState } from "react";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { rowMatchesGridSearch, useGridSearch } from "../lib/use-grid-search";
import { canAccessScreenKey, readCachedSession } from "../lib/session";

export const dynamic = "force-dynamic";

const SCREEN_TITLE = "Movimentações de Combustível";

type LossItem = {
  id_filial: number;
  filial_nome: string;
  id_tanque: number;
  combustivel: string;
  dia: string;
  dia_anterior: string;
  leitura_anterior_l: number;
  leitura_atual_l: number;
  dif_leitura_l?: number;
  delta_sensor_l?: number;
  movimentacao_l?: number;
  vendas_l?: number;
  saidas_l?: number;
  entradas_l?: number;
  diferenca_l?: number | null;
  perda_l?: number | null;
  status: string;
};

type LossPayload = {
  kpis?: {
    filiais: number;
    pares: number;
    diferenca_l?: number;
    perda_l?: number;
    dias_entrada?: number;
    dias_reposicao?: number;
  };
  filiais?: {
    id_filial: number;
    filial_nome: string;
    diferenca_l?: number;
    perda_l?: number;
    dias_entrada?: number;
    dias_reposicao?: number;
    itens: LossItem[];
  }[];
};

type AfericaoItem = {
  id_filial: number;
  filial_nome: string;
  id_afericao: number;
  id_bico: number;
  bico_label: string;
  produto_nome: string;
  turno_operacional: number;
  turno_label: string;
  qtde_l: number;
  dia: string;
  operador_nome: string;
  liberador_nome: string;
};

type AfericoesPayload = {
  kpis?: {
    afericoes: number;
    litros: number;
    filiais: number;
  };
  itens?: AfericaoItem[];
};

function fmtL(value: unknown, digits = 1): string {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toLocaleString("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })} L`;
}

function fmtDia(iso: string | undefined): string {
  if (!iso) return "—";
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return String(iso);
  return `${m[3]}/${m[2]}/${m[1]}`;
}

/** Sinal invertido visual: sobe = verde (+), desce = vermelho (−). */
function SignedLiters({ value }: { value: number | null | undefined }) {
  if (value == null || !Number.isFinite(Number(value))) return <span>—</span>;
  const n = Number(value);
  const tone =
    n > 0.5
      ? "var(--color-positive, var(--positive, #22c55e))"
      : n < -0.5
        ? "var(--color-negative, #ef4444)"
        : "var(--muted)";
  const prefix = n > 0 ? "+" : "";
  return (
    <span style={{ color: tone, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
      {prefix}
      {fmtL(n)}
    </span>
  );
}

function sortTankItems(items: LossItem[]): LossItem[] {
  return [...items].sort((a, b) => {
    const da = String(a.dia || "");
    const db = String(b.dia || "");
    if (da !== db) return db.localeCompare(da);
    const ca = String(a.combustivel || "").localeCompare(String(b.combustivel || ""), "pt-BR");
    if (ca !== 0) return ca;
    return Number(a.id_tanque || 0) - Number(b.id_tanque || 0);
  });
}

export default function FuelLossPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const session = readCachedSession();
  const allowed = canAccessScreenKey(session, "fuel_loss");

  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<LossPayload>({
      moduleKey: "inventory_fuel_loss",
      scope,
      errorMessage: "Falha ao carregar movimentações de combustível",
      buildRequestUrl: (currentScope) => {
        if (!allowed) return null;
        return `/bi/estoque/perda?${buildScopeParams(currentScope).toString()}`;
      },
    });

  const {
    data: afericoesData,
    error: afericoesError,
    loading: afericoesLoading,
  } = useBiScopeData<AfericoesPayload>({
    moduleKey: "inventory_fuel_afericoes",
    scope,
    errorMessage: "Falha ao carregar aferições de bico",
    keepPreviousData: true,
    buildRequestUrl: (currentScope) => {
      if (!allowed) return null;
      return `/bi/estoque/afericoes?${buildScopeParams(currentScope).toString()}`;
    },
  });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("movimentações de combustível")
    : buildModuleLoadingCopy("movimentações de combustível");

  const kpis = data?.kpis;
  const [lossQuery, setLossQuery] = useState("");
  const filiais = useMemo(() => {
    const base = [...(data?.filiais || [])]
      .map((f) => ({ ...f, itens: sortTankItems(f.itens || []) }))
      .sort((a, b) => a.filial_nome.localeCompare(b.filial_nome, "pt-BR"));
    if (!lossQuery.trim()) return base;
    return base
      .map((f) => ({
        ...f,
        itens: f.itens.filter((item) =>
          rowMatchesGridSearch(
            { ...item, filial_nome: f.filial_nome },
            lossQuery,
            { excludeKeys: /^id_/ },
          ),
        ),
      }))
      .filter((f) => f.itens.length > 0);
  }, [data?.filiais, lossQuery]);
  const afericoesOrdenadas = useMemo(
    () =>
      [...(afericoesData?.itens || [])].sort((a, b) => {
        const fa = a.filial_nome.localeCompare(b.filial_nome, "pt-BR");
        if (fa !== 0) return fa;
        const da = String(b.dia || "").localeCompare(String(a.dia || ""));
        if (da !== 0) return da;
        return String(a.produto_nome || "").localeCompare(String(b.produto_nome || ""), "pt-BR");
      }),
    [afericoesData?.itens],
  );
  const {
    query: afericoesQuery,
    setQuery: setAfericoesQuery,
    filteredRows: afericoes,
  } = useGridSearch(afericoesOrdenadas, { excludeKeys: /^id_/ });
  const afericoesKpis = afericoesData?.kpis;
  const diferencaKpi = Number(kpis?.diferenca_l ?? kpis?.perda_l ?? 0);

  if (!allowed && session) {
    return (
      <div>
        <AppNav title={SCREEN_TITLE} userLabel={userLabel} />
        <div className="container">
          <div className="bi-grid">
            <div className="card col-12">Sem permissão para {SCREEN_TITLE}.</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <AppNav title={SCREEN_TITLE} userLabel={userLabel} />
      <div className="container">
        <div className="bi-grid">
          <header className="pageHeader col-12">
            <div>
              <div className="sectionEyebrow">Operação</div>
              <h1>{SCREEN_TITLE}</h1>
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
                <div className="label">Pares D−1×D</div>
                <div className="value">{kpis.pares}</div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Diferença no período</div>
                <div className="value">
                  <SignedLiters value={diferencaKpi} />
                </div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Dias c/ entrada</div>
                <div className="value">{kpis.dias_entrada ?? kpis.dias_reposicao ?? 0}</div>
              </div>

              <div
                className="col-12"
                style={{ display: "flex", justifyContent: "flex-start" }}
              >
                <GridSearchInput value={lossQuery} onChange={setLossQuery} />
              </div>

              {!filiais.length ? (
                <div className="col-12">
                  <EmptyState
                    title={lossQuery.trim() ? "Nada encontrado" : "Sem pares de leitura"}
                    detail={
                      lossQuery.trim()
                        ? "Nenhuma linha corresponde à pesquisa no período."
                        : "É preciso ter leitura do tanque em dias consecutivos no período selecionado."
                    }
                  />
                </div>
              ) : (
                filiais.map((filial) => (
                  <section key={filial.id_filial} className="card col-12" style={{ marginBottom: 16 }}>
                    <div
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        gap: 12,
                        flexWrap: "wrap",
                        marginBottom: 12,
                      }}
                    >
                      <div>
                        <div className="eyebrow">Filial</div>
                        <h2 style={{ margin: 0, fontSize: "1.15rem" }}>{filial.filial_nome}</h2>
                      </div>
                      <div className="muted" style={{ fontSize: 13 }}>
                        Diferença{" "}
                        <SignedLiters value={Number(filial.diferenca_l ?? filial.perda_l ?? 0)} />
                        {(filial.dias_entrada ?? filial.dias_reposicao)
                          ? ` · ${filial.dias_entrada ?? filial.dias_reposicao} dia(s) c/ entrada`
                          : ""}
                      </div>
                    </div>

                    <div className="tableScroll">
                      <table className="table compact" style={{ minWidth: 920 }}>
                        <thead>
                          <tr>
                            <th>Dia</th>
                            <th>Tanque</th>
                            <th>Combustível</th>
                            <th>Abertura</th>
                            <th>Fechamento</th>
                            <th>Dif Leitura</th>
                            <th>Movimentação</th>
                            <th>Diferença</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filial.itens.map((item) => {
                            const difLeitura = Number(
                              item.dif_leitura_l ?? item.delta_sensor_l ?? 0,
                            );
                            const mov = Number(item.movimentacao_l ?? item.vendas_l ?? 0);
                            const dif = item.diferenca_l ?? item.perda_l;
                            return (
                              <tr key={`${item.id_filial}-${item.id_tanque}-${item.dia}`}>
                                <td>{fmtDia(item.dia)}</td>
                                <td>{item.id_tanque || "—"}</td>
                                <td>{item.combustivel}</td>
                                <td>{fmtL(item.leitura_anterior_l)}</td>
                                <td>{fmtL(item.leitura_atual_l)}</td>
                                <td>
                                  <SignedLiters value={difLeitura} />
                                </td>
                                <td>
                                  <SignedLiters value={mov} />
                                </td>
                                <td>
                                  <SignedLiters value={dif} />
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </section>
                ))
              )}
            </>
          ) : null}

          <section className="card col-12">
            <div style={{ marginBottom: 12 }}>
              <div className="sectionEyebrow">Operação</div>
              <h2 style={{ margin: "0 0 10px", fontSize: "1.15rem" }}>Aferições de bico</h2>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 12,
                  flexWrap: "wrap",
                  alignItems: "center",
                }}
              >
                <GridSearchInput value={afericoesQuery} onChange={setAfericoesQuery} />
                {afericoesKpis ? (
                  <div className="muted" style={{ fontSize: 13 }}>
                    {afericoesKpis.afericoes} registro(s)
                    {afericoesKpis.litros > 0 ? ` · ${fmtL(afericoesKpis.litros)}` : ""}
                  </div>
                ) : null}
              </div>
            </div>

            {afericoesError ? (
              <div className="errorCard" style={{ marginBottom: 8 }}>
                {afericoesError}
              </div>
            ) : null}

            {afericoesLoading && !afericoesData ? (
              <p className="muted" style={{ fontSize: 13 }}>
                Carregando aferições…
              </p>
            ) : !afericoes.length ? (
              <EmptyState
                title={afericoesQuery.trim() ? "Nada encontrado" : "Sem aferições no período"}
                detail={
                  afericoesQuery.trim()
                    ? "Nenhuma aferição corresponde à pesquisa."
                    : "Não há registros de aferição de bico para as filiais selecionadas."
                }
              />
            ) : (
              <div className="tableScroll">
                <table className="table compact" style={{ minWidth: 880 }}>
                  <thead>
                    <tr>
                      <th>Filial</th>
                      <th>Data</th>
                      <th>Bico</th>
                      <th>Produto</th>
                      <th>Turno</th>
                      <th>Litros</th>
                      <th>Operador</th>
                      <th>Autorizado por</th>
                    </tr>
                  </thead>
                  <tbody>
                    {afericoes.map((item) => (
                      <tr key={`${item.id_filial}-${item.id_afericao}`}>
                        <td>{item.filial_nome}</td>
                        <td>{fmtDia(item.dia)}</td>
                        <td>{item.bico_label}</td>
                        <td>{item.produto_nome}</td>
                        <td>{item.turno_label}</td>
                        <td style={{ fontVariantNumeric: "tabular-nums" }}>{fmtL(item.qtde_l)}</td>
                        <td>{item.operador_nome}</td>
                        <td>{item.liberador_nome}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
