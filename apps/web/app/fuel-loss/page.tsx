"use client";

import { useMemo } from "react";

import AppNav from "../components/AppNav";
import EmptyState from "../components/ui/EmptyState";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import { buildUserLabel } from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { canAccessScreenKey, readCachedSession } from "../lib/session";

export const dynamic = "force-dynamic";

type LossItem = {
  id_filial: number;
  filial_nome: string;
  id_tanque: number;
  combustivel: string;
  dia: string;
  dia_anterior: string;
  leitura_anterior_l: number;
  leitura_atual_l: number;
  delta_sensor_l: number;
  vendas_l: number;
  entrada_aparente_l: number;
  perda_l: number | null;
  status: string;
};

type LossPayload = {
  kpis?: {
    filiais: number;
    pares: number;
    perda_l: number;
    dias_reposicao: number;
  };
  filiais?: {
    id_filial: number;
    filial_nome: string;
    perda_l: number;
    dias_reposicao: number;
    itens: LossItem[];
  }[];
  disclaimer?: string;
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
  disclaimer?: string;
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

function PerdaCell({ value, status }: { value: number | null; status: string }) {
  if (status === "reposicao") {
    return <span className="muted">Reposição</span>;
  }
  if (value == null) return <span>—</span>;
  const tone =
    value > 0.5
      ? "var(--color-negative, #ef4444)"
      : value < -0.5
        ? "var(--positive, #22c55e)"
        : "var(--muted)";
  return (
    <span style={{ color: tone, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
      {fmtL(value)}
    </span>
  );
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
      errorMessage: "Falha ao carregar perda de combustível",
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
    errorMessage: "Falha ao carregar aferições",
    keepPreviousData: true,
    buildRequestUrl: (currentScope) => {
      if (!allowed) return null;
      return `/bi/estoque/afericoes?${buildScopeParams(currentScope).toString()}`;
    },
  });

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("perda de combustível")
    : buildModuleLoadingCopy("perda de combustível");

  const kpis = data?.kpis;
  const filiais = useMemo(
    () =>
      [...(data?.filiais || [])].sort((a, b) =>
        a.filial_nome.localeCompare(b.filial_nome, "pt-BR"),
      ),
    [data?.filiais],
  );
  const afericoes = useMemo(
    () =>
      [...(afericoesData?.itens || [])].sort((a, b) =>
        a.filial_nome.localeCompare(b.filial_nome, "pt-BR"),
      ),
    [afericoesData?.itens],
  );
  const afericoesKpis = afericoesData?.kpis;

  if (!allowed && session) {
    return (
      <div>
        <AppNav title="Perda de combustível" userLabel={userLabel} />
        <div className="container">
          <div className="bi-grid">
            <div className="card col-12">Sem permissão para Perda de combustível.</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <AppNav title="Perda de combustível" userLabel={userLabel} />
      <div className="container">
        <div className="bi-grid">
          <header className="pageHeader col-12">
            <div>
              <div className="sectionEyebrow">Operação</div>
              <h1>Perda de combustível</h1>
              <p className="muted" style={{ marginTop: 4, maxWidth: 760 }}>
                Conciliação entre leituras do tanque e vendas para identificar perdas.
              </p>
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
              <div className="label">Perda no período</div>
              <div className="value" style={{ color: kpis.perda_l > 0.5 ? "var(--color-negative, #ef4444)" : undefined }}>
                {fmtL(kpis.perda_l)}
              </div>
            </div>
            <div className="card kpi col-3">
              <div className="label">Dias c/ reposição</div>
              <div className="value">{kpis.dias_reposicao}</div>
            </div>

          {!filiais.length ? (
            <div className="col-12">
              <EmptyState
                title="Sem pares de leitura"
                detail="É preciso ter leitura do sensor em dias consecutivos no período selecionado."
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
                    Perda {fmtL(filial.perda_l)}
                    {filial.dias_reposicao
                      ? ` · ${filial.dias_reposicao} reposição(ões)`
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
                        <th>Leitura D−1</th>
                        <th>Leitura D</th>
                        <th>Δ sensor</th>
                        <th>Vendas (D−1)</th>
                        <th>Perda</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filial.itens.map((item) => (
                        <tr key={`${item.id_filial}-${item.id_tanque}-${item.dia}`}>
                          <td>{fmtDia(item.dia)}</td>
                          <td>#{item.id_tanque}</td>
                          <td>{item.combustivel}</td>
                          <td>{fmtL(item.leitura_anterior_l)}</td>
                          <td>{fmtL(item.leitura_atual_l)}</td>
                          <td>{fmtL(item.delta_sensor_l)}</td>
                          <td>{fmtL(item.vendas_l)}</td>
                          <td>
                            <PerdaCell value={item.perda_l} status={item.status} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            ))
          )}

        </>
      ) : null}

      <section className="card col-12">
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 12,
            flexWrap: "wrap",
            marginBottom: 12,
            alignItems: "baseline",
          }}
        >
          <div>
            <div className="sectionEyebrow">Operação</div>
            <h2 style={{ margin: 0, fontSize: "1.15rem" }}>Aferições no período</h2>
            <p className="muted" style={{ margin: "4px 0 0", fontSize: 13 }}>
              Litros aferidos por bico no período selecionado.
            </p>
          </div>
          {afericoesKpis ? (
            <div className="muted" style={{ fontSize: 13 }}>
              {afericoesKpis.afericoes} registro(s)
              {afericoesKpis.litros > 0 ? ` · ${fmtL(afericoesKpis.litros)}` : ""}
            </div>
          ) : null}
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
            title="Sem aferições no período"
            detail="Não há registros de aferição para as filiais selecionadas."
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
                  <th>Liberador</th>
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
