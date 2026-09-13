"use client";

import { useMemo } from "react";

import AppNav from "../components/AppNav";
import CategoryRankChart from "../components/ui/CategoryRankChart";
import EmptyState from "../components/ui/EmptyState";
import GridChrome from "../components/ui/GridChrome";
import GridSearchInput from "../components/ui/GridSearchInput";
import SortableTh from "../components/ui/SortableTh";
import ScopeTransitionState from "../components/ui/ScopeTransitionState";
import {
  buildUserLabel,
  formatCurrency,
  formatDateKey,
  formatDateTime,
  formatHoursLabel,
  formatTurnoLabel,
  formatTurnoPeriod,
} from "../lib/format";
import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
} from "../lib/reading-state.mjs";
import { compareGridRows, sortGridRows } from "../lib/grid-sort";
import { useRecordGrid } from "../lib/use-record-grid";
import { buildScopeParams, useEnsureScopedProductUrl, useScopeQuery } from "../lib/scope";
import { useBiScopeData } from "../lib/use-bi-scope-data";
import { useGridSearch } from "../lib/use-grid-search";

export const dynamic = "force-dynamic";

function severityTone(value: string) {
  const severity = String(value || "").toUpperCase();
  if (severity === "CRITICAL") {
    return { bg: "rgba(239, 68, 68, 0.14)", border: "rgba(248, 113, 113, 0.32)" };
  }
  if (severity === "HIGH") {
    return { bg: "rgba(245, 158, 11, 0.14)", border: "rgba(251, 191, 36, 0.28)" };
  }
  if (severity === "WARN") {
    return { bg: "rgba(56, 189, 248, 0.14)", border: "rgba(96, 165, 250, 0.28)" };
  }
  return { bg: "rgba(52, 211, 153, 0.12)", border: "rgba(74, 222, 128, 0.24)" };
}

function formatStockQuantity(value: any) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return numeric.toLocaleString("pt-BR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 3,
  });
}

export default function CashPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const { claims, data, error, loading, pendingUnavailable } =
    useBiScopeData<any>({
      moduleKey: "cash_overview",
      scope,
      errorMessage: "Falha ao carregar o módulo de Caixa",
      buildRequestUrl: (currentScope) =>
        `/bi/cash/overview?${buildScopeParams(currentScope).toString()}`,
    });
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy("caixa")
    : buildModuleLoadingCopy("caixa");

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const historical = data?.historical || {};
  const commercial = data?.commercial || {};
  const commercialKpis = commercial?.kpis || {};
  const liveNow = data?.live_now || {};
  const liveKpis = liveNow?.kpis || {};
  const dreSummary = data?.dre_summary || {};
  const paymentMix = historical?.payment_mix || [];
  const commercialByDay = commercial?.by_day || [];
  const isOperationalTurno = (item: any) => {
    const idTurno = Number(item?.id_turno || 0);
    if (!Number.isFinite(idTurno) || idTurno <= 0) return false;
    const tv = Number(item?.turno_value);
    if (Number.isFinite(tv) && tv >= 1) return true;
    const label = String(item?.turno_label || "").trim().toLowerCase();
    if (!label || label.includes("não resolvido") || label.includes("nao resolvido")) {
      return false;
    }
    if (label.includes("caixa geral")) return false;
    return /turno\s*[1-9]\d*/.test(label) || (Number.isFinite(Number(label)) && Number(label) >= 1);
  };

  const topTurnos = (commercial?.top_turnos || []).filter(isOperationalTurno);
  const openBoxes = useMemo(
    () =>
      sortGridRows(
        (liveNow?.open_boxes || data?.open_boxes || []).filter(isOperationalTurno),
        (i: any) => ({
          filial: i.filial_label ?? i.id_filial,
          data: i.abertura_ts,
          nome: i.usuario_label,
        }),
      ),
    [liveNow?.open_boxes, data?.open_boxes],
  );
  const staleBoxes = (liveNow?.stale_boxes || data?.stale_boxes || []).filter(isOperationalTurno);
  const alerts = liveNow?.alerts || data?.alerts || [];
  const inutilizacoes = data?.inutilizacoes || {};
  const inutItems = useMemo(
    () =>
      sortGridRows(inutilizacoes?.items || [], (i: any) => ({
        filial: i.filial_label ?? i.id_filial,
        data: i.data_emissao_nfe || i.dt,
        nome: i.usuario_label,
      })),
    [inutilizacoes?.items],
  );
  const hasInutilizacoes = Number(inutilizacoes?.qtd || 0) > 0;
  const { query: turnosQ, setQuery: setTurnosQ, filteredRows: filteredTurnos } = useGridSearch(
    topTurnos as Record<string, unknown>[],
  );
  const { query: inutilizacoesQ, setQuery: setInutilizacoesQ, filteredRows: filteredInutilizacoes } = useGridSearch(
    inutItems as Record<string, unknown>[],
  );
  const { query: caixasQ, setQuery: setCaixasQ, filteredRows: filteredCaixas } = useGridSearch(
    openBoxes as Record<string, unknown>[],
  );
  const turnosGrid = useRecordGrid<any>({
    rows: filteredTurnos,
    resetKey: `${scope.scope_key}:${turnosQ}`,
    getTieId: (row) => `${row.id_filial}-${row.id_turno}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.abertura_ts, nome: a.usuario_label },
        { filial: b.filial_label ?? b.id_filial, data: b.abertura_ts, nome: b.usuario_label },
      ),
    summableKeys: ["qtd_vendas", "total_vendas", "total_cancelamentos", "total_pagamentos"],
    columns: {
      filial_label: { type: "text" },
      turno_label: { type: "text", getValue: (r) => r.turno_label || r.turno_operacional },
      abertura_ts: { type: "date" },
      usuario_label: { type: "text" },
      qtd_vendas: { type: "number" },
      total_vendas: { type: "number" },
      total_cancelamentos: { type: "number" },
      total_pagamentos: { type: "number" },
    },
  });
  const inutGrid = useRecordGrid<any>({
    rows: filteredInutilizacoes,
    resetKey: `${scope.scope_key}:${inutilizacoesQ}`,
    getTieId: (row) => `${row.id_comprovante}-${row.id_nfe}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.data_emissao_nfe || a.dt, nome: a.usuario_label },
        { filial: b.filial_label ?? b.id_filial, data: b.data_emissao_nfe || b.dt, nome: b.usuario_label },
      ),
    summableKeys: ["valor_comprovante"],
    columns: {
      filial_label: { type: "text" },
      data_emissao_nfe: { type: "date", getValue: (r) => r.data_emissao_nfe || r.dt },
      usuario_label: { type: "text" },
      numero_nfe: { type: "text" },
      valor_comprovante: { type: "number" },
    },
  });
  const caixasGrid = useRecordGrid<any>({
    rows: filteredCaixas,
    resetKey: `${scope.scope_key}:${caixasQ}`,
    getTieId: (row) => `${row.id_filial}-${row.id_turno}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { filial: a.filial_label ?? a.id_filial, data: a.abertura_ts, nome: a.usuario_label },
        { filial: b.filial_label ?? b.id_filial, data: b.abertura_ts, nome: b.usuario_label },
      ),
    summableKeys: ["total_vendas", "total_cancelamentos", "total_pagamentos"],
    columns: {
      filial_label: { type: "text" },
      turno_label: { type: "text", getValue: (r) => r.turno_label || r.turno_operacional },
      usuario_label: { type: "text" },
      horas_aberto: { type: "number" },
      total_vendas: { type: "number" },
      total_cancelamentos: { type: "number" },
      total_pagamentos: { type: "number" },
    },
  });
  function formatNfeDateTime(item: any) {
    if (item?.data_emissao_nfe) return formatDateTime(item.data_emissao_nfe);
    if (item?.dt) {
      const hourValue = Number(item?.hora);
      if (Number.isFinite(hourValue) && hourValue >= 0) {
        const hour = String(Math.trunc(hourValue)).padStart(2, "0");
        return formatDateTime(`${item.dt}T${hour}:00:00`);
      }
      return formatDateTime(`${item.dt}T00:00:00`);
    }
    return "-";
  }

  return (
    <div>
      <AppNav title="Caixa" userLabel={userLabel} />
      <div className="container">
        {error ? <div className="card errorCard">{error}</div> : null}
        {!data ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? "unavailable" : "loading"}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={5}
              panels={5}
            />
          </div>
        ) : (
          <>
            <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="col-6" style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
                <div className="card kpi">
                  <div className="label">Vendas no período</div>
                  <div className="value">{loading ? "..." : formatCurrency(commercialKpis?.total_vendas)}</div>
                </div>
                <div className="card kpi">
                  <div className="label">Cancelamentos</div>
                  <div className="value">{loading ? "..." : formatCurrency(commercialKpis?.total_cancelamentos)}</div>
                </div>
                <div className="card kpi">
                  <div className="label">Recebimentos</div>
                  <div className="value">{loading ? "..." : formatCurrency(commercialKpis?.total_pagamentos)}</div>
                  {!loading && Math.abs(Number(commercialKpis?.diferenca_conciliacao || 0)) > 0.01 ? (
                    <div className="muted" style={{ marginTop: 6, fontSize: 12 }}>
                      Não conciliado: {formatCurrency(commercialKpis?.diferenca_conciliacao)}
                    </div>
                  ) : null}
                </div>
                <div className="card kpi">
                  <div className="label">Saldo comercial</div>
                  <div className="value">{loading ? "..." : formatCurrency(commercialKpis?.saldo_comercial)}</div>
                </div>
              </div>

              <div className="card col-6 chartCard">
                <h2>Formas de Pagamento Vendas</h2>
                <div className="muted" style={{ marginTop: 8 }}>
                  Distribuição conciliada dos recebimentos de vendas do período por forma de pagamento.
                </div>
                {!loading && !paymentMix.length ? (
                  <EmptyState
                    title="Sem pagamentos conciliados no período."
                    detail="A distribuição por forma aparece quando existem recebimentos conciliados no período."
                  />
                ) : null}
                <CategoryRankChart
                  kind="category"
                  barFill="#818cf8"
                  axisWidth={168}
                  rows={paymentMix.map((item: { label?: string; total_valor?: number }, index: number) => ({
                    id: `${String(item.label || "forma")}:${index}`,
                    name: String(item.label || "—"),
                    value: Number(item.total_valor || 0),
                  }))}
                  axisFormatter={(value) => formatCurrency(value)}
                  valueFormatter={(value) => formatCurrency(value)}
                />
              </div>

              <div className="card col-12">
                <h2>Fluxo do período selecionado</h2>
                <div style={{ margin: "8px 0" }}><GridSearchInput value={turnosQ} onChange={setTurnosQ} /></div>
                {!loading && !topTurnos.length ? (
                  <EmptyState
                    title="Sem turnos comerciais no período."
                    detail="Os turnos aparecem quando comprovantes e recebimentos ficam vinculados ao período."
                  />
                ) : null}
                {topTurnos.length ? (
                  <div className="tableScroll">
                    <table className="table compact">
                      <thead>
                        <tr>
                          <SortableTh label="Filial" sortKey="filial_label" ariaSort={turnosGrid.ariaSort("filial_label")} onToggle={turnosGrid.toggleSort} />
                          <SortableTh label="Turno" sortKey="turno_label" ariaSort={turnosGrid.ariaSort("turno_label")} onToggle={turnosGrid.toggleSort} />
                          <SortableTh label="Período do turno" sortKey="abertura_ts" ariaSort={turnosGrid.ariaSort("abertura_ts")} onToggle={turnosGrid.toggleSort} />
                          <SortableTh label="Operador" sortKey="usuario_label" ariaSort={turnosGrid.ariaSort("usuario_label")} onToggle={turnosGrid.toggleSort} />
                          <SortableTh label="Qtd. vendas" sortKey="qtd_vendas" ariaSort={turnosGrid.ariaSort("qtd_vendas")} onToggle={turnosGrid.toggleSort} align="right" />
                          <SortableTh label="Faturamento" sortKey="total_vendas" ariaSort={turnosGrid.ariaSort("total_vendas")} onToggle={turnosGrid.toggleSort} align="right" />
                          <SortableTh label="Cancel." sortKey="total_cancelamentos" ariaSort={turnosGrid.ariaSort("total_cancelamentos")} onToggle={turnosGrid.toggleSort} align="right" />
                          <SortableTh label="Receb." sortKey="total_pagamentos" ariaSort={turnosGrid.ariaSort("total_pagamentos")} onToggle={turnosGrid.toggleSort} align="right" />
                          <th>Saldo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {turnosGrid.slice.map((item: any) => (
                          <tr key={`${item.id_filial}-${item.id_turno}`}>
                            <td>{item.filial_label}</td>
                            <td>
                              {item.turno_label ||
                                formatTurnoLabel(
                                  item.turno_operacional ?? item.turno_numero,
                                  item.turno_label
                                )}
                            </td>
                            <td>{formatTurnoPeriod(item.abertura_ts, item.fechamento_ts)}</td>
                            <td>{item.usuario_label || item.nome_operador || "Operador sem cadastro"}</td>
                            <td>{Number(item.qtd_vendas || 0)}</td>
                            <td>{formatCurrency(item.total_vendas)}</td>
                            <td>{formatCurrency(item.total_cancelamentos)}</td>
                            <td>{formatCurrency(item.total_pagamentos)}</td>
                            <td>{formatCurrency(item.saldo_comercial)}</td>
                          </tr>
                        ))}
                      </tbody>
                      {turnosGrid.slice.length ? (
                        <tfoot>
                          <tr>
                            <td colSpan={4}>Total da página ({turnosGrid.slice.length})</td>
                            <td>{turnosGrid.pageTotals.qtd_vendas}</td>
                            <td>{formatCurrency(turnosGrid.pageTotals.total_vendas)}</td>
                            <td>{formatCurrency(turnosGrid.pageTotals.total_cancelamentos)}</td>
                            <td>{formatCurrency(turnosGrid.pageTotals.total_pagamentos)}</td>
                            <td />
                          </tr>
                        </tfoot>
                      ) : null}
                    </table>
                  </div>
                ) : null}
                {topTurnos.length ? (
                  <GridChrome
                    page={turnosGrid.page}
                    totalPages={turnosGrid.totalPages}
                    total={turnosGrid.total}
                    from={turnosGrid.range.from}
                    to={turnosGrid.range.to}
                    onPrev={turnosGrid.onPrev}
                    onNext={turnosGrid.onNext}
                    onResetOrder={turnosGrid.resetOrder}
                    isDefaultOrder={turnosGrid.isDefaultOrder}
                  />
                ) : null}
              </div>

              {hasInutilizacoes ? (
                <div className="card col-12">
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                    <h2 style={{ margin: 0, color: "var(--accent-copper)" }}>Notas Fiscais Inutilizadas</h2>
                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                      <div className="card" style={{ padding: "8px 14px" }}>
                        <div className="label">Qtd. inutilizadas</div>
                        <div style={{ fontSize: 20, fontWeight: 800 }}>{inutilizacoes.qtd || 0}</div>
                      </div>
                      <div className="card" style={{ padding: "8px 14px" }}>
                        <div className="label">Valor total</div>
                        <div style={{ fontSize: 20, fontWeight: 800 }}>{formatCurrency(inutilizacoes.valor_total)}</div>
                      </div>
                    </div>
                  </div>
                  <div style={{ marginBottom: 12 }}>
                    <GridSearchInput value={inutilizacoesQ} onChange={setInutilizacoesQ} />
                  </div>
                  {!loading && !inutItems.length ? (
                    <EmptyState
                      title="Lista detalhada em preparação"
                      detail="Existem notas inutilizadas no período, mas a lista detalhada ainda está sendo preparada."
                    />
                  ) : null}
                  {inutItems.length ? (
                    <div className="tableScroll">
                      <table className="table compact">
                        <thead>
                          <tr>
                            <SortableTh label="Filial" sortKey="filial_label" ariaSort={inutGrid.ariaSort("filial_label")} onToggle={inutGrid.toggleSort} />
                            <SortableTh label="Data/hora" sortKey="data_emissao_nfe" ariaSort={inutGrid.ariaSort("data_emissao_nfe")} onToggle={inutGrid.toggleSort} />
                            <th>Turno/caixa</th>
                            <SortableTh label="Operador" sortKey="usuario_label" ariaSort={inutGrid.ariaSort("usuario_label")} onToggle={inutGrid.toggleSort} />
                            <SortableTh label="Documento" sortKey="numero_nfe" ariaSort={inutGrid.ariaSort("numero_nfe")} onToggle={inutGrid.toggleSort} />
                            <SortableTh label="Valor" sortKey="valor_comprovante" ariaSort={inutGrid.ariaSort("valor_comprovante")} onToggle={inutGrid.toggleSort} align="right" />
                            <th>Chave / protocolo</th>
                          </tr>
                        </thead>
                        <tbody>
                          {inutGrid.slice.map((item: any) => (
                            <tr key={`inut-${item.id_comprovante}-${item.id_nfe}`}>
                              <td>{item.filial_label}</td>
                              <td>{formatNfeDateTime(item)}</td>
                              <td>{formatTurnoLabel(item.turno_operacional ?? item.turno_numero, item.turno_label)}</td>
                              <td>{item.usuario_label}</td>
                              <td>{item.numero_nfe || "-"}</td>
                              <td>{formatCurrency(item.valor_comprovante)}</td>
                              <td>{item.protocolo || item.chave_nfe || "-"}</td>
                            </tr>
                          ))}
                        </tbody>
                        {inutGrid.slice.length ? (
                          <tfoot>
                            <tr>
                              <td colSpan={5}>Total da página ({inutGrid.slice.length})</td>
                              <td>{formatCurrency(inutGrid.pageTotals.valor_comprovante)}</td>
                              <td />
                            </tr>
                          </tfoot>
                        ) : null}
                      </table>
                    </div>
                  ) : null}
                  {inutItems.length ? (
                    <GridChrome
                      page={inutGrid.page}
                      totalPages={inutGrid.totalPages}
                      total={inutGrid.total}
                      from={inutGrid.range.from}
                      to={inutGrid.range.to}
                      onPrev={inutGrid.onPrev}
                      onNext={inutGrid.onNext}
                      onResetOrder={inutGrid.resetOrder}
                      isDefaultOrder={inutGrid.isDefaultOrder}
                    />
                  ) : null}
                </div>
              ) : null}

              <div className="card col-12">
                <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                  <h2 style={{ margin: 0, color: "var(--accent-copper)" }}>Caixa Agora</h2>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Caixas abertos</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : Number(liveKpis.caixas_abertos || 0)}</div>
                    </div>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Vendas abertas</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : formatCurrency(liveKpis.total_vendas_abertas)}</div>
                    </div>
                    <div className="card" style={{ padding: "8px 14px" }}>
                      <div className="label">Cancelamentos abertos</div>
                      <div style={{ fontSize: 20, fontWeight: 800 }}>{loading ? "..." : formatCurrency(liveKpis.total_cancelamentos_abertos)}</div>
                    </div>
                  </div>
                </div>
                <div style={{ marginBottom: 12 }}>
                  <GridSearchInput value={caixasQ} onChange={setCaixasQ} />
                </div>
                {!loading && !openBoxes.length ? (
                  <EmptyState
                    title="Nenhum turno aberto na janela operacional."
                    detail="Quando houver caixa aberto com atividade recente, ele aparece aqui."
                  />
                ) : null}
                {openBoxes.length ? (
                  <div className="tableScroll">
                    <table className="table compact">
                      <thead>
                        <tr>
                          <SortableTh label="Filial" sortKey="filial_label" ariaSort={caixasGrid.ariaSort("filial_label")} onToggle={caixasGrid.toggleSort} />
                          <SortableTh label="Turno" sortKey="turno_label" ariaSort={caixasGrid.ariaSort("turno_label")} onToggle={caixasGrid.toggleSort} />
                          <SortableTh label="Operador" sortKey="usuario_label" ariaSort={caixasGrid.ariaSort("usuario_label")} onToggle={caixasGrid.toggleSort} />
                          <SortableTh label="Aberto há" sortKey="horas_aberto" ariaSort={caixasGrid.ariaSort("horas_aberto")} onToggle={caixasGrid.toggleSort} />
                          <th>Sem movimento</th>
                          <SortableTh label="Vendas" sortKey="total_vendas" ariaSort={caixasGrid.ariaSort("total_vendas")} onToggle={caixasGrid.toggleSort} align="right" />
                          <SortableTh label="Cancel." sortKey="total_cancelamentos" ariaSort={caixasGrid.ariaSort("total_cancelamentos")} onToggle={caixasGrid.toggleSort} align="right" />
                          <SortableTh label="Receb." sortKey="total_pagamentos" ariaSort={caixasGrid.ariaSort("total_pagamentos")} onToggle={caixasGrid.toggleSort} align="right" />
                        </tr>
                      </thead>
                      <tbody>
                        {caixasGrid.slice.map((item: any) => (
                          <tr key={`${item.id_filial}-${item.id_turno}`}>
                            <td>{item.filial_label}</td>
                            <td>{formatTurnoLabel(item.turno_operacional ?? item.turno_numero, item.turno_label)}</td>
                            <td>{item.usuario_label}</td>
                            <td>{formatHoursLabel(item.horas_aberto)}</td>
                            <td>{formatHoursLabel(item.horas_sem_movimento)}</td>
                            <td>{formatCurrency(item.total_vendas)}</td>
                            <td>{formatCurrency(item.total_cancelamentos)}</td>
                            <td>{formatCurrency(item.total_pagamentos)}</td>
                          </tr>
                        ))}
                      </tbody>
                      {caixasGrid.slice.length ? (
                        <tfoot>
                          <tr>
                            <td colSpan={5}>Total da página ({caixasGrid.slice.length})</td>
                            <td>{formatCurrency(caixasGrid.pageTotals.total_vendas)}</td>
                            <td>{formatCurrency(caixasGrid.pageTotals.total_cancelamentos)}</td>
                            <td>{formatCurrency(caixasGrid.pageTotals.total_pagamentos)}</td>
                          </tr>
                        </tfoot>
                      ) : null}
                    </table>
                  </div>
                ) : null}
                {openBoxes.length ? (
                  <GridChrome
                    page={caixasGrid.page}
                    totalPages={caixasGrid.totalPages}
                    total={caixasGrid.total}
                    from={caixasGrid.range.from}
                    to={caixasGrid.range.to}
                    onPrev={caixasGrid.onPrev}
                    onNext={caixasGrid.onNext}
                    onResetOrder={caixasGrid.resetOrder}
                    isDefaultOrder={caixasGrid.isDefaultOrder}
                  />
                ) : null}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
