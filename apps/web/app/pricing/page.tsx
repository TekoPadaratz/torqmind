'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import ReadingStatusBanner from '../components/ui/ReadingStatusBanner';
import ScopeTransitionState from '../components/ui/ScopeTransitionState';
import { apiGet, apiPost } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildUserLabel, formatCurrency, formatDateOnly, formatFilialLabel } from '../lib/format';
import { buildProductHref, createScopeEpoch } from '../lib/product-scope.mjs';
import { resolvePricingOverviewRequest } from '../lib/pricing-request.mjs';
import { buildModuleLoadingCopy, buildModuleUnavailableCopy } from '../lib/reading-state.mjs';
import { describeCacheBanner } from '../lib/reading-copy.mjs';
import { startScopeTransition } from '../lib/scope-runtime';
import { useScopeQuery } from '../lib/scope';
import { useBiScopeData } from '../lib/use-bi-scope-data';

export const dynamic = 'force-dynamic';

function fmtNum(v: any, digits = 3) {
  return Number(v || 0).toLocaleString('pt-BR', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export default function PricingPage() {
  const router = useRouter();
  const scope = useScopeQuery();
  const { claims, data, error: loadError, loading, pendingUnavailable } = useBiScopeData<any>({
    moduleKey: 'pricing_competitor_overview',
    scope,
    errorMessage: 'Falha ao carregar painel de concorrência',
    buildRequestUrl: (currentScope, session) => resolvePricingOverviewRequest(currentScope, session).requestUrl,
  });
  const [saving, setSaving] = useState(false);
  const [actionError, setActionError] = useState('');
  const [saveMsg, setSaveMsg] = useState('');
  const [priceInputs, setPriceInputs] = useState<Record<string, string>>({});
  const [filialLabel, setFilialLabel] = useState('');

  const userLabel = useMemo(() => {
    return buildUserLabel(claims);
  }, [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy('preço da concorrência')
    : buildModuleLoadingCopy('preço da concorrência');
  const pricingScope = useMemo(
    () => resolvePricingOverviewRequest(scope, claims),
    [claims, scope],
  );
  const error = actionError || loadError || (!loading ? pricingScope.error || '' : '');

  useEffect(() => {
    setActionError('');
    setSaveMsg('');
    setFilialLabel('');
    setPriceInputs({});
  }, [scope.scope_key, scope.scope_epoch]);

  const fuelItems = useMemo(() => {
    return (data?.items || []).filter((row: any) => Boolean(row?.familia_combustivel));
  }, [data]);

  useEffect(() => {
    if (!data) return;

    const map: Record<string, string> = {};
    for (const row of fuelItems) {
      const p = Number(row?.competitor_price || 0);
      map[String(row.id_produto)] = fmtNum(p, 3);
    }
    setPriceInputs(map);
  }, [data, fuelItems]);

  useEffect(() => {
    if (!data || pricingScope.error || !pricingScope.branchId) {
      setFilialLabel('');
      return;
    }

    let active = true;
    const loadBranchLabel = async () => {
      try {
        const branchList = await apiGet(`/bi/filiais${pricingScope.companyId ? `?id_empresa=${pricingScope.companyId}` : ''}`);
        if (!active) return;
        const selected = (branchList?.items || []).find((item: any) => String(item.id_filial) === String(pricingScope.branchId));
        setFilialLabel(formatFilialLabel(pricingScope.branchId, selected?.nome));
      } catch {
        if (active) setFilialLabel(formatFilialLabel(pricingScope.branchId));
      }
    };

    void loadBranchLabel();
    return () => {
      active = false;
    };
  }, [data, pricingScope.branchId, pricingScope.companyId, pricingScope.error]);

  const onSavePrices = async () => {
    if (!items.length) return;
    setSaving(true);
    setSaveMsg('');
    setActionError('');
    try {
      if (pricingScope.error || !pricingScope.branchId) throw new Error(pricingScope.error || 'Selecione uma filial no escopo.');

      const payloadItems: any[] = [];
      for (const row of items) {
        const raw = String(priceInputs[String(row.id_produto)] || '').replace(',', '.').trim();
        if (!raw) continue;
        const price = Number(raw);
        if (!Number.isFinite(price) || price <= 0) continue;
        payloadItems.push({ id_produto: Number(row.id_produto), competitor_price: price });
      }

      const qs = new URLSearchParams({ id_filial: String(pricingScope.branchId) });
      if (pricingScope.companyId) qs.set('id_empresa', String(pricingScope.companyId));

      const res = await apiPost(`/bi/pricing/competitor/prices?${qs.toString()}`, { items: payloadItems });
      setSaveMsg(`Preços salvos: ${Number(res?.saved || 0)} combustível(is). Atualizando cenário...`);

      const nextScope = {
        ...scope,
        id_empresa: pricingScope.companyId || scope.id_empresa,
        id_filial: pricingScope.branchId,
        id_filiais: [pricingScope.branchId],
        scope_epoch: createScopeEpoch(),
      };
      startScopeTransition(nextScope, 'pricing_competitor_overview');
      router.replace(buildProductHref('/pricing', nextScope));
    } catch (err: any) {
      setActionError(extractApiError(err, 'Falha ao salvar preços da concorrência'));
    } finally {
      setSaving(false);
    }
  };

  const summary = data?.summary || {};
  const items = fuelItems;

  return (
    <div>
      <AppNav title="Preço da Concorrência" userLabel={userLabel} />
      <div className="container">
        <div className="card toolbar">
          <div>
            <div className="muted">Simulação 10 dias</div>
            <div className="scopeLine">
              Período base: <strong>{formatDateOnly(scope.dt_ini)}</strong> até <strong>{formatDateOnly(scope.dt_fim)}</strong> · Filial{' '}
              <strong>{filialLabel || formatFilialLabel(scope.id_filial || claims?.id_filial)}</strong>
            </div>
          </div>
          <div>
            <button className="btn" onClick={onSavePrices} disabled={saving || loading}>
              {saving ? 'Salvando...' : 'Salvar preços da concorrência'}
            </button>
          </div>
        </div>

        {saveMsg ? <div className="card" style={{ marginTop: 12 }}>{saveMsg}</div> : null}
        {error ? <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div> : null}
        {!data && !error ? (
          <div style={{ marginTop: 12 }}>
            <ScopeTransitionState
              mode={pendingUnavailable ? 'unavailable' : 'loading'}
              headline={transitionCopy.headline}
              detail={transitionCopy.detail}
              metrics={4}
              panels={2}
            />
          </div>
        ) : data ? (
          <>
            <ReadingStatusBanner message={describeCacheBanner(data?._snapshot_cache, 'preço da concorrência')} />

          <div className="bi-grid" style={{ marginTop: 12 }}>
              <div className="card kpi col-3">
                <div className="label">Tipos de combustível</div>
                <div className="value">{loading ? '...' : Number(summary.fuel_types || 0)}</div>
              </div>
              <div className="card kpi col-3 riskCard">
                <div className="label">Perda se não mudar (10d)</div>
                <div className="value">{loading ? '...' : formatCurrency(summary.total_lost_if_no_change_10d)}</div>
              </div>
              <div className="card kpi col-3">
                <div className="label">Impacto ao igualar (10d vs atual)</div>
                <div className="value">{loading ? '...' : formatCurrency(summary.total_match_vs_current_10d)}</div>
              </div>
              <div className="card kpi col-3 scoreCard">
                <div className="label">Ganho vs não mudar (10d)</div>
                <div className="value">{loading ? '...' : formatCurrency(summary.total_match_vs_no_change_10d)}</div>
              </div>

              <div className="card col-12">
                <h2>Preço por combustível: posto x concorrência</h2>
                {!loading && !items.length ? (
                  <EmptyState
                    title="Nenhum combustível elegível neste recorte."
                    detail="Selecione uma filial com movimento em combustíveis para calcular o impacto competitivo."
                  />
                ) : null}
                <table className="table compact">
                  <thead>
                    <tr>
                      <th>Combustível</th>
                      <th>Vol. médio/dia</th>
                      <th>Preço médio posto</th>
                      <th>Preço concorrência</th>
                      <th>Gap (posto - concorr.)</th>
                      <th>Perda se não mudar (10d)</th>
                      <th>Impacto igualar vs atual (10d)</th>
                      <th>Impacto igualar vs. não mudar (10d)</th>
                      <th>Recomendação</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((row: any) => (
                      <tr key={row.id_produto}>
                        <td>
                          <div><strong>{row.produto_nome}</strong></div>
                          <div className="muted">{row.familia_combustivel} · {row.grupo_nome}</div>
                        </td>
                        <td>{fmtNum(row.avg_daily_volume, 3)}</td>
                        <td>{formatCurrency(row.avg_price_current)}</td>
                        <td>
                          <input
                            className="input"
                            style={{ minWidth: 120 }}
                            inputMode="decimal"
                            placeholder="0,000"
                            value={priceInputs[String(row.id_produto)] || '0,000'}
                            onChange={(e) =>
                              setPriceInputs((prev) => ({
                                ...prev,
                                [String(row.id_produto)]: e.target.value,
                              }))
                            }
                          />
                        </td>
                        <td>{formatCurrency(row.station_price_gap)}</td>
                        <td>{formatCurrency(row.scenario_no_change?.lost_revenue_10d)}</td>
                        <td>{formatCurrency(row.scenario_match_competitor?.impact_vs_current_10d)}</td>
                        <td>{formatCurrency(row.scenario_match_competitor?.impact_vs_no_change_10d)}</td>
                        <td>{row.recommendation}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : null}
      </div>
    </div>
  );
}
