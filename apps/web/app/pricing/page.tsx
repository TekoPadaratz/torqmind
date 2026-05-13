'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import EmptyState from '../components/ui/EmptyState';
import ScopeTransitionState from '../components/ui/ScopeTransitionState';
import { apiGet, apiPost, apiPut, apiDelete } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildUserLabel, formatCurrency, formatDateOnly, formatFilialLabel } from '../lib/format';
import { buildProductHref, createScopeEpoch } from '../lib/product-scope.mjs';
import { resolvePricingOverviewRequest } from '../lib/pricing-request.mjs';
import { buildModuleLoadingCopy, buildModuleUnavailableCopy } from '../lib/reading-state.mjs';
import { startScopeTransition } from '../lib/scope-runtime';
import { useEnsureScopedProductUrl, useScopeQuery } from '../lib/scope';
import { useBiScopeData } from '../lib/use-bi-scope-data';

export const dynamic = 'force-dynamic';

function fmtNum(v: any, digits = 3) {
  return Number(v || 0).toLocaleString('pt-BR', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

type Station = { id: string; station_name: string; is_active?: boolean; city?: string; state?: string };
type FuelProduct = { id_produto: number; nome: string; grupo_nome: string; fuel_type: string; unidade: string; custo_medio: number };
type Capture = { id: string; station_id: string; station_name: string; capture_date: string; registered_by: string; registered_at: string; item_count: number };

export default function PricingPage() {
  const router = useRouter();
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();
  const { claims, data, error: loadError, loading, pendingUnavailable } = useBiScopeData<any>({
    moduleKey: 'pricing_competitor_overview',
    scope,
    errorMessage: 'Falha ao carregar painel de concorr\u00eancia',
    buildRequestUrl: (currentScope, session) => resolvePricingOverviewRequest(currentScope, session).requestUrl,
  });

  const [tab, setTab] = useState<'simulation' | 'stations' | 'capture' | 'history'>('stations');
  const [actionError, setActionError] = useState('');
  const [actionMsg, setActionMsg] = useState('');
  const [stations, setStations] = useState<Station[]>([]);
  const [stationsLoading, setStationsLoading] = useState(false);
  const [showNewStation, setShowNewStation] = useState(false);
  const [newStationName, setNewStationName] = useState('');
  const [newStationCity, setNewStationCity] = useState('');
  const [newStationState, setNewStationState] = useState('');
  const [fuelProducts, setFuelProducts] = useState<FuelProduct[]>([]);
  const [selectedStationId, setSelectedStationId] = useState('');
  const [captureDate, setCaptureDate] = useState(todayISO());
  const [captureInputs, setCaptureInputs] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [captures, setCaptures] = useState<Capture[]>([]);
  const [capturesLoading, setCapturesLoading] = useState(false);
  const [priceInputs, setPriceInputs] = useState<Record<string, string>>({});
  const [filialLabel, setFilialLabel] = useState('');

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);
  const transitionCopy = pendingUnavailable
    ? buildModuleUnavailableCopy('pre\u00e7o da concorr\u00eancia')
    : buildModuleLoadingCopy('pre\u00e7o da concorr\u00eancia');
  const pricingScope = useMemo(() => resolvePricingOverviewRequest(scope, claims), [claims, scope]);
  const error = actionError || loadError || (!loading ? pricingScope.error || '' : '');

  const qs = useMemo(() => {
    const p = new URLSearchParams();
    if (pricingScope.branchId) p.set('id_filial', String(pricingScope.branchId));
    if (pricingScope.companyId) p.set('id_empresa', String(pricingScope.companyId));
    return p.toString();
  }, [pricingScope.branchId, pricingScope.companyId]);

  useEffect(() => { setActionError(''); setActionMsg(''); }, [scope.scope_key, scope.scope_epoch]);

  useEffect(() => {
    if (!pricingScope.branchId) { setFilialLabel(''); return; }
    let active = true;
    (async () => {
      try {
        const url = `/bi/filiais${pricingScope.companyId ? `?id_empresa=${pricingScope.companyId}` : ''}`;
        const branchList = await apiGet(url);
        if (!active) return;
        const selected = (branchList?.items || []).find((item: any) => String(item.id_filial) === String(pricingScope.branchId));
        setFilialLabel(formatFilialLabel(pricingScope.branchId, selected?.nome));
      } catch { if (active) setFilialLabel(formatFilialLabel(pricingScope.branchId)); }
    })();
    return () => { active = false; };
  }, [pricingScope.branchId, pricingScope.companyId]);

  const loadStations = useCallback(async () => {
    if (!qs) return;
    setStationsLoading(true);
    try { const res = await apiGet(`/bi/pricing/competitor/stations?${qs}`); setStations(res?.items || []); }
    catch (e: any) { setActionError(extractApiError(e, 'Falha ao carregar postos')); }
    finally { setStationsLoading(false); }
  }, [qs]);

  const loadFuelProducts = useCallback(async () => {
    if (!qs) return;
    try { const res = await apiGet(`/bi/pricing/competitor/fuel-products?${qs}`); setFuelProducts(res?.items || []); }
    catch { /* silent */ }
  }, [qs]);

  const loadCaptures = useCallback(async () => {
    if (!qs) return;
    setCapturesLoading(true);
    try {
      const res = await apiGet(`/bi/pricing/competitor/captures?${qs}&dt_ini=${scope.dt_ini}&dt_fim=${scope.dt_fim}`);
      setCaptures(res?.items || []);
    } catch (e: any) { setActionError(extractApiError(e, 'Falha ao carregar hist\u00f3rico')); }
    finally { setCapturesLoading(false); }
  }, [qs, scope.dt_ini, scope.dt_fim]);

  useEffect(() => {
    if (!pricingScope.branchId || pricingScope.error) return;
    void loadStations(); void loadFuelProducts(); void loadCaptures();
  }, [pricingScope.branchId, pricingScope.error, loadStations, loadFuelProducts, loadCaptures]);

  const fuelItems = useMemo(() => (data?.items || []).filter((row: any) => Boolean(row?.familia_combustivel)), [data]);
  useEffect(() => {
    if (!data) return;
    const map: Record<string, string> = {};
    for (const row of fuelItems) { map[String(row.id_produto)] = fmtNum(Number(row?.competitor_price || 0), 3); }
    setPriceInputs(map);
  }, [data, fuelItems]);

  const handleCreateStation = async () => {
    if (!newStationName.trim()) return;
    setSaving(true); setActionError('');
    try {
      await apiPost(`/bi/pricing/competitor/stations?${qs}`, {
        station_name: newStationName.trim(),
        city: newStationCity.trim() || undefined,
        state: newStationState.trim() || undefined,
      });
      setActionMsg('Posto criado com sucesso.');
      setShowNewStation(false); setNewStationName(''); setNewStationCity(''); setNewStationState('');
      await loadStations();
    } catch (e: any) { setActionError(extractApiError(e, 'Falha ao criar posto')); }
    finally { setSaving(false); }
  };

  const handleDeleteStation = async (stationId: string) => {
    if (!confirm('Remover este posto concorrente?')) return;
    try {
      await apiDelete(`/bi/pricing/competitor/stations/${stationId}?${qs}`);
      setActionMsg('Posto removido.');
      await loadStations();
    } catch (e: any) { setActionError(extractApiError(e, 'Falha ao remover posto')); }
  };

  const handleSaveCapture = async () => {
    if (!selectedStationId) { setActionError('Selecione um posto concorrente.'); return; }
    const items: any[] = [];
    for (const fp of fuelProducts) {
      const raw = String(captureInputs[String(fp.id_produto)] || '').replace(',', '.').trim();
      if (!raw) continue;
      const price = Number(raw);
      if (!Number.isFinite(price) || price <= 0) continue;
      items.push({ id_produto: fp.id_produto, price, product_name: fp.nome, fuel_type: fp.fuel_type });
    }
    if (!items.length) { setActionError('Informe pelo menos um pre\u00e7o.'); return; }
    setSaving(true); setActionError('');
    try {
      const res = await apiPost(`/bi/pricing/competitor/captures?${qs}`, {
        station_id: selectedStationId,
        capture_date: captureDate,
        items,
      });
      setActionMsg(`Pre\u00e7os registrados: ${res?.created || 0} novo(s), ${res?.updated || 0} atualizado(s).`);
      setCaptureInputs({}); await loadCaptures();
    } catch (e: any) { setActionError(extractApiError(e, 'Falha ao salvar captura')); }
    finally { setSaving(false); }
  };

  const handleLegacySave = async () => {
    if (!fuelItems.length) return;
    setSaving(true); setActionError('');
    try {
      if (pricingScope.error || !pricingScope.branchId) throw new Error(pricingScope.error || 'Selecione uma filial.');
      const payloadItems: any[] = [];
      for (const row of fuelItems) {
        const raw = String(priceInputs[String(row.id_produto)] || '').replace(',', '.').trim();
        if (!raw) continue;
        const price = Number(raw);
        if (!Number.isFinite(price) || price <= 0) continue;
        payloadItems.push({ id_produto: Number(row.id_produto), competitor_price: price });
      }
      const saveQs = new URLSearchParams({ id_filial: String(pricingScope.branchId) });
      if (pricingScope.companyId) saveQs.set('id_empresa', String(pricingScope.companyId));
      await apiPost(`/bi/pricing/competitor/prices?${saveQs.toString()}`, { items: payloadItems });
      setActionMsg('Pre\u00e7os salvos. Atualizando cen\u00e1rio...');
      const nextScope = {
        ...scope,
        id_empresa: pricingScope.companyId || scope.id_empresa,
        id_filial: pricingScope.branchId,
        id_filiais: [pricingScope.branchId],
        scope_epoch: createScopeEpoch(),
      };
      startScopeTransition(nextScope, 'pricing_competitor_overview');
      router.replace(buildProductHref('/pricing', nextScope));
    } catch (e: any) { setActionError(extractApiError(e, 'Falha ao salvar pre\u00e7os')); }
    finally { setSaving(false); }
  };

  const summary = data?.summary || {};

  return (
    <div>
      <AppNav title="Pre\u00e7o da Concorr\u00eancia" userLabel={userLabel} />
      <div className="container">
        <div className="card toolbar">
          <div>
            <div className="scopeLine">
              Filial <strong>{filialLabel || formatFilialLabel(scope.id_filial || claims?.id_filial)}</strong>
              {' \u00b7 '}Per\u00edodo: <strong>{formatDateOnly(scope.dt_ini)}</strong> a <strong>{formatDateOnly(scope.dt_fim)}</strong>
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
          {(['stations', 'capture', 'history', 'simulation'] as const).map(t => (
            <button key={t} className={`btn ${tab === t ? '' : 'btn-outline'}`} onClick={() => { setTab(t); setActionError(''); setActionMsg(''); }}>
              {t === 'stations' ? 'Postos' : t === 'capture' ? 'Registrar Pre\u00e7os' : t === 'history' ? 'Hist\u00f3rico' : 'Simula\u00e7\u00e3o'}
            </button>
          ))}
        </div>

        {actionMsg ? <div className="card" style={{ marginTop: 12, background: '#e8f5e9' }}>{actionMsg}</div> : null}
        {error ? <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div> : null}

        {tab === 'stations' && (
          <div style={{ marginTop: 12 }}>
            <div className="card">
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                <h2 style={{ margin: 0 }}>Postos Concorrentes</h2>
                <button className="btn" onClick={() => setShowNewStation(!showNewStation)}>
                  {showNewStation ? 'Cancelar' : '+ Novo Posto'}
                </button>
              </div>
              {showNewStation && (
                <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
                  <input className="input" placeholder="Nome do posto *" value={newStationName}
                    onChange={e => setNewStationName(e.target.value)} style={{ minWidth: 200 }} />
                  <input className="input" placeholder="Cidade" value={newStationCity}
                    onChange={e => setNewStationCity(e.target.value)} style={{ minWidth: 140 }} />
                  <input className="input" placeholder="UF" value={newStationState}
                    onChange={e => setNewStationState(e.target.value)} style={{ maxWidth: 60 }} />
                  <button className="btn" onClick={handleCreateStation} disabled={saving || !newStationName.trim()}>
                    {saving ? 'Salvando...' : 'Criar'}
                  </button>
                </div>
              )}
              {stationsLoading ? <div className="muted">Carregando postos...</div> : !stations.length ? (
                <EmptyState title="Nenhum posto concorrente cadastrado."
                  detail="Clique em '+ Novo Posto' para adicionar o primeiro." />
              ) : (
                <table className="table compact">
                  <thead><tr><th>Nome</th><th>Cidade</th><th>UF</th><th>Status</th><th></th></tr></thead>
                  <tbody>
                    {stations.map(s => (
                      <tr key={s.id}>
                        <td><strong>{s.station_name}</strong></td>
                        <td>{s.city || '-'}</td>
                        <td>{s.state || '-'}</td>
                        <td>{s.is_active !== false
                          ? <span style={{ color: '#2e7d32' }}>Ativo</span>
                          : <span className="muted">Inativo</span>}
                        </td>
                        <td>
                          <button className="btn btn-outline btn-sm"
                            onClick={() => handleDeleteStation(s.id)}>Remover</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {tab === 'capture' && (
          <div style={{ marginTop: 12 }}>
            <div className="card">
              <h2>Registrar Pre\u00e7os do Concorrente</h2>
              <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap', alignItems: 'end' }}>
                <div>
                  <div className="muted" style={{ marginBottom: 4 }}>Posto concorrente</div>
                  <select className="input" value={selectedStationId}
                    onChange={e => setSelectedStationId(e.target.value)} style={{ minWidth: 200 }}>
                    <option value="">Selecione...</option>
                    {stations.filter(s => s.is_active !== false).map(s => (
                      <option key={s.id} value={s.id}>{s.station_name}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <div className="muted" style={{ marginBottom: 4 }}>Data da pesquisa</div>
                  <input className="input" type="date" value={captureDate}
                    onChange={e => setCaptureDate(e.target.value)} />
                </div>
              </div>
              {!fuelProducts.length ? (
                <EmptyState title="Nenhum combust\u00edvel dispon\u00edvel."
                  detail="A filial selecionada n\u00e3o possui produtos de combust\u00edvel cadastrados." />
              ) : (
                <>
                  <table className="table compact">
                    <thead><tr><th>Combust\u00edvel</th><th>Tipo</th><th>Pre\u00e7o concorrente (R$/L)</th></tr></thead>
                    <tbody>
                      {fuelProducts.map(fp => (
                        <tr key={fp.id_produto}>
                          <td><strong>{fp.nome}</strong><div className="muted">{fp.grupo_nome}</div></td>
                          <td>{fp.fuel_type || '-'}</td>
                          <td>
                            <input className="input" style={{ minWidth: 120 }} inputMode="decimal"
                              placeholder="0,000"
                              value={captureInputs[String(fp.id_produto)] || ''}
                              onChange={e => setCaptureInputs(prev => ({
                                ...prev, [String(fp.id_produto)]: e.target.value
                              }))} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <div style={{ marginTop: 12 }}>
                    <button className="btn" onClick={handleSaveCapture}
                      disabled={saving || !selectedStationId}>
                      {saving ? 'Salvando...' : 'Registrar Pre\u00e7os'}
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        )}

        {tab === 'history' && (
          <div style={{ marginTop: 12 }}>
            <div className="card">
              <h2>Hist\u00f3rico de Capturas</h2>
              {capturesLoading ? <div className="muted">Carregando...</div> : !captures.length ? (
                <EmptyState title="Nenhuma captura registrada no per\u00edodo."
                  detail="Registre pre\u00e7os na aba 'Registrar Pre\u00e7os' para come\u00e7ar a construir o hist\u00f3rico." />
              ) : (
                <table className="table compact">
                  <thead>
                    <tr><th>Data</th><th>Posto</th><th>Produtos</th><th>Registrado por</th><th>Registrado em</th></tr>
                  </thead>
                  <tbody>
                    {captures.map(c => (
                      <tr key={c.id}>
                        <td><strong>{formatDateOnly(c.capture_date)}</strong></td>
                        <td>{c.station_name}</td>
                        <td>{c.item_count}</td>
                        <td>{c.registered_by}</td>
                        <td className="muted">
                          {c.registered_at ? new Date(c.registered_at).toLocaleString('pt-BR') : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {tab === 'simulation' && (
          <div style={{ marginTop: 12 }}>
            {!data && !loadError ? (
              <ScopeTransitionState
                mode={pendingUnavailable ? 'unavailable' : 'loading'}
                headline={transitionCopy.headline}
                detail={transitionCopy.detail}
                metrics={4} panels={2} />
            ) : data ? (
              <div className="bi-grid">
                <div className="card kpi col-3">
                  <div className="label">Tipos de combust\u00edvel</div>
                  <div className="value">{loading ? '...' : Number(summary.fuel_types || 0)}</div>
                </div>
                <div className="card kpi col-3 riskCard">
                  <div className="label">Perda se n\u00e3o mudar (10d)</div>
                  <div className="value">{loading ? '...' : formatCurrency(summary.total_lost_if_no_change_10d)}</div>
                </div>
                <div className="card kpi col-3">
                  <div className="label">Impacto ao igualar (10d vs atual)</div>
                  <div className="value">{loading ? '...' : formatCurrency(summary.total_match_vs_current_10d)}</div>
                </div>
                <div className="card kpi col-3 scoreCard">
                  <div className="label">Ganho vs n\u00e3o mudar (10d)</div>
                  <div className="value">{loading ? '...' : formatCurrency(summary.total_match_vs_no_change_10d)}</div>
                </div>
                <div className="card col-12">
                  <h2>Pre\u00e7o por combust\u00edvel: posto x concorr\u00eancia</h2>
                  {!loading && !fuelItems.length ? (
                    <EmptyState title="Nenhum combust\u00edvel eleg\u00edvel neste per\u00edodo."
                      detail="Selecione uma filial com movimento em combust\u00edveis." />
                  ) : null}
                  <table className="table compact">
                    <thead>
                      <tr>
                        <th>Combust\u00edvel</th><th>Vol. m\u00e9dio/dia</th><th>Pre\u00e7o posto</th>
                        <th>Pre\u00e7o concorr\u00eancia</th><th>Gap</th><th>Perda (10d)</th>
                        <th>Impacto igualar</th><th>Recomenda\u00e7\u00e3o</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fuelItems.map((row: any) => (
                        <tr key={row.id_produto}>
                          <td>
                            <div><strong>{row.produto_nome}</strong></div>
                            <div className="muted">{row.familia_combustivel} \u00b7 {row.grupo_nome}</div>
                          </td>
                          <td>{fmtNum(row.avg_daily_volume, 3)}</td>
                          <td>{formatCurrency(row.avg_price_current)}</td>
                          <td>
                            <input className="input" style={{ minWidth: 120 }} inputMode="decimal"
                              placeholder="0,000"
                              value={priceInputs[String(row.id_produto)] || '0,000'}
                              onChange={e => setPriceInputs(prev => ({
                                ...prev, [String(row.id_produto)]: e.target.value
                              }))} />
                          </td>
                          <td>{formatCurrency(row.station_price_gap)}</td>
                          <td>{formatCurrency(row.scenario_no_change?.lost_revenue_10d)}</td>
                          <td>{formatCurrency(row.scenario_match_competitor?.impact_vs_no_change_10d)}</td>
                          <td>{row.recommendation}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <div style={{ marginTop: 12 }}>
                    <button className="btn" onClick={handleLegacySave} disabled={saving || loading}>
                      {saving ? 'Salvando...' : 'Salvar pre\u00e7os'}
                    </button>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
