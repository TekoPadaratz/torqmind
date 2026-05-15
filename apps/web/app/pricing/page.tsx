'use client';

import { useCallback, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import { apiGet, apiPost, apiPatch } from '../lib/api';
import { formatBusinessCalendarDate } from '../lib/calendar-date.mjs';
import { extractApiError } from '../lib/errors';
import { useScopeQuery, useEnsureScopedProductUrl } from '../lib/scope';
import { buildProductHref, createScopeEpoch } from '../lib/product-scope.mjs';
import { startScopeTransition } from '../lib/scope-runtime';
import { readCachedSession } from '../lib/session';

export const dynamic = 'force-dynamic';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function todayBusinessDate(): string {
  return formatBusinessCalendarDate(new Date()) || new Date().toISOString().slice(0, 10);
}

function fmtPrice(v: string | number | null | undefined, digits = 3): string {
  if (v == null || v === '') return '-';
  return Number(v).toLocaleString('pt-BR', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function scopeParams(scope: { id_empresa: string | null; id_filial: string | null }) {
  const p = new URLSearchParams();
  if (scope.id_empresa) p.set('id_empresa', scope.id_empresa);
  if (scope.id_filial) p.set('id_filial', scope.id_filial);
  return p.toString();
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
type FuelProduct = {
  product_id: number;
  product_name: string;
  fuel_type: string | null;
  grupo_nome: string;
  own_current_price: string | null;
  own_price_source: string | null;
};

type CaptureItem = {
  item_id: string;
  product_id: number;
  product_name: string;
  fuel_type: string | null;
  price: string;
  is_valid: boolean;
  latest_revision_number: number;
  previous_price: string | null;
  created_by_user_name: string;
  last_updated_by_user_name: string | null;
  last_updated_at: string | null;
  change_reason: string | null;
};

type Capture = {
  capture_id: string;
  station_name: string;
  capture_date: string;
  captured_at: string;
  registered_by_user_name: string;
  observation: string | null;
  items: CaptureItem[];
};

type ComparisonRow = {
  product_id: number;
  product_name: string;
  fuel_type: string | null;
  own_current_price: string | null;
  own_price_source: string | null;
  competitor_avg_price: string | null;
  competitor_min_price: string | null;
  competitor_min_station_name: string | null;
  competitor_count: number;
  diff_value: string | null;
  diff_percent: string | null;
  status: string;
};

type Tab = 'register' | 'history' | 'comparison';

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function PricingPage() {
  const scope = useScopeQuery();
  useEnsureScopedProductUrl();

  const [tab, setTab] = useState<Tab>('register');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const clearMessages = () => { setError(''); setSuccess(''); };

  // Detect single scope — hide empresa/filial selectors on mobile
  const session = readCachedSession();
  const accesses: any[] = session?.accesses || [];
  const uniqueEmpresas = new Set(accesses.map((a: any) => a?.id_empresa).filter(Boolean));
  const uniqueFiliais = new Set(accesses.map((a: any) => a?.id_filial).filter(Boolean));
  const isSingleScope = uniqueEmpresas.size <= 1 && uniqueFiliais.size <= 1;

  const tabs: { key: Tab; label: string }[] = [
    { key: 'register', label: 'Registrar Preços' },
    { key: 'history', label: 'Histórico' },
    { key: 'comparison', label: 'Comparação' },
  ];

  return (
    <>
      <AppNav title="Preço Concorrente" hideScopeOnMobile={isSingleScope} />
      <div className="container">
        <h1 className="pageTitle">Preço Concorrente</h1>

        {/* Tabs */}
        <div className="pricingTabs">
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => { setTab(t.key); clearMessages(); }}
              className={`pricingTab ${tab === t.key ? 'pricingTabActive' : ''}`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* Messages */}
        {error && <div className="alertError">{error}</div>}
        {success && <div className="alertSuccess">{success}</div>}

        {/* Tab Content */}
        <div className="card">
          {tab === 'register' && (
            <RegisterTab scope={scope} setError={setError} setSuccess={setSuccess} />
          )}
          {tab === 'history' && (
            <HistoryTab scope={scope} setError={setError} setSuccess={setSuccess} />
          )}
          {tab === 'comparison' && (
            <ComparisonTab scope={scope} setError={setError} />
          )}
        </div>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Tab 1: Register Prices
// ---------------------------------------------------------------------------
function RegisterTab({
  scope,
  setError,
  setSuccess,
}: {
  scope: ReturnType<typeof useScopeQuery>;
  setError: (m: string) => void;
  setSuccess: (m: string) => void;
}) {
  const router = useRouter();
  const [fuels, setFuels] = useState<FuelProduct[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [stationName, setStationName] = useState('');
  const [captureDate, setCaptureDate] = useState(todayBusinessDate());
  const [observation, setObservation] = useState('');
  const [prices, setPrices] = useState<Record<number, string>>({});

  const loadFuels = useCallback(async () => {
    if (!scope.id_empresa || !scope.id_filial) return;
    setLoading(true);
    try {
      const res = await apiGet(`/bi/pricing/competitor/fuels?${scopeParams(scope)}`);
      setFuels(res.data || []);
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao carregar combustíveis'));
    } finally {
      setLoading(false);
    }
  }, [scope.id_empresa, scope.id_filial, setError]);

  useEffect(() => { loadFuels(); }, [loadFuels]);

  const handlePriceChange = (productId: number, value: string) => {
    const cleaned = value.replace(/[^0-9.,]/g, '').replace(',', '.');
    setPrices(prev => ({ ...prev, [productId]: cleaned }));
  };

  const handleSubmit = async () => {
    setError('');
    setSuccess('');

    if (!stationName.trim() || stationName.trim().length < 3) {
      setError('Informe o nome do posto (mínimo 3 caracteres).');
      return;
    }

    const items = Object.entries(prices)
      .filter(([, v]) => v && parseFloat(v) > 0)
      .map(([productId, price]) => ({
        product_id: parseInt(productId, 10),
        price: price,
      }));

    if (items.length === 0) {
      setError('Informe ao menos um preço.');
      return;
    }

    setSaving(true);
    try {
      const res = await apiPost(`/bi/pricing/competitor/captures?${scopeParams(scope)}`, {
        station_name: stationName.trim(),
        capture_date: captureDate,
        observation: observation.trim() || null,
        items,
      });
      setSuccess(`Captura salva com sucesso! ${res.data?.items_saved || items.length} preços registrados.`);
      setStationName('');
      setObservation('');
      setPrices({});
      const nextScope = { ...scope, scope_epoch: createScopeEpoch() };
      startScopeTransition(nextScope, 'pricing_register');
      router.replace(buildProductHref('/pricing', nextScope));
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao salvar captura'));
    } finally {
      setSaving(false);
    }
  };

  if (!scope.id_filial) {
    return <p className="muted">Selecione uma filial para registrar preços.</p>;
  }

  return (
    <div>
      {/* Station Name */}
      <div className="pricingField full" style={{ marginBottom: 16 }}>
        <label className="pricingLabel">Nome do Posto Concorrente</label>
        <input
          type="text"
          value={stationName}
          onChange={e => setStationName(e.target.value)}
          placeholder="Ex: Posto Shell Centro"
          className="input"
          maxLength={200}
        />
      </div>

      {/* Date + Observation */}
      <div className="pricingFormRow">
        <div className="pricingField">
          <label className="pricingLabel">Data</label>
          <input
            type="date"
            value={captureDate}
            onChange={e => setCaptureDate(e.target.value)}
            className="input"
          />
        </div>
        <div className="pricingField">
          <label className="pricingLabel">Observação (opcional)</label>
          <input
            type="text"
            value={observation}
            onChange={e => setObservation(e.target.value)}
            placeholder="Ex: Preços de placa"
            className="input"
            maxLength={500}
          />
        </div>
      </div>

      {/* Fuel Prices Table */}
      {loading ? (
        <p className="muted">Carregando combustíveis...</p>
      ) : fuels.length === 0 ? (
        <p className="muted">Nenhum combustível encontrado para esta filial.</p>
      ) : (
        <>
        <div className="tableScroll pricingDesktopOnly" style={{ marginBottom: 20 }}>
          <table className="table">
            <thead>
              <tr>
                <th>Combustível</th>
                <th style={{ textAlign: 'right' }}>Meu Preço</th>
                <th style={{ textAlign: 'right' }}>Preço Concorrente</th>
              </tr>
            </thead>
            <tbody>
              {fuels.map(fuel => (
                <tr key={fuel.product_id}>
                  <td>
                    <span style={{ fontWeight: 600 }}>{fuel.product_name}</span>
                  </td>
                  <td style={{ textAlign: 'right', color: 'var(--muted)' }}>
                    {fuel.own_current_price
                      ? `R$ ${fmtPrice(fuel.own_current_price)}`
                      : <span style={{ fontSize: 11, opacity: 0.6 }}>Sem preço próprio</span>}
                  </td>
                  <td style={{ textAlign: 'right' }}>
                    <input
                      type="text"
                      inputMode="decimal"
                      value={prices[fuel.product_id] || ''}
                      onChange={e => handlePriceChange(fuel.product_id, e.target.value)}
                      placeholder="0,000"
                      className="inputInline"
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {/* Mobile: vertical fuel cards */}
        <div className="pricingMobileOnly" style={{ marginBottom: 20 }}>
          {fuels.map(fuel => (
            <div key={fuel.product_id} className="fuelCardMobile">
              <div className="fuelCardName">{fuel.product_name}</div>
              <div className="fuelCardOwnPrice">
                {fuel.own_current_price
                  ? `Meu preço: R$ ${fmtPrice(fuel.own_current_price)}`
                  : <span style={{ opacity: 0.5 }}>Meu preço: sem dados</span>}
              </div>
              <div className="fuelCardLabel">Preço Concorrente</div>
              <input
                type="text"
                inputMode="decimal"
                value={prices[fuel.product_id] || ''}
                onChange={e => handlePriceChange(fuel.product_id, e.target.value)}
                placeholder="0,000"
                className="fuelCardInput"
              />
            </div>
          ))}
        </div>
        </>
      )}

      {/* Submit */}
      <button
        onClick={handleSubmit}
        disabled={saving}
        className="btnPrimary"
      >
        {saving ? 'Salvando...' : 'Registrar Preços'}
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 2: History
// ---------------------------------------------------------------------------
function HistoryTab({
  scope,
  setError,
  setSuccess,
}: {
  scope: ReturnType<typeof useScopeQuery>;
  setError: (m: string) => void;
  setSuccess: (m: string) => void;
}) {
  const [captures, setCaptures] = useState<Capture[]>([]);
  const [loading, setLoading] = useState(false);
  const [historyDate, setHistoryDate] = useState(todayBusinessDate());
  const [editingItem, setEditingItem] = useState<string | null>(null);
  const [editPrice, setEditPrice] = useState('');

  const loadHistory = useCallback(async () => {
    if (!scope.id_empresa || !scope.id_filial) return;
    setLoading(true);
    try {
      const params = `${scopeParams(scope)}&capture_date=${historyDate}`;
      const res = await apiGet(`/bi/pricing/competitor/history?${params}`);
      setCaptures(res.data || []);
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao carregar histórico'));
    } finally {
      setLoading(false);
    }
  }, [scope.id_empresa, scope.id_filial, historyDate, setError]);

  useEffect(() => { loadHistory(); }, [loadHistory]);

  const handleEditSave = async (itemId: string) => {
    setError('');
    setSuccess('');
    const cleaned = editPrice.replace(',', '.');
    if (!cleaned || parseFloat(cleaned) <= 0) {
      setError('Preço inválido.');
      return;
    }
    try {
      await apiPatch(`/bi/pricing/competitor/items/${itemId}?${scopeParams(scope)}`, {
        new_price: cleaned,
        change_reason: 'Correção manual',
      });
      setSuccess('Preço atualizado.');
      setEditingItem(null);
      setEditPrice('');
      loadHistory();
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao atualizar preço'));
    }
  };

  if (!scope.id_filial) {
    return <p className="muted">Selecione uma filial.</p>;
  }

  return (
    <div>
      {/* Date picker */}
      <div className="pricingField" style={{ marginBottom: 16, maxWidth: 220 }}>
        <label className="pricingLabel">Data</label>
        <input
          type="date"
          value={historyDate}
          onChange={e => setHistoryDate(e.target.value)}
          className="input"
        />
      </div>

      {loading ? (
        <p className="muted">Carregando...</p>
      ) : captures.length === 0 ? (
        <p className="muted">Nenhuma captura encontrada para esta data.</p>
      ) : (
        <div>
          {captures.map(cap => (
            <div key={cap.capture_id} className="historyCapture">
              <div className="captureHeader">
                <span className="captureStation">{cap.station_name}</span>
                <span className="captureMeta">
                  {cap.captured_at ? new Date(cap.captured_at).toLocaleString('pt-BR') : ''}
                </span>
                <span className="captureMeta">por {cap.registered_by_user_name}</span>
              </div>
              {cap.observation && (
                <p className="captureObs">{cap.observation}</p>
              )}

              {/* Items — Desktop */}
              <div className="pricingDesktopOnly">
              <table className="table compact">
                <thead>
                  <tr>
                    <th>Produto</th>
                    <th style={{ textAlign: 'right' }}>Preço</th>
                    <th>Registrado por</th>
                    <th>Última alteração</th>
                    <th style={{ textAlign: 'center', width: 100 }}>Ação</th>
                  </tr>
                </thead>
                <tbody>
                  {cap.items.map(item => (
                    <tr key={item.item_id}>
                      <td>
                        <span>{item.product_name}</span>
                        {item.previous_price && (
                          <span className="muted" style={{ marginLeft: 6, fontSize: 11 }}>
                            (anterior: R$ {fmtPrice(item.previous_price)})
                          </span>
                        )}
                      </td>
                      <td style={{ textAlign: 'right' }}>
                        {editingItem === item.item_id ? (
                          <input
                            type="text"
                            inputMode="decimal"
                            value={editPrice}
                            onChange={e => setEditPrice(e.target.value.replace(/[^0-9.,]/g, '').replace(',', '.'))}
                            className="inputInline"
                            style={{ width: 90 }}
                            autoFocus
                          />
                        ) : (
                          <span style={{ fontWeight: 600 }}>R$ {fmtPrice(item.price)}</span>
                        )}
                      </td>
                      <td style={{ color: 'var(--muted)', fontSize: 12 }}>
                        {item.created_by_user_name}
                      </td>
                      <td style={{ fontSize: 12 }}>
                        {item.last_updated_by_user_name ? (
                          <span>
                            <span style={{ fontWeight: 500 }}>{item.last_updated_by_user_name}</span>
                            {item.last_updated_at && (
                              <span className="muted" style={{ marginLeft: 4 }}>
                                {new Date(item.last_updated_at).toLocaleString('pt-BR')}
                              </span>
                            )}
                            {item.change_reason && (
                              <span className="muted" style={{ display: 'block', fontSize: 11 }}>
                                {item.change_reason}
                              </span>
                            )}
                          </span>
                        ) : (
                          <span className="muted">-</span>
                        )}
                      </td>
                      <td style={{ textAlign: 'center' }}>
                        {editingItem === item.item_id ? (
                          <span style={{ display: 'inline-flex', gap: 4 }}>
                            <button
                              onClick={() => handleEditSave(item.item_id)}
                              className="btnLink btnLinkGood"
                            >
                              Salvar
                            </button>
                            <button
                              onClick={() => { setEditingItem(null); setEditPrice(''); }}
                              className="btnLink btnLinkMuted"
                            >
                              Cancelar
                            </button>
                          </span>
                        ) : (
                          <button
                            onClick={() => { setEditingItem(item.item_id); setEditPrice(item.price); }}
                            className="btnLink btnLinkAccent"
                          >
                            Editar
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              </div>
              {/* Items — Mobile */}
              <div className="pricingMobileOnly">
                {cap.items.map(item => (
                  <div key={item.item_id} className="histItemMobile">
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 14 }}>{item.product_name}</div>
                      {editingItem === item.item_id ? (
                        <input
                          type="text"
                          inputMode="decimal"
                          value={editPrice}
                          onChange={e => setEditPrice(e.target.value.replace(/[^0-9.,]/g, '').replace(',', '.'))}
                          className="fuelCardInput"
                          style={{ width: 120, minHeight: 40, fontSize: 14, marginTop: 4 }}
                          autoFocus
                        />
                      ) : (
                        <span style={{ fontWeight: 600, fontSize: 15 }}>R$ {fmtPrice(item.price)}</span>
                      )}
                    </div>
                    <div>
                      {editingItem === item.item_id ? (
                        <span style={{ display: 'flex', gap: 8 }}>
                          <button onClick={() => handleEditSave(item.item_id)} className="btnLink btnLinkGood">Salvar</button>
                          <button onClick={() => { setEditingItem(null); setEditPrice(''); }} className="btnLink btnLinkMuted">Cancelar</button>
                        </span>
                      ) : (
                        <button
                          onClick={() => { setEditingItem(item.item_id); setEditPrice(item.price); }}
                          className="btnLink btnLinkAccent"
                          style={{ minHeight: 44, padding: '8px 12px' }}
                        >
                          Editar
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 3: Comparison
// ---------------------------------------------------------------------------
function ComparisonTab({
  scope,
  setError,
}: {
  scope: ReturnType<typeof useScopeQuery>;
  setError: (m: string) => void;
}) {
  const [rows, setRows] = useState<ComparisonRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [compDate, setCompDate] = useState(todayBusinessDate());

  const loadComparison = useCallback(async () => {
    if (!scope.id_empresa || !scope.id_filial) return;
    setLoading(true);
    try {
      const params = `${scopeParams(scope)}&capture_date=${compDate}`;
      const res = await apiGet(`/bi/pricing/competitor/comparison?${params}`);
      setRows(res.data || []);
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao carregar comparativo'));
    } finally {
      setLoading(false);
    }
  }, [scope.id_empresa, scope.id_filial, compDate, setError]);

  useEffect(() => { loadComparison(); }, [loadComparison]);

  const statusInfo = (s: string) => {
    switch (s) {
      case 'MEU_POSTO_MAIS_BARATO': return { text: 'Mais barato', cls: 'statusGood' };
      case 'MEU_POSTO_MAIS_CARO': return { text: 'Mais caro', cls: 'statusBad' };
      case 'IGUAL_AO_MENOR': return { text: 'Igual', cls: 'statusNeutral' };
      case 'SEM_CONCORRENTE': return { text: 'Sem dados', cls: 'statusMuted' };
      case 'SEM_PRECO_PROPRIO': return { text: 'Sem preço próprio', cls: 'statusWarn' };
      default: return { text: s, cls: 'statusMuted' };
    }
  };

  if (!scope.id_filial) {
    return <p className="muted">Selecione uma filial.</p>;
  }

  return (
    <div>
      {/* Date picker */}
      <div className="pricingField" style={{ marginBottom: 16, maxWidth: 220 }}>
        <label className="pricingLabel">Data de referência</label>
        <input
          type="date"
          value={compDate}
          onChange={e => setCompDate(e.target.value)}
          className="input"
        />
      </div>

      {loading ? (
        <p className="muted">Carregando comparativo...</p>
      ) : rows.length === 0 ? (
        <p className="muted">Nenhum dado de comparação encontrado.</p>
      ) : (
        <>
        <div className="tableScroll pricingDesktopOnly">
          <table className="table">
            <thead>
              <tr>
                <th>Combustível</th>
                <th style={{ textAlign: 'right' }}>Meu Preço</th>
                <th style={{ textAlign: 'right' }}>Menor Concorrente</th>
                <th>Posto</th>
                <th style={{ textAlign: 'right' }}>Média</th>
                <th style={{ textAlign: 'right' }}>Diferença</th>
                <th style={{ textAlign: 'center' }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(row => {
                const st = statusInfo(row.status);
                return (
                  <tr key={row.product_id}>
                    <td>
                      <span style={{ fontWeight: 600 }}>{row.product_name}</span>
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {row.own_current_price ? `R$ ${fmtPrice(row.own_current_price)}` : <span style={{ fontSize: 11, opacity: 0.6 }}>Sem preço</span>}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {row.competitor_min_price ? `R$ ${fmtPrice(row.competitor_min_price)}` : '-'}
                    </td>
                    <td style={{ color: 'var(--muted)', fontSize: 12 }}>
                      {row.competitor_min_station_name || '-'}
                    </td>
                    <td style={{ textAlign: 'right', color: 'var(--muted)' }}>
                      {row.competitor_avg_price ? `R$ ${fmtPrice(row.competitor_avg_price)}` : '-'}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {row.diff_value != null ? (
                        <span className={Number(row.diff_value) > 0 ? 'statusBad' : 'statusGood'}>
                          {Number(row.diff_value) > 0 ? '+' : ''}R$ {fmtPrice(row.diff_value)}
                          {row.diff_percent != null && (
                            <span style={{ fontSize: 11, marginLeft: 4 }}>
                              ({Number(row.diff_percent) > 0 ? '+' : ''}{fmtPrice(row.diff_percent, 1)}%)
                            </span>
                          )}
                        </span>
                      ) : '-'}
                    </td>
                    <td style={{ textAlign: 'center' }}>
                      <span className={st.cls} style={{ fontSize: 12, fontWeight: 600 }}>
                        {st.text}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {/* Mobile comparison cards */}
        <div className="pricingMobileOnly">
          {rows.map(row => {
            const st = statusInfo(row.status);
            return (
              <div key={row.product_id} className="compCardMobile">
                <div className="fuelCardName">{row.product_name}</div>
                <div className="compCardRow">
                  <span>Meu preço</span>
                  <span style={{ fontWeight: 600, color: 'var(--text)' }}>
                    {row.own_current_price ? `R$ ${fmtPrice(row.own_current_price)}` : 'Sem preço'}
                  </span>
                </div>
                <div className="compCardRow">
                  <span>Menor concorrente</span>
                  <span style={{ fontWeight: 600, color: 'var(--text)' }}>
                    {row.competitor_min_price ? `R$ ${fmtPrice(row.competitor_min_price)}` : '-'}
                  </span>
                </div>
                {row.competitor_min_station_name && (
                  <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>
                    {row.competitor_min_station_name}
                  </div>
                )}
                <div className="compCardRow" style={{ marginTop: 6 }}>
                  <span>Diferença</span>
                  <span>
                    {row.diff_value != null ? (
                      <span className={Number(row.diff_value) > 0 ? 'statusBad' : 'statusGood'} style={{ fontWeight: 600 }}>
                        {Number(row.diff_value) > 0 ? '+' : ''}R$ {fmtPrice(row.diff_value)}
                        {row.diff_percent != null && (
                          <span style={{ fontSize: 11, marginLeft: 4 }}>
                            ({Number(row.diff_percent) > 0 ? '+' : ''}{fmtPrice(row.diff_percent, 1)}%)
                          </span>
                        )}
                      </span>
                    ) : '-'}
                  </span>
                </div>
                <div style={{ marginTop: 6, textAlign: 'right' }}>
                  <span className={st.cls} style={{ fontSize: 12, fontWeight: 600 }}>
                    {st.text}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
        </>
      )}
    </div>
  );
}
