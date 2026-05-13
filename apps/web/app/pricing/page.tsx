'use client';

import { useCallback, useEffect, useState } from 'react';

import AppNav from '../components/AppNav';
import { apiGet, apiPost, apiPatch } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { formatCurrency } from '../lib/format';
import { useScopeQuery, useEnsureScopedProductUrl } from '../lib/scope';

export const dynamic = 'force-dynamic';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function todayISO() {
  return new Date().toISOString().slice(0, 10);
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

  const tabs: { key: Tab; label: string }[] = [
    { key: 'register', label: 'Registrar Precos' },
    { key: 'history', label: 'Historico' },
    { key: 'comparison', label: 'Comparacao' },
  ];

  return (
    <>
      <AppNav title="Preco Concorrente" />
      <div className="max-w-5xl mx-auto px-4 py-6">
        <h1 className="text-2xl font-bold mb-4">Preco Concorrente</h1>

        {/* Tabs */}
        <div className="flex gap-2 mb-6 border-b border-gray-200">
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => { setTab(t.key); clearMessages(); }}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                tab === t.key
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* Messages */}
        {error && (
          <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-700 rounded text-sm">
            {error}
          </div>
        )}
        {success && (
          <div className="mb-4 p-3 bg-green-50 border border-green-200 text-green-700 rounded text-sm">
            {success}
          </div>
        )}

        {/* Tab Content */}
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
  const [fuels, setFuels] = useState<FuelProduct[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [stationName, setStationName] = useState('');
  const [captureDate, setCaptureDate] = useState(todayISO());
  const [observation, setObservation] = useState('');
  const [prices, setPrices] = useState<Record<number, string>>({});

  const loadFuels = useCallback(async () => {
    if (!scope.id_empresa || !scope.id_filial) return;
    setLoading(true);
    try {
      const res = await apiGet(`/bi/pricing/competitor/fuels?${scopeParams(scope)}`);
      setFuels(res.data || []);
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao carregar combustiveis'));
    } finally {
      setLoading(false);
    }
  }, [scope.id_empresa, scope.id_filial, setError]);

  useEffect(() => { loadFuels(); }, [loadFuels]);

  const handlePriceChange = (productId: number, value: string) => {
    // Allow only valid decimal input
    const cleaned = value.replace(/[^0-9.,]/g, '').replace(',', '.');
    setPrices(prev => ({ ...prev, [productId]: cleaned }));
  };

  const handleSubmit = async () => {
    setError('');
    setSuccess('');

    if (!stationName.trim() || stationName.trim().length < 3) {
      setError('Informe o nome do posto (minimo 3 caracteres).');
      return;
    }

    const items = Object.entries(prices)
      .filter(([, v]) => v && parseFloat(v) > 0)
      .map(([productId, price]) => ({
        product_id: parseInt(productId, 10),
        price: price,
      }));

    if (items.length === 0) {
      setError('Informe ao menos um preco.');
      return;
    }

    setSaving(true);
    try {
      const res = await apiPost('/bi/pricing/competitor/captures', {
        station_name: stationName.trim(),
        capture_date: captureDate,
        observation: observation.trim() || null,
        items,
      });
      setSuccess(`Captura salva com sucesso! ${res.data?.items_saved || items.length} precos registrados.`);
      setStationName('');
      setObservation('');
      setPrices({});
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao salvar captura'));
    } finally {
      setSaving(false);
    }
  };

  if (!scope.id_filial) {
    return <p className="text-gray-500 text-sm">Selecione uma filial para registrar precos.</p>;
  }

  return (
    <div>
      {/* Station Name */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Nome do Posto Concorrente
        </label>
        <input
          type="text"
          value={stationName}
          onChange={e => setStationName(e.target.value)}
          placeholder="Ex: Posto Shell Centro"
          className="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          maxLength={200}
        />
      </div>

      {/* Date + Observation */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Data</label>
          <input
            type="date"
            value={captureDate}
            onChange={e => setCaptureDate(e.target.value)}
            className="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Observacao (opcional)</label>
          <input
            type="text"
            value={observation}
            onChange={e => setObservation(e.target.value)}
            placeholder="Ex: Precos de placa"
            className="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            maxLength={500}
          />
        </div>
      </div>

      {/* Fuel Prices Table */}
      {loading ? (
        <p className="text-gray-500 text-sm">Carregando combustiveis...</p>
      ) : fuels.length === 0 ? (
        <p className="text-gray-500 text-sm">Nenhum combustivel encontrado para esta filial.</p>
      ) : (
        <div className="overflow-x-auto mb-4">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="border-b border-gray-200 text-left">
                <th className="py-2 pr-3 font-medium text-gray-600">Combustivel</th>
                <th className="py-2 pr-3 font-medium text-gray-600 text-right">Meu Preco</th>
                <th className="py-2 font-medium text-gray-600 text-right">Preco Concorrente</th>
              </tr>
            </thead>
            <tbody>
              {fuels.map(fuel => (
                <tr key={fuel.product_id} className="border-b border-gray-100">
                  <td className="py-2 pr-3">
                    <div className="font-medium">{fuel.product_name}</div>
                    {fuel.fuel_type && (
                      <span className="text-xs text-gray-400">{fuel.fuel_type}</span>
                    )}
                  </td>
                  <td className="py-2 pr-3 text-right text-gray-500">
                    {fuel.own_current_price
                      ? `R$ ${fmtPrice(fuel.own_current_price)}`
                      : '-'}
                  </td>
                  <td className="py-2 text-right">
                    <input
                      type="text"
                      inputMode="decimal"
                      value={prices[fuel.product_id] || ''}
                      onChange={e => handlePriceChange(fuel.product_id, e.target.value)}
                      placeholder="0.000"
                      className="w-24 text-right border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Submit */}
      <button
        onClick={handleSubmit}
        disabled={saving}
        className="w-full sm:w-auto px-6 py-2 bg-blue-600 text-white rounded text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {saving ? 'Salvando...' : 'Registrar Precos'}
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
  const [historyDate, setHistoryDate] = useState(todayISO());
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
      setError(extractApiError(e, 'Erro ao carregar historico'));
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
      setError('Preco invalido.');
      return;
    }
    try {
      await apiPatch(`/bi/pricing/competitor/items/${itemId}`, {
        new_price: cleaned,
        change_reason: 'Correcao manual',
      });
      setSuccess('Preco atualizado.');
      setEditingItem(null);
      setEditPrice('');
      loadHistory();
    } catch (e: any) {
      setError(extractApiError(e, 'Erro ao atualizar preco'));
    }
  };

  if (!scope.id_filial) {
    return <p className="text-gray-500 text-sm">Selecione uma filial.</p>;
  }

  return (
    <div>
      {/* Date picker */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">Data</label>
        <input
          type="date"
          value={historyDate}
          onChange={e => setHistoryDate(e.target.value)}
          className="border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {loading ? (
        <p className="text-gray-500 text-sm">Carregando...</p>
      ) : captures.length === 0 ? (
        <p className="text-gray-500 text-sm">Nenhuma captura encontrada para esta data.</p>
      ) : (
        <div className="space-y-6">
          {captures.map(cap => (
            <div key={cap.capture_id} className="bg-white border border-gray-200 rounded-lg p-4">
              <div className="flex flex-wrap items-center gap-2 mb-3">
                <span className="font-semibold text-gray-800">{cap.station_name}</span>
                <span className="text-xs text-gray-400">
                  {cap.captured_at ? new Date(cap.captured_at).toLocaleString('pt-BR') : ''}
                </span>
                <span className="text-xs text-gray-400">por {cap.registered_by_user_name}</span>
              </div>
              {cap.observation && (
                <p className="text-xs text-gray-500 mb-2 italic">{cap.observation}</p>
              )}

              {/* Items */}
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 text-left text-gray-500">
                    <th className="py-1 pr-2 font-medium">Produto</th>
                    <th className="py-1 text-right font-medium">Preco</th>
                    <th className="py-1 text-center font-medium w-20">Acao</th>
                  </tr>
                </thead>
                <tbody>
                  {cap.items.map(item => (
                    <tr key={item.item_id} className="border-b border-gray-50">
                      <td className="py-1 pr-2">
                        <span>{item.product_name}</span>
                        {item.previous_price && (
                          <span className="text-xs text-gray-400 ml-1">
                            (anterior: R$ {fmtPrice(item.previous_price)})
                          </span>
                        )}
                      </td>
                      <td className="py-1 text-right">
                        {editingItem === item.item_id ? (
                          <input
                            type="text"
                            inputMode="decimal"
                            value={editPrice}
                            onChange={e => setEditPrice(e.target.value.replace(/[^0-9.,]/g, '').replace(',', '.'))}
                            className="w-20 text-right border border-blue-300 rounded px-1 py-0.5 text-sm focus:outline-none"
                            autoFocus
                          />
                        ) : (
                          <span className="font-medium">R$ {fmtPrice(item.price)}</span>
                        )}
                      </td>
                      <td className="py-1 text-center">
                        {editingItem === item.item_id ? (
                          <div className="flex gap-1 justify-center">
                            <button
                              onClick={() => handleEditSave(item.item_id)}
                              className="text-green-600 text-xs hover:underline"
                            >
                              Salvar
                            </button>
                            <button
                              onClick={() => { setEditingItem(null); setEditPrice(''); }}
                              className="text-gray-400 text-xs hover:underline"
                            >
                              Cancelar
                            </button>
                          </div>
                        ) : (
                          <button
                            onClick={() => { setEditingItem(item.item_id); setEditPrice(item.price); }}
                            className="text-blue-600 text-xs hover:underline"
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
  const [compDate, setCompDate] = useState(todayISO());

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

  const statusLabel = (s: string) => {
    switch (s) {
      case 'MEU_POSTO_MAIS_BARATO': return { text: 'Mais barato', color: 'text-green-600' };
      case 'MEU_POSTO_MAIS_CARO': return { text: 'Mais caro', color: 'text-red-600' };
      case 'IGUAL_AO_MENOR': return { text: 'Igual', color: 'text-blue-600' };
      case 'SEM_CONCORRENTE': return { text: 'Sem dados', color: 'text-gray-400' };
      case 'SEM_PRECO_PROPRIO': return { text: 'Sem preco proprio', color: 'text-yellow-600' };
      default: return { text: s, color: 'text-gray-500' };
    }
  };

  if (!scope.id_filial) {
    return <p className="text-gray-500 text-sm">Selecione uma filial.</p>;
  }

  return (
    <div>
      {/* Date picker */}
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">Data de referencia</label>
        <input
          type="date"
          value={compDate}
          onChange={e => setCompDate(e.target.value)}
          className="border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {loading ? (
        <p className="text-gray-500 text-sm">Carregando comparativo...</p>
      ) : rows.length === 0 ? (
        <p className="text-gray-500 text-sm">Nenhum dado de comparacao encontrado.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="border-b border-gray-200 text-left">
                <th className="py-2 pr-3 font-medium text-gray-600">Combustivel</th>
                <th className="py-2 pr-3 font-medium text-gray-600 text-right">Meu Preco</th>
                <th className="py-2 pr-3 font-medium text-gray-600 text-right">Menor Concorrente</th>
                <th className="py-2 pr-3 font-medium text-gray-600">Posto</th>
                <th className="py-2 pr-3 font-medium text-gray-600 text-right">Media</th>
                <th className="py-2 pr-3 font-medium text-gray-600 text-right">Diferenca</th>
                <th className="py-2 font-medium text-gray-600 text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(row => {
                const st = statusLabel(row.status);
                return (
                  <tr key={row.product_id} className="border-b border-gray-100">
                    <td className="py-2 pr-3">
                      <div className="font-medium">{row.product_name}</div>
                      {row.fuel_type && (
                        <span className="text-xs text-gray-400">{row.fuel_type}</span>
                      )}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {row.own_current_price ? `R$ ${fmtPrice(row.own_current_price)}` : '-'}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {row.competitor_min_price ? `R$ ${fmtPrice(row.competitor_min_price)}` : '-'}
                    </td>
                    <td className="py-2 pr-3 text-gray-500 text-xs">
                      {row.competitor_min_station_name || '-'}
                    </td>
                    <td className="py-2 pr-3 text-right text-gray-500">
                      {row.competitor_avg_price ? `R$ ${fmtPrice(row.competitor_avg_price)}` : '-'}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {row.diff_value != null ? (
                        <span className={Number(row.diff_value) > 0 ? 'text-red-600' : 'text-green-600'}>
                          {Number(row.diff_value) > 0 ? '+' : ''}R$ {fmtPrice(row.diff_value)}
                          {row.diff_percent != null && (
                            <span className="text-xs ml-1">({Number(row.diff_percent) > 0 ? '+' : ''}{fmtPrice(row.diff_percent, 1)}%)</span>
                          )}
                        </span>
                      ) : '-'}
                    </td>
                    <td className={`py-2 text-center text-xs font-medium ${st.color}`}>
                      {st.text}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
