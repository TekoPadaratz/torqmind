'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import { apiGet, apiPost } from '../lib/api';
import { requireAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import { useScopeQuery } from '../lib/scope';

function fmtMoney(v: any) {
  const n = Number(v || 0);
  return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function fmtNum(v: any, digits = 3) {
  return Number(v || 0).toLocaleString('pt-BR', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export default function PricingPage() {
  const router = useRouter();
  const scope = useScopeQuery();

  const [claims, setClaims] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [saveMsg, setSaveMsg] = useState('');
  const [priceInputs, setPriceInputs] = useState<Record<string, string>>({});

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    return [claims.role, claims.id_empresa ? `E${claims.id_empresa}` : '', claims.id_filial ? `F${claims.id_filial}` : '']
      .filter(Boolean)
      .join(' · ');
  }, [claims]);

  const load = async () => {
    if (!scope.dt_ini || !scope.dt_fim) return;
    setLoading(true);
    setError('');
    try {
      const me = await apiGet('/auth/me');
      setClaims(me);

      const filial = scope.id_filial || me?.id_filial;
      const empresa = scope.id_empresa || me?.id_empresa;
      if (!filial) {
        setError('Selecione uma filial no Escopo para usar o painel de preço da concorrência.');
        setData(null);
        return;
      }

      const qs = new URLSearchParams({
        dt_ini: scope.dt_ini,
        dt_fim: scope.dt_fim,
        dt_ref: scope.dt_ref || scope.dt_fim,
        days_simulation: '10',
        id_filial: String(filial),
      });
      if (empresa) qs.set('id_empresa', String(empresa));

      const res = await apiGet(`/bi/pricing/competitor/overview?${qs.toString()}`);
      setData(res);

      const map: Record<string, string> = {};
      for (const row of res?.items || []) {
        const p = Number(row?.competitor_price || 0);
        map[String(row.id_produto)] = p > 0 ? p.toFixed(3) : '';
      }
      setPriceInputs(map);
    } catch (err: any) {
      setError(extractApiError(err, 'Falha ao carregar painel de concorrência'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!scope.ready) return;
    if (!requireAuth()) {
      router.push('/');
      return;
    }
    if (!scope.dt_ini || !scope.dt_fim) {
      router.push('/scope');
      return;
    }
    load();
  }, [scope.ready, scope.dt_ini, scope.dt_fim, scope.dt_ref, scope.id_empresa, scope.id_filial]);

  const onSavePrices = async () => {
    if (!data?.items?.length) return;
    setSaving(true);
    setSaveMsg('');
    setError('');
    try {
      const me = claims || (await apiGet('/auth/me'));
      const filial = scope.id_filial || me?.id_filial;
      const empresa = scope.id_empresa || me?.id_empresa;
      if (!filial) throw new Error('Selecione uma filial no escopo.');

      const items: any[] = [];
      for (const row of data.items || []) {
        const raw = String(priceInputs[String(row.id_produto)] || '').replace(',', '.').trim();
        if (!raw) continue;
        const price = Number(raw);
        if (!Number.isFinite(price) || price <= 0) continue;
        items.push({ id_produto: Number(row.id_produto), competitor_price: price });
      }

      const qs = new URLSearchParams({ id_filial: String(filial) });
      if (empresa) qs.set('id_empresa', String(empresa));

      const res = await apiPost(`/bi/pricing/competitor/prices?${qs.toString()}`, { items });
      setSaveMsg(`Preços salvos: ${Number(res?.saved || 0)} combustível(is). Recalculando cenário...`);
      await load();
    } catch (err: any) {
      setError(extractApiError(err, 'Falha ao salvar preços da concorrência'));
    } finally {
      setSaving(false);
    }
  };

  const summary = data?.summary || {};
  const items = data?.items || [];

  return (
    <div>
      <AppNav title="Preço da Concorrência" userLabel={userLabel} />
      <div className="container">
        <div className="card toolbar">
          <div>
            <div className="muted">Simulação 10 dias</div>
            <div className="scopeLine">
              Período base: <strong>{scope.dt_ini}</strong> até <strong>{scope.dt_fim}</strong> · Ref <strong>{scope.dt_ref || scope.dt_fim}</strong> · Filial{' '}
              <strong>{scope.id_filial || claims?.id_filial || '-'}</strong>
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

        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card kpi col-3">
            <div className="label">Tipos de combustível</div>
            <div className="value">{loading ? '...' : Number(summary.fuel_types || 0)}</div>
          </div>
          <div className="card kpi col-3 riskCard">
            <div className="label">Perda se não mudar (10d)</div>
            <div className="value">{loading ? '...' : fmtMoney(summary.total_lost_if_no_change_10d)}</div>
          </div>
          <div className="card kpi col-3">
            <div className="label">Impacto ao igualar (10d vs atual)</div>
            <div className="value">{loading ? '...' : fmtMoney(summary.total_match_vs_current_10d)}</div>
          </div>
          <div className="card kpi col-3 scoreCard">
            <div className="label">Ganho vs não mudar (10d)</div>
            <div className="value">{loading ? '...' : fmtMoney(summary.total_match_vs_no_change_10d)}</div>
          </div>

          <div className="card col-12">
            <h2>Preço por combustível: posto x concorrência</h2>
            <p className="muted" style={{ marginTop: 8 }}>
              O gerente informa o preço da concorrência e o sistema simula os impactos para os próximos 10 dias, por combustível.
            </p>
            {!loading && !items.length ? (
              <p className="muted">Nenhum combustível identificado no período/filial selecionado.</p>
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
                  <th>Impacto igualar vs não mudar (10d)</th>
                  <th>Recomendação</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row: any) => (
                  <tr key={row.id_produto}>
                    <td>
                      <div><strong>{row.produto_nome}</strong></div>
                      <div className="muted">{row.grupo_nome}</div>
                    </td>
                    <td>{fmtNum(row.avg_daily_volume, 3)}</td>
                    <td>{fmtMoney(row.avg_price_current)}</td>
                    <td>
                      <input
                        className="input"
                        style={{ minWidth: 120 }}
                        inputMode="decimal"
                        placeholder="0,000"
                        value={priceInputs[String(row.id_produto)] || ''}
                        onChange={(e) =>
                          setPriceInputs((prev) => ({
                            ...prev,
                            [String(row.id_produto)]: e.target.value,
                          }))
                        }
                      />
                    </td>
                    <td>{fmtMoney(row.station_price_gap)}</td>
                    <td>{fmtMoney(row.scenario_no_change?.lost_revenue_10d)}</td>
                    <td>{fmtMoney(row.scenario_match_competitor?.impact_vs_current_10d)}</td>
                    <td>{fmtMoney(row.scenario_match_competitor?.impact_vs_no_change_10d)}</td>
                    <td>{row.recommendation}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
