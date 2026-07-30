'use client';

import { useEffect, useMemo, useState } from 'react';

import EmptyState from '../components/ui/EmptyState';
import GridPager, { GRID_PAGE_SIZE } from '../components/ui/GridPager';
import GridSearchInput from '../components/ui/GridSearchInput';
import PresetFilterChips from '../components/ui/PresetFilterChips';
import { formatCurrency, formatDateOnly } from '../lib/format';
import { apiGet } from '../lib/api';
import { extractApiError } from '../lib/errors';
import { buildScopeParams, type ScopeQuery } from '../lib/scope';

type TitleRow = {
  id_filial: number;
  filial_nome?: string;
  id_titulo: number;
  entidade_nome?: string;
  dt_lancamento?: string | null;
  dt_vencimento?: string | null;
  valor?: number;
  valor_pago?: number;
  valor_aberto?: number;
  status?: string;
};

type TitlesPayload = {
  items?: TitleRow[];
  total?: number;
  page?: number;
  page_size?: number;
  page_totals?: { valor?: number; valor_aberto?: number };
  totals?: { valor?: number; valor_aberto?: number };
};

const PRESETS = [
  { id: 'vencidos', label: 'Vencidos' },
  { id: 'a_vencer_7d', label: 'A vencer 7 dias' },
  { id: 'a_vencer_mes', label: 'A vencer no mês' },
  { id: 'a_vencer', label: 'A vencer' },
];

type Props = {
  tipo: 0 | 1;
  scope: ScopeQuery;
  entidadeLabel: string;
};

export default function FinanceTitlesSection({ tipo, scope, entidadeLabel }: Props) {
  const [q, setQ] = useState('');
  const [debouncedQ, setDebouncedQ] = useState('');
  const [preset, setPreset] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [data, setData] = useState<TitlesPayload | null>(null);

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedQ(q.trim()), 250);
    return () => window.clearTimeout(t);
  }, [q]);

  useEffect(() => {
    setPage(1);
  }, [debouncedQ, preset, scope.scope_key, tipo]);

  useEffect(() => {
    if (!scope.dt_ini || !scope.dt_fim) return;
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const params = buildScopeParams(scope);
        params.set('tipo', String(tipo));
        params.set('page', String(page));
        params.set('page_size', String(GRID_PAGE_SIZE));
        if (debouncedQ) params.set('q', debouncedQ);
        if (preset) params.set('preset', preset);
        const payload = await apiGet(`/bi/finance/titles?${params.toString()}`, {
          signal: controller.signal,
        });
        setData(payload as TitlesPayload);
      } catch (err: any) {
        if (controller.signal.aborted) return;
        setError(extractApiError(err, 'Falha ao carregar títulos'));
        setData(null);
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };
    void load();
    return () => controller.abort();
  }, [debouncedQ, page, preset, scope, tipo]);

  const items = data?.items || [];
  const total = Number(data?.total || 0);
  const totalPages = Math.max(1, Math.ceil(total / GRID_PAGE_SIZE) || 1);
  const safePage = Math.min(page, totalPages);
  const pageTotals = data?.page_totals || {};
  const grandTotals = data?.totals || {};

  const statusLabel = useMemo(
    () =>
      ({
        vencido: 'Vencido',
        a_vencer: 'A vencer',
        pago: 'Pago',
      }) as Record<string, string>,
    [],
  );

  return (
    <div className="card col-12">
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 12,
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 12,
        }}
      >
        <div>
          <div className="sectionEyebrow">Financeiro</div>
          <h2 style={{ marginTop: 4 }}>
            {tipo === 0 ? 'Contas a pagar' : 'Contas a receber'}
          </h2>
        </div>
        <GridSearchInput
          value={q}
          onChange={setQ}
          placeholder={`Pesquisar ${entidadeLabel.toLowerCase()}, valor, data…`}
        />
      </div>

      <PresetFilterChips
        options={PRESETS}
        value={preset}
        onChange={setPreset}
      />

      {error ? <div className="errorCard" style={{ marginBottom: 8 }}>{error}</div> : null}

      {loading && !data ? (
        <p className="muted" style={{ fontSize: 13 }}>
          Carregando títulos…
        </p>
      ) : !items.length ? (
        <EmptyState
          title="Sem títulos no filtro"
          detail="Ajuste o período, a filial ou os filtros rápidos."
        />
      ) : (
        <>
          <div className="tableScroll">
            <table className="table compact">
              <thead>
                <tr>
                  <th>Filial</th>
                  <th>Lançamento</th>
                  <th>Vencimento</th>
                  <th>{entidadeLabel}</th>
                  <th>Valor</th>
                  <th>Aberto</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row) => (
                  <tr key={`${row.id_filial}-${row.id_titulo}`}>
                    <td>{row.filial_nome || `Filial ${row.id_filial}`}</td>
                    <td>{formatDateOnly(row.dt_lancamento)}</td>
                    <td>{formatDateOnly(row.dt_vencimento)}</td>
                    <td>{row.entidade_nome || '—'}</td>
                    <td style={{ fontVariantNumeric: 'tabular-nums' }}>
                      {formatCurrency(row.valor)}
                    </td>
                    <td style={{ fontVariantNumeric: 'tabular-nums' }}>
                      {formatCurrency(row.valor_aberto)}
                    </td>
                    <td>{statusLabel[String(row.status || '')] || row.status || '—'}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={4} style={{ fontWeight: 600 }}>
                    Total da página
                  </td>
                  <td style={{ fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
                    {formatCurrency(pageTotals.valor)}
                  </td>
                  <td style={{ fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
                    {formatCurrency(pageTotals.valor_aberto)}
                  </td>
                  <td />
                </tr>
                <tr>
                  <td colSpan={4} className="muted">
                    Total do filtro ({total.toLocaleString('pt-BR')} títulos)
                  </td>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>
                    {formatCurrency(grandTotals.valor)}
                  </td>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>
                    {formatCurrency(grandTotals.valor_aberto)}
                  </td>
                  <td />
                </tr>
              </tfoot>
            </table>
          </div>
          <GridPager
            page={safePage}
            totalPages={totalPages}
            total={total}
            pageSize={GRID_PAGE_SIZE}
            onPrev={() => setPage((p) => Math.max(1, p - 1))}
            onNext={() => setPage((p) => Math.min(totalPages, p + 1))}
          />
        </>
      )}
    </div>
  );
}
