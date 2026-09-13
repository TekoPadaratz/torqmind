'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import GridChrome from '../../components/ui/GridChrome';
import GridSearchInput from '../../components/ui/GridSearchInput';
import SortableTh from '../../components/ui/SortableTh';
import { api, apiGet } from '../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../lib/format';
import { compareGridRows } from '../../lib/grid-sort';
import { loadSession } from '../../lib/session';
import { useGridSearch } from '../../lib/use-grid-search';
import { useRecordGrid } from '../../lib/use-record-grid';

export const dynamic = 'force-dynamic';

function toDatetimeInput(value: any) {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toISOString().slice(0, 16);
}

export default function PlatformChannelPayablesPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [selected, setSelected] = useState<any>(null);
  const [filters, setFilters] = useState({ status: '' });
  const [payForm, setPayForm] = useState({ paid_at: '', notes: '' });
  const [noteForm, setNoteForm] = useState({ notes: '' });
  const [error, setError] = useState('');
  const payablesSearch = useGridSearch(items);
  const payablesGrid = useRecordGrid<any>({
    rows: payablesSearch.filteredRows,
    resetKey: `${payablesSearch.query}:${filters.status}`,
    getTieId: (row) => row.id,
    defaultCompare: (a, b) =>
      compareGridRows(
        { data: a.competence_month, nome: a.channel_name },
        { data: b.competence_month, nome: b.channel_name },
      ),
    summableKeys: ['gross_amount', 'payable_amount'],
    columns: {
      channel_name: { type: 'text' },
      tenant_name: { type: 'text' },
      competence_month: { type: 'date' },
      gross_amount: { type: 'number' },
      commission_pct: { type: 'number' },
      payable_amount: { type: 'number' },
      status: { type: 'text' },
      paid_at: { type: 'date' },
    },
  });

  async function load(session: any, currentFilters = filters) {
    const query = new URLSearchParams({ limit: '200' });
    if (currentFilters.status) query.set('status', currentFilters.status);
    const res = await apiGet(`/platform/channel-payables?${query.toString()}`);
    setItems(res?.items || []);
    if (selected) {
      const fresh = (res?.items || []).find((item: any) => item.id === selected.id);
      setSelected(fresh || null);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session, { status: '' });
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar payables.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  function selectPayable(item: any) {
    setSelected(item);
    setPayForm({ paid_at: toDatetimeInput(item.paid_at), notes: item.notes || '' });
    setNoteForm({ notes: item.notes || '' });
  }

  async function markPaid() {
    if (!selected) return;
    try {
      await api.post(`/platform/channel-payables/${selected.id}/pay`, {
        paid_at: payForm.paid_at || null,
        notes: payForm.notes || null,
      });
      await load(me, filters);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao marcar payable como pago.');
    }
  }

  async function cancel() {
    if (!selected) return;
    try {
      await api.post(`/platform/channel-payables/${selected.id}/cancel`, { notes: noteForm.notes || null });
      await load(me, filters);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao cancelar payable.');
    }
  }

  return (
    <PlatformShell
      title="Contas a pagar de canal"
      subtitle="Repasse calculado sobre o valor efetivamente recebido, com baixa manual, cancelamento controlado e origem rastreável no recebível pago."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Filtro</div>
              <h2>Fila de pagamento</h2>
            </div>
          </div>
          <div className="platformFormGrid">
            <select className="input" value={filters.status} onChange={(e) => setFilters({ status: e.target.value })}>
              <option value="">Todos os status</option>
              <option value="pending">pending</option>
              <option value="released">released</option>
              <option value="paid">paid</option>
              <option value="cancelled">cancelled</option>
            </select>
            <button className="btn" type="button" onClick={() => load(me, filters)}>
              Aplicar filtro
            </button>
          </div>
        </div>

        {selected ? (
          <div className="card">
            <div className="platformSectionHead">
              <div>
                <div className="platformSectionEyebrow">Payable selecionado</div>
                <h2>
                  {selected.channel_name} · {selected.tenant_name}
                </h2>
              </div>
            </div>
            <div className="platformSummaryList">
              <div>Competência: {formatDateOnly(selected.competence_month)}</div>
              <div>Base recebida: {formatCurrency(selected.gross_amount)}</div>
              <div>Comissão: {selected.commission_pct}%</div>
              <div>Valor a pagar: {formatCurrency(selected.payable_amount)}</div>
              <div>Status: {selected.status}</div>
            </div>
            <div className="platformFormGrid">
              <input className="input" type="datetime-local" value={payForm.paid_at} onChange={(e) => setPayForm({ ...payForm, paid_at: e.target.value })} />
              <input className="input" placeholder="Notas do pagamento" value={payForm.notes} onChange={(e) => setPayForm({ ...payForm, notes: e.target.value })} />
              <button className="btn" type="button" onClick={markPaid}>
                Marcar pago
              </button>
            </div>
            <div className="platformFormGrid">
              <input className="input" placeholder="Motivo de cancelamento" value={noteForm.notes} onChange={(e) => setNoteForm({ notes: e.target.value })} />
              <button className="btn" type="button" onClick={cancel}>
                Cancelar payable
              </button>
            </div>
          </div>
        ) : null}
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <GridSearchInput value={payablesSearch.query} onChange={payablesSearch.setQuery} aria-label="Pesquisar contas a pagar" />
        <table className="table">
          <thead>
            <tr>
              <SortableTh label="Canal" sortKey="channel_name" ariaSort={payablesGrid.ariaSort('channel_name')} onToggle={payablesGrid.toggleSort} />
              <SortableTh label="Empresa" sortKey="tenant_name" ariaSort={payablesGrid.ariaSort('tenant_name')} onToggle={payablesGrid.toggleSort} />
              <SortableTh label="Competência" sortKey="competence_month" ariaSort={payablesGrid.ariaSort('competence_month')} onToggle={payablesGrid.toggleSort} />
              <SortableTh label="Base" sortKey="gross_amount" ariaSort={payablesGrid.ariaSort('gross_amount')} onToggle={payablesGrid.toggleSort} align="right" />
              <SortableTh label="%" sortKey="commission_pct" ariaSort={payablesGrid.ariaSort('commission_pct')} onToggle={payablesGrid.toggleSort} />
              <SortableTh label="Valor a pagar" sortKey="payable_amount" ariaSort={payablesGrid.ariaSort('payable_amount')} onToggle={payablesGrid.toggleSort} align="right" />
              <SortableTh label="Status" sortKey="status" ariaSort={payablesGrid.ariaSort('status')} onToggle={payablesGrid.toggleSort} />
              <SortableTh label="Pago em" sortKey="paid_at" ariaSort={payablesGrid.ariaSort('paid_at')} onToggle={payablesGrid.toggleSort} />
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {payablesGrid.slice.map((item) => (
              <tr key={item.id}>
                <td>{item.channel_name}</td>
                <td>{item.tenant_name}</td>
                <td>{formatDateOnly(item.competence_month)}</td>
                <td>{formatCurrency(item.gross_amount)}</td>
                <td>{item.commission_pct}%</td>
                <td>{formatCurrency(item.payable_amount)}</td>
                <td>{item.status}</td>
                <td>{item.paid_at ? formatDateTime(item.paid_at) : '-'}</td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => selectPayable(item)}>
                    Operar
                  </button>
                </td>
              </tr>
            ))}
            {!payablesGrid.total ? (
              <tr>
                <td colSpan={9}>Nenhuma conta a pagar encontrada.</td>
              </tr>
            ) : null}
          </tbody>
          {payablesGrid.slice.length ? (
            <tfoot>
              <tr>
                <td colSpan={3}>Total da página ({payablesGrid.slice.length})</td>
                <td>{formatCurrency(payablesGrid.pageTotals.gross_amount)}</td>
                <td />
                <td>{formatCurrency(payablesGrid.pageTotals.payable_amount)}</td>
                <td colSpan={3} />
              </tr>
            </tfoot>
          ) : null}
        </table>
        <GridChrome
          page={payablesGrid.page}
          totalPages={payablesGrid.totalPages}
          total={payablesGrid.total}
          from={payablesGrid.range.from}
          to={payablesGrid.range.to}
          onPrev={payablesGrid.onPrev}
          onNext={payablesGrid.onNext}
          onResetOrder={payablesGrid.resetOrder}
          isDefaultOrder={payablesGrid.isDefaultOrder}
        />
      </div>
    </PlatformShell>
  );
}
