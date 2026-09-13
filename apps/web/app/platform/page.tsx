'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../components/PlatformShell';
import GridChrome from '../components/ui/GridChrome';
import GridSearchInput from '../components/ui/GridSearchInput';
import SortableTh from '../components/ui/SortableTh';
import { apiGet } from '../lib/api';
import { buildUserLabel, formatCurrency, formatDateOnly } from '../lib/format';
import { compareGridRows } from '../lib/grid-sort';
import { loadSession } from '../lib/session';
import { useGridSearch } from '../lib/use-grid-search';
import { useRecordGrid } from '../lib/use-record-grid';

export const dynamic = 'force-dynamic';

export default function PlatformHomePage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [companies, setCompanies] = useState<any[]>([]);
  const [users, setUsers] = useState<any[]>([]);
  const [receivables, setReceivables] = useState<any[]>([]);
  const [payables, setPayables] = useState<any[]>([]);
  const companiesSearch = useGridSearch(companies);
  const usersSearch = useGridSearch(users);
  const receivablesSearch = useGridSearch(receivables);
  const payablesSearch = useGridSearch(payables);
  const companiesGrid = useRecordGrid<any>({
    rows: companiesSearch.filteredRows,
    resetKey: companiesSearch.query,
    getTieId: (row) => row.id_empresa,
    defaultCompare: (a, b) => compareGridRows({ nome: a.nome }, { nome: b.nome }),
    columns: {
      id_empresa: { type: 'number' },
      nome: { type: 'text' },
      status: { type: 'text' },
      channel_name: { type: 'text' },
      valid_until: { type: 'date' },
    },
  });
  const usersGrid = useRecordGrid<any>({
    rows: usersSearch.filteredRows,
    resetKey: usersSearch.query,
    getTieId: (row) => row.id,
    defaultCompare: (a, b) => compareGridRows({ nome: a.nome }, { nome: b.nome }),
    columns: {
      nome: { type: 'text' },
      role: { type: 'text' },
      last_login_at: { type: 'date' },
    },
  });
  const receivablesGrid = useRecordGrid<any>({
    rows: receivablesSearch.filteredRows,
    resetKey: receivablesSearch.query,
    getTieId: (row) => row.id,
    defaultCompare: (a, b) =>
      compareGridRows(
        { data: a.competence_month, nome: a.tenant_name },
        { data: b.competence_month, nome: b.tenant_name },
      ),
    summableKeys: ['amount'],
    columns: {
      competence_month: { type: 'date' },
      tenant_name: { type: 'text' },
      amount: { type: 'number' },
      status: { type: 'text' },
    },
  });
  const payablesGrid = useRecordGrid<any>({
    rows: payablesSearch.filteredRows,
    resetKey: payablesSearch.query,
    getTieId: (row) => row.id,
    defaultCompare: (a, b) =>
      compareGridRows(
        { data: a.competence_month, nome: a.channel_name },
        { data: b.competence_month, nome: b.channel_name },
      ),
    summableKeys: ['payable_amount'],
    columns: {
      channel_name: { type: 'text' },
      tenant_name: { type: 'text' },
      competence_month: { type: 'date' },
      payable_amount: { type: 'number' },
    },
  });

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError('');
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);

      try {
        const tasks: Promise<any>[] = [
          apiGet('/platform/companies?limit=6'),
          apiGet('/platform/users?limit=6'),
        ];
        if (session?.access?.platform_finance) {
          tasks.push(apiGet('/platform/receivables?limit=6'));
          tasks.push(apiGet('/platform/channel-payables?limit=6'));
        }

        const [companiesRes, usersRes, receivablesRes, payablesRes] = await Promise.all(tasks);
        setCompanies(companiesRes?.items || []);
        setUsers(usersRes?.items || []);
        setReceivables(receivablesRes?.items || []);
        setPayables(payablesRes?.items || []);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar a área platform.');
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router]);

  const stats = useMemo(() => {
    const activeCompanies = companies.filter((item) => item.is_active).length;
    const overdueCompanies = companies.filter((item) => ['overdue', 'grace', 'suspended_readonly', 'suspended_total'].includes(String(item.status))).length;
    const financeOpen = receivables
      .filter((item) => !['paid', 'cancelled'].includes(String(item.status)))
      .reduce((sum, item) => sum + Number(item.amount || 0), 0);
    const payablesOpen = payables
      .filter((item) => !['paid', 'cancelled'].includes(String(item.status)))
      .reduce((sum, item) => sum + Number(item.payable_amount || 0), 0);
    return { activeCompanies, overdueCompanies, financeOpen, payablesOpen };
  }, [companies, receivables, payables]);

  if (!me) return null;

  return (
    <PlatformShell
      title="Resumo da plataforma"
      subtitle="Visão executiva da operação interna, separada do produto do cliente e com atalhos para gestão operacional e financeira."
      me={{ ...me, userLabel: buildUserLabel(me) }}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="row row-4">
        <div className="card platformStat">
          <div className="platformStatLabel">Empresas ativas</div>
          <div className="platformStatValue">{loading ? '...' : stats.activeCompanies}</div>
        </div>
        <div className="card platformStat">
          <div className="platformStatLabel">Empresas com atenção</div>
          <div className="platformStatValue">{loading ? '...' : stats.overdueCompanies}</div>
        </div>
        <div className="card platformStat">
          <div className="platformStatLabel">Receber em aberto</div>
          <div className="platformStatValue">{loading ? '...' : formatCurrency(stats.financeOpen)}</div>
        </div>
        <div className="card platformStat">
          <div className="platformStatLabel">Canal a liquidar</div>
          <div className="platformStatValue">{loading ? '...' : formatCurrency(stats.payablesOpen)}</div>
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Base</div>
              <h2>Empresas recentes</h2>
            </div>
            <Link className="btn" href="/platform/companies">
              Abrir gestão
            </Link>
          </div>
          <GridSearchInput value={companiesSearch.query} onChange={companiesSearch.setQuery} aria-label="Pesquisar empresas recentes" />
          <table className="table compact">
            <thead>
              <tr>
                <SortableTh label="ID" sortKey="id_empresa" ariaSort={companiesGrid.ariaSort('id_empresa')} onToggle={companiesGrid.toggleSort} />
                <SortableTh label="Empresa" sortKey="nome" ariaSort={companiesGrid.ariaSort('nome')} onToggle={companiesGrid.toggleSort} />
                <SortableTh label="Status" sortKey="status" ariaSort={companiesGrid.ariaSort('status')} onToggle={companiesGrid.toggleSort} />
                <SortableTh label="Canal" sortKey="channel_name" ariaSort={companiesGrid.ariaSort('channel_name')} onToggle={companiesGrid.toggleSort} />
                <SortableTh label="Vigência" sortKey="valid_until" ariaSort={companiesGrid.ariaSort('valid_until')} onToggle={companiesGrid.toggleSort} />
              </tr>
            </thead>
            <tbody>
              {companiesGrid.slice.map((item) => (
                <tr key={item.id_empresa}>
                  <td>{item.id_empresa}</td>
                  <td>
                    <Link href={`/platform/companies/${item.id_empresa}`}>{item.nome}</Link>
                  </td>
                  <td>{item.status || '-'}</td>
                  <td>{item.channel_name || '-'}</td>
                  <td>{formatDateOnly(item.valid_until || item.valid_from)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <GridChrome
            page={companiesGrid.page}
            totalPages={companiesGrid.totalPages}
            total={companiesGrid.total}
            from={companiesGrid.range.from}
            to={companiesGrid.range.to}
            onPrev={companiesGrid.onPrev}
            onNext={companiesGrid.onNext}
            onResetOrder={companiesGrid.resetOrder}
            isDefaultOrder={companiesGrid.isDefaultOrder}
            truncatedNote="Amostra das 6 mais recentes."
          />
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Acesso</div>
              <h2>Usuários em foco</h2>
            </div>
            <Link className="btn" href="/platform/users">
              Abrir usuários
            </Link>
          </div>
          <GridSearchInput value={usersSearch.query} onChange={usersSearch.setQuery} aria-label="Pesquisar usuários em foco" />
          <table className="table compact">
            <thead>
              <tr>
                <SortableTh label="Nome" sortKey="nome" ariaSort={usersGrid.ariaSort('nome')} onToggle={usersGrid.toggleSort} />
                <SortableTh label="Papel" sortKey="role" ariaSort={usersGrid.ariaSort('role')} onToggle={usersGrid.toggleSort} />
                <SortableTh label="Último acesso" sortKey="last_login_at" ariaSort={usersGrid.ariaSort('last_login_at')} onToggle={usersGrid.toggleSort} />
                <th>Telegram</th>
              </tr>
            </thead>
            <tbody>
              {usersGrid.slice.map((item) => (
                <tr key={item.id}>
                  <td>{item.nome}</td>
                  <td>{item.role}</td>
                  <td>{formatDateOnly(item.last_login_at)}</td>
                  <td>{item.telegram_configured ? 'OK' : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <GridChrome
            page={usersGrid.page}
            totalPages={usersGrid.totalPages}
            total={usersGrid.total}
            from={usersGrid.range.from}
            to={usersGrid.range.to}
            onPrev={usersGrid.onPrev}
            onNext={usersGrid.onNext}
            onResetOrder={usersGrid.resetOrder}
            isDefaultOrder={usersGrid.isDefaultOrder}
            truncatedNote="Amostra das 6 mais recentes."
          />
        </div>
      </div>

      {me?.access?.platform_finance ? (
        <>
          <div style={{ height: 16 }} />
          <div className="platformGrid">
            <div className="card">
              <div className="platformSectionHead">
                <div>
                  <div className="platformSectionEyebrow">Billing</div>
                  <h2>Recebíveis recentes</h2>
                </div>
                <Link className="btn" href="/platform/receivables">
                  Abrir financeiro
                </Link>
              </div>
              <GridSearchInput value={receivablesSearch.query} onChange={receivablesSearch.setQuery} aria-label="Pesquisar recebíveis recentes" />
              <table className="table compact">
                <thead>
                  <tr>
                    <SortableTh label="Competência" sortKey="competence_month" ariaSort={receivablesGrid.ariaSort('competence_month')} onToggle={receivablesGrid.toggleSort} />
                    <SortableTh label="Empresa" sortKey="tenant_name" ariaSort={receivablesGrid.ariaSort('tenant_name')} onToggle={receivablesGrid.toggleSort} />
                    <SortableTh label="Valor" sortKey="amount" ariaSort={receivablesGrid.ariaSort('amount')} onToggle={receivablesGrid.toggleSort} align="right" />
                    <SortableTh label="Status" sortKey="status" ariaSort={receivablesGrid.ariaSort('status')} onToggle={receivablesGrid.toggleSort} />
                  </tr>
                </thead>
                <tbody>
                  {receivablesGrid.slice.map((item) => (
                    <tr key={item.id}>
                      <td>{formatDateOnly(item.competence_month)}</td>
                      <td>{item.tenant_name}</td>
                      <td>{formatCurrency(item.amount)}</td>
                      <td>{item.status}</td>
                    </tr>
                  ))}
                </tbody>
                {receivablesGrid.slice.length ? (
                  <tfoot>
                    <tr>
                      <td colSpan={2}>Total da página ({receivablesGrid.slice.length})</td>
                      <td>{formatCurrency(receivablesGrid.pageTotals.amount)}</td>
                      <td />
                    </tr>
                  </tfoot>
                ) : null}
              </table>
              <GridChrome
                page={receivablesGrid.page}
                totalPages={receivablesGrid.totalPages}
                total={receivablesGrid.total}
                from={receivablesGrid.range.from}
                to={receivablesGrid.range.to}
                onPrev={receivablesGrid.onPrev}
                onNext={receivablesGrid.onNext}
                onResetOrder={receivablesGrid.resetOrder}
                isDefaultOrder={receivablesGrid.isDefaultOrder}
                truncatedNote="Amostra das 6 mais recentes."
              />
            </div>

            <div className="card">
              <div className="platformSectionHead">
                <div>
                  <div className="platformSectionEyebrow">Comissão</div>
                  <h2>Payables de canal</h2>
                </div>
                <Link className="btn" href="/platform/channel-payables">
                  Abrir canal
                </Link>
              </div>
              <GridSearchInput value={payablesSearch.query} onChange={payablesSearch.setQuery} aria-label="Pesquisar contas a pagar de canal" />
              <table className="table compact">
                <thead>
                  <tr>
                    <SortableTh label="Canal" sortKey="channel_name" ariaSort={payablesGrid.ariaSort('channel_name')} onToggle={payablesGrid.toggleSort} />
                    <SortableTh label="Empresa" sortKey="tenant_name" ariaSort={payablesGrid.ariaSort('tenant_name')} onToggle={payablesGrid.toggleSort} />
                    <SortableTh label="Competência" sortKey="competence_month" ariaSort={payablesGrid.ariaSort('competence_month')} onToggle={payablesGrid.toggleSort} />
                    <SortableTh label="Valor" sortKey="payable_amount" ariaSort={payablesGrid.ariaSort('payable_amount')} onToggle={payablesGrid.toggleSort} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {payablesGrid.slice.map((item) => (
                    <tr key={item.id}>
                      <td>{item.channel_name}</td>
                      <td>{item.tenant_name}</td>
                      <td>{formatDateOnly(item.competence_month)}</td>
                      <td>{formatCurrency(item.payable_amount)}</td>
                    </tr>
                  ))}
                </tbody>
                {payablesGrid.slice.length ? (
                  <tfoot>
                    <tr>
                      <td colSpan={3}>Total da página ({payablesGrid.slice.length})</td>
                      <td>{formatCurrency(payablesGrid.pageTotals.payable_amount)}</td>
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
                truncatedNote="Amostra das 6 mais recentes."
              />
            </div>
          </div>
        </>
      ) : null}
    </PlatformShell>
  );
}
