'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../components/PlatformShell';
import { apiGet } from '../lib/api';
import { buildUserLabel, formatCurrency, formatDateOnly } from '../lib/format';
import { loadSession } from '../lib/session';

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
          <table className="table compact">
            <thead>
              <tr>
                <th>ID</th>
                <th>Empresa</th>
                <th>Status</th>
                <th>Canal</th>
                <th>Vigência</th>
              </tr>
            </thead>
            <tbody>
              {companies.map((item) => (
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
          <table className="table compact">
            <thead>
              <tr>
                <th>Nome</th>
                <th>Papel</th>
                <th>Último acesso</th>
                <th>Telegram</th>
              </tr>
            </thead>
            <tbody>
              {users.map((item) => (
                <tr key={item.id}>
                  <td>{item.nome}</td>
                  <td>{item.role}</td>
                  <td>{formatDateOnly(item.last_login_at)}</td>
                  <td>{item.telegram_configured ? 'OK' : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
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
              <table className="table compact">
                <thead>
                  <tr>
                    <th>Competência</th>
                    <th>Empresa</th>
                    <th>Valor</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {receivables.map((item) => (
                    <tr key={item.id}>
                      <td>{formatDateOnly(item.competence_month)}</td>
                      <td>{item.tenant_name}</td>
                      <td>{formatCurrency(item.amount)}</td>
                      <td>{item.status}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
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
              <table className="table compact">
                <thead>
                  <tr>
                    <th>Canal</th>
                    <th>Empresa</th>
                    <th>Competência</th>
                    <th>Valor</th>
                  </tr>
                </thead>
                <tbody>
                  {payables.map((item) => (
                    <tr key={item.id}>
                      <td>{item.channel_name}</td>
                      <td>{item.tenant_name}</td>
                      <td>{formatDateOnly(item.competence_month)}</td>
                      <td>{formatCurrency(item.payable_amount)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}
    </PlatformShell>
  );
}
