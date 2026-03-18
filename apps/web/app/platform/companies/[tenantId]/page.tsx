'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';

import PlatformShell from '../../../components/PlatformShell';
import { api, apiGet } from '../../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../../lib/format';
import { loadSession } from '../../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformCompanyDetailPage() {
  const router = useRouter();
  const params = useParams();
  const tenantId = Number((params?.tenantId as string) || 0);

  const [me, setMe] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [companyForm, setCompanyForm] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  async function loadCompany(session: any) {
    setLoading(true);
    try {
      const detail = await apiGet(`/platform/companies/${tenantId}`);
      setData(detail);
      setCompanyForm({
        nome: detail.nome || '',
        cnpj: detail.cnpj || '',
        is_enabled: Boolean(detail.is_active),
        valid_from: detail.valid_from || '',
        valid_until: detail.valid_until || '',
        status: detail.status || 'active',
        billing_status: detail.billing_status || 'current',
        suspended_reason: detail.suspended_reason || '',
      });
      setError('');
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao carregar empresa.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      await loadCompany(session);
    };
    boot();
  }, [router, tenantId]);

  if (!me || !companyForm) return null;

  async function saveCompany(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
    try {
      await api.patch(`/platform/companies/${tenantId}`, {
        ...companyForm,
        cnpj: companyForm.cnpj || null,
        valid_from: companyForm.valid_from || null,
        valid_until: companyForm.valid_until || null,
        suspended_reason: companyForm.suspended_reason || null,
        status: me?.access?.platform_finance ? companyForm.status : null,
        billing_status: me?.access?.platform_finance ? companyForm.billing_status : null,
      });
      await loadCompany(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar empresa.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <PlatformShell
      title={`Empresa #${tenantId}`}
      subtitle="Dados gerais, filiais sincronizadas da Xpert, usuários, contrato e trilha resumida de auditoria em uma única visão operacional."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}
      {data?.status && data.status !== 'active' ? (
        <div className="card">
          <div className="platformSectionEyebrow">Status comercial</div>
          <h2 style={{ marginTop: 8 }}>{data.status}</h2>
          <div className="platformFieldHint">{data.suspended_reason || 'A empresa está fora do ciclo comercial padrão e exige acompanhamento interno.'}</div>
        </div>
      ) : null}

      <div className="platformDetailGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Dados gerais</div>
              <h2>{data?.nome}</h2>
            </div>
          </div>
          <form className="platformFormGrid" onSubmit={saveCompany}>
            <input className="input" value={companyForm.nome} onChange={(e) => setCompanyForm({ ...companyForm, nome: e.target.value })} />
            <input className="input" value={companyForm.cnpj} onChange={(e) => setCompanyForm({ ...companyForm, cnpj: e.target.value })} placeholder="CNPJ" />
            <input className="input" type="date" value={companyForm.valid_from} onChange={(e) => setCompanyForm({ ...companyForm, valid_from: e.target.value })} />
            <input className="input" type="date" value={companyForm.valid_until} onChange={(e) => setCompanyForm({ ...companyForm, valid_until: e.target.value })} />
            {me?.access?.platform_finance ? (
              <>
                <select className="input" value={companyForm.status} onChange={(e) => setCompanyForm({ ...companyForm, status: e.target.value })}>
                  <option value="active">active</option>
                  <option value="trial">trial</option>
                  <option value="overdue">overdue</option>
                  <option value="grace">grace</option>
                  <option value="suspended_readonly">suspended_readonly</option>
                  <option value="suspended_total">suspended_total</option>
                  <option value="cancelled">cancelled</option>
                </select>
                <select className="input" value={companyForm.billing_status} onChange={(e) => setCompanyForm({ ...companyForm, billing_status: e.target.value })}>
                  <option value="current">current</option>
                  <option value="overdue">overdue</option>
                  <option value="grace">grace</option>
                  <option value="suspended">suspended</option>
                  <option value="cancelled">cancelled</option>
                </select>
                <input
                  className="input"
                  value={companyForm.suspended_reason}
                  onChange={(e) => setCompanyForm({ ...companyForm, suspended_reason: e.target.value })}
                  placeholder="Motivo da suspensão"
                />
              </>
            ) : (
              <div className="platformFieldHint">Status comercial e cobrança são geridos apenas pelo Master.</div>
            )}
            <label className="platformCheckbox">
              <input
                type="checkbox"
                checked={companyForm.is_enabled}
                onChange={(e) => setCompanyForm({ ...companyForm, is_enabled: e.target.checked })}
              />
              Empresa habilitada
            </label>
            <button className="btn" type="submit" disabled={saving}>
              {saving ? 'Salvando...' : 'Salvar dados'}
            </button>
          </form>

          <div className="platformMetaStrip">
            <div>
              <div className="platformMetaLabel">Canal</div>
              <div className="platformMetaValue">{data?.channel_name || '-'}</div>
            </div>
            <div>
              <div className="platformMetaLabel">Plano</div>
              <div className="platformMetaValue">{data?.plan_name || '-'}</div>
            </div>
            <div>
              <div className="platformMetaLabel">Mensalidade</div>
              <div className="platformMetaValue">{data?.monthly_amount ? formatCurrency(data.monthly_amount) : '-'}</div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Filiais</div>
              <h2>Sincronização via Xpert</h2>
            </div>
          </div>
          <div className="platformFieldHint">
            As filiais usam o par oficial `id_empresa` + `id_filial` da Xpert e são atualizadas automaticamente pelo dataset
            {' '}
            <code>filiais</code>
            {' '}
            durante a ingestão/ETL. Não existe cadastro manual nessa tela.
          </div>

          <table className="table compact">
            <thead>
              <tr>
                <th>ID</th>
                <th>Nome</th>
                <th>Habilitada</th>
                <th>Vigência</th>
              </tr>
            </thead>
            <tbody>
              {(data?.branches || []).map((branch: any) => (
                <tr key={branch.id_filial}>
                  <td>{branch.id_filial}</td>
                  <td>{branch.nome}</td>
                  <td>{branch.is_active ? 'Sim' : 'Não'}</td>
                  <td>{formatDateOnly(branch.valid_until || branch.valid_from)}</td>
                </tr>
              ))}
              {!(data?.branches || []).length ? (
                <tr>
                  <td colSpan={4}>Nenhuma filial sincronizada ainda para esta empresa.</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Usuários</div>
              <h2>Equipe vinculada</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Nome</th>
                <th>Email</th>
                <th>Papel</th>
                <th>Telegram</th>
              </tr>
            </thead>
            <tbody>
              {(data?.users || []).map((user: any) => (
                <tr key={user.id}>
                  <td>{user.nome}</td>
                  <td>{user.email}</td>
                  <td>{user.role}</td>
                  <td>{user.telegram_configured ? 'OK' : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Contrato</div>
              <h2>Resumo comercial</h2>
            </div>
          </div>
          {data?.contract ? (
            <div className="platformSummaryList">
              <div>Plano: {data.contract.plan_name}</div>
              <div>Mensalidade: {formatCurrency(data.contract.monthly_amount)}</div>
              <div>Emissão: dia {data.contract.issue_day}</div>
              <div>Vencimento: dia {data.contract.billing_day}</div>
              <div>Canal: {data.contract.channel_name || '-'}</div>
              <div>1º ano: {data.contract.commission_first_year_pct}%</div>
              <div>Recorrente: {data.contract.commission_recurring_pct}%</div>
            </div>
          ) : (
            <div className="platformFieldHint">Sem contrato disponível para este perfil ou empresa.</div>
          )}
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Notificações</div>
              <h2>Assinaturas</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Usuário</th>
                <th>Evento</th>
                <th>Canal</th>
                <th>Severity</th>
              </tr>
            </thead>
            <tbody>
              {(data?.notification_subscriptions || []).map((item: any) => (
                <tr key={item.id}>
                  <td>{item.user_name}</td>
                  <td>{item.event_type}</td>
                  <td>{item.channel}</td>
                  <td>{item.severity_min || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Histórico</div>
              <h2>Auditoria resumida</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Quando</th>
                <th>Ação</th>
                <th>Entidade</th>
              </tr>
            </thead>
            <tbody>
              {(data?.audit || []).map((item: any) => (
                <tr key={item.id}>
                  <td>{formatDateTime(item.created_at)}</td>
                  <td>{item.action}</td>
                  <td>{item.entity_type}</td>
                </tr>
              ))}
              {!data?.audit?.length ? (
                <tr>
                  <td colSpan={3}>Sem eventos recentes para este perfil.</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </PlatformShell>
  );
}
