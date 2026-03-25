import { formatCurrencyValue } from './currency-format.mjs';

const dateFormatter = new Intl.DateTimeFormat('pt-BR', {
  day: '2-digit',
  month: '2-digit',
  year: 'numeric',
});

const dateTimeFormatter = new Intl.DateTimeFormat('pt-BR', {
  day: '2-digit',
  month: '2-digit',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
});

export function formatCurrency(value: any) {
  return formatCurrencyValue(value);
}

export function formatDateOnly(value: any) {
  if (!value) return '-';
  if (typeof value === 'number' || /^\d{8}$/.test(String(value))) {
    return formatDateKey(value);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return dateFormatter.format(parsed);
}

export function formatDateTime(value: any) {
  if (!value) return '-';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return dateTimeFormatter.format(parsed);
}

export function formatHoursLabel(value: any) {
  const hours = Number(value || 0);
  if (!Number.isFinite(hours) || hours <= 0) return '0h';
  if (hours >= 24) {
    const days = Math.floor(hours / 24);
    const remHours = Math.round(hours % 24);
    return remHours > 0 ? `${days}d ${remHours}h` : `${days}d`;
  }
  if (hours >= 1) return `${hours.toFixed(1)}h`;
  return `${Math.round(hours * 60)}min`;
}

export function formatDateKey(value: any) {
  const digits = String(value || '');
  if (!/^\d{8}$/.test(digits)) return String(value || '-');
  return `${digits.slice(6, 8)}/${digits.slice(4, 6)}/${digits.slice(0, 4)}`;
}

export function formatDateKeyShort(value: any) {
  const digits = String(value || '');
  if (!/^\d{8}$/.test(digits)) return String(value || '-');
  return `${digits.slice(6, 8)}/${digits.slice(4, 6)}`;
}

export function formatFilialLabel(idFilial: any, filialNome?: string | null) {
  const nome = String(filialNome || '').trim();
  if (nome) return nome;
  if (idFilial === null || idFilial === undefined || idFilial === '') return 'Todas as filiais';
  return `Filial #${idFilial}`;
}

export function formatRoleLabel(role: any) {
  const raw = String(role || '');
  const value = raw.toUpperCase();
  if (value === 'MASTER') return 'Master';
  if (value === 'OWNER') return 'Diretoria';
  if (value === 'MANAGER') return 'Gerência';
  if (raw === 'platform_master') return 'Platform Master';
  if (raw === 'platform_admin') return 'Platform Admin';
  if (raw === 'product_global') return 'Produto Global';
  if (raw === 'channel_admin') return 'Canal';
  if (raw === 'tenant_admin') return 'Tenant Admin';
  if (raw === 'tenant_manager') return 'Tenant Manager';
  if (raw === 'tenant_viewer') return 'Tenant Viewer';
  return value || 'Usuário';
}

export function buildUserLabel(claims: any) {
  if (!claims) return undefined;
  const primary = formatRoleLabel(claims.user_role || claims.role);
  const tenant = claims.id_empresa ? `E${claims.id_empresa}` : '';
  const branch = claims.id_filial ? `F${claims.id_filial}` : '';
  return [primary, tenant, branch].filter(Boolean).join(' · ');
}

export function formatTurnoLabel(idTurno: any) {
  if (idTurno === null || idTurno === undefined || Number(idTurno) < 0) return 'Turno não informado';
  return `Turno ${idTurno}`;
}
