const moneyFormatter = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
});

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
  return moneyFormatter.format(Number(value || 0));
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
  const value = String(role || '').toUpperCase();
  if (value === 'MASTER') return 'Master';
  if (value === 'OWNER') return 'Diretoria';
  if (value === 'MANAGER') return 'Gerência';
  return value || 'Usuário';
}

export function buildUserLabel(claims: any) {
  if (!claims) return undefined;
  return formatRoleLabel(claims.role);
}

export function formatTurnoLabel(idTurno: any) {
  if (idTurno === null || idTurno === undefined || Number(idTurno) < 0) return 'Turno não informado';
  return `Turno ${idTurno}`;
}
