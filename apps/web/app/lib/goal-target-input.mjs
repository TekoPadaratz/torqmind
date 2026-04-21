const brlFormatter = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
});

function normalizeDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function normalizeDisplay(value) {
  return String(value || '').replace(/\u00a0/g, ' ');
}

export function formatGoalTargetInputFromNumber(value) {
  const parsed = Number(value || 0);
  if (!Number.isFinite(parsed) || parsed <= 0) return '';
  return normalizeDisplay(brlFormatter.format(parsed));
}

export function normalizeGoalTargetInput(value) {
  const digits = normalizeDigits(value);
  if (!digits) return '';
  return formatGoalTargetInputFromNumber(Number(digits) / 100);
}

export function parseGoalTargetInput(value) {
  const digits = normalizeDigits(value);
  if (!digits) return 0;
  return Number(digits) / 100;
}
