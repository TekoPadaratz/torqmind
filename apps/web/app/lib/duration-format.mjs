function normalizeHours(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function formatDurationHours(value) {
  const hours = normalizeHours(value);
  if (hours <= 0) return '0min';

  const totalMinutes = Math.max(0, Math.round(hours * 60));
  const days = Math.floor(totalMinutes / (24 * 60));
  const hoursPart = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutesPart = totalMinutes % 60;

  if (days > 0) {
    return hoursPart > 0 ? `${days}d ${hoursPart}h` : `${days}d`;
  }

  if (hoursPart > 0) {
    return minutesPart > 0 ? `${hoursPart}h ${minutesPart}min` : `${hoursPart}h`;
  }

  return `${minutesPart}min`;
}
