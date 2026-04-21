export default function RiskBadge({ level }: { level?: string }) {
  const normalized = String(level || 'INFO').toUpperCase();
  const cls = normalized === 'CRITICAL' ? 'critical' : normalized === 'WARN' ? 'warn' : 'info';
  return <span className={`badge ${cls}`}>{normalized}</span>;
}
