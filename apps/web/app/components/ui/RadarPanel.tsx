import Link from 'next/link';

type RadarMetric = { label: string; value: string };

export default function RadarPanel({
  title,
  metrics,
  href,
  actionLabel = 'Ver detalhes',
}: {
  title: string;
  metrics: RadarMetric[];
  href: string;
  actionLabel?: string;
}) {
  return (
    <section className="card radarPanel">
      <div className="radarHeader">
        <h2>{title}</h2>
        <Link className="btn" href={href}>{actionLabel}</Link>
      </div>
      <div className="radarMetrics">
        {metrics.map((m) => (
          <div className="radarMetric" key={m.label}>
            <span className="muted">{m.label}</span>
            <strong>{m.value}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}
