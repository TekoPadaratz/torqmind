import Skeleton from './Skeleton';

export default function ScopeTransitionState({
  mode = 'loading',
  headline,
  detail,
  metrics = 4,
  panels = 3,
}: {
  mode?: 'loading' | 'unavailable';
  headline: string;
  detail: string;
  metrics?: number;
  panels?: number;
}) {
  const toneClass = mode === 'unavailable' ? 'scopeTransitionCard is-warning' : 'scopeTransitionCard';
  const pill = mode === 'unavailable' ? 'Atualização em andamento' : 'Atualizando recorte';

  return (
    <div className="scopeTransitionState">
      <section className={`card ${toneClass}`}>
        <div className="scopeTransitionPill">{pill}</div>
        <h2 className="scopeTransitionHeadline">{headline}</h2>
        <p className="scopeTransitionDetail">{detail}</p>
      </section>

      <section className="scopeTransitionMetricGrid" aria-hidden="true">
        {Array.from({ length: metrics }).map((_, index) => (
          <div key={`scope-metric-${index}`} className="card">
            <Skeleton height={102} />
          </div>
        ))}
      </section>

      <section className="scopeTransitionPanelGrid" aria-hidden="true">
        {Array.from({ length: panels }).map((_, index) => (
          <div key={`scope-panel-${index}`} className="card">
            <Skeleton height={220} />
          </div>
        ))}
      </section>
    </div>
  );
}
