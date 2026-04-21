import Link from 'next/link';

import EvidenceChips from './EvidenceChips';
import RiskBadge from './RiskBadge';

type ActionCardProps = {
  title: string;
  severity?: string;
  impactLabel: string;
  evidence: string[];
  checklist: string[];
  detailsHref: string;
};

export default function ActionCard({
  title,
  severity,
  impactLabel,
  evidence,
  checklist,
  detailsHref,
}: ActionCardProps) {
  return (
    <article className="actionCard">
      <div className="actionHead">
        <RiskBadge level={severity} />
        <strong>{title}</strong>
        <span className="actionImpact">Impacto: {impactLabel}</span>
      </div>
      <EvidenceChips items={evidence} />
      <ul className="insightList actionChecklist">
        {checklist.slice(0, 3).map((item, idx) => (
          <li key={`${item}-${idx}`}>{item}</li>
        ))}
      </ul>
      <Link href={detailsHref} className="btn">Ver evidencias</Link>
    </article>
  );
}
