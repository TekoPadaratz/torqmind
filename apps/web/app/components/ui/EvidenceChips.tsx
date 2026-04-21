export default function EvidenceChips({ items }: { items: string[] }) {
  if (!items.length) return null;
  return (
    <div className="evidenceRow">
      {items.slice(0, 4).map((item, idx) => (
        <span key={`${item}-${idx}`} className="evidenceChip">{item}</span>
      ))}
    </div>
  );
}
