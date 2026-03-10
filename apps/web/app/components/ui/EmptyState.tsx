type EmptyStateProps = {
  title: string;
  detail?: string;
};

export default function EmptyState({ title, detail }: EmptyStateProps) {
  return (
    <div className="muted" style={{ padding: '10px 0' }}>
      <strong>{title}</strong>
      {detail ? <div style={{ marginTop: 4 }}>{detail}</div> : null}
    </div>
  );
}
