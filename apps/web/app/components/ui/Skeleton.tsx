export default function Skeleton({ height = 72 }: { height?: number }) {
  return <div className="skeleton" style={{ height }} aria-hidden="true" />;
}
