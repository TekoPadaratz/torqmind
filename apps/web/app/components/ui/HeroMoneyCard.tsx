export default function HeroMoneyCard({
  title,
  subtitle,
  value,
}: {
  title: string;
  subtitle: string;
  value: string;
}) {
  return (
    <section className="heroCard">
      <div className="heroEyebrow">{title}</div>
      <div className="heroValue">{value}</div>
      <div className="heroSubtitle">{subtitle}</div>
    </section>
  );
}
