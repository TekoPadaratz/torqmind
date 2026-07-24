'use client';

/**
 * Environment banner — shows an unmistakable, on-brand ribbon when the app is
 * running in a non-production environment (e.g. homologation/staging).
 *
 * Controlled by the build-time flag NEXT_PUBLIC_APP_ENV. In production the flag
 * is unset, so this renders nothing. Purely visual (pointer-events: none) — it
 * never intercepts clicks or shifts layout.
 */
const ENV = (process.env.NEXT_PUBLIC_APP_ENV || '').toLowerCase();

const LABELS: Record<string, string> = {
  homolog: 'Homologação · ambiente de testes',
  homologacao: 'Homologação · ambiente de testes',
  staging: 'Staging · ambiente de testes',
  dev: 'Desenvolvimento',
  local: 'Local',
};

export default function EnvBanner() {
  const label = LABELS[ENV];
  if (!label) return null;

  return (
    <div className="envBanner" aria-hidden>
      <div className="envBannerHairline" />
      <div className="envBannerPill">
        <span className="envBannerDot" />
        {label}
      </div>
    </div>
  );
}
