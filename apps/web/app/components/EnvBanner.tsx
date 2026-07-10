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
    <div
      aria-hidden
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        zIndex: 2147483000,
        pointerEvents: 'none',
        display: 'flex',
        justifyContent: 'center',
      }}
    >
      {/* Thin copper hairline across the very top of the viewport */}
      <div
        style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          height: 3,
          background:
            'linear-gradient(90deg, rgba(184,115,51,0) 0%, #b87333 18%, #f0c28d 50%, #b87333 82%, rgba(184,115,51,0) 100%)',
        }}
      />
      {/* Centered pill */}
      <div
        style={{
          marginTop: 7,
          padding: '4px 15px',
          borderRadius: 999,
          background:
            'linear-gradient(180deg, rgba(13,19,23,0.97), rgba(20,27,33,0.97))',
          border: '1px solid rgba(184,115,51,0.55)',
          boxShadow: '0 8px 22px rgba(0,0,0,0.38)',
          color: '#f0c28d',
          fontSize: 11,
          fontWeight: 700,
          letterSpacing: '0.16em',
          textTransform: 'uppercase',
          fontFamily:
            "'IBM Plex Sans', 'Segoe UI', system-ui, -apple-system, sans-serif",
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          whiteSpace: 'nowrap',
        }}
      >
        <span
          style={{
            width: 7,
            height: 7,
            borderRadius: 999,
            background: '#b87333',
            boxShadow: '0 0 8px #b87333',
          }}
        />
        {label}
      </div>
    </div>
  );
}
