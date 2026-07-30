'use client';

import { useTheme } from '../lib/theme';

function SunIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="12" cy="12" r="4" stroke="currentColor" strokeWidth="1.75" />
      <path
        d="M12 2v2.5M12 19.5V22M4.93 4.93l1.77 1.77M17.3 17.3l1.77 1.77M2 12h2.5M19.5 12H22M4.93 19.07l1.77-1.77M17.3 6.7l1.77-1.77"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
      />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M20 14.5A8.5 8.5 0 0 1 9.5 4 7 7 0 1 0 20 14.5Z"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/**
 * Toggle claro/escuro sempre visível no topo.
 * Clique troca em tempo real (preferência explícita dark|light).
 */
export default function ThemeToggleButton({ className = '' }: { className?: string }) {
  const { resolved, setPreference } = useTheme();
  const isDark = resolved === 'dark';
  const next = isDark ? 'light' : 'dark';

  return (
    <button
      type="button"
      className={`btn themeToggleBtn${className ? ` ${className}` : ''}`}
      onClick={() => setPreference(next)}
      aria-label={isDark ? 'Ativar tema claro' : 'Ativar tema escuro'}
      title={isDark ? 'Mudar para tema claro' : 'Mudar para tema escuro'}
    >
      <span className="themeToggleIcon">{isDark ? <SunIcon /> : <MoonIcon />}</span>
      <span className="themeToggleLabel">{isDark ? 'Claro' : 'Escuro'}</span>
    </button>
  );
}
