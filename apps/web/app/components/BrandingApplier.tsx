'use client';

import { useEffect } from 'react';

import { apiGet } from '../lib/api';
import { readCachedSession } from '../lib/session';

/**
 * Applies the active company's background image as an ambient layer.
 *
 * - Default: reads ``session.branding.background_url`` (the logged-in user's
 *   own company), versioned/cache-busted by the API.
 * - Multi-company users: on a ``torqmind:company`` event (company switch) it
 *   fetches the lightweight ``/branding/{id}`` metadata and applies that.
 * - Falls back to the default TorqMind identity when a company has no branding.
 * - Never touches the favicon (TorqMind identity stays).
 */
function applyBrandingUrl(url: string | null) {
  if (typeof document === 'undefined') return;
  const root = document.documentElement;
  if (url) {
    root.style.setProperty('--brand-company-bg', `url("${url}")`);
    root.setAttribute('data-company-bg', '1');
  } else {
    root.style.removeProperty('--brand-company-bg');
    root.removeAttribute('data-company-bg');
  }
}

function applyFromSession(session: any) {
  applyBrandingUrl(session?.branding?.background_url || null);
}

export default function BrandingApplier() {
  useEffect(() => {
    const session = readCachedSession();
    applyFromSession(session);

    let activeCompany = session?.branding?.id_empresa || session?.id_empresa || null;

    const onSession = (event: Event) => {
      const detail = (event as CustomEvent)?.detail || readCachedSession();
      applyFromSession(detail);
    };

    const onCompany = async (event: Event) => {
      const id = Number((event as CustomEvent)?.detail || 0);
      if (!id || id === Number(activeCompany)) return;
      activeCompany = id;
      try {
        const meta = await apiGet(`/branding/${id}`);
        applyBrandingUrl(meta?.background_url || null);
      } catch {
        applyBrandingUrl(null);
      }
    };

    const onFocus = () => applyFromSession(readCachedSession());

    window.addEventListener('torqmind:session', onSession as EventListener);
    window.addEventListener('torqmind:company', onCompany as EventListener);
    window.addEventListener('focus', onFocus);
    return () => {
      window.removeEventListener('torqmind:session', onSession as EventListener);
      window.removeEventListener('torqmind:company', onCompany as EventListener);
      window.removeEventListener('focus', onFocus);
    };
  }, []);

  return null;
}
