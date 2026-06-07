'use client';

import { useEffect } from 'react';

import { apiGet } from '../lib/api';
import { readCachedSession } from '../lib/session';

/**
 * Applies the active company's background image as an ambient layer.
 *
 * The source of truth is the ACTIVE (selected) company, not the raw session:
 * - Single-company users (owner/manager): the session already resolves their
 *   company's branding, so it is applied directly (no extra request).
 * - Internal users (platform_master/platform_admin): their session carries the
 *   default identity (no ``id_empresa``); the active company comes from the
 *   ``torqmind:company`` event the AppNav dispatches when a company is selected.
 *   Session refreshes (``torqmind:session``) and window ``focus`` must NOT wipe
 *   the active company's background back to the default.
 * - Falls back to the default TorqMind identity when no company / no branding.
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

/** Company the session itself resolves branding for (0/null for internal users). */
function sessionCompanyId(session: any): number | null {
  return Number(session?.id_empresa || session?.branding?.id_empresa || 0) || null;
}

export default function BrandingApplier() {
  useEffect(() => {
    // Company currently shown as the background (source of truth).
    let activeCompany: number | null = null;
    // Company whose URL is already applied — avoids refetch on focus/refresh.
    let appliedFor: number | null = null;

    const applyForCompany = async (id: number | null, session: any) => {
      activeCompany = id;
      if (!id) {
        appliedFor = null;
        applyBrandingUrl(null);
        return;
      }
      if (appliedFor === id) return; // already applied, keep it
      // If the session already resolves this exact company, use it (no request).
      if (sessionCompanyId(session) === id && session?.branding) {
        appliedFor = id;
        applyBrandingUrl(session.branding.background_url || null);
        return;
      }
      try {
        const meta = await apiGet(`/branding/${id}`);
        if (activeCompany === id) {
          appliedFor = id;
          applyBrandingUrl(meta?.background_url || null);
        }
      } catch {
        if (activeCompany === id) {
          appliedFor = id;
          applyBrandingUrl(null);
        }
      }
    };

    // A selected company (from AppNav) wins over the session's own company so a
    // session refresh / focus never resets an internal user's chosen background.
    const resolveFromSession = (session: any) => {
      applyForCompany(activeCompany || sessionCompanyId(session), session);
    };

    resolveFromSession(readCachedSession());

    const onSession = (event: Event) => {
      const detail = (event as CustomEvent)?.detail || readCachedSession();
      resolveFromSession(detail);
    };

    const onCompany = (event: Event) => {
      const id = Number((event as CustomEvent)?.detail || 0) || null;
      if (id === activeCompany) return;
      applyForCompany(id, readCachedSession());
    };

    const onFocus = () => resolveFromSession(readCachedSession());

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
