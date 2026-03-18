'use client';

import Link from 'next/link';
import Image from 'next/image';
import { useEffect, useMemo, useState } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { clearAuth, getClaims } from '../lib/auth';
import { apiGet } from '../lib/api';

function buildHref(path: string, params: URLSearchParams) {
  const qp = new URLSearchParams(params.toString());
  // keep only what matters across pages
  const keep = ['dt_ini', 'dt_fim', 'dt_ref', 'id_filial', 'id_empresa'];
  const clean = new URLSearchParams();
  for (const k of keep) {
    const v = qp.get(k);
    if (v) clean.set(k, v);
  }
  const qs = clean.toString();
  return qs ? `${path}?${qs}` : path;
}

function NavLink({
  path,
  href,
  label,
}: {
  path: string;
  href: string;
  label: string;
}) {
  const pathname = usePathname();
  const isActive = pathname === path;
  return (
    <Link
      href={href}
      className={`navlink ${isActive ? 'navlinkActive' : ''}`}
    >
      {label}
    </Link>
  );
}

export default function AppNav({
  title,
  userLabel,
}: {
  title: string;
  userLabel?: string;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const [search, setSearch] = useState('');
  const [unread, setUnread] = useState(0);
  const [menuOpen, setMenuOpen] = useState(false);
  const [claims, setClaims] = useState<any>(null);
  const params = useMemo(() => new URLSearchParams(search), [search]);

  useEffect(() => {
    const syncSearch = () => setSearch(window.location.search);
    syncSearch();
    setClaims(getClaims());
    window.addEventListener('popstate', syncSearch);
    return () => window.removeEventListener('popstate', syncSearch);
  }, []);

  useEffect(() => {
    setSearch(window.location.search);
  }, [pathname]);

  useEffect(() => {
    const load = async () => {
      try {
        const qs = new URLSearchParams();
        const idEmpresa = params.get('id_empresa');
        const idFilial = params.get('id_filial');
        if (idEmpresa) qs.set('id_empresa', idEmpresa);
        if (idFilial) qs.set('id_filial', idFilial);
        const res = await apiGet(`/bi/notifications/unread-count?${qs.toString()}`);
        setUnread(Number(res?.unread || 0));
      } catch {
        setUnread(0);
      }
    };
    load();
  }, [params]);

  useEffect(() => {
    setMenuOpen(false);
  }, [pathname, search]);

  const onLogout = () => {
    clearAuth();
    router.push('/');
  };

  const links = [
    { path: '/dashboard', label: 'Dashboard Geral' },
    { path: '/sales', label: 'Vendas & Stores' },
    { path: '/cash', label: 'Caixa' },
    { path: '/fraud', label: 'Sistema Anti-Fraude' },
    { path: '/customers', label: 'Análise de Clientes' },
    { path: '/finance', label: 'Financeiro' },
    { path: '/pricing', label: 'Preço Concorrente' },
    { path: '/goals', label: 'Metas & Equipe' },
  ];

  const hrefs = links.map((l) => ({
    path: l.path,
    href: buildHref(l.path, params),
    label: l.label,
  }));

  return (
    <div className="nav">
      <div className="brand">
        <Image src="/brand/Logo_Icone.png" alt="TorqMind" width={28} height={28} priority />
        <span className="pill">{title}</span>
        {userLabel ? <span className="pill">{userLabel}</span> : null}
      </div>

      <button
        className="navToggle"
        onClick={() => setMenuOpen((current) => !current)}
        aria-label={menuOpen ? 'Fechar navegação' : 'Abrir navegação'}
        aria-expanded={menuOpen}
      >
        <span />
        <span />
        <span />
      </button>

      <div className={`navRight ${menuOpen ? 'navRightOpen' : ''}`}>
        <div className="navLinks">
          {hrefs.map((l) => (
            <NavLink key={l.path} path={l.path} href={l.href} label={l.label} />
          ))}
        </div>
        <div className="navActions">
          <Link className="btn" href={buildHref('/dashboard', params)} aria-label="Alertas">
            Alertas ({unread})
          </Link>
          {claims?.access?.platform ? (
            <Link className="btn" href="/platform">
              Platform
            </Link>
          ) : null}
          <Link className="btn" href="/scope">
            Escopo
          </Link>
          <button className="btn" onClick={onLogout} aria-label="Sair da conta">
            Sair
          </button>
        </div>
      </div>
    </div>
  );
}
