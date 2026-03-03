'use client';

import Link from 'next/link';
import Image from 'next/image';
import { useEffect, useState } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { clearAuth } from '../lib/auth';
import { apiGet } from '../lib/api';

function buildHref(path: string, params: URLSearchParams) {
  const qp = new URLSearchParams(params.toString());
  // keep only what matters across pages
  const keep = ['dt_ini', 'dt_fim', 'id_filial', 'id_empresa'];
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
  const [unread, setUnread] = useState(0);
  const params = new URLSearchParams(
    typeof window === 'undefined' ? '' : window.location.search
  );

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

  const onLogout = () => {
    clearAuth();
    router.push('/');
  };

  const links = [
    { path: '/dashboard', label: 'Dashboard Geral' },
    { path: '/sales', label: 'Vendas & Stores' },
    { path: '/fraud', label: 'Sistema Anti-Fraude' },
    { path: '/customers', label: 'Análise de Clientes' },
    { path: '/finance', label: 'Financeiro' },
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

      <div className="navRight">
        <div className="navLinks">
          {hrefs.map((l) => (
            <NavLink key={l.path} path={l.path} href={l.href} label={l.label} />
          ))}
        </div>
        <div className="navActions">
          <Link className="btn" href={buildHref('/dashboard', params)} aria-label="Alertas">
            Alertas ({unread})
          </Link>
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
