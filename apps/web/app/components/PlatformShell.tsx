'use client';

import Link from 'next/link';
import Image from 'next/image';
import { usePathname, useRouter } from 'next/navigation';

import { clearAuth } from '../lib/auth';

type PlatformShellProps = {
  title: string;
  subtitle?: string;
  me: any;
  children: React.ReactNode;
};

export default function PlatformShell({ title, subtitle, me, children }: PlatformShellProps) {
  const pathname = usePathname();
  const router = useRouter();

  const links = [
    { href: '/platform', label: 'Resumo', visible: true },
    { href: '/platform/companies', label: 'Empresas', visible: true },
    { href: '/platform/users', label: 'Usuários', visible: true },
    { href: '/platform/notifications', label: 'Notificações', visible: true },
    { href: '/platform/channels', label: 'Canais', visible: Boolean(me?.access?.platform_finance) },
    { href: '/platform/contracts', label: 'Contratos', visible: Boolean(me?.access?.platform_finance) },
    { href: '/platform/receivables', label: 'Contas a Receber', visible: Boolean(me?.access?.platform_finance) },
    { href: '/platform/channel-payables', label: 'Contas de Canal', visible: Boolean(me?.access?.platform_finance) },
    { href: '/platform/audit', label: 'Auditoria', visible: Boolean(me?.access?.platform_finance) },
  ].filter((item) => item.visible);

  return (
    <div className="platformRoot">
      <div className="platformNav">
        <div className="platformBrand">
          <Image src="/brand/Logo_Icone.png" alt="TorqMind" width={28} height={28} priority />
          <div>
            <div className="platformBrandEyebrow">TorqMind Platform</div>
            <div className="platformBrandTitle">Gestão interna da plataforma</div>
          </div>
        </div>

        <div className="platformNavLinks">
          {links.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={`platformNavLink ${pathname === item.href ? 'platformNavLinkActive' : ''}`}
            >
              {item.label}
            </Link>
          ))}
        </div>

        <div className="platformNavActions">
          {me?.access?.product ? (
            <Link className="btn" href="/scope">
              Produto
            </Link>
          ) : null}
          <button
            className="btn"
            onClick={() => {
              clearAuth();
              router.push('/');
            }}
          >
            Sair
          </button>
        </div>
      </div>

      <div className="container">
        <div className="platformHero">
          <div>
            <div className="platformHeroEyebrow">{me?.role_label || me?.user_role || 'Platform'}</div>
            <h1>{title}</h1>
            {subtitle ? <p className="platformSubtitle">{subtitle}</p> : null}
          </div>
          <div className="platformHeroMeta">
            <div className="platformMetaLabel">Usuário</div>
            <div className="platformMetaValue">{me?.name || me?.email}</div>
            <div className="platformMetaHint">{me?.email}</div>
          </div>
        </div>

        {children}
      </div>
    </div>
  );
}
