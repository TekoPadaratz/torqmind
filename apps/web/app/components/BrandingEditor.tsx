'use client';

import { useEffect, useRef, useState } from 'react';

import { api, apiGet, apiDelete } from '../lib/api';

type BrandingState = {
  background_url?: string | null;
  logo_url?: string | null;
  uses_default?: boolean;
  background_mime_type?: string | null;
  logo_mime_type?: string | null;
};

const ACCEPT = '.jpg,.jpeg,.png,.webp,.gif,image/png,image/jpeg,image/webp,image/gif';
const MAX_MB = 6;

/**
 * Platform > Empresa: "Identidade Visual" editor.
 *
 * Upload/remove the company background and logo. Files are sent as the raw
 * request body (no multipart). The background ambient applies to users of this
 * company; the logo is shown in identification areas without replacing the
 * TorqMind system icon. When nothing is set, the default TorqMind identity is used.
 */
export default function BrandingEditor({ tenantId }: { tenantId: number }) {
  const [branding, setBranding] = useState<BrandingState | null>(null);
  const [busy, setBusy] = useState<string>('');
  const [error, setError] = useState<string>('');
  const [okMsg, setOkMsg] = useState<string>('');
  const bgInput = useRef<HTMLInputElement | null>(null);
  const logoInput = useRef<HTMLInputElement | null>(null);

  async function load() {
    try {
      const data = await apiGet(`/platform/companies/${tenantId}/branding`);
      setBranding(data);
    } catch {
      setBranding(null);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tenantId]);

  async function upload(kind: 'background' | 'logo', file: File) {
    setError('');
    setOkMsg('');
    if (file.size > MAX_MB * 1024 * 1024) {
      setError(`Arquivo acima do limite de ${MAX_MB} MB.`);
      return;
    }
    setBusy(kind);
    try {
      const data = await api
        .post(`/platform/companies/${tenantId}/branding/${kind}`, file, {
          headers: { 'Content-Type': file.type || 'application/octet-stream' },
        })
        .then((r) => r.data);
      setBranding(data);
      setOkMsg(kind === 'background' ? 'Imagem de fundo atualizada.' : 'Logo atualizada.');
    } catch (e: any) {
      setError(e?.response?.data?.detail?.message || 'Falha no envio da imagem.');
    } finally {
      setBusy('');
      if (bgInput.current) bgInput.current.value = '';
      if (logoInput.current) logoInput.current.value = '';
    }
  }

  async function remove(kind: 'background' | 'logo') {
    setError('');
    setOkMsg('');
    setBusy(kind);
    try {
      const data = await apiDelete(`/platform/companies/${tenantId}/branding/${kind}`);
      setBranding(data);
      setOkMsg('Identidade padrão TorqMind restaurada.');
    } catch (e: any) {
      setError(e?.response?.data?.detail?.message || 'Falha ao remover a imagem.');
    } finally {
      setBusy('');
    }
  }

  const usesDefault = !branding || branding.uses_default;

  return (
    <div className="card">
      <div className="platformSectionHead">
        <div>
          <div className="platformSectionEyebrow">Identidade Visual</div>
          <h2>Logo e fundo da empresa</h2>
        </div>
      </div>

      {usesDefault ? (
        <div className="platformFieldHint">Usando identidade padrão TorqMind.</div>
      ) : null}
      {error ? <div className="card errorCard" style={{ marginTop: 8 }}>{error}</div> : null}
      {okMsg ? <div className="platformFieldHint" style={{ color: 'var(--color-positive, #3fb950)' }}>{okMsg}</div> : null}

      <div className="platformFieldHint" style={{ marginTop: 8 }}>
        A imagem de fundo é aplicada como ambientação visual da plataforma para os
        usuários desta empresa. A logo do cliente é exibida em áreas de
        identificação visual, sem substituir o ícone do sistema TorqMind. O ícone
        do navegador permanece TorqMind. Formatos: PNG, JPG, WebP ou GIF (até {MAX_MB} MB).
        Fundo ideal: 1920×1080 ou maior. Logo ideal: PNG/WebP com fundo transparente.
      </div>

      {/* Background */}
      <div style={{ marginTop: 16 }}>
        <strong>Imagem de fundo</strong>
        <div
          style={{
            marginTop: 8,
            height: 120,
            borderRadius: 10,
            border: '1px solid var(--border, #2b2f36)',
            backgroundColor: '#0b0d12',
            backgroundImage: branding?.background_url ? `url("${branding.background_url}")` : 'none',
            backgroundSize: 'cover',
            backgroundPosition: 'center',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#7d8590',
          }}
        >
          {branding?.background_url ? '' : 'Sem imagem de fundo'}
        </div>
        <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
          <input
            ref={bgInput}
            type="file"
            accept={ACCEPT}
            style={{ display: 'none' }}
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) upload('background', f);
            }}
          />
          <button type="button" className="btn" disabled={busy === 'background'} onClick={() => bgInput.current?.click()}>
            {busy === 'background' ? 'Enviando...' : branding?.background_url ? 'Trocar fundo' : 'Enviar fundo'}
          </button>
          {branding?.background_url ? (
            <button type="button" className="btn btnGhost" disabled={busy === 'background'} onClick={() => remove('background')}>
              Remover / restaurar padrão
            </button>
          ) : null}
        </div>
      </div>

      {/* Logo */}
      <div style={{ marginTop: 16 }}>
        <strong>Logo da empresa</strong>
        <div
          style={{
            marginTop: 8,
            height: 96,
            borderRadius: 10,
            border: '1px solid var(--border, #2b2f36)',
            background:
              'repeating-conic-gradient(#11141a 0% 25%, #0b0d12 0% 50%) 50% / 18px 18px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#7d8590',
            padding: 8,
          }}
        >
          {branding?.logo_url ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img src={branding.logo_url} alt="Logo da empresa" style={{ maxHeight: 80, maxWidth: '100%', objectFit: 'contain' }} />
          ) : (
            'Sem logo'
          )}
        </div>
        <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
          <input
            ref={logoInput}
            type="file"
            accept={ACCEPT}
            style={{ display: 'none' }}
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) upload('logo', f);
            }}
          />
          <button type="button" className="btn" disabled={busy === 'logo'} onClick={() => logoInput.current?.click()}>
            {busy === 'logo' ? 'Enviando...' : branding?.logo_url ? 'Trocar logo' : 'Enviar logo'}
          </button>
          {branding?.logo_url ? (
            <button type="button" className="btn btnGhost" disabled={busy === 'logo'} onClick={() => remove('logo')}>
              Remover / restaurar padrão
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
