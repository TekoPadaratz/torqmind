'use client';

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div style={{ padding: 48, textAlign: 'center', maxWidth: 480, margin: '80px auto' }}>
      <h2 style={{ fontSize: 20, fontWeight: 600, marginBottom: 12 }}>
        Ocorreu um erro inesperado
      </h2>
      <p style={{ color: '#666', marginBottom: 24 }}>
        {error?.message || 'Não foi possível carregar esta página.'}
      </p>
      <button
        onClick={reset}
        style={{
          padding: '10px 24px',
          borderRadius: 6,
          border: '1px solid #ccc',
          background: '#fff',
          cursor: 'pointer',
          fontSize: 14,
        }}
      >
        Tentar novamente
      </button>
    </div>
  );
}
