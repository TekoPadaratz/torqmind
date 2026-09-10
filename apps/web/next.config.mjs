const INTERNAL_API_BASE = (process.env.API_INTERNAL_URL || "http://api:8000").replace(/\/+$/, "");

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Otimização de imagem: só WebP (sem AVIF). Reduz superfície ligada a
  // GHSA-2xp9-vwfh-vxw4 / pipelines AVIF; branding usa assets estáticos locais.
  images: {
    formats: ["image/webp"],
    dangerouslyAllowSVG: false,
    contentDispositionType: "attachment",
    // Sem remotePatterns: next/image só para assets locais/public.
  },
  async rewrites() {
    // Destino fixo via env de build/runtime — não aceitar host do cliente.
    return [
      {
        source: "/api/:path*",
        destination: `${INTERNAL_API_BASE}/:path*`,
      },
      {
        source: "/docs",
        destination: `${INTERNAL_API_BASE}/docs`,
      },
      {
        source: "/openapi.json",
        destination: `${INTERNAL_API_BASE}/openapi.json`,
      },
      {
        source: "/health",
        destination: `${INTERNAL_API_BASE}/health`,
      },
    ];
  },
};
export default nextConfig;
