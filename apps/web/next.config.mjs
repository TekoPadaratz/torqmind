const INTERNAL_API_BASE = (process.env.API_INTERNAL_URL || "http://api:8000").replace(/\/+$/, "");

const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
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
