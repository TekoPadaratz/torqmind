import "./globals.css";
export const metadata = { title: "TorqMind", description: "Inteligência operacional para postos" };
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
