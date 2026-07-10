import type { Metadata } from "next";
import "./globals.css";
import BrandingApplier from "./components/BrandingApplier";
import EnvBanner from "./components/EnvBanner";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "TorqMind",
  description: "Inteligência operacional para postos",
  icons: {
    icon: "/brand/Logo_Icone.png",
    shortcut: "/brand/Logo_Icone.png",
    apple: "/brand/Logo_Icone.png",
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body>
        <EnvBanner />
        <BrandingApplier />
        {children}
      </body>
    </html>
  );
}
