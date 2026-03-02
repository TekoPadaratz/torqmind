import type { Metadata } from "next";
import "./globals.css";

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
      <body>{children}</body>
    </html>
  );
}
