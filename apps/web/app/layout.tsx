import type { Metadata } from "next";
import "./globals.css";
import BrandingApplier from "./components/BrandingApplier";
import EnvBanner from "./components/EnvBanner";
import IntelligenceHost from "./components/intelligence/IntelligenceHost";
import { ThemeProvider } from "./lib/theme";

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

const themeBootScript = `
(function(){
  try {
    var key = 'torqmind.theme';
    var pref = localStorage.getItem(key) || 'dark';
    if (pref !== 'light' && pref !== 'system' && pref !== 'dark') pref = 'dark';
    var resolved = pref === 'system'
      ? (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark')
      : pref;
    document.documentElement.setAttribute('data-theme', resolved);
    document.documentElement.setAttribute('data-theme-preference', pref);
    document.documentElement.style.colorScheme = resolved;
  } catch (e) {
    document.documentElement.setAttribute('data-theme', 'dark');
  }
})();
`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR" data-theme="dark" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeBootScript }} />
      </head>
      <body>
        <ThemeProvider>
          <EnvBanner />
          <BrandingApplier />
          {children}
          <IntelligenceHost />
        </ThemeProvider>
      </body>
    </html>
  );
}
