/** Mês de competência YYYYMM no fuso America/Sao_Paulo. Sem mês futuro. */

export function currentAnoMesSP(now = new Date()) {
  const today = now.toLocaleDateString("en-CA", { timeZone: "America/Sao_Paulo" });
  return Number(today.slice(0, 4)) * 100 + Number(today.slice(5, 7));
}

export function fmtAnoMes(am) {
  const n = Number(am);
  if (!Number.isFinite(n) || n < 190001) return "—";
  const s = String(Math.trunc(n)).padStart(6, "0");
  return `${s.slice(4, 6)}/${s.slice(0, 4)}`;
}

export function splitAnoMes(am) {
  const n = Number(am) || 0;
  return { year: Math.floor(n / 100), month: n % 100 };
}

export function joinAnoMes(year, month) {
  return Number(year) * 100 + Number(month);
}

export function clampAnoMes(am, now = currentAnoMesSP()) {
  const n = Number(am);
  if (!Number.isFinite(n) || n < 190001) return now;
  return n > now ? now : n;
}

/** Janela local (mais recente primeiro) + extras da API, nunca além do mês atual. */
export function buildMesesDisponiveis(extra = [], monthsBack = 18, now = currentAnoMesSP()) {
  const set = new Set();
  let y = Math.floor(now / 100);
  let m = now % 100;
  for (let i = 0; i < monthsBack; i += 1) {
    set.add(y * 100 + m);
    m -= 1;
    if (m === 0) {
      m = 12;
      y -= 1;
    }
  }
  for (const raw of extra) {
    const n = Number(raw);
    if (Number.isFinite(n) && n >= 190001 && n <= now) set.add(n);
  }
  return Array.from(set).sort((a, b) => b - a);
}
