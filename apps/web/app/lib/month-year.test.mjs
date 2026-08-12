import test from "node:test";
import assert from "node:assert/strict";

import {
  buildMesesDisponiveis,
  clampAnoMes,
  currentAnoMesSP,
  fmtAnoMes,
  joinAnoMes,
  splitAnoMes,
} from "./month-year.mjs";

test("fmtAnoMes formats YYYYMM as MM/YYYY", () => {
  assert.equal(fmtAnoMes(202607), "07/2026");
  assert.equal(fmtAnoMes(202601), "01/2026");
});

test("split/join anoMes round-trip", () => {
  assert.deepEqual(splitAnoMes(202608), { year: 2026, month: 8 });
  assert.equal(joinAnoMes(2026, 8), 202608);
});

test("buildMesesDisponiveis never includes future months and starts at current", () => {
  const now = 202608;
  const months = buildMesesDisponiveis([202612, 202507], 6, now);
  assert.equal(months[0], now);
  assert.ok(!months.includes(202612));
  assert.ok(months.includes(202507));
  assert.ok(months.every((m) => m <= now));
  assert.deepEqual(months, [...months].sort((a, b) => b - a));
});

test("clampAnoMes drops future values", () => {
  assert.equal(clampAnoMes(202612, 202608), 202608);
  assert.equal(clampAnoMes(202603, 202608), 202603);
});

test("currentAnoMesSP is a 6-digit YYYYMM", () => {
  const n = currentAnoMesSP();
  assert.ok(n >= 202001 && n <= 210012);
});
