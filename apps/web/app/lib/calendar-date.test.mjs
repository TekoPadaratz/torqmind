import assert from 'node:assert/strict';
import test from 'node:test';

import { buildQuickShortcutRanges, formatBusinessCalendarDate, formatCalendarDate, normalizeCalendarDate } from './calendar-date.mjs';

function rangeById(ranges, id) {
  return ranges.find((entry) => entry.id === id);
}

test('formatCalendarDate keeps the local calendar day stable at night', () => {
  const localNight = new Date(2026, 3, 15, 23, 40, 0);
  assert.equal(formatCalendarDate(localNight), '2026-04-15');
});

test('formatBusinessCalendarDate uses Sao Paulo business day without UTC rollover', () => {
  const utcAfterSaoPauloNight = new Date('2026-04-16T00:30:00.000Z');
  assert.equal(formatBusinessCalendarDate(utcAfterSaoPauloNight), '2026-04-15');
});

test('quick shortcuts keep Hoje and Ontem stable in a Sao Paulo-like late-night scenario', () => {
  const referenceDate = new Date(2026, 3, 15, 23, 40, 0);
  const ranges = buildQuickShortcutRanges(referenceDate);

  assert.deepEqual(rangeById(ranges, 'today')?.range, ['2026-04-15', '2026-04-15']);
  assert.deepEqual(rangeById(ranges, 'yesterday')?.range, ['2026-04-14', '2026-04-14']);
});

test('quick shortcuts keep recent windows and current month on the expected local calendar dates', () => {
  const referenceDate = new Date(2026, 3, 15, 23, 40, 0);
  const ranges = buildQuickShortcutRanges(referenceDate);

  assert.deepEqual(rangeById(ranges, 'last_15_days')?.range, ['2026-04-01', '2026-04-15']);
  assert.deepEqual(rangeById(ranges, 'last_30_days')?.range, ['2026-03-17', '2026-04-15']);
  assert.deepEqual(rangeById(ranges, 'this_month')?.range, ['2026-04-01', '2026-04-15']);
});

test('normalizeCalendarDate accepts real calendar dates and rejects impossible ones', () => {
  assert.equal(normalizeCalendarDate('2026-04-15'), '2026-04-15');
  assert.equal(normalizeCalendarDate('2026-02-31'), null);
});
