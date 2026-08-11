import assert from 'node:assert/strict';
import test from 'node:test';

import { formatBusinessCalendarDate } from './calendar-date.mjs';
import { buildBrowserLocalDefaultScope } from './local-scope-defaults.mjs';

test('browser local default scope keeps the browser calendar day even when server_today is ahead', () => {
  const scope = buildBrowserLocalDefaultScope({
    id_empresa: 7,
    id_filial: 11,
    server_today: '2026-04-16',
    default_scope: {
      id_empresa: 7,
      id_filial: 11,
      id_filiais: [11],
      days: 7,
      dt_ini: '2026-04-10',
      dt_fim: '2026-04-16',
      dt_ref: '2026-04-16',
      source: 'business_today_default',
    },
  });

  const localToday = new Date();
  const expectedDtFim = formatBusinessCalendarDate(localToday);

  assert.equal(scope.dt_fim, expectedDtFim);
  assert.equal(scope.dt_ref, expectedDtFim);
  assert.equal(scope.id_empresa, '7');
  assert.deepEqual(scope.id_filiais, ['11']);
  assert.equal(scope.source, 'browser_local_default');
});

test('browser local default scope uses Sao Paulo business date after 21h without jumping forward', () => {
  const RealDate = globalThis.Date;
  const fixedNow = new RealDate('2026-04-16T00:30:00.000Z');

  globalThis.Date = class extends RealDate {
    constructor(...args) {
      if (args.length === 0) return new RealDate(fixedNow);
      return new RealDate(...args);
    }

    static now() {
      return fixedNow.getTime();
    }

    static parse(value) {
      return RealDate.parse(value);
    }

    static UTC(...args) {
      return RealDate.UTC(...args);
    }
  };

  try {
    const scope = buildBrowserLocalDefaultScope({ id_empresa: 7, id_filial: 11, default_scope: { days: 1 } });
    assert.equal(scope.dt_fim, '2026-04-15');
    assert.equal(scope.dt_ref, '2026-04-15');
    assert.equal(scope.dt_ini, '2026-04-15');
  } finally {
    globalThis.Date = RealDate;
  }
});

test('browser local default scope derives all accessible branches for company-level sessions', () => {
  const scope = buildBrowserLocalDefaultScope({
    id_empresa: 7,
    id_filial: null,
    accesses: [
      { id_empresa: 7, id_filial: 11 },
      { id_empresa: 7, id_filial: 13 },
      { id_empresa: 7, id_filial: null },
      { id_empresa: 9, id_filial: 21 },
    ],
    default_scope: {
      id_empresa: 7,
      days: 14,
      dt_ini: '2026-04-01',
      dt_fim: '2026-04-14',
      dt_ref: '2026-04-14',
      source: 'business_today_default',
    },
  });

  assert.equal(scope.id_empresa, '7');
  assert.equal(scope.id_filial, null);
  assert.deepEqual(scope.id_filiais, ['11', '13']);
  assert.equal(scope.branch_scope, 'all');
});

test('browser local default scope falls back to tenant_ids[0] for platform sessions', () => {
  const scope = buildBrowserLocalDefaultScope({
    id_empresa: null,
    id_filial: null,
    tenant_ids: [1, 2],
    accesses: [
      { id_empresa: 1, id_filial: 14458 },
      { id_empresa: 1, id_filial: 10169 },
    ],
    default_scope: { days: 30 },
  });

  assert.equal(scope.id_empresa, '1');
  assert.deepEqual(scope.id_filiais, ['10169', '14458']);
  assert.equal(scope.branch_scope, 'all');
});

test('browser local default scope uses branch_scope=all when empresa has no branches yet', () => {
  const scope = buildBrowserLocalDefaultScope({
    id_empresa: null,
    id_filial: null,
    tenant_ids: [1, 2],
    accesses: [],
    default_scope: { days: 30 },
  });

  assert.equal(scope.id_empresa, '1');
  assert.deepEqual(scope.id_filiais, []);
  assert.equal(scope.branch_scope, 'all');
});
