import assert from 'node:assert/strict';
import test from 'node:test';

import {
  RECORD_PAGE_SIZE,
  clampPage,
  compareTyped,
  pageRange,
  paginateRows,
  sortRecordRows,
  sumFields,
} from './record-grid.mjs';

test('page size canônico é 30', () => {
  assert.equal(RECORD_PAGE_SIZE, 30);
});

test('paginate 0, 1, 29, 30, 31', () => {
  assert.deepEqual(paginateRows([], 1).slice, []);
  assert.equal(paginateRows([{ id: 1 }], 1).total, 1);
  const rows29 = Array.from({ length: 29 }, (_, i) => ({ id: i + 1 }));
  assert.equal(paginateRows(rows29, 1).totalPages, 1);
  assert.equal(paginateRows(rows29, 1).slice.length, 29);
  const rows30 = Array.from({ length: 30 }, (_, i) => ({ id: i + 1 }));
  assert.equal(paginateRows(rows30, 1).totalPages, 1);
  const rows31 = Array.from({ length: 31 }, (_, i) => ({ id: i + 1 }));
  assert.equal(paginateRows(rows31, 1).totalPages, 2);
  assert.equal(paginateRows(rows31, 1).slice.length, 30);
  assert.deepEqual(paginateRows(rows31, 2).slice.map((r) => r.id), [31]);
});

test('filtro reduz páginas e clamp volta para última válida', () => {
  assert.equal(clampPage(5, 31, 30), 2);
  assert.equal(clampPage(9, 10, 30), 1);
  const { from, to } = pageRange(2, 30, 31);
  assert.deepEqual({ from, to }, { from: 31, to: 31 });
});

test('ordenação numérica, data, acento e nulo por último', () => {
  const nums = sortRecordRows(
    [{ v: 10, id: 1 }, { v: 2, id: 2 }, { v: null, id: 3 }],
    { key: 'v', type: 'number', dir: 'asc', getTieId: (r) => r.id },
  );
  assert.deepEqual(nums.map((r) => r.id), [2, 1, 3]);
  const dates = sortRecordRows(
    [{ v: '2026-01-02', id: 1 }, { v: '2026-01-01', id: 2 }],
    { key: 'v', type: 'date', dir: 'desc', getTieId: (r) => r.id },
  );
  assert.equal(dates[0].id, 1);
  assert.ok(compareTyped('Ágata', 'bruno', 'text') < 0);
  const money = sortRecordRows(
    [{ v: 'R$ 1.234,56', id: 1 }, { v: 'R$ 2,00', id: 2 }],
    { key: 'v', type: 'number', dir: 'asc', getTieId: (r) => r.id },
  );
  assert.deepEqual(money.map((r) => r.id), [2, 1]);
});

test('desempate estável por id real, não pelo índice', () => {
  const rows = [
    { nome: 'Ana', id: 20 },
    { nome: 'Ana', id: 3 },
  ];
  const sorted = sortRecordRows(rows, {
    key: 'nome',
    type: 'text',
    dir: 'asc',
    getTieId: (r) => r.id,
  });
  assert.deepEqual(sorted.map((r) => r.id), [3, 20]);
});

test('totais somam só medidas; não inventam conjunto pela página', () => {
  const all = [
    { valor: 10, preco: 2, id: 1 },
    { valor: 5, preco: 9, id: 2 },
    { valor: 1, preco: 1, id: 3 },
  ];
  const page = paginateRows(all, 1, 2).slice;
  assert.deepEqual(sumFields(page, ['valor']), { valor: 15 });
  assert.deepEqual(sumFields(all, ['valor']), { valor: 16 });
});
