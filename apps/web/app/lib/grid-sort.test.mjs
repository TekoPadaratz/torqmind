import assert from 'node:assert/strict';
import test from 'node:test';

import { compareGridRows, sortGridRows } from './grid-sort.mjs';

test('sortGridRows: Filial ASC antes de Data DESC e Nome ASC', () => {
  const rows = [
    { filial: 'B', data: '2026-07-20', nome: 'Zed' },
    { filial: 'A', data: '2026-07-19', nome: 'Ana' },
    { filial: 'A', data: '2026-07-21', nome: 'Bruno' },
    { filial: 'A', data: '2026-07-21', nome: 'Ana' },
  ];
  const sorted = sortGridRows(rows, (r) => r);
  assert.deepEqual(
    sorted.map((r) => `${r.filial}|${r.data}|${r.nome}`),
    [
      'A|2026-07-21|Ana',
      'A|2026-07-21|Bruno',
      'A|2026-07-19|Ana',
      'B|2026-07-20|Zed',
    ],
  );
});

test('compareGridRows: sem filial/data/nome empata (0)', () => {
  assert.equal(compareGridRows({}, {}), 0);
});

test('sortGridRows: só nome ASC quando sem filial/data', () => {
  const sorted = sortGridRows(
    [{ nome: 'Carlos' }, { nome: 'ana' }, { nome: 'Bruno' }],
    (r) => ({ nome: r.nome }),
  );
  assert.deepEqual(
    sorted.map((r) => r.nome),
    ['ana', 'Bruno', 'Carlos'],
  );
});
