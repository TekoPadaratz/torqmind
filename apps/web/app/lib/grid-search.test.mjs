import assert from 'node:assert/strict';
import test from 'node:test';

import { numericSearchVariants, rowMatchesGridSearch } from './grid-search.mjs';

test('numeric variants aceitam vírgula decimal pt-BR', () => {
  const v = numericSearchVariants('150,00');
  assert.ok(v.includes('150,00'));
  assert.ok(v.includes('150.00') || v.includes('150'));
  assert.ok(v.some((x) => x === '150' || x === '150.0' || x === '150.00'));
});

test('busca 150,00 encontra valor numérico 150', () => {
  assert.equal(rowMatchesGridSearch({ valor: 150, nome: 'x' }, '150,00'), true);
});

test('busca 150,00 encontra R$ formatado', () => {
  assert.equal(rowMatchesGridSearch({ valor: 'R$ 150,00' }, '150,00'), true);
  assert.equal(rowMatchesGridSearch({ valor: 'R$\u00a0150,00' }, '150,00'), true);
});

test('busca 1.234,56 encontra 1234.56', () => {
  assert.equal(rowMatchesGridSearch({ valor: 1234.56 }, '1.234,56'), true);
});

test('busca texto continua case-insensitive', () => {
  assert.equal(rowMatchesGridSearch({ nome: 'Ilson Bueno' }, 'ilson'), true);
  assert.equal(rowMatchesGridSearch({ nome: 'Ilson Bueno' }, 'xyz'), false);
});
