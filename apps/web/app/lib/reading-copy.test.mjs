import test from 'node:test';
import assert from 'node:assert/strict';

import {
  describeCacheBanner,
  describeCommercialCoverage,
  describeDataFreshness,
  describeChurnCoverage,
  describeFinanceCoverage,
  describeLastSync,
  describeSyncMessage,
  describeServerBaseDate,
  summarizeSnapshotStatus,
  summarizeSourceStatus,
} from './reading-copy.mjs';

test('customer coverage copy avoids technical snapshot jargon', () => {
  const text = describeChurnCoverage({ snapshot_status: 'best_effort', effective_dt_ref: '2026-03-26' });
  assert.ok(text.includes('base mais recente disponível'));
  assert.ok(!text.includes('best_effort'));
  assert.ok(!text.includes('snapshot'));
});

test('finance coverage copy avoids exact/best_effort wording', () => {
  const exact = describeFinanceCoverage({ snapshot_status: 'exact', effective_dt_ref: '2026-03-26' });
  const best = describeFinanceCoverage({ snapshot_status: 'best_effort', effective_dt_ref: '2026-03-24' });
  assert.ok(exact.includes('Financeiro pronto'));
  assert.ok(best.includes('base financeira mais recente disponível'));
  assert.ok(!exact.includes('exact'));
  assert.ok(!best.includes('best_effort'));
});

test('cache banner copy is humanized and direct', () => {
  const text = describeCacheBanner({ source: 'snapshot', message: 'Mostrando a última leitura consolidada.' }, 'dashboard');
  assert.equal(text, 'Mostrando a última leitura consolidada.');
  assert.equal(
    describeCacheBanner({ source: 'fallback', fallback_state: 'operational_current' }, 'vendas'),
    'Mostrando a leitura atual de vendas enquanto os demais detalhes terminam de fechar.',
  );
  assert.equal(summarizeSnapshotStatus('operational_current'), 'Leitura atual do dia');
  assert.equal(summarizeSourceStatus('value_gap'), 'Em atualização');
});

test('data freshness copy explains hybrid operational reads without cache jargon', () => {
  assert.equal(
    describeDataFreshness(
      {
        freshness: {
          mode: 'hybrid_live',
          historical_through_dt: '2026-03-29',
          live_through_at: '2026-03-30T18:35:00+00:00',
        },
      },
      'vendas',
    ),
    'Leitura híbrida ativa em vendas: histórico publicado até 29/03/2026 e trilho operacional do dia até 30/03/2026 15:35.',
  );
});

test('commercial coverage copy explains latest compatible fallback without raw iso dates', () => {
  const text = describeCommercialCoverage(
    {
      mode: 'shifted_latest',
      latest_available_dt: '2026-03-31',
      effective_dt_ini: '2026-03-01',
      effective_dt_fim: '2026-03-31',
    },
    'vendas',
  );
  assert.equal(
    text,
    'A base comercial de vendas ainda vai ate 31/03/2026. Mostrando o ultimo periodo comparavel entre 01/03/2026 e 31/03/2026.',
  );
});

test('data freshness prefers commercial coverage message when the requested window is only partially published', () => {
  assert.equal(
    describeDataFreshness(
      {
        commercial_coverage: {
          mode: 'partial_requested',
          effective_dt_fim: '2026-03-31',
        },
      },
      'clientes',
    ),
    'A base comercial de clientes cobre o recorte somente ate 31/03/2026. Os dias posteriores ainda aguardam ingestao da origem.',
  );
});

test('server base date uses pt-BR civil formatting', () => {
  assert.equal(describeServerBaseDate('2026-03-27'), '27/03/2026');
});

test('last sync copy uses pt-BR date and Sao Paulo clock', () => {
  assert.equal(
    describeLastSync({
      available: true,
      last_sync_at: '2026-03-27T08:31:00+00:00',
      operational: { last_sync_at: '2026-03-27T09:31:00+00:00' },
    }),
    '27/03/2026 06:31',
  );
  assert.equal(
    describeLastSync({ available: true, last_sync_at: '2026-03-27T08:31:00+00:00' }),
    '27/03/2026 05:31',
  );
  assert.equal(
    describeLastSync({ available: false, last_sync_at: null }),
    'A primeira base pronta ainda está sendo preparada.',
  );
  assert.equal(
    describeSyncMessage({
      operational: { last_sync_at: '2026-03-27T09:31:00+00:00' },
      analytics: { last_sync_at: '2026-03-27T08:31:00+00:00' },
    }),
    'Trilho operacional em 27/03/2026 06:31. Publicação analítica mais recente em 27/03/2026 05:31.',
  );
});
