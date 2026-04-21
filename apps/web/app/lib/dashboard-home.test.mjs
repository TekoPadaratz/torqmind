import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildExecutiveCards,
  buildExecutiveSummary,
  buildPriorityCards,
} from './dashboard-home.mjs';

const scope = {
  dt_ini: '2026-04-08',
  dt_fim: '2026-04-08',
  dt_ref: '2026-04-08',
  id_empresa: '7',
  id_filiais: ['11', '13'],
};

test('executive cards turn bureau states into decision cards with valid shortcuts', () => {
  const cards = buildExecutiveCards({
    fraudImpact: 1450,
    fraudCancelamentos: 4,
    modeledRiskKpis: { impacto_total: 900, eventos_alto_risco: 6 },
    cashLiveNow: {
      kpis: {
        caixas_abertos: 2,
        caixas_criticos: 1,
        caixas_em_monitoramento: 1,
        caixas_alto_risco: 0,
        total_vendas_abertas: 3200,
      },
      open_boxes: [{ turno_label: 'Turno 3' }],
    },
    financeData: { aging: { receber_total_vencido: 2100, pagar_total_vencido: 500 } },
    churnData: {
      summary: { total_top_risk: 3 },
      top_risk: [
        { cliente_nome: 'Transportes Alfa', revenue_at_risk_30d: 1800, churn_score: 82 },
      ],
    },
    scope,
  });

  assert.equal(cards.length, 4);
  assert.equal(cards[0].key, 'cash');
  assert.equal(cards[0].severity, 'CRITICAL');
  assert.equal(cards[0].href, '/cash?dt_ini=2026-04-08&dt_fim=2026-04-08&id_empresa=7&id_filiais=11&id_filiais=13&dt_ref=2026-04-08');
  assert.match(cards[1].title, /Fraude que exige auditoria/);
  assert.match(cards[2].title, /Financeiro que exige ação hoje/);
  assert.match(cards[3].title, /Receita ameaçada por churn/);
});

test('executive cards stay useful when there is no material pressure', () => {
  const cards = buildExecutiveCards({
    fraudImpact: 0,
    fraudCancelamentos: 0,
    modeledRiskKpis: { impacto_total: 0, eventos_alto_risco: 0 },
    cashLiveNow: { kpis: { caixas_abertos: 0, caixas_criticos: 0, total_vendas_abertas: 0 }, open_boxes: [] },
    financeData: { aging: { receber_total_vencido: 0, pagar_total_vencido: 0 } },
    churnData: { summary: { total_top_risk: 0 }, top_risk: [] },
    scope,
  });

  assert.equal(cards[0].title, 'Caixa sem ruptura relevante agora');
  assert.equal(cards[1].title, 'Fraude sob controle no recorte');
  assert.equal(cards[2].title, 'Fluxo financeiro sem pressão material');
  assert.equal(cards[3].title, 'Clientes recorrentes dentro do padrão');
});

test('priority cards rank urgent fronts and preserve action links', () => {
  const cards = buildPriorityCards({
    fraudImpact: 1600,
    fraudCancelamentos: 3,
    modeledRiskKpis: { impacto_total: 700, eventos_alto_risco: 5 },
    churnImpact: 900,
    churnTop: [{ cliente_nome: 'Cliente 1', churn_score: 75 }],
    cashPressure: 1200,
    financeData: { aging: { receber_total_vencido: 1200, pagar_total_vencido: 0, top5_concentration_pct: 52 } },
    cashLiveNow: { kpis: { caixas_criticos: 1, caixas_abertos: 1, total_vendas_abertas: 2400 } },
    scope,
  });

  assert.equal(cards.length, 3);
  assert.equal(cards[0].kind, 'cash');
  assert.equal(cards[0].severity, 'CRITICAL');
  assert.equal(cards[1].kind, 'fraud');
  assert.equal(cards[2].kind, 'churn');
  assert.ok(cards.every((item) => typeof item.href === 'string' && item.href.startsWith('/')));
});

test('executive summary uses explicit jarvis shortcuts when available', () => {
  const summary = buildExecutiveSummary({
    overview: {
      jarvis: {
        headline: 'Cobrar vencidos com maior impacto.',
        summary: 'A carteira vencida já pressiona o caixa.',
        primary_shortcut: { path: '/finance', label: 'Abrir financeiro', kind: 'finance' },
        secondary_focus: [
          {
            label: 'Auditar cancelamentos relevantes',
            impact_label: 'R$ 1.200,00',
            priority: 'Hoje',
            kind: 'fraud',
            shortcut_path: '/fraud',
            shortcut_label: 'Abrir antifraude',
          },
        ],
        highlights: ['Cobrar os 5 maiores vencidos hoje.'],
      },
    },
    scope,
    priorityCards: [],
  });

  assert.equal(summary.primaryShortcut.href, '/finance?dt_ini=2026-04-08&dt_fim=2026-04-08&id_empresa=7&id_filiais=11&id_filiais=13&dt_ref=2026-04-08');
  assert.equal(summary.secondaryShortcuts[0].href, '/fraud?dt_ini=2026-04-08&dt_fim=2026-04-08&id_empresa=7&id_filiais=11&id_filiais=13&dt_ref=2026-04-08');
  assert.equal(summary.highlights[0], 'Cobrar os 5 maiores vencidos hoje.');
});

test('executive summary falls back to priority cards when jarvis shortcut is absent', () => {
  const summary = buildExecutiveSummary({
    overview: { jarvis: { headline: 'Recuperar clientes prioritários.' } },
    scope,
    priorityCards: [
      {
        kind: 'churn',
        cta: 'Abrir clientes',
        href: '/customers?dt_ini=2026-04-08&dt_fim=2026-04-08&id_empresa=7&id_filiais=11&id_filiais=13&dt_ref=2026-04-08',
        severity: 'WARN',
        impact: 'R$ 900,00',
      },
    ],
  });

  assert.equal(summary.primaryShortcut.label, 'Abrir clientes');
  assert.equal(summary.primaryShortcut.href, '/customers?dt_ini=2026-04-08&dt_fim=2026-04-08&id_empresa=7&id_filiais=11&id_filiais=13&dt_ref=2026-04-08');
});

test('executive summary converts structured copilot signals into actionable highlights', () => {
  const summary = buildExecutiveSummary({
    overview: {
      jarvis: {
        headline: 'Operação em acompanhamento.',
        signals: {
          peak_hours: {
            peak_hours: [{ label: '07h' }, { label: '08h' }],
            off_peak_hours: [{ label: '14h' }, { label: '15h' }],
            recommendations: {
              peak: 'Reforce cobertura nessas horas.',
              off_peak: 'Use a faixa para rotina operacional.',
            },
          },
          declining_products: {
            items: [
              {
                produto_nome: 'Diesel S10',
                variation_pct: -18.4,
                recommendation: 'Revise preço de bomba e ruptura.',
              },
            ],
          },
        },
      },
    },
    scope,
    priorityCards: [],
  });

  assert.match(summary.highlights[0], /07h.*08h/i);
  assert.match(summary.highlights[1], /14h.*15h/i);
  assert.match(summary.highlights[2], /Diesel S10/i);
});
