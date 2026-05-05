import { formatCurrencyValue } from './currency-format.mjs';
import { buildProductHref } from './product-scope.mjs';

function amount(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function count(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function firstItem(items) {
  return Array.isArray(items) && items.length ? items[0] : null;
}

function normalizeSeverity(level) {
  const normalized = String(level || 'INFO').toUpperCase();
  if (normalized === 'HIGH') return 'CRITICAL';
  if (normalized === 'CRITICAL' || normalized === 'WARN') return normalized;
  return 'INFO';
}

function shortcutForKind(kind, scope) {
  const normalized = String(kind || '').toLowerCase();
  const mapping = {
    cash: { path: '/cash', label: 'Abrir caixa' },
    churn: { path: '/customers', label: 'Abrir clientes' },
    finance: { path: '/finance', label: 'Abrir financeiro' },
    fraud: { path: '/fraud', label: 'Abrir antifraude' },
    payments: { path: '/finance', label: 'Abrir financeiro' },
    pricing: { path: '/pricing', label: 'Abrir preço concorrente' },
  };

  const target = mapping[normalized];
  if (!target) return null;

  return {
    kind: normalized,
    label: target.label,
    path: target.path,
    href: buildProductHref(target.path, scope),
  };
}

function severityRank(level) {
  const normalized = normalizeSeverity(level);
  if (normalized === 'CRITICAL') return 3;
  if (normalized === 'WARN') return 2;
  return 1;
}

function buildHourLabels(hours) {
  const labels = (hours || [])
    .map((item) => item?.label || null)
    .filter(Boolean)
    .slice(0, 3);

  if (!labels.length) return '';
  if (labels.length === 1) return labels[0];
  return `${labels.slice(0, -1).join(', ')} e ${labels.at(-1)}`;
}

function buildSignalHighlights(signals) {
  const highlights = [];
  const peakHours = signals?.peak_hours || {};
  const decliningProducts = signals?.declining_products || {};
  const peakLabels = buildHourLabels(peakHours?.peak_hours);
  const offPeakLabels = buildHourLabels(peakHours?.off_peak_hours);
  const topDecline = Array.isArray(decliningProducts?.items) && decliningProducts.items.length
    ? decliningProducts.items[0]
    : null;

  if (peakLabels) {
    highlights.push(`Picos recentes em ${peakLabels}. ${peakHours?.recommendations?.peak || 'Reforce cobertura e execução nessas horas.'}`);
  }

  if (offPeakLabels) {
    highlights.push(`Horas de menor fluxo em ${offPeakLabels}. ${peakHours?.recommendations?.off_peak || 'Use essa faixa para rotina e reposição.'}`);
  }

  if (topDecline?.produto_nome) {
    const variation = Number(topDecline?.variation_pct || 0).toLocaleString('pt-BR', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
    highlights.push(
      `${topDecline.produto_nome} caiu ${variation}% vs janela anterior. ${topDecline?.recommendation || 'Revise preço, ruptura e execução comercial.'}`,
    );
  }

  return highlights;
}

export function buildPriorityCards({
  fraudImpact,
  fraudCancelamentos,
  modeledRiskKpis,
  churnImpact,
  churnTop,
  cashPressure,
  financeData,
  cashLiveNow,
  scope,
}) {
  const cards = [];
  const modeledImpact = amount(modeledRiskKpis?.impacto_total);
  const highRiskEvents = count(modeledRiskKpis?.eventos_alto_risco);
  const churnLeader = firstItem(churnTop);
  const financeAging = financeData?.aging || {};
  const cashKpis = cashLiveNow?.kpis || {};
  const cashCritical = count(cashKpis?.caixas_criticos);
  const cashOpen = count(cashKpis?.caixas_abertos);
  const totalOpenSales = amount(cashKpis?.total_vendas_abertas);

  if (cashCritical > 0) {
    const shortcut = shortcutForKind('cash', scope);
    cards.push({
      kind: 'cash',
      title: 'Fechar caixas fora da janela segura',
      severity: 'CRITICAL',
      impact: formatCurrencyValue(totalOpenSales),
      sortWeight: totalOpenSales,
      summary: `${cashCritical} caixa(s) crítico(s) concentram ${formatCurrencyValue(totalOpenSales)} em vendas ainda abertas e pedem fechamento antes do próximo pico.`,
      cta: shortcut?.label || 'Abrir caixa',
      href: shortcut?.href || buildProductHref('/cash', scope),
    });
  } else if (cashOpen > 0) {
    const shortcut = shortcutForKind('cash', scope);
    cards.push({
      kind: 'cash',
      title: 'Acompanhar caixas ainda abertos',
      severity: 'WARN',
      impact: formatCurrencyValue(totalOpenSales),
      sortWeight: totalOpenSales,
      summary: `${cashOpen} caixa(s) seguem abertos no momento e merecem acompanhamento para evitar fechamento tardio ou conciliação incompleta.`,
      cta: shortcut?.label || 'Abrir caixa',
      href: shortcut?.href || buildProductHref('/cash', scope),
    });
  }

  if (fraudImpact > 0 || modeledImpact > 0 || highRiskEvents > 0) {
    const shortcut = shortcutForKind('fraud', scope);
    cards.push({
      kind: 'fraud',
      title:
        fraudImpact >= modeledImpact
          ? 'Auditar cancelamentos materiais do período'
          : 'Investigar eventos de alto risco antes do próximo fechamento',
      severity: highRiskEvents >= 5 || fraudImpact >= 1000 ? 'CRITICAL' : 'WARN',
      impact: formatCurrencyValue(Math.max(fraudImpact, modeledImpact)),
      sortWeight: Math.max(fraudImpact, modeledImpact),
      summary:
        fraudImpact >= modeledImpact
          ? `${fraudCancelamentos} cancelamento(s) relevantes somam ${formatCurrencyValue(fraudImpact)} e já justificam auditoria de turno, operador e justificativa.`
          : `${highRiskEvents} evento(s) de alto risco modelado somam ${formatCurrencyValue(modeledImpact)} e pedem revisão imediata do trilho antifraude.`,
      cta: shortcut?.label || 'Abrir antifraude',
      href: shortcut?.href || buildProductHref('/fraud', scope),
    });
  }

  if (cashPressure > 0) {
    const receiving = amount(financeAging?.receber_total_vencido);
    const paying = amount(financeAging?.pagar_total_vencido);
    const shortcut = shortcutForKind('finance', scope);
    cards.push({
      kind: 'finance',
      title: 'Atacar vencidos com maior impacto no caixa',
      severity:
        amount(financeAging?.top5_concentration_pct) >= 50 || receiving >= paying
          ? 'WARN'
          : 'INFO',
      impact: formatCurrencyValue(cashPressure),
      sortWeight: cashPressure,
      summary:
        receiving >= paying
          ? `Receber vencido em ${formatCurrencyValue(receiving)} concentra mais caixa travado que o passivo vencido e deve entrar na régua de cobrança hoje.`
          : `Pagar vencido em ${formatCurrencyValue(paying)} já exige reordenação de pagamentos e renegociação para proteger o caixa operacional.`,
      cta: shortcut?.label || 'Abrir financeiro',
      href: shortcut?.href || buildProductHref('/finance', scope),
    });
  }

  if (churnImpact > 0) {
    const shortcut = shortcutForKind('churn', scope);
    cards.push({
      kind: 'churn',
      title: 'Recuperar clientes que saíram do padrão de retorno',
      severity: count(churnLeader?.churn_score) >= 70 ? 'CRITICAL' : 'WARN',
      impact: formatCurrencyValue(churnImpact),
      sortWeight: churnImpact,
      summary: churnLeader?.cliente_nome
        ? `${churnLeader.cliente_nome} lidera a fila de reativação e ajuda a recuperar ${formatCurrencyValue(churnImpact)} em receita ameaçada.`
        : `${churnTop.length} cliente(s) prioritário(s) já mostram perda de frequência, ticket ou intervalo de recompra.`,
      cta: shortcut?.label || 'Abrir clientes',
      href: shortcut?.href || buildProductHref('/customers', scope),
    });
  }

  return cards
    .sort((left, right) => {
      const severityDelta = severityRank(right.severity) - severityRank(left.severity);
      if (severityDelta !== 0) return severityDelta;
      return amount(right.sortWeight) - amount(left.sortWeight);
    })
    .slice(0, 3);
}

export function buildExecutiveCards({
  fraudImpact,
  fraudCancelamentos,
  modeledRiskKpis,
  cashLiveNow,
  financeData,
  churnData,
  scope,
}) {
  const modeledImpact = amount(modeledRiskKpis?.impacto_total);
  const highRiskEvents = count(modeledRiskKpis?.eventos_alto_risco);
  const financeAging = financeData?.aging || {};
  const receiving = amount(financeAging?.receber_total_vencido);
  const paying = amount(financeAging?.pagar_total_vencido);
  const cashPressure = receiving + paying;
  const churnTop = churnData?.top_risk || [];
  const churnLeader = firstItem(churnTop);
  const churnImpact = churnTop.reduce(
    (sum, item) => sum + amount(item?.revenue_at_risk_30d),
    0,
  );
  const churnCount =
    count(churnData?.summary?.total_top_risk) || count(churnTop.length);
  const cashKpis = cashLiveNow?.kpis || {};
  const openBoxes = cashLiveNow?.open_boxes || [];
  const firstOpenBox = firstItem(openBoxes);
  const criticalBoxes = count(cashKpis?.caixas_criticos);
  const monitoredBoxes = count(cashKpis?.caixas_em_monitoramento) + count(cashKpis?.caixas_alto_risco);
  const totalOpenSales = amount(cashKpis?.total_vendas_abertas);
  const cashOpen = count(cashKpis?.caixas_abertos);
  const cashShortcut = shortcutForKind('cash', scope);
  const fraudShortcut = shortcutForKind('fraud', scope);
  const financeShortcut = shortcutForKind('finance', scope);
  const churnShortcut = shortcutForKind('churn', scope);

  return [
    {
      key: 'cash',
      severity:
        criticalBoxes > 0 ? 'CRITICAL' : cashOpen > 0 || monitoredBoxes > 0 ? 'WARN' : 'INFO',
      section: 'Caixa agora',
      title:
        criticalBoxes > 0
          ? 'Caixa aberto fora da zona segura'
          : cashOpen > 0
            ? 'Caixa pede acompanhamento do agora'
            : 'Caixa sem ruptura relevante agora',
      value: formatCurrencyValue(totalOpenSales),
      detail:
        criticalBoxes > 0
          ? `${criticalBoxes} caixa(s) crítico(s) seguem abertos. ${firstOpenBox?.turno_label ? `${firstOpenBox.turno_label} concentra a maior urgência.` : 'O fechamento precisa acontecer antes do próximo pico.'}`
          : cashOpen > 0
            ? `${cashOpen} caixa(s) seguem abertos e ${monitoredBoxes} já merecem acompanhamento para evitar fechamento tardio.`
            : 'Nenhum caixa aberto ultrapassou a linha de intervenção imediata no monitor do momento.',
      action:
        criticalBoxes > 0
          ? 'Priorize fechamento, conferência de operador e conciliação das vendas mais expostas ainda hoje.'
          : cashOpen > 0
            ? 'Acompanhe os turnos em aberto antes que a janela segura vire exceção operacional.'
            : 'Use o módulo Caixa para acompanhar abertura, conciliação e fechamento conforme o ritmo do dia.',
      cta: cashShortcut?.label || 'Abrir caixa',
      href: cashShortcut?.href || buildProductHref('/cash', scope),
    },
    {
      key: 'fraud',
      severity:
        highRiskEvents >= 5 || fraudImpact >= 1000 ? 'CRITICAL' : fraudImpact > 0 || modeledImpact > 0 ? 'WARN' : 'INFO',
      section: 'Fraude e risco',
      title:
        fraudImpact > 0 || modeledImpact > 0
          ? 'Fraude que exige auditoria'
          : 'Fraude sob controle no período',
      value: formatCurrencyValue(Math.max(fraudImpact, modeledImpact)),
      detail:
        fraudImpact > 0 || modeledImpact > 0
          ? fraudImpact >= modeledImpact
            ? `${fraudCancelamentos} cancelamento(s) relevantes somam ${formatCurrencyValue(fraudImpact)} no período e pedem revisão de turno, operador e justificativa.`
            : `${highRiskEvents} evento(s) de alto risco modelado concentram ${formatCurrencyValue(modeledImpact)} em exposição potencial.`
          : 'Nenhum cancelamento ou evento de alto risco ultrapassou a linha de intervenção imediata.',
      action:
        fraudImpact > 0 || modeledImpact > 0
          ? 'Abra o antifraude e valide o turno mais sensível antes do próximo fechamento.'
          : 'Mantenha a rotina de auditoria amostral para impedir que o desvio cresça silenciosamente.',
      cta: fraudShortcut?.label || 'Abrir antifraude',
      href: fraudShortcut?.href || buildProductHref('/fraud', scope),
    },
    {
      key: 'finance',
      severity: cashPressure > 0 ? 'WARN' : 'INFO',
      section: 'Financeiro',
      title:
        cashPressure > 0
          ? 'Financeiro que exige ação hoje'
          : 'Fluxo financeiro sem pressão material',
      value: formatCurrencyValue(cashPressure),
      detail:
        cashPressure > 0
          ? `Receber vencido em ${formatCurrencyValue(receiving)} e pagar vencido em ${formatCurrencyValue(paying)} já pressionam a liquidez do período.`
          : 'Nenhum vencido relevante superou a linha de atenção no período analisado.',
      action:
        cashPressure > 0
          ? receiving >= paying
            ? 'Ataque os maiores recebíveis vencidos e a filial que concentra mais caixa parado.'
            : 'Reordene pagamentos e renegocie os maiores compromissos vencidos para proteger a semana.'
          : 'Siga acompanhando concentração, atraso e disciplina de cobrança para evitar pressão futura.',
      cta: financeShortcut?.label || 'Abrir financeiro',
      href: financeShortcut?.href || buildProductHref('/finance', scope),
    },
    {
      key: 'customers',
      severity: churnImpact > 0 && count(churnLeader?.churn_score) >= 70 ? 'CRITICAL' : churnImpact > 0 ? 'WARN' : 'INFO',
      section: 'Clientes',
      title:
        churnImpact > 0
          ? 'Receita ameaçada por churn'
          : 'Clientes recorrentes dentro do padrão',
      value: formatCurrencyValue(churnImpact),
      detail:
        churnImpact > 0
          ? churnLeader?.cliente_nome
            ? `${churnLeader.cliente_nome} lidera a fila de reativação. ${churnCount} cliente(s) já saíram do padrão esperado.`
            : `${churnCount} cliente(s) estão na fila de recuperação comercial com impacto potencial no próximo ciclo.`
          : 'Nenhum grupo material de clientes saiu do padrão de retorno no período analisado.',
      action:
        churnImpact > 0
          ? 'Abra Clientes e acione primeiro a carteira prioritária antes do próximo ciclo de compra.'
          : 'Mantenha o CRM ativo e acompanhe frequência, ticket e intervalo de recompra dos principais clientes.',
      cta: churnShortcut?.label || 'Abrir clientes',
      href: churnShortcut?.href || buildProductHref('/customers', scope),
    },
  ];
}

export function buildExecutiveSummary({ overview, scope, priorityCards = [] }) {
  const copiloto = overview?.jarvis || {};
  const explicitHighlights = Array.isArray(copiloto?.highlights)
    ? copiloto.highlights.filter(Boolean)
    : [];
  const signalHighlights = buildSignalHighlights(copiloto?.signals);
  const highlights = [
    ...explicitHighlights.slice(0, 1),
    ...signalHighlights,
    ...explicitHighlights.slice(1),
  ].slice(0, 3);
  const primaryShortcut =
    copiloto?.primary_shortcut?.path
      ? {
          kind: copiloto?.primary_shortcut?.kind || copiloto?.primary_kind || null,
          label: copiloto?.primary_shortcut?.label || 'Abrir detalhe',
          path: copiloto?.primary_shortcut?.path,
          href: buildProductHref(copiloto.primary_shortcut.path, scope),
        }
      : priorityCards[0]
        ? {
            kind: priorityCards[0].kind,
            label: priorityCards[0].cta,
            path: null,
            href: priorityCards[0].href,
          }
        : null;

  const explicitSecondary = Array.isArray(copiloto?.secondary_focus)
    ? copiloto.secondary_focus
        .filter((item) => item?.shortcut_path)
        .map((item) => ({
          kind: item.kind || null,
          label: item.shortcut_label || item.label || 'Abrir detalhe',
          href: buildProductHref(item.shortcut_path, scope),
          impactLabel: item.impact_label || null,
          priority: item.priority || null,
        }))
    : [];

  const fallbackSecondary = priorityCards
    .filter((card, index) => !(primaryShortcut && index === 0))
    .slice(0, 2)
    .map((card) => ({
      kind: card.kind,
      label: card.cta,
      href: card.href,
      impactLabel: card.impact,
      priority: card.severity,
    }));

  return {
    title: copiloto.title || 'Copiloto operacional',
    headline: copiloto.headline || 'Operação estável no período atual.',
    problem:
      copiloto.problem ||
      copiloto.headline ||
      'Sem foco crítico acima da linha de corte.',
    summary: copiloto.summary || 'Sem foco crítico acima da linha de corte.',
    impactLabel: copiloto.impact_label || 'Sem exposição crítica material',
    action: copiloto.action || 'Manter a rotina de acompanhamento diário.',
    priority: copiloto.priority || 'Acompanhar',
    cause:
      copiloto.cause || copiloto.summary || 'Sem desvio material identificado.',
    confidenceLabel: copiloto.confidence_label || 'Alta',
    confidenceReason:
      copiloto.confidence_reason || 'Base pronta e coerente para este período.',
    evidence: Array.isArray(copiloto.evidence) ? copiloto.evidence : [],
    highlights,
    secondaryFocus: Array.isArray(copiloto.secondary_focus)
      ? copiloto.secondary_focus
      : [],
    signals: copiloto?.signals || {},
    secondaryShortcuts: explicitSecondary.length ? explicitSecondary : fallbackSecondary,
    primaryShortcut,
    status: copiloto.status || 'ok',
  };
}
