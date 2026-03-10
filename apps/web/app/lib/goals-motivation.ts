type RankedSeller = {
  rank: number;
  funcionario_nome: string;
  faturamento: number;
  margem: number;
  vendas: number;
  scoreRisco: number;
};

export function buildGoalsMotivation(rows: RankedSeller[]) {
  if (!rows.length) {
    return {
      headline: 'O ranking ganha força assim que a equipe volta a pontuar.',
      subheadline: 'Sem movimento suficiente no período para montar a leitura competitiva.',
      indicators: [
        { label: 'No ranking hoje', value: '0', detail: 'aguardando vendas' },
      ],
    };
  }

  const leader = rows[0];
  const second = rows[1];
  const third = rows[2];
  const fourth = rows[3];
  const fifth = rows[4];

  const leadGap = second ? leader.faturamento - second.faturamento : leader.faturamento;
  const podiumGap = third && fourth ? third.faturamento - fourth.faturamento : 0;
  const chaseGap = fourth && fifth ? fourth.faturamento - fifth.faturamento : 0;
  const top5Sales = rows.slice(0, 5).reduce((sum, row) => sum + row.vendas, 0);
  const avgRisk = rows.reduce((sum, row) => sum + row.scoreRisco, 0) / rows.length;

  let headline = 'Consistencia esta decidindo o ranking comercial.';
  let subheadline = 'Uma boa arrancada agora pode redesenhar o podio antes do fim do turno.';

  if (second && leadGap <= Math.max(300, leader.faturamento * 0.03)) {
    headline = 'A lideranca esta aberta. Um bom turno muda completamente esse podio.';
    subheadline = `A diferenca entre ${leader.funcionario_nome} e ${second.funcionario_nome} segue curta.`;
  } else if (third && fourth && podiumGap <= Math.max(250, third.faturamento * 0.04)) {
    headline = 'A disputa pelo top 3 esta viva e qualquer arrancada muda a conversa.';
    subheadline = 'As posicoes centrais do podio seguem em margem curta de diferenca.';
  } else if (fourth && fifth && chaseGap <= Math.max(200, fourth.faturamento * 0.04)) {
    headline = 'O pelotao de reacao segue forte e o top 5 ainda nao esta fechado.';
    subheadline = 'Pequenos ganhos agora fazem muita diferenca na leitura da reuniao.';
  } else if (avgRisk >= 65) {
    headline = 'O ranking avanca, mas disciplina operacional ainda separa as melhores posicoes.';
    subheadline = 'Quem combinar volume com consistencia deve ganhar vantagem nas proximas horas.';
  } else if (top5Sales >= 120) {
    headline = 'O time entrou em ritmo forte e o ranking virou uma vitrine de performance.';
    subheadline = 'A tendencia e de disputa ate o fim do periodo, com pressao real sobre o podio.';
  }

  return {
    headline,
    subheadline,
    indicators: [
      {
        label: 'Distancia para o lider',
        value: second ? formatCompactCurrency(leadGap) : 'Lider isolado',
        detail: second ? `${second.funcionario_nome} em 2º` : 'sem 2º colocado',
      },
      {
        label: 'Disputa pelo top 3',
        value: third && fourth ? formatCompactCurrency(podiumGap) : 'Sem disputa',
        detail: third && fourth ? `${third.funcionario_nome} x ${fourth.funcionario_nome}` : 'top 3 consolidado',
      },
      {
        label: 'No ranking hoje',
        value: String(rows.length),
        detail: 'colaboradores validos',
      },
      {
        label: 'Vendas do top 5',
        value: String(top5Sales),
        detail: 'negocios fechados',
      },
    ],
  };
}

export function getSellerBadge(row: RankedSeller, rows: RankedSeller[]) {
  if (row.rank === 1) return { label: 'Lider do dia', tone: '#fbbf24' };

  const prev = rows[row.rank - 2];
  const next = rows[row.rank];
  const closeToPrev = prev ? Math.abs(prev.faturamento - row.faturamento) <= Math.max(250, prev.faturamento * 0.04) : false;
  const closeToNext = next ? Math.abs(row.faturamento - next.faturamento) <= Math.max(220, row.faturamento * 0.04) : false;

  if (row.scoreRisco >= 70) return { label: 'Atencao operacional', tone: '#fb7185' };
  if (row.vendas >= 25 || row.faturamento >= rows[0].faturamento * 0.75) return { label: 'Ritmo forte', tone: '#34d399' };
  if (closeToPrev && row.rank <= 3) return { label: 'Pressionando o podio', tone: '#67e8f9' };
  if (closeToNext) return { label: 'Posicao ameaçada', tone: '#f59e0b' };
  return { label: 'Operacao estavel', tone: '#cbd5e1' };
}

function formatCompactCurrency(value: number) {
  const abs = Math.abs(Number(value || 0));
  if (abs >= 1000) return `R$ ${(value / 1000).toFixed(1).replace('.', ',')} mil`;
  return `R$ ${value.toFixed(0).replace('.', ',')}`;
}
