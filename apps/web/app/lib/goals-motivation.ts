import { formatCurrency } from './format';

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

  let headline = 'Consistência está decidindo o ranking comercial.';
  let subheadline = 'Uma boa arrancada agora pode redesenhar o pódio antes do fim do turno.';

  if (second && leadGap <= Math.max(300, leader.faturamento * 0.03)) {
    headline = 'A liderança está aberta. Um bom turno muda completamente esse pódio.';
    subheadline = `A diferença entre ${leader.funcionario_nome} e ${second.funcionario_nome} segue curta.`;
  } else if (third && fourth && podiumGap <= Math.max(250, third.faturamento * 0.04)) {
    headline = 'A disputa pelo top 3 está viva e qualquer arrancada muda a conversa.';
    subheadline = 'As posições centrais do pódio seguem em margem curta de diferença.';
  } else if (fourth && fifth && chaseGap <= Math.max(200, fourth.faturamento * 0.04)) {
    headline = 'O pelotão de reação segue forte e o top 5 ainda não está fechado.';
    subheadline = 'Pequenos ganhos agora fazem muita diferença na leitura da reunião.';
  } else if (avgRisk >= 65) {
    headline = 'O ranking avança, mas disciplina operacional ainda separa as melhores posições.';
    subheadline = 'Quem combinar volume com consistência deve ganhar vantagem nas próximas horas.';
  } else if (top5Sales >= 120) {
    headline = 'O time entrou em ritmo forte e o ranking virou uma vitrine de performance.';
    subheadline = 'A tendência é de disputa até o fim do período, com pressão real sobre o pódio.';
  }

  return {
    headline,
    subheadline,
    indicators: [
      {
        label: 'Distância para o líder',
        value: second ? formatCurrency(leadGap) : 'Líder isolado',
        detail: second ? `${second.funcionario_nome} em 2º` : 'sem 2º colocado',
      },
      {
        label: 'Disputa pelo top 3',
        value: third && fourth ? formatCurrency(podiumGap) : 'Sem disputa',
        detail: third && fourth ? `${third.funcionario_nome} x ${fourth.funcionario_nome}` : 'top 3 consolidado',
      },
      {
        label: 'No ranking hoje',
        value: String(rows.length),
        detail: 'colaboradores válidos',
      },
      {
        label: 'Vendas do top 5',
        value: String(top5Sales),
        detail: 'negócios fechados',
      },
    ],
  };
}

export function getSellerBadge(row: RankedSeller, rows: RankedSeller[]) {
  if (row.rank === 1) return { label: 'Líder do dia', tone: '#fbbf24' };

  const prev = rows[row.rank - 2];
  const next = rows[row.rank];
  const closeToPrev = prev ? Math.abs(prev.faturamento - row.faturamento) <= Math.max(250, prev.faturamento * 0.04) : false;
  const closeToNext = next ? Math.abs(row.faturamento - next.faturamento) <= Math.max(220, row.faturamento * 0.04) : false;

  if (row.scoreRisco >= 70) return { label: 'Atenção operacional', tone: '#fb7185' };
  if (row.vendas >= 25 || row.faturamento >= rows[0].faturamento * 0.75) return { label: 'Ritmo forte', tone: '#34d399' };
  if (closeToPrev && row.rank <= 3) return { label: 'Pressionando o pódio', tone: '#67e8f9' };
  if (closeToNext) return { label: 'Posição ameaçada', tone: '#f59e0b' };
  return { label: 'Operação estável', tone: '#cbd5e1' };
}
