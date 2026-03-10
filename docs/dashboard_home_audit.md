# Dashboard Home Audit

## Critério de produto

Pergunta aplicada a cada bloco da home:

> Isso ajuda o dono ou o gerente a agir hoje?

Se a resposta foi `não`, o bloco saiu da área nobre da home.

## Auditoria bloco a bloco

| Bloco | Fonte | Status | Motivo | Decisão |
| --- | --- | --- | --- | --- |
| Escopo ativo | `/bi/dashboard/overview` + `/bi/filiais` | CONFIÁVEL | Datas, filial e empresa estão corretas e ajudam a leitura do recorte. | Manter acima da dobra |
| Card principal de impacto | `/bi/dashboard/overview.risk.kpis` + `/bi/clients/churn` + `/bi/finance/overview.aging` | CONFIÁVEL | Soma sinais fortes e financeiramente relevantes: fraude, churn e pressão de caixa. | Manter acima da dobra |
| KPI `Fraude em risco` | `/bi/dashboard/overview.risk.kpis` | CONFIÁVEL | Base histórica recalculada e impacto estimado consistente. | Manter acima da dobra |
| KPI `Clientes em risco` | `/bi/clients/churn` | CONFIÁVEL | Lista top 10 e receita em risco estão vivas no recorte auditado. | Manter acima da dobra |
| KPI `Caixa sob pressão` | `/bi/finance/overview.aging` | CONFIÁVEL | Aging está consistente e traduz risco financeiro real. | Manter acima da dobra |
| Prioridades do dia | combinação de antifraude, churn e aging | CONFIÁVEL | Hoje a melhor ação vem da combinação dos módulos fortes, não da lista longa de alertas. | Manter acima da dobra com no máximo 2 cards |
| Resumo IA | `/bi/dashboard/overview.jarvis.bullets` | CONFIÁVEL | O resumo está consistente, curto e já orienta ação executiva. | Subir para área nobre |
| Foco operacional | antifraude + churn + financeiro | CONFIÁVEL | Consolida dinheiro, risco e oportunidade em 3 blocos fortes. | Manter logo abaixo da dobra inicial |
| Alertas de notificações | `/bi/notifications` | PARCIAL | A tabela `app.notifications` está vazia no ambiente auditado. | Rebaixar; usar sinais sintéticos só quando necessário |
| Insights gerados detalhados | `app.insights_gerados` | PARCIAL | A base está forte, mas a lista longa polui a home e funciona melhor como insumo de prioridade. | Rebaixar; usar apenas como síntese |
| Monitor de turnos | `/bi/dashboard/overview.open_cash` + `stg.turnos` | NÃO PRONTO | `stg.turnos` está vazio; a leitura operacional ainda não é confiável para a home. | Remover da home |
| Mix de pagamentos | `mart.agg_pagamentos_diaria` | NÃO PRONTO | No recorte auditado, os valores conciliados vieram zerados. | Remover da home |
| Formas em validação | `mart.agg_pagamentos_diaria` | NÃO PRONTO | Sem valor comercial forte enquanto a conciliação estiver parcial. | Remover da home |
| Anomalias de pagamento | `mart.pagamentos_anomalias_diaria` | NÃO PRONTO | Não houve eventos no recorte e a semântica ainda está crua para a home. | Remover da home |
| Recorrência anônima detalhada | `mart.anonymous_retention_daily` | PARCIAL | O sinal existe, mas o detalhamento ainda não sustenta protagonismo na home. | Rebaixar para módulo próprio |

## Hierarquia final da home

### Acima da dobra

1. Escopo ativo
2. Card principal de impacto
3. Três KPIs executivos
   - Fraude em risco
   - Clientes em risco
   - Caixa sob pressão
4. Prioridades do dia
   - até 2 cards curtos e acionáveis
5. Resumo IA com protagonismo

### Logo abaixo da dobra

1. Foco operacional
   - maior foco do período
   - oportunidade de recuperação
   - pressão imediata de caixa

## O que saiu da área nobre

- Monitor de turnos
- Mix de pagamentos
- Formas em validação
- Anomalias de pagamento
- Recorrência anônima detalhada
- Lista longa de alertas e insights

## Evidências do banco no recorte auditado

Recorte auditado: `2025-09-01` a `2025-09-18`

- `fraudeImpacto`: `R$ 1.745.701,87`
- `payments_total`: `R$ 0,00`
- `open_cash.source_status`: `unavailable`
- `open_cash.summary`: `Dados de turnos ainda não chegaram da operação para esta filial.`
- `payments_by_day_nonzero`: `0`
- `notifications_unread`: `0`
- `generated_insights`: presentes e fortes
- `jarvis.bullets`: presentes e úteis para leitura executiva

## Dependências da Xpert para próxima fase

### Turnos / caixa em aberto

- Tabela: `dbo.TURNOS`
- Pipeline atual: `dataset turnos -> stg.turnos`
- Campos a validar:
  - abertura
  - fechamento
  - status
  - filial
  - operador
  - identificador do turno
- Join de produto:
  - `dbo.TURNOS` / `stg.turnos` com `dw.fact_comprovante.id_turno`
- Valor para o produto:
  - permitir monitor real de turno aberto, turno antigo e fechamento fora do prazo

### Pagamentos

- Tabela: `dbo.FORMAS_PGTO_COMPROVANTES`
- Tabela relacionada: `dbo.COMPROVANTES`
- Pipeline atual: `dataset formas_pgto_comprovantes -> stg.formas_pgto_comprovantes`
- Campos a validar:
  - valor monetário conciliado
  - tipo de forma
  - comprovante
  - turno
  - filial
- Join de produto:
  - `stg.formas_pgto_comprovantes` com `dw.fact_comprovante`
- Valor para o produto:
  - fechar mix de pagamentos, ranking financeiro por turno e anomalias com semântica comercial
