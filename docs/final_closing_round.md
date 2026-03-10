# Final Closing Round

## Itens corrigidos nesta rodada

- `Dashboard Geral` fechado como mesa de comando, com ações do dia mais objetivas, alertas com fallback inteligente e bloco concreto de oportunidades de recuperação.
- `Header` revisado para mobile com navegação colapsável, sem quebra visual em telas pequenas.
- `Sistema Anti-Fraude` reforçado com foco do período, colaborador mais exposto e ponto crítico mais recente.
- `Financeiro` limpo para reduzir ruído quando pagamentos não têm valor conciliado no recorte.
- `Preço da Concorrência` restrito ao contexto correto de combustíveis por filial.
- `Análise de Clientes` com sinais de churn mais operacionais para contexto de posto.
- `Top nav` estabilizado, eliminando recarga repetitiva do contador de alertas.

## Itens ainda dependentes de fonte da Xpert

### Turnos / caixa em aberto

- Estado atual:
  - `stg.turnos` está vazio neste ambiente
  - por isso o monitor não consegue afirmar turno aberto, turno antigo ou turno sem fechamento
- O que falta validar na Xpert:
  - tabela `dbo.TURNOS`
  - campos de abertura no payload do dataset `turnos`
  - campos de fechamento no payload do dataset `turnos`
  - status do turno
  - vínculo com filial e operador
- Join esperado no pipeline:
  - `dbo.TURNOS` -> `stg.turnos`
  - chave operacional por empresa/filial/turno
  - correlação posterior com `dw.fact_comprovante.id_turno`
- Valor para o produto:
  - fechar uma das dores mais fortes do dono e do gerente

### Pagamentos

- Estado atual:
  - `mart.agg_pagamentos_diaria` e `mart.agg_pagamentos_turno` existem e estão populadas
  - no recorte auditado, os valores conciliados vieram zerados
  - `mart.pagamentos_anomalias_diaria` está vazio
- O que precisa ser confirmado na Xpert:
  - tabela `dbo.FORMAS_PGTO_COMPROVANTES`
  - vínculo com `dbo.COMPROVANTES`
  - valor monetário conciliado por forma
  - vínculo consistente com turno
  - códigos `TIPO_FORMA` para mapeamento comercial
- Join esperado no pipeline:
  - `dbo.FORMAS_PGTO_COMPROVANTES` -> `stg.formas_pgto_comprovantes`
  - combinação por empresa/filial/referência e `tipo_forma`
  - ligação com `dw.fact_comprovante` para data, turno e comprovante
- Valor para o produto:
  - fechar mix de pagamento, ranking por turno e motor de anomalias financeiras

### Recorrência anônima

- Estado atual:
  - `mart.anonymous_retention_daily` existe e tem base
  - o bloco detalhado por coorte ainda não trouxe amostra útil no recorte auditado
- O que precisa ser confirmado:
  - granularidade ideal por faixa horária, dia da semana e local

## Itens prontos para demo

- Dashboard Geral
- Vendas & Stores
- Sistema Anti-Fraude
- Preço da Concorrência
- Metas & Equipe

## Itens parcialmente prontos

- Financeiro
  - forte em aging e leitura executiva
  - ainda depende de conciliação e categorização melhor de pagamentos
- Análise de Clientes
  - churn já ajuda a agir
  - recorrência anônima ainda depende de leitura mais rica no recorte
- Turnos / caixa em aberto
  - estado premium e honesto
  - aguardando extração real da Xpert

## Próxima fase

- Extrair e mapear turnos reais da Xpert
- Fechar conciliação e categorização de pagamentos
- Evoluir alertas com notificação mais completa por canal
- Dar profundidade ao radar de recorrência anônima

## Telegram

- Estado atual:
  - infraestrutura existe no backend
  - tabelas e serviço de disparo estão implementados
  - há endpoint de teste e integração com geração de insights/notificações
- O que falta para uso pleno em cliente:
  - configuração efetiva por empresa
  - definição operacional de quais eventos sobem para Telegram
  - regra de throttling / deduplicação por janela

### Eventos candidatos a Telegram

- cancelamentos críticos fora do padrão
- anomalias financeiras críticas
- pico de fraude por filial/turno/canal
- turnos abertos críticos, assim que a fonte existir

### Critérios desejados

- severidade `CRITICAL` como padrão mínimo
- deduplicação por empresa, filial, tipo de evento e janela diária
- supressão quando já existir alerta recente equivalente
