# Módulo de Caixa

## Auditoria do pipeline

| Tabela Xpert | Status no pipeline | Situação atual | Decisão |
| --- | --- | --- | --- |
| `TURNOS` | Extraída para `stg.turnos` | Estrutura já existia, mas sem ETL próprio e sem leitura reconciliadora de fechamento | Passa a alimentar `dw.fact_caixa_turno` e marts próprias |
| `COMPROVANTES` | Forte | Já consolidada em `dw.fact_comprovante` | Reaproveitada como fonte principal de vendas e cancelamentos do caixa |
| `FORMAS_PGTO_COMPROVANTES` | Forte | Já consolidada em `dw.fact_pagamento_comprovante` | Reaproveitada com taxonomia comercial específica do Caixa |
| `USUARIOS` | Gap real | Não existia no ingest nem em STG/DW | Adicionada ao ingest, STG e DW para identificar o operador do caixa |
| `MOVLCTOS` | Gap real | Não existia no ingest nem em STG | Adicionada ao ingest e STG para preparar a próxima evolução operacional do Caixa |

## Gaps encontrados

- O monitor de caixa existente lia diretamente `stg.turnos` e fazia parsing em runtime.
- `TURNOS` não tinha ETL reconciliador: um caixa aberto poderia continuar “aberto para sempre” no TorqMind se o fechamento não fosse reprocessado.
- `USUARIOS` não estava no pipeline, então o nome do operador do caixa não podia ser exibido de forma confiável.
- `MOVLCTOS` estava fora do pipeline; por isso o módulo de Caixa não podia evoluir para divergências operacionais mais finas.
- O domínio de Caixa estava misturado com antifraude e financeiro, sem marts dedicadas.

## Decisões de modelagem

- Caixa vira domínio próprio, separado de Financeiro.
- `dw.fact_caixa_turno` passa a ser a verdade operacional de abertura/fechamento.
- `dw.dim_usuario_caixa` concentra o nome do operador do caixa via `USUARIOS`.
- As leituras operacionais da tela passam a usar materialized views:
  - `mart.agg_caixa_turno_aberto`
  - `mart.agg_caixa_forma_pagamento`
  - `mart.agg_caixa_cancelamentos`
  - `mart.alerta_caixa_aberto`

## Joins adotados

- Operador do caixa:
  - `TURNOS.ID_USUARIO` ou `TURNOS.ID_USUARIOS`
  - `USUARIOS.ID_USUARIO` + `USUARIOS.ID_FILIAL`
- Vendas e cancelamentos do caixa:
  - `dw.fact_caixa_turno.id_turno = dw.fact_comprovante.id_turno`
  - filtro operacional: `CFOP > 5000`
- Pagamentos do caixa:
  - `dw.fact_caixa_turno.id_turno = dw.fact_pagamento_comprovante.id_turno`
  - origem do vínculo já validada na Xpert:
    - `COMPROVANTES.REFERENCIA = FORMAS_PGTO_COMPROVANTES.ID_REFERENCIA`
    - respeitando `ID_FILIAL` e `ID_DB`

## Regras de negócio implementadas

- Caixa aberto:
  - `ENCERRANTEFECHAMENTO = 0`
- Alerta crítico:
  - caixa aberto há mais de 24 horas
- Vendas do caixa:
  - apenas comprovantes com `CFOP > 5000`
- Cancelamentos:
  - `CANCELADO = 1`
- Taxonomia de meios de pagamento:
  - `0 = DINHEIRO`
  - `1 = PRAZO`
  - `2 = CHEQUE PRE`
  - `3 = CARTÃO DE CRÉDITO`
  - `4 = CARTÃO DE DÉBITO`
  - `5 = CARTA FRETE`
  - `6 = CHEQUE A PAGAR`
  - `7 = CHEQUE A VISTA`
  - `8 = MOEDAS DIFERESAS`
  - `9 = OUTROS PAGOS`
  - `10 = CHEQUE PRÓPRIO`
  - `28 = PIX`
  - demais = `NÃO IDENTIFICADO`

## Regras de ETL incremental

- `TURNOS` não roda em modo append-only.
- O extractor passa a revisitar registros ainda abertos mesmo sem avanço de watermark.
- `dw.fact_caixa_turno` é atualizado por UPSERT; quando a Xpert fecha o turno, o fechamento é reconciliado no TorqMind.
- O watermark continua preservando performance, mas sem abandonar caixas antes abertos.

## Telegram

- Alertas críticos de caixa aberto passam a gerar payload compatível com a infraestrutura já existente de Telegram.
- A integração usa a mesma deduplicação diária do projeto por `insight_id`/hash.
- Se o Telegram ainda não estiver configurado, o pipeline continua operando sem falha.

## Dependências ainda externas

- O módulo nasce funcional mesmo sem `TURNOS` carregados no ambiente atual, mas os cards operacionais ficam naturalmente vazios até a próxima ingestão real da Xpert.
- `MOVLCTOS` já entra no pipeline nesta rodada, mas fica preparado para a próxima evolução de divergências e conferência operacional.
