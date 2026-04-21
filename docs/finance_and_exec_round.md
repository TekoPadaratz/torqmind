# Finance And Exec Round

## Resumo executivo desta rodada

- O bloco antes tratado como `Resumo IA` foi auditado e rebaixado conceitualmente para `Briefing executivo`.
- A raiz da fraqueza de pagamentos não estava na UI: o ETL estava ignorando `VALOR_PAGO` do payload da Xpert e gravando `valor = 0` em `dw.fact_pagamento_comprovante`.
- O módulo `Financeiro` estava misturando posição financeira com operação de caixa/turnos. A fronteira foi formalizada e a tela passou a evitar esse ruído.
- A home estava pedindo um payload grande demais para o que realmente usa. Foi criada uma versão compacta do overview para reduzir cálculo e payload em runtime.

## 1. Auditoria do bloco executivo

### Origem atual auditada

- Endpoint: `GET /bi/dashboard/overview`
- Função original: `repos_mart.jarvis_briefing`
- Regra original:
  - comparação `dt_ref` vs `dt_ref - 1`
  - heurísticas simples de faturamento, margem, cancelamentos e recebíveis vencidos
- Problema:
  - leitura curta demais
  - centrada em ontem vs hoje
  - pouco aderente ao que o dono precisa ver no período

### Decisão

- O framing `IA` foi considerado exagerado para o motor atual.
- A home passou a usar `Briefing executivo`, derivado dos sinais fortes já carregados:
  - fraude em risco
  - clientes em risco
  - pressão de caixa
  - insight mais relevante do período

### Estado atual

- Problema de dado: não
- Problema de modelagem: sim, o motor antigo era diário demais para a home
- Problema de UI: sim, o naming vendia mais sofisticação do que o motor realmente entregava

## 2. Auditoria do Financeiro

| Bloco | Fonte | Status | Motivo | Ação recomendada |
| --- | --- | --- | --- | --- |
| KPIs de pagar/receber | `mart.financeiro_vencimentos_diaria` | CONFIÁVEL | Totais e abertos batem com a leitura de vencimentos. | Manter |
| Aging | `mart.finance_aging_daily` | CONFIÁVEL | É hoje a leitura mais forte do módulo. | Dar protagonismo |
| Fluxo por vencimento | `mart.financeiro_vencimentos_diaria` | CONFIÁVEL | Série simples e consistente. | Manter |
| Pagamentos por dia | `mart.agg_pagamentos_diaria` | PARCIAL | STG cheio, mas ETL gravava valor zero por bug de campo. | Corrigir ETL e reconstruir marts |
| Ranking por turno de pagamentos | `mart.agg_pagamentos_turno` | PARCIAL | Mesmo problema de valor + mistura de domínio com operação de caixa. | Rebaixar semanticamente para leitura operacional |
| Anomalias de pagamento | `mart.pagamentos_anomalias_diaria` | NÃO PRONTO | Sem valor monetário confiável, o motor não sustenta alerta útil. | Reprocessar depois da correção de pagamentos |
| Turnos / caixa em aberto | `stg.turnos` | NÃO PRONTO | `stg.turnos` está vazio. Não é problema da tela, é ausência da fonte. | Tirar do Financeiro e tratar como domínio próprio |

## 3. Separação correta dos domínios

### Financeiro

- contas a pagar
- contas a receber
- aging
- vencidos
- exposição de caixa
- concentração de recebíveis
- posição financeira

### Caixa / Turnos / Operação

- turnos
- abertura e fechamento
- divergência por turno
- formas de pagamento por turno
- fechamento operacional
- anomalias de caixa

### Decisão aplicada

- O bloco de turnos saiu do protagonismo do Financeiro.
- A tela financeira passou a afirmar explicitamente que caixa/turnos é outro domínio.
- O ranking por turno de pagamentos foi mantido apenas como leitura auxiliar enquanto o módulo próprio não nasce.

## 4. Xpert: mapeamento da raiz dos dados faltantes

### Turnos

- Dataset configurado: `turnos`
- Fonte no agent: `dbo.TURNOS`
- Destino atual: `stg.turnos`
- Estado no ambiente auditado: `0` linhas
- Campos mínimos esperados:
  - `id_turno`
  - `id_filial`
  - `abertura`
  - `fechamento`
  - `status`
  - `operador`
- O que falta:
  - extração real da Xpert ou habilitação do dataset no agent do cliente

### Formas de pagamento por comprovante

- Dataset configurado: `formas_pgto_comprovantes`
- Fonte no agent: `dbo.FORMAS_PGTO_COMPROVANTES`
- Relação operacional: `dbo.COMPROVANTES`
- Destino atual: `stg.formas_pgto_comprovantes`
- Estado no ambiente auditado:
  - `stg.formas_pgto_comprovantes`: `11.354.326` linhas
  - `mart.agg_pagamentos_diaria`: populada, mas com valor zerado no recorte auditado antes da correção
- Quebra encontrada:
  - o ETL lia `VALOR`, `VLR` e `VLRPAGO`
  - o payload real traz `VALOR_PAGO`
  - consequência: `dw.fact_pagamento_comprovante.valor = 0`

### TIPO_FORMA

- Tabela de-para já existe: `app.payment_type_map`
- Estado:
  - havia taxonomia para `1,2,3,4,5,6,999`
  - o tipo `0` era o mais frequente e estava sem semântica comercial
- Ação desta rodada:
  - mapeamento provisório de `tipo_forma = 0` como `CAIXA_LOCAL`, categoria `DINHEIRO`
- Observação:
  - é um mapeamento operacional seguro para reduzir ruído
  - a taxonomia final ainda deve ser validada com a equipe da Xpert/cliente

## 5. Performance

### Gargalos encontrados

1. `GET /bi/dashboard/overview`
- antes desta rodada a home pedia:
  - KPIs gerais
  - séries
  - insights base
  - insights gerados
  - pagamentos
  - turnos
  - risco
  - score operacional
  - health score
  - briefing
- medição observada:
  - `117,4 ms`
  - payload de `206.264 bytes`
- problema:
  - a home já não usa boa parte disso

2. `GET /bi/finance/overview`
- a tela financeira recebia também `open_cash`
- problema:
  - custo desnecessário para uma leitura que já não pertence ao módulo

3. `GET /bi/customers/overview`
- medição observada:
  - `1.095,5 ms`
- hoje é o overview mais caro entre os principais auditados
- causa provável:
  - combinação de churn, top customers e recorrência anônima em um único request

### Otimizações executadas

- `dashboard/overview` ganhou `compact=true`
  - a home agora pede só:
    - `risk.kpis`
    - `risk.window`
    - `insights_generated`
- `finance/overview` ganhou parâmetros opcionais:
  - `include_series`
  - `include_payments`
  - `include_operational`
- a home passou a chamar `finance/overview` sem séries, sem pagamentos e sem turnos
- a tela financeira passou a chamar `finance/overview` sem o bloco operacional de turnos

### Otimizações recomendadas para a próxima fase

- criar snapshot executivo próprio para a home no banco
- separar `customers/overview` em leitura executiva e leitura analítica
- consolidar anomalias de pagamento somente depois do ETL monetário estabilizado
- criar domínio próprio de `Caixa & Turnos`

## 5.1 Refactor do hot path comprovante/pagamento

- O gargalo real não estava no volume de pagamentos em si.
- A lentidão vinha de reresolver a relação `formas_pgto_comprovantes -> comprovantes` dentro de `etl.load_fact_pagamento_comprovante`, a cada ciclo, sobre histórico profundo de `stg.comprovantes`.
- O desenho novo separa as responsabilidades:
  - `etl.load_fact_comprovante` mantém a resolução canônica do comprovante e sincroniza `etl.pagamento_comprovante_bridge`
  - `etl.load_fact_pagamento_comprovante_detail` lê apenas o delta de pagamentos e faz join tipado pela bridge
  - o upsert final em `dw.fact_pagamento_comprovante` compara `row_hash` sem depender de payload JSON inteira no hot path
- Telemetria adicionada ao step:
  - `candidate_count`
  - `bridge_miss_count`
  - `bridge_resolve_ms`
  - `upsert_inserts`
  - `upsert_updates`
  - `conflict_count`
  - `notification_rows`
  - `notification_ms`
  - `total_ms`

## 5.2 Contrato canônico de anomalias de pagamento

- `mart.pagamentos_anomalias_diaria.insight_id` voltou a ser a chave lógica textual e estável.
- `mart.pagamentos_anomalias_diaria.insight_id_hash` é `bigint` derivado via hash tipado.
- `etl.sync_payment_anomaly_notifications` agora trabalha nesse contrato de forma coerente, sem `COALESCE` misturando `text` e `bigint`.

## 6. O que era problema de dado

- `stg.turnos` vazio
- `dw.fact_pagamento_comprovante.valor` zerado por campo incorreto no ETL
- taxonomia incompleta de `TIPO_FORMA`

## 7. O que era problema de modelagem

- chamar o bloco de `IA` quando a lógica era apenas heurística diária
- misturar `Financeiro` com operação de caixa/turnos
- usar o mesmo overview pesado para a home já enxugada

## 8. O que era problema de UI

- Financeiro exibia turnos como se fossem parte da posição financeira
- bloco executivo da home prometia mais profundidade do que realmente entregava
- pagamentos pareciam quebrados mesmo quando a raiz estava no ETL

## 9. Próxima fase recomendada

1. Fechar extração real de `dbo.TURNOS`
2. Validar taxonomia final de `TIPO_FORMA` com operação/Xpert
3. Criar módulo próprio de `Caixa & Turnos`
4. Separar `customers/overview` em versão executiva mais leve
5. Materializar snapshot executivo da home no banco
