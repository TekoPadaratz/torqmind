# Caixa e Antifraude: Verdade Operacional

## Arquitetura efetiva

- `stg.turnos` preserva a leitura bruta de `TURNOS`.
- `stg.usuarios` preserva a leitura bruta de `USUARIOS`.
- `dw.fact_caixa_turno` é a verdade operacional de abertura e fechamento do caixa.
- `dw.dim_usuario_caixa` é a dimensão oficial do operador de caixa.
- `dw.fact_comprovante` e `dw.fact_pagamento_comprovante` carregam os comprovantes e pagamentos reconciliados por turno.
- `mart.agg_caixa_turno_aberto` separa o que está realmente ao vivo do que ficou stale na fonte.
- `mart.fraude_cancelamentos_diaria` e `mart.fraude_cancelamentos_eventos` usam a mesma base operacional do Caixa.
- `mart.agg_risco_diaria`, `mart.risco_top_funcionarios_diaria` e `mart.risco_turno_local_diaria` representam apenas a leitura modelada de risco.

## Regras implementadas

- `TURNOS.ENCERRANTEFECHAMENTO = 0` significa caixa aberto.
- `TURNOS.ENCERRANTEFECHAMENTO <> 0` significa caixa fechado.
- Operador de caixa:
  - fonte primária: `TURNOS.ID_USUARIOS`
  - resolução nominal: `USUARIOS.NOMEUSUARIOS`
  - fallback controlado: nome no payload de `TURNOS`; sem isso, exibir `Operador não identificado`
- Turno exibido na UI:
  - fonte primária: campo de turno amigável no payload da Xpert (`TURNO`, `NO_TURNO`, `NUMTURNO`, similares)
  - fallback aceito: `id_turno` apenas quando representar claramente `1`, `2`, `3` ou `4`
  - qualquer outro identificador fica como metadado interno, não como nome de tela
- Filial exibida na UI:
  - fonte primária: `auth.filiais.nome`
  - fallback controlado: `Filial não identificada`
- Cancelamentos operacionais:
  - usam comprovantes cancelados com `CFOP > 5000`
  - o responsável exibido é o operador de caixa do turno
  - o `id_usuario` do comprovante só entra como fallback quando o turno não resolve o operador
- Antifraude:
  - `operational` mostra o que realmente aconteceu no recorte selecionado
  - `modeled_risk` mostra apenas o que a janela do motor de risco cobre
  - quando a modelagem não cobre o recorte inteiro, a UI avisa isso sem apagar os eventos operacionais
- Turnos stale:
  - continuam abertos na fonte
  - saem da contagem ao vivo quando ficam sem atividade operacional recente
  - permanecem visíveis para diagnóstico e reparo
- Data-base:
  - toda decisão de "hoje" usa `BUSINESS_TIMEZONE` ou override em `BUSINESS_TENANT_TIMEZONES`
  - o host Ubuntu não define sozinho o dia operacional

## CLI operacional

Diagnóstico:

```bash
TENANT_ID=1 make operational-truth-diagnose
TENANT_ID=1 BRANCH_ID=14122 DT_INI=2026-03-01 DT_FIM=2026-03-25 make operational-truth-diagnose
```

Purge cirúrgico do domínio:

```bash
TENANT_ID=1 SCOPE=cash-fraud make operational-truth-purge
TENANT_ID=1 BRANCH_ID=14122 SCOPE=cash INCLUDE_STAGING=1 make operational-truth-purge
```

Rebuild do tenant pela orquestração oficial:

```bash
TENANT_ID=1 REF_DATE=2026-03-25 make operational-truth-rebuild
TENANT_ID=1 REF_DATE=2026-03-25 WITH_RISK=1 make operational-truth-rebuild
```

Validação do alinhamento:

```bash
TENANT_ID=1 DT_INI=2026-03-01 DT_FIM=2026-03-25 make operational-truth-validate
```

## Estratégia de reparo

### 1. Quando usar purge parcial

Use `make operational-truth-purge` quando:

- o problema estiver restrito a caixa, usuários, comprovantes ou pagamentos de um tenant;
- houver divergência entre operador do caixa e cancelamento;
- houver turnos stale ou valores desalinhados, mas o staging ainda estiver íntegro.

### 2. Quando usar `INCLUDE_STAGING=1`

Use purge com staging quando:

- o staging do tenant estiver claramente duplicado ou corrompido;
- a reingestão a partir da fonte Xpert for necessária;
- não houver confiança no material já capturado.

### 3. Quando usar reset completo

Use `make resetdb` apenas em dev/homolog quando:

- a base inteira estiver descartável;
- várias áreas além de caixa/antifraude estiverem comprometidas;
- o objetivo for reconstrução total do ambiente, não reparo cirúrgico.
> **Aviso:** o `make resetdb` é protegido por um guard no `Makefile`; é necessário definir `RESET_CONFIRM=1` no ambiente para confirmar que você entende o caráter destrutivo do comando.

## Pré-requisito do agent

Para manter a semântica correta em produção:

- `datasets.usuarios.enabled = true`
- `datasets.turnos.enabled = true`

Sem `USUARIOS` e `TURNOS`, o TorqMind até consegue mostrar parte do domínio, mas perde a verdade operacional do operador de caixa e passa a depender de fallback.

## Reconciliação

As queries operacionais de conferência para Vendas, Caixa e Antifraude ficam em:

- [docs/reconciliation_queries.md](/home/eko/projects/TorqMind/docs/reconciliation_queries.md)
