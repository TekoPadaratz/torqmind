# TorqMind Derived Rebuild From STG Runbook

Objetivo: reconstruir apenas camadas derivadas seguras do PostgreSQL a partir da STG canônica, sem apagar STG, sem tocar volumes e sem publicar ClickHouse por este script.

Script principal:

- `deploy/scripts/prod-rebuild-derived-from-stg.sh`

Atalho no Makefile:

- `make prod-rebuild-derived-from-stg FROM_DATE=2025-01-01 ID_EMPRESA=1`
- `make prod-rebuild-derived-from-stg FROM_DATE=2025-01-01 ID_EMPRESA=1 INCLUDE_DIMENSIONS=1`

## Quando usar

- quando `dw.fact_comprovante`, `dw.fact_venda`, `dw.fact_venda_item`, `dw.fact_pagamento_comprovante`, `dw.fact_caixa_turno` ou `dw.fact_financeiro` precisarem ser refeitos desde a STG;
- quando a STG estiver correta, mas o DW estiver inconsistente ou incompleto;
- antes de um `prod-clickhouse-init.sh` ou do modo integrado `--rebuild-dw-from-stg` no homologation apply.

## O que o script faz

1. valida `ENV_FILE`, compose, datas e escopo `ID_EMPRESA`/`ID_FILIAL`;
2. audita a cobertura da STG canônica em `stg.comprovantes` e `stg.itenscomprovantes`;
3. bloqueia o fluxo com `--yes` se a STG nao alcanca `FROM_DATE`, exigindo confirmacao consciente sem automacao cega;
4. purga apenas camadas derivadas seguras no PostgreSQL:
   - `etl.pagamento_comprovante_bridge`
   - `dw.fact_pagamento_comprovante`
   - `dw.fact_venda_item`
   - `dw.fact_venda`
   - `dw.fact_comprovante`
   - `dw.fact_caixa_turno`
   - `dw.fact_financeiro`
  - com `--include-dimensions`, tambem purga dimensoes reconstruiveis do DW em rebuild tenant-wide aberto:
    - `dw.dim_usuario_caixa`
    - `dw.dim_cliente`
    - `dw.dim_funcionario`
    - `dw.dim_produto`
    - `dw.dim_local_venda`
    - `dw.dim_grupo_produto`
    - `dw.dim_filial`
5. roda `deploy/scripts/prod-etl-incremental.sh` com `TRACK=full`, `FORCE_FULL=true`, `FROM_DATE`, `TO_DATE` opcional e `BRANCH_ID` opcional;
6. imprime um snapshot de verificacao STG vs DW dentro da janela solicitada.

Como o runtime scope funciona:

- a migration `072_derived_rebuild_runtime_scope.sql` instala helpers que leem `current_setting('etl.from_date', true)`, `current_setting('etl.to_date', true)`, `current_setting('etl.branch_id', true)` e `current_setting('etl.force_full_scan', true)`;
- quando `etl.force_full_scan=true`, os loaders operacionais relêem a STG dentro da janela pedida, mesmo com watermarks avancados;
- quando `etl.force_full_scan=false`, o comportamento incremental normal continua governado por watermark e hot window;
- em rebuild escopado, os watermarks ficam preservados e o full scan vale apenas para a selecao da janela, nao como reset global do tenant.

Importante:

- o script nao apaga `stg.comprovantes`, `stg.itenscomprovantes` nem `stg.formas_pgto_comprovantes`;
- o script nao publica ClickHouse; use o apply integrado ou rode o bootstrap de ClickHouse depois;
- a origem canônica de vendas permanece `comprovantes`/`itenscomprovantes`; `movprodutos` e `itensmovprodutos` nao entram nesse rebuild.
- `--full-clickhouse` e outra coisa: ele republica ClickHouse a partir do DW atual. O rebuild derivado deste runbook reconstrói primeiro o DW PostgreSQL a partir da STG.

## Uso

Tenant inteiro desde 2025-01-01:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes --id-empresa 1 --from-date 2025-01-01
```

Filial única, janela limitada:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --id-empresa 1 --id-filial 14458 --from-date 2025-01-01 --to-date 2025-03-31
```

Dry-run seguro:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --dry-run --id-empresa 1 --from-date 2025-01-01
```

Tenant inteiro incluindo dimensoes reconstruiveis:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes --include-dimensions --id-empresa 1 --from-date 2025-01-01
```

## Flags

- `--yes`: pula a confirmacao destrutiva principal.
- `--dry-run`: nao deleta nem roda ETL; imprime as contagens que seriam afetadas.
- `--include-dimensions`: inclui purge das dimensoes DW reconstruiveis. So e permitido em rebuild tenant-wide aberto, sem `--id-filial` e sem `--to-date`.
- `--skip-purge`: nao remove fatos derivados antes do ETL.
- `--skip-etl`: nao roda o ETL canônico; util para auditoria/purge isolado.
- `--skip-verify`: nao imprime o snapshot final STG vs DW.
- `--id-empresa <id>`: tenant alvo. Default `1`.
- `--id-filial <id>`: escopo opcional por filial.
- `--from-date <YYYY-MM-DD>`: inicio da janela do rebuild. Default `2025-01-01`.
- `--to-date <YYYY-MM-DD>`: fim opcional da janela.

## Semântica de watermarks

- rebuild tenant-wide e aberto ate o presente:
  - o ETL reseta apenas watermarks derivados do trilho operacional;
  - watermarks de ingestao permanecem preservados.
  - `--include-dimensions` pode ser usado aqui porque os loaders dimensionais e seus watermarks sao reconstruiveis nesse modo.
- rebuild escopado por filial ou `to-date`:
  - o ETL faz full scan controlado apenas dentro da janela;
  - os watermarks do tenant nao sao zerados para evitar salto incorreto em janelas futuras.
  - `--include-dimensions` e bloqueado para evitar purge dimensional sem reset seguro de watermark.

## Integracao com prod-homologation-apply.sh

No fluxo integrado, o apply separa dois conceitos de filial:

- `--id-filial <id>` = escopo de auditoria/validacao (default 14458), usado nos passos de reconcile, semantic audit e history coverage.
- `--rebuild-id-filial <id>` = escopo do rebuild derivado. Quando omitido, reconstrói todas as filiais do tenant.
- `--all-filiais` = alias explícito para rebuild de todas as filiais (conflita com `--rebuild-id-filial`).

Exemplo rebuild total (todas as filiais):

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01 --id-empresa 1 --id-filial 14458
```

Exemplo rebuild de uma filial apenas:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --rebuild-id-filial 14458 --from-date 2025-01-01 --id-empresa 1 --id-filial 14458
```

## Passo seguinte natural

Depois de um rebuild derivado bem-sucedido, publique ClickHouse com uma destas opcoes:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-clickhouse-init.sh
```

ou, no fluxo integrado:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01 --id-empresa 1
```