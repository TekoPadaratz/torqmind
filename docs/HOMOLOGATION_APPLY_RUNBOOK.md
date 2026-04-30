# TorqMind Homologation Apply Runbook

Objetivo: aplicar a versao atual do TorqMind em homologacao com um unico comando, sem apagar volumes, sem `down -v`, sem expor segredos e sem cortar a API para o streaming 2.0.

Script principal:

- `deploy/scripts/prod-homologation-apply.sh`

Atalhos no Makefile:

- `make prod-homologation-apply`
- `make prod-homologation-apply-streaming`
- `make prod-rebuild-derived-from-stg FROM_DATE=2025-01-01 ID_EMPRESA=1`

## Uso

ClickHouse completo, sem streaming:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --id-empresa 1 --id-filial 14458
```

ClickHouse completo com streaming em paralelo:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --with-streaming --id-empresa 1 --id-filial 14458
```

Incremental, sem streaming:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --no-streaming
```

Rebuild completo desde a STG, seguido de ClickHouse full dentro do apply:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01 --id-empresa 1 --id-filial 14458
```

Rebuild derivado incluindo dimensoes DW reconstruiveis:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --include-dimensions --from-date 2025-01-01 --id-empresa 1
```

Rebuild somente do PostgreSQL DW, sem republicar ClickHouse:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --allow-dw-only --skip-clickhouse --from-date 2025-01-01 --id-empresa 1
```

Rebuild derivado isolado, sem tocar ClickHouse:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes --id-empresa 1 --from-date 2025-01-01
```

Dry-run seguro:

```bash
ENV_FILE=.env.production.example ./deploy/scripts/prod-homologation-apply.sh --dry-run --full-clickhouse --with-streaming --id-empresa 1 --id-filial 14458
```

Observacao: no host real o log continua em `/home/deploy/logs`. Em workstations onde esse caminho nao for gravavel, o `--dry-run` cai para `${TMPDIR:-/tmp}/torqmind-logs` e avisa explicitamente no inicio.

## Flags disponiveis

- `--yes`: pula confirmacao interativa antes das operacoes pesadas.
- `--dry-run`: imprime todas as etapas e comandos sem executar mutate/build/migrate/apply real.
- `--skip-build`: nao rebuilda nem recria API/Web/Nginx.
- `--skip-migrate`: nao roda `prod-migrate.sh`.
- `--full-clickhouse`: executa `prod-clickhouse-init.sh` e refaz DW nativo + marts completas.
- `--rebuild-dw-from-stg`: purga apenas camadas derivadas seguras no PostgreSQL, roda o ETL canônico desde a STG e força `--full-clickhouse` no mesmo apply.
- `--skip-derived-rebuild`: desabilita explicitamente a etapa acima quando o flag de rebuild nao for desejado.
- `--include-dimensions`: repassa ao rebuild derivado a purge explicita das dimensoes DW reconstruiveis. So use em rebuild tenant-wide aberto.
- `--from-date <YYYY-MM-DD>`: janela inicial do rebuild derivado. Default `2025-01-01`.
- `--to-date <YYYY-MM-DD>`: janela final opcional do rebuild derivado.
- `--allow-dw-only`: permite combinar rebuild derivado com `--skip-clickhouse` para reprocessar apenas o DW PostgreSQL. Nesse modo, audits dependentes de ClickHouse ficam pulados por design.
- `--skip-clickhouse`: nao roda nenhuma etapa de ClickHouse no apply.
- `--with-streaming`: sobe e valida o streaming 2.0 em paralelo ao stack principal.
- `--skip-streaming` e `--no-streaming`: mantem o streaming fora do apply.
- `--streaming-non-blocking`: se o streaming falhar, registra WARN e segue sem derrubar a API atual.
- `--skip-cron`: nao para nem reinstala o cron do host.
- `--skip-audits`: pula reconcile, semantic audit, history coverage e orphan report.
- `--id-empresa <id>`: escopo default para audits e CDC validation. Default `1`.
- `--id-filial <id>`: escopo default para audits. Default `14458`.

## Ordem executada pelo orquestrador

0. Preflight local: raiz do repo, `ENV_FILE`, Docker, daemon, compose, scripts, `chmod +x`, branch, commit e log file.
1. Checagem de seguranca do env: valida ambiente efetivo, placeholders, `API_JWT_SECRET`, `POSTGRES_PASSWORD`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` e `INGEST_REQUIRE_KEY`.
2. Pausa do cron: `sudo systemctl stop cron` e validacao do estado real do servico.
3. Validacao do compose: `docker compose ... config --quiet` para prod e streaming quando solicitado.
4. Build/recreate: rebuild de API/Web e recreate de API/Web/Nginx.
5. Migracoes PostgreSQL: usa o script oficial `deploy/scripts/prod-migrate.sh`.
6. Rebuild derivado opcional: `prod-rebuild-derived-from-stg.sh` audita a cobertura da STG, purga apenas tabelas derivadas seguras e roda `app.cli.etl_incremental --track full --force-full --from-date ... [--to-date] [--branch-id ...]`.
7. ClickHouse: full com `prod-clickhouse-init.sh` ou incremental com `prod-clickhouse-sync-dw.sh` + `prod-clickhouse-refresh-marts.sh`.
8. Audits: `prod-data-reconcile.sh`, `prod-semantic-marts-audit.sh` e, quando presentes, `prod-history-coverage-audit.sh` e `prod-sales-orphans-report.sh`.
9. Streaming opcional: `streaming-init-clickhouse.sh`, `streaming-prepare-postgres.sh`, `streaming-up.sh`, `streaming-register-debezium.sh`, `streaming-validate-cdc.sh` com timeout e `streaming-status.sh`.
10. Limpeza de cache: `TRUNCATE app.snapshot_cache` via container PostgreSQL.
11. Post boot check: `prod-post-boot-check.sh` e probe Python dentro do container API para `settings.use_clickhouse`, `schemas_bi`, `SELECT 1` no ClickHouse e inventory do facade analitico.
12. Cron final: instala o cron oficial com `prod-install-cron.sh`, executa `systemctl enable --now cron` e imprime `crontab -l`.
13. Relatorio final: commit, branch, status do runtime, caminho dos logs e proximos comandos de acompanhamento.

## Quando usar cada modo

Use `--full-clickhouse` quando:

- for a primeira aplicacao em homolog apos restore ou mudanca estrutural;
- o DW nativo/marts precisarem ser refeitos por inteiro;
- uma auditoria tiver pedido rebuild semantico completo.

Semântica exata:

- `--full-clickhouse` reconstrói ClickHouse a partir do DW PostgreSQL que ja existe naquele momento;
- `--rebuild-dw-from-stg` reconstrói primeiro o DW PostgreSQL a partir da STG canônica e depois republica ClickHouse;
- `force_full_scan` nao apaga STG nem vira reset amplo de ingestao: ele so manda os loaders relerem a janela/filial pedida, ignorando watermark apenas na selecao daquele recorte.

Use `--rebuild-dw-from-stg` quando:

- o PostgreSQL DW precisar ser refeito a partir das fontes canônicas `stg.comprovantes`, `stg.itenscomprovantes` e `stg.formas_pgto_comprovantes`;
- voce quiser preservar STG e camadas transacionais, mas reconstruir fatos derivados e republicar depois no ClickHouse;
- a janela historica desejada comecar em `2025-01-01` ou outra data explicita.

Guard rails desse modo:

- autoativa `--full-clickhouse`, exceto quando `--allow-dw-only --skip-clickhouse` forem passados explicitamente;
- rejeita `--skip-migrate`;
- rejeita `--skip-clickhouse` sem `--allow-dw-only`;
- exige que a STG cubra `--from-date` ou confirmacao interativa especifica;
- repassa `--include-dimensions` apenas para rebuild tenant-wide aberto, sem `--id-filial` e sem `--to-date`;
- em rebuild escopado por `--id-filial` ou `--to-date`, preserva watermarks do tenant e faz varredura controlada da janela sem apagamento amplo;
- a migration 072 sobrescreve os loaders SQL do hot path para respeitar `from_date`, `to_date`, `branch_id` e `force_full_scan` diretamente no banco.

Use o modo incremental quando:

- o DW nativo ja estiver saudavel;
- a mudanca for de codigo, API, frontend, cron ou ajustes de sync/janela;
- voce quiser o menor impacto operacional possivel.

Use `--with-streaming` quando:

- quiser validar o stack Redpanda + Debezium + CDC Consumer em paralelo;
- quiser acompanhar lag, raw/current/ops e reconciliacao CDC sem cortar a API atual.

Use `--streaming-non-blocking` quando:

- o objetivo principal for aplicar API/Web/ClickHouse atual;
- o streaming estiver sendo validado como trilho paralelo e nao como gate de liberacao da API.

Importante: o streaming 2.0 continua paralelo. Ele nao corta a API para `torqmind_current` nem reativa `MaterializedPostgreSQL`.

## O que bloqueia e o que vira warning

Bloqueantes por default:

- preflight local;
- env inseguro em ambiente prod/homolog/staging;
- compose invalido;
- falha de build/recreate;
- falha de migrate;
- falha de ClickHouse full/incremental;
- falha em `prod-data-reconcile.sh`;
- falha em `prod-semantic-marts-audit.sh`;
- falha no post-boot check;
- falha ao reinstalar e religar o cron;
- falha do streaming quando `--with-streaming` for usado sem `--streaming-non-blocking`.

Warnings por default:

- env inseguro durante `--dry-run` com arquivo de exemplo;
- `prod-history-coverage-audit.sh` falhando como auditoria complementar;
- `prod-sales-orphans-report.sh`, porque o proprio script ja classifica o caso como WARN de qualidade de dados;
- falha ao limpar `app.snapshot_cache`;
- falha do streaming quando `--streaming-non-blocking` estiver ativo.

## Logs e acompanhamento

Logs principais:

- `/home/deploy/logs/torqmind-homologation-apply-YYYYMMDD_HHMMSS.log`
- `/home/deploy/logs/torqmind-etl-pipeline.log`

Comandos de acompanhamento:

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env ps
tail -f /home/deploy/logs/torqmind-homologation-apply-YYYYMMDD_HHMMSS.log
tail -f /home/deploy/logs/torqmind-etl-pipeline.log
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-status.sh
```

## Rollback basico

1. Se o apply falhar depois da pausa do cron, mantenha o cron parado ate concluir o diagnostico.
2. Revise o log do apply e os logs dos containers antes de qualquer nova tentativa.
3. Se precisar voltar a versao anterior, faca checkout do commit estavel anterior e rerode o mesmo `prod-homologation-apply.sh` com as flags adequadas.
4. Se o problema estiver restrito ao streaming, rerode sem `--with-streaming` ou com `--streaming-non-blocking`; a API atual continua no hot path.
5. Nao use `docker compose down -v` e nao apague volumes de PostgreSQL ou ClickHouse como mecanismo de rollback.