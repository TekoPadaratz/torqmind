# TorqMind Release Ops Checklist

Runbook curto para release em producao viva no Ubuntu 24.04 com Docker Compose.

## 1. Pre-deploy

- confirmar backup/logical dump recente do PostgreSQL antes da janela
- revisar `/etc/torqmind/prod.env`
- garantir valores explicitos e nao-placeholder para:
  - `POSTGRES_PASSWORD`
  - `API_JWT_SECRET`
  - `SEED_PASSWORD`
  - `PLATFORM_MASTER_EMAIL`
  - `PLATFORM_MASTER_PASSWORD`
  - `CHANNEL_BOOTSTRAP_EMAIL`
  - `CHANNEL_BOOTSTRAP_PASSWORD`
- confirmar `INGEST_REQUIRE_KEY=true`
- deixar `APP_CORS_ORIGINS` e `APP_CORS_ORIGIN_REGEX` vazios se web+api estiverem no mesmo dominio/nginx
- validar branch/tag da release e registrar hash do commit
- confirmar se existe janela segura para um smoke com `RUN_ETL=1`

## 2. Ordem Canonica De Deploy

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-up.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-migrate.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-seed.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-install-cron.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-post-boot-check.sh
```

Observacoes:

- `prod-seed.sh` agora falha de forma segura se detectar bootstrap inseguro
- `prod-post-boot-check.sh` faz smoke readonly por padrao
- para validar refresh operacional controlado, rode:

```bash
RUN_ETL=1 ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-post-boot-check.sh
```

## 3. Seed Em Producao

- usar apenas `SEED_MODE=master-only`
- nunca rodar seed demo em producao
- o seed nao deve ser usado para criar tenant demo, filial demo ou reciclar dados reais
- o comando nao imprime mais senha bootstrap em claro; guarde as credenciais no vault/processo operacional

## 4. Rollback Mental / Contencao

- se `prod-up.sh` falhar por env inseguro: corrigir `prod.env`, nao force override no compose
- se `prod-migrate.sh` falhar:
  - nao editar migration antiga aplicada
  - corrigir com nova migration ou rollback da release de aplicacao
- se smoke falhar em login/BI:
  - congelar liberacao para usuarios
  - manter cron desligado se o erro indicar inconsistencias de runtime
  - verificar `docker compose ps`, `docker compose logs api web nginx`
- se reconciliacao de dados falhar:
  - nao resetar watermark sem evidencia
  - nao rebuildar snapshot historico sem confirmar escopo/tenant
  - usar `operational_truth` e queries auditaveis antes de qualquer purge/rebuild

## 5. O Que Nao Fazer Em Producao

- nao rodar `make resetdb`
- nao aplicar `torqmind_reset_db_v2.sql`
- nao truncar `stg`, `dw` ou `mart` para “limpar ambiente”
- nao remover `pgdata_prod`
- nao desabilitar `INGEST_REQUIRE_KEY`
- nao trocar secret por placeholder para “subir rapido”
- nao executar seed demo para “testar login”

## 6. Smoke Pos-deploy

O smoke canonico ja esta em:

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-post-boot-check.sh
```

Ele valida:

- docker e cron ativos
- cron do pipeline instalado
- containers em execucao
- `/health` interno e publico
- login do `platform_master`
- `/auth/me`
- selecao efetiva de empresa/filial
- `/bi/sync/status`
- dashboard geral
- vendas
- caixa
- financeiro
- antifraude
- ingest dry-run com `X-Ingest-Key`
- refresh ETL operacional opcional via `RUN_ETL=1`

## 7. Reconciliacao Minima

- vendas do dia e ultimos 7 dias
- formas de pagamento
- comprovantes/cancelamentos
- turnos e caixa aberto
- top clientes
- financeiro basico
- antifraude operacional vs cancelamentos reais

Referencia:

- [docs/reconciliation_queries.md](/home/eko/projects/TorqMind/docs/reconciliation_queries.md)
- [docs/cash_fraud_operational_truth.md](/home/eko/projects/TorqMind/docs/cash_fraud_operational_truth.md)
