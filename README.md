# TorqMind Monorepo (Web + API + DW)

Este repositório entrega um **BI multi-tenant** com:
- `apps/api`  : **FastAPI** (Auth JWT, ingest NDJSON, ETL STG→DW→MART, endpoints BI)
- `apps/web`  : **Next.js** (dashboards: geral, vendas, anti-fraude, clientes, financeiro, metas)
- `sql/migrations`: scripts de inicialização do Postgres (schemas, tabelas, ETL SQL e materialized views)
- `sql/torqmind_reset_db_v2.sql`: script único de reset completo (para dev / homolog)

---

## Rodar local com Docker

1) Garanta que existe um arquivo `.env` na raiz (já vem pronto neste pacote).

2) Suba tudo:

```bash
docker compose up --build
```

Acesse:
- Web: http://localhost:3000
- API: http://localhost:8000/docs
- Postgres: localhost:5432

Para acessar de outra máquina na LAN ou Radmin VPN, use o IP da máquina servidora:
- Web: `http://IP_DO_SERVIDOR:3000`
- API: `http://IP_DO_SERVIDOR:8000/docs`

---

## Deploy de produção em servidor Linux

Estratégia simples para um único servidor Ubuntu via SSH:
- tudo sobe só com Docker Compose;
- não precisa instalar PostgreSQL no host;
- apenas o `nginx` publica porta;
- `web` fica em `/`;
- `api` fica atrás do `nginx` em `/api`, `/docs`, `/openapi.json` e `/health`.

Arquivos de produção:
- `docker-compose.prod.yml`
- `deploy/nginx/default.conf`
- `.env.production.example`
- `deploy/scripts/prod-up.sh`
- `deploy/scripts/prod-migrate.sh`
- `deploy/scripts/prod-logs.sh`
- `deploy/scripts/prod-seed.sh`
- `deploy/scripts/prod-etl-incremental.sh`
- `deploy/scripts/prod-purge-sales-history.sh`

Passo a passo no Linux:

1. Clonar o repositório no servidor.
2. Criar o diretório de ambiente externo e o arquivo `/etc/torqmind/prod.env` a partir do exemplo seguro:

```bash
sudo mkdir -p /etc/torqmind
sudo cp .env.production.example /etc/torqmind/prod.env
sudo chmod 600 /etc/torqmind/prod.env
```

3. Preencher em `/etc/torqmind/prod.env` pelo menos:
- `POSTGRES_PASSWORD`
- `API_JWT_SECRET`
- `SEED_PASSWORD`
- `PLATFORM_MASTER_PASSWORD` se quiser trocar a senha bootstrap do Master real
- `OPENAI_API_KEY` se quiser Jarvis IA ativo
- `TELEGRAM_BOT_TOKEN` se quiser notificações Telegram
- `POSTGRES_SHM_SIZE`, `POSTGRES_SHARED_BUFFERS` e `DB_POOL_MAX_SIZE` conforme a memória do host

4. Subir a stack:

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env up -d --build
```

Ou usar o script:

```bash
./deploy/scripts/prod-up.sh
```

5. Aplicar migrations da release:

```bash
./deploy/scripts/prod-migrate.sh
```

Esse é o caminho canônico para:
- banco novo/vazio;
- banco que já está sob controle de `app.schema_migrations`.

O migrator agora registra histórico em `app.schema_migrations`, calcula checksum por arquivo
e aplica apenas migrations novas. Ele não reexecuta mais a cadeia inteira em toda release.

Se você estiver adotando o migrator em um banco de produção já existente e saudável, mas ainda
sem `app.schema_migrations`, rode uma única vez:

```bash
./deploy/scripts/prod-migrate.sh --baseline-current
```

Esse baseline registra a cadeia atual sem executar SQL. O modo padrão falha de forma segura em
bancos existentes sem histórico para impedir replay de migrations destrutivas como `003_mart_demo.sql`.

Para auditoria/verificação:

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env exec -T postgres \
  psql -U postgres -d TORQMIND -P pager=off -c \
  "SELECT filename, execution_kind, applied_at FROM app.schema_migrations ORDER BY filename;"
```

Para checar apenas o runtime sem aplicar migrations:

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env exec -T api \
  python -m app.cli.migrate --verify-only
```

6. Rodar seed inicial:

```bash
./deploy/scripts/prod-seed.sh
```

Em produção, esse seed primeiro garante o migrate e depois cria/atualiza apenas o usuário
interno real `platform_master`, rebaixa o usuário interno de canal para `channel_admin`
e sincroniza o canal bootstrap. Ele não cria tenant nem filial demo.

7. Rodar um incremental manual de validação:

```bash
./deploy/scripts/prod-etl-incremental.sh
```

Esse é o caminho canônico para atualizar STG→DW→MART de todas as empresas ativas em produção.
O script usa `flock` no host para não sobrepor execuções do cron.
O ciclo incremental canônico agora segue uma única espinha dorsal:
- fase por tenant para STG→DW e captura de mudanças;
- um único refresh global de marts por ciclo, somente quando houver mudança relevante;
- fase pós-refresh por tenant tocado para notificações, insights e snapshots operacionais curtos.
- a orquestração Python executa loaders e pós-refresh por etapas explícitas, com `COMMIT` por etapa e `etl.run_log` visível em tempo real (`running` → `ok`/`failed`), evitando uma transação monolítica única no backbone incremental.

O backfill histórico pesado (`etl.run_operational_snapshot_backfill` / `make backfill-snapshots`) fica reservado para rebuilds dedicados e não faz parte do ciclo normal de 10 minutos.

Antes de habilitar o cron do incremental em produção:
- conclua a primeira carga controlada da trilha comercial curta do tenant;
- rode `make analyze-hot-tables` uma vez após a carga inicial relevante;
- só então habilite o ETL de 10 minutos.

O expurgo diário da trilha comercial curta roda separado do ETL incremental:

```bash
./deploy/scripts/prod-purge-sales-history.sh
```

8. Validar no navegador:
- `http://IP_DO_SERVIDOR/`
- `http://IP_DO_SERVIDOR/docs`
- `http://IP_DO_SERVIDOR/health`

Observações:
- nessa estratégia, a porta pública é apenas a `80`;
- `postgres`, `api` e `web` não ficam expostos diretamente;
- o volume `pgdata_prod` garante persistência do banco dentro do Docker;
- HTTPS pode ser adicionado depois quando houver domínio e certificado.

---

## Fluxo rápido local (3 comandos)

Pré-requisitos:
- Docker Desktop com integração WSL habilitada
- `docker compose` disponível no terminal

1) Subir stack:
```bash
docker compose up --build -d
```

2) Seed de usuários + tenant demo:
```bash
docker compose exec api python -m app.cli.seed
```

3) Carga demo (ingest STG + ETL STG→DW→MART):
```bash
docker compose exec api python -m app.cli.demo_load
```

Depois, acesse:
- Web: http://localhost:3000
- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health
- Debug DB (dev): http://localhost:8000/debug/db

Os containers publicam:
- Web em `0.0.0.0:3000`
- API em `0.0.0.0:8000`

Comandos úteis:

```bash
make logs   # acompanha logs
make migrate   # aplica a cadeia oficial sql/migrations e valida o runtime
make resetdb   # recria o banco via cadeia oficial de migrations (DEV/HOMOLOG)
make etl-incremental   # roda o incremental canônico para tenants ativos
make analyze-hot-tables   # ANALYZE targeted nas tabelas quentes após carga inicial ou manutenção
make purge-sales-history   # expurga histórico comercial curto antigo e refresca marts dependentes
make lint   # valida build do web + compilação Python
make down   # derruba os serviços
make platform-billing-daily   # gera receivables / atualiza overdue do backoffice
```

### Backfill de snapshots executivos

Os snapshots históricos de `churn`, `health score` e `aging financeiro` são persistidos por `dt_ref`.
Os endpoints quentes não zeram mais quando o snapshot exato falta: cada leitura devolve metadata de cobertura com `snapshot_status` / `source_kind`, distinguindo `exact`, `best_effort`, `operational` e `missing`.
O backfill é resumível e registra progresso em:
- `app.snapshot_backfill_runs`
- `app.snapshot_backfill_steps`

Rodar um backfill inicial:

```bash
START_DT=2024-01-01 END_DT=2024-12-31 STEP_DAYS=7 ID_EMPRESA=1 make backfill-snapshots
```

Retomar um backfill interrompido:

```bash
START_DT=2024-01-01 END_DT=2024-12-31 STEP_DAYS=7 ID_EMPRESA=1 make backfill-snapshots-resume
```

No backoffice da empresa, o cadastro manual de novas filiais continua bloqueado.
O slice suportado é a edição operacional de filiais já sincronizadas, preservando nome administrativo, vigência, bloqueio e habilitação sem o ETL sobrescrever essas decisões.

## Política operacional da fase

- `sales_history_days = 365` por tenant, aplicado apenas à trilha comercial curta: `comprovantes`, `movprodutos`, `itensmovprodutos`, `formas_pgto_comprovantes` e fatos/snapshots derivados dessa trilha.
- `default_product_scope_days = 30` por tenant, usado no login para montar o escopo padrão do dashboard.
- `clientes`, `contaspagar`, `contasreceber`, `financeiro` e `dw.fact_financeiro` continuam com histórico completo nesta fase.
- `platform_master`, `product_global`, `tenant_admin` e `tenant_manager` entram direto em `/dashboard?...`.
- A navegação principal do produto é feita pelo menu lateral com seleção de empresa, filial e intervalo `de/até`.
- `dt_ref` continua aceito nos links legados e nos snapshots internos, mas deixou de ser campo editável no fluxo principal da UI. A data-base executiva passa a ser a data atual do servidor, com fallback explícito por cobertura.
- Usuários com acesso amplo escolhem empresa/filial no menu lateral; `tenant_manager` permanece travado na própria filial.
- O ingest protege o produto contra histórico comercial antigo demais em `comprovantes` e `movprodutos`, mesmo que o emissor esteja mal configurado.

---

## Seed de usuários e tenant

Depois de subir, rode:

```bash
docker compose exec api python -m app.cli.seed
```

No modo padrão local/dev, cria/atualiza:
- **MASTER REAL**   → `teko94@gmail.com` / `PLATFORM_MASTER_PASSWORD` (padrão: `@Crmjr105`)
- **CANAL INTERNO** → `master@torqmind.com` / `CHANNEL_BOOTSTRAP_PASSWORD` ou `SEED_PASSWORD`
- **OWNER**   → `owner@empresa1.com` / valor definido em `SEED_PASSWORD`  (Empresa 1)
- **MANAGER** → `manager@empresa1.com` / valor definido em `SEED_PASSWORD` (Empresa 1, Filial 1)

Também cria/atualiza o canal bootstrap `Canal TorqMind`, vincula a Empresa 1 demo a ele
e imprime o `ingest_key` da Empresa 1 (útil para o Agent).

No script de produção `./deploy/scripts/prod-seed.sh`, o seed roda em modo `master-only`:
- cria/atualiza `teko94@gmail.com` como `platform_master`
- cria/atualiza `master@torqmind.com` como `channel_admin`
- cria/atualiza o canal bootstrap `Canal TorqMind`
- não cria tenant demo
- não cria filial demo
- roda `prod-migrate.sh` antes do seed para evitar drift de schema

Para trocar essas credenciais no futuro sem SQL manual:
- ajuste `PLATFORM_MASTER_EMAIL`, `PLATFORM_MASTER_PASSWORD`, `CHANNEL_BOOTSTRAP_EMAIL` e `CHANNEL_BOOTSTRAP_PASSWORD` em `/etc/torqmind/prod.env`;
- rode novamente `./deploy/scripts/prod-seed.sh`.

---

## Promoção dev local -> Ubuntu por dump lógico

Fluxo canônico:
- validar primeiro a base real no PostgreSQL local de benchmark;
- gerar dump lógico com `pg_dump -Fc`;
- restaurar no Ubuntu com `pg_restore -j`;
- religar API, web, agent e cron só depois da restauração validada.

Origem validada no benchmark local:

```bash
pg_dump -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -Fc -f torqmind_dev_validado_$(date +%Y%m%d_%H%M%S).dump
```

Destino Ubuntu:

```bash
sudo mkdir -p /etc/torqmind
sudo cp .env.production.example /etc/torqmind/prod.env
sudo chmod 600 /etc/torqmind/prod.env

docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env up -d postgres
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env exec -T postgres \
  psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS TORQMIND;"
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env exec -T postgres \
  psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE TORQMIND;"
cat torqmind_dev_validado.dump | docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env exec -T postgres \
  pg_restore -U postgres -d TORQMIND -j 4 --clean --if-exists
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-migrate.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-seed.sh
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-up.sh
```

Nunca promover via `PGDATA`, cópia física de volume ou cópia do cluster Windows -> Ubuntu.

---

## Carregar dados demo (dashboards cheios)

Se você quiser ver os dashboards funcionando imediatamente (sem integrar SQL Server ainda):

```bash
docker compose exec api python -m app.cli.demo_load
```

Isso gera dados sintéticos em `stg.*` e executa `etl.run_all(1, true, true)`.

---

## Ingestão (NDJSON)

Endpoint:
- `POST /ingest/{dataset}`

Headers recomendados:
- `X-Ingest-Key: <uuid>`  (mapeia para `app.tenants.ingest_key`)

Datasets suportados:
- `filiais`
- `funcionarios`
- `clientes` / `entidades`
- `produtos`
- `grupoprodutos`
- `localvendas`
- `turnos`
- `movprodutos`
- `itensmovprodutos`
- `formas_pgto_comprovantes`
- `comprovantes`
- `contaspagar`
- `contasreceber`
- `financeiro`

### Configuração segura do Agent

No diretório `apps/agent`, use sempre:

```bash
cp config.example.yaml config.local.yaml
```

Em produção Windows, o diretório final do cliente deve conter apenas `config.enc`.  
Use YAML apenas para desenvolvimento local ou migração para `config.enc`.

Para manter a verdade operacional de Caixa e Antifraude:

- `datasets.usuarios.enabled = true`
- `datasets.turnos.enabled = true`

Se `USUARIOS` ou `TURNOS` ficarem desabilitados, o TorqMind perde a resolução correta do operador de caixa e passa a depender de fallback.

---

## Jarvis IA (Responses API) com custo controlado

Endpoints:
- `POST /bi/jarvis/generate?dt_ref=YYYY-MM-DD&id_filial=&id_empresa=&limit=10&force=false`
- `GET /bi/admin/ai-usage?days=30&id_filial=&id_empresa=`

Política:
- IA roda apenas nos top N insights por impacto (configurável via `JARVIS_AI_TOP_N`).
- Cache por hash em `app.insight_ai_cache` para evitar chamadas repetidas.
- Fallback determinístico automático quando a API de IA falhar ou não estiver configurada.

Variáveis de ambiente relevantes:
- `OPENAI_API_KEY`
- `JARVIS_MODEL_FAST` (default `gpt-4.1-mini`)
- `JARVIS_MODEL_STRONG` (default `gpt-4.1`)
- `JARVIS_AI_TOP_N` (default `10`)
- `JARVIS_AI_MAX_OUTPUT_TOKENS` (default `500`)

Pricing:
- custo é por token e varia por modelo ao longo do tempo; mantenha os coeficientes via env:
  - `JARVIS_AI_INPUT_COST_PER_1M`
  - `JARVIS_AI_OUTPUT_COST_PER_1M`

Parâmetros:
- `run_etl=true` (opcional)
- `refresh_mart=true` (se `run_etl=true`)

---

## Dashboards (Web)

O fluxo de produção entra direto em `/dashboard`. O antigo `/scope` virou compatibilidade de links legados e apenas redireciona para o dashboard com os filtros atuais.

O menu lateral concentra:
- navegação entre módulos;
- seleção de empresa para `platform_master` e `product_global`;
- seleção de filial conforme o papel do usuário;
- filtro de período `de/até`.

Semântica executiva padronizada:
- `Dashboard Geral` compõe os cards a partir das mesmas leituras usadas pelos módulos especialistas e exibe cobertura por bloco.
- `Antifraude` separa fraude operacional/cancelamentos de risco modelado.
- `Clientes` usa snapshot exato quando existe e cai para `latest <= dt_ref` ou visão operacional atual com metadata clara.
- `Financeiro` informa se o aging veio de snapshot exato, snapshot best effort ou fallback operacional.
- `Caixa` separa histórico do período filtrado da visão operacional em tempo real.

Páginas:
- `/dashboard` → Dashboard Geral + Jarvis briefing
- `/sales` → Vendas & Stores
- `/fraud` → Sistema Anti-Fraude
- `/customers` → Análise de Clientes

---

## Backoffice de Plataforma

Nova área interna:
- `/platform`

Objetivo:
- gerir empresas/clientes, usuários e acessos;
- configurar Telegram/notificações por usuário;
- gerir canais, contratos, contas a receber e contas a pagar de canal;
- aplicar suspensão e reativação comercial sem misturar essas telas ao produto do cliente.

Perfis:
- `platform_master`: acesso total, incluindo financeiro/comercial, canais, contratos e auditoria global.
- `platform_admin`: gestão operacional de empresas, usuários, acessos e notificações; sem cobrança/comissão.
- `product_global`: acesso a todo o produto e a todas as empresas, sem acesso ao menu/rotas Platform.
- `channel_admin`: acesso à Platform apenas para a própria carteira e acesso ao produto somente para empresas vinculadas ao seu canal; sem financeiro global e sem poderes soberanos.
- `tenant_admin`, `tenant_manager`, `tenant_viewer`: continuam no produto do cliente com validação reforçada de vigência e escopo.

Bootstrap padrão desta release:
- `teko94@gmail.com`: Master real da plataforma (`platform_master`) e soberano explícito da instalação.
- `master@torqmind.com`: usuário interno do canal (`channel_admin`), sem acesso financeiro/comercial global, mas com acesso ao produto quando houver empresas na carteira do canal bootstrap.

Validação de login/sessão:
- usuário deve existir, estar habilitado e dentro da vigência;
- vínculo de acesso deve estar habilitado e válido;
- empresa e filial vinculadas são revalidadas no backend a cada sessão;
- `overdue` e `grace` mantêm login com aviso;
- `suspended_readonly` mantém acesso em modo leitura;
- `suspended_total` bloqueia login do cliente.

Fluxo operacional:
1. cadastrar empresa em `/platform/companies`;
2. instalar/configurar o agent do cliente com a `ingest_key` da empresa;
3. sincronizar `filiais` via ingest/ETL;
4. cadastrar usuários e acessos explícitos por empresa/filial/canal;
5. criar contrato em `/platform/contracts`;
6. gerar cobranças em `/platform/receivables` ou via CLI agendada;
7. marcar `emitido` manualmente;
8. marcar `pago` manualmente;
9. na baixa, o sistema gera automaticamente `billing.channel_payables` quando houver canal/comissão aplicável.

Regras de filial:
- `auth.filiais` usa o mesmo par oficial `id_empresa` + `id_filial` vindo da Xpert.
- O dataset `filiais` entra por ingest, passa no ETL e sincroniza o catálogo operacional de filiais.
- O backoffice pode editar filiais já existentes para nome, CNPJ, habilitação, vigência e bloqueio operacional.
- A criação manual continua bloqueada; novas filiais entram pela origem.
- O ETL continua criando filiais faltantes, mas não sobrescreve o estado administrativo em `auth.filiais`.

Job agendável de billing:

```bash
make platform-billing-daily
```

Exemplo com escopo e data explícitos:

```bash
AS_OF=2026-03-17 COMPETENCE_MONTH=2026-03-01 MONTHS_AHEAD=1 TENANT_ID=1 make platform-billing-daily
```

Wrapper de produção:

```bash
./deploy/scripts/platform-billing-daily.sh
```

Exemplo via `cron` no Ubuntu:

```bash
0 6 * * * cd /opt/torqmind && ENV_FILE=/opt/torqmind/.env COMPOSE_FILE=docker-compose.prod.yml /opt/torqmind/deploy/scripts/platform-billing-daily.sh >> /var/log/torqmind-platform-billing.log 2>&1
```

Exemplo de `systemd`:

```ini
[Unit]
Description=TorqMind Platform Billing Daily
After=docker.service

[Service]
Type=oneshot
WorkingDirectory=/opt/torqmind
Environment=ENV_FILE=/opt/torqmind/.env
Environment=COMPOSE_FILE=docker-compose.prod.yml
ExecStart=/opt/torqmind/deploy/scripts/platform-billing-daily.sh
```

O comando é idempotente: não duplica receivables por competência nem payables por receivable, e já executa o refresh de overdue.

Job agendável de ETL incremental:

```bash
make etl-incremental
```

Trilhos oficiais:

```bash
make etl-operational
make etl-risk
TRACK=full make etl-incremental
```

Rodar manualmente para um tenant específico:

```bash
TENANT_ID=1 make etl-incremental
TENANT_ID=1 make etl-operational
TENANT_ID=1 make etl-risk
```

Wrapper canônico de produção:

```bash
./deploy/scripts/prod-etl-incremental.sh
./deploy/scripts/prod-etl-operational.sh
./deploy/scripts/prod-etl-risk.sh
```

O wrapper:
- roda `python -m app.cli.etl_incremental` dentro do container `api`;
- aceita `--track full|operational|risk` e `--skip-busy-tenants`;
- processa todas as empresas com `app.tenants.is_active = true`, em ordem de `id_empresa`;
- usa a orquestração compartilhada `app.services.etl_orchestrator.run_incremental_cycle(...)`;
- executa fase por tenant, agrega um plano único de refresh e roda o refresh global no máximo uma vez por ciclo completo;
- usa advisory lock por trilho (`operational`, `risk`, `full`) e lock separado por tenant, então:
  - `operational` não dispara `compute_risk_events`;
  - `risk` pode rodar separado do operacional;
  - `full` continua disponível como compatibilidade e toma os dois locks de trilho;
- combina gatilhos `data-driven` e `clock-driven`:
  - rollover diário atualiza `mart.clientes_churn_risco` e snapshots diários de churn, aging e health score quando `ref_date` avança;
  - caixa aberto atualiza `mart.agg_caixa_turno_aberto`, `mart.alerta_caixa_aberto` e a sincronização de notificações mesmo sem ingestão nova;
- usa os watermarks existentes do ETL para o caminho data-driven, então continua incrementalmente de onde parou;
- não substitui o backfill histórico inicial/rebuild: para recomputar janelas antigas ou recuperar lacunas grandes use `etl.run_operational_snapshot_backfill(...)` em job dedicado;
- usa `flock` no arquivo `/tmp/torqmind-prod-etl-incremental.lock` por padrão;
- os wrappers dedicados usam locks de host diferentes:
  - `/tmp/torqmind-prod-etl-operational.lock`
  - `/tmp/torqmind-prod-etl-risk.lock`
- usa também advisory lock no banco para impedir dois ciclos canônicos ao mesmo tempo;
- se já existir execução em andamento, registra a mensagem e sai sem iniciar uma segunda execução.

Estratégia operacional recomendada:
- baseline segura em produção Ubuntu 24.04 + Docker Compose:
  - ETL operacional: `*/5 * * * *`
  - ETL risk: `*/10 * * * *`
- evidência local medida em `2026-03-25` com `tenant_id=1`:
  - `operational`: `122.64s`
  - `risk` logo após o operacional, com 43 eventos: `87.84s`
- conclusão: cron de 1 minuto fica lock-safe, mas não sustenta cadência real nesta massa; o menor intervalo observado sem sobreposição recorrente é 3 minutos para o trilho operacional. Use `*/3` só depois de observar folga consistente em produção.

Exemplo via `cron` no Ubuntu:

```bash
*/5 * * * * cd /opt/torqmind && ENV_FILE=/etc/torqmind/prod.env COMPOSE_FILE=docker-compose.prod.yml /opt/torqmind/deploy/scripts/prod-etl-operational.sh >> /var/log/torqmind-etl-operational.log 2>&1
*/10 * * * * cd /opt/torqmind && ENV_FILE=/etc/torqmind/prod.env COMPOSE_FILE=docker-compose.prod.yml /opt/torqmind/deploy/scripts/prod-etl-risk.sh >> /var/log/torqmind-etl-risk.log 2>&1
```

Para conferir se já está executando:
- verifique os locks em `/tmp/torqmind-prod-etl-operational.lock` e `/tmp/torqmind-prod-etl-risk.lock`;
- ou rode `ps`/`pgrep` no host para o script;
- ou acompanhe `docker compose -f docker-compose.prod.yml --env-file .env logs -f api`.
- `/finance` → Financeiro
- `/pricing` → Preço da Concorrência (input manual + simulação 10 dias)
- `/goals` → Metas & Equipe

## Reconciliação de vendas

A visão de grupos de vendas agora usa o grupo operacional cru do mart (`mart.agg_grupos_diaria`), sem bucketização heurística.
O problema real observado em produção/local era semântico: a query antiga de `Top grupos` colapsava descrições diferentes no mesmo bucket textual, por exemplo:

- `COMBUSTIVEIS`
- `FILTROS DE COMBUSTIVEIS`

Isso fazia o TorqMind somar itens de grupos distintos e gerar deltas recorrentes de dezenas/centenas de reais frente ao SQL operacional do cliente.

Comandos canônicos para validar sem escrever SQL manual:

```bash
TENANT_ID=1 DATE=2026-03-07 BRANCH_ID=14122 GROUP=COMBUSTIVEIS make reconcile-sales
```

Em produção Ubuntu + Docker Compose:

```bash
TENANT_ID=1 DATE=2026-03-07 BRANCH_ID=14122 GROUP_NAME=COMBUSTIVEIS \
  docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T api python -m app.cli.reconcile_sales
```

Ou via wrapper:

```bash
TENANT_ID=1 DATE=2026-03-07 BRANCH_ID=14122 GROUP_NAME=COMBUSTIVEIS \
  ./deploy/scripts/prod-check-sales-reconciliation.sh
```

O diagnóstico retorna:
- total da fonte operacional capturada, quando houver STG disponível para a data/grupo;
- total do DW;
- total do mart;
- total do endpoint atual;
- bucket legado para comparação;
- delta consolidado;
- grupos/comprovantes/itens extras que o bucket antigo engolia.

Exemplo real validado nesta release:
- tenant `1`, filial `14122`, data `2026-03-07`, grupo `COMBUSTIVEIS`;
- `dw = mart = endpoint = 115336.56`;
- bucket legado = `115425.56`;
- delta do bucket legado = `89.00`;
- origem do delta: item `FILTRO DE COMBUSTIVEL TECFIL PSC75` no grupo `FILTROS DE COMBUSTIVEIS`, comprovante `3435815`.

---

## Repair de Caixa / Antifraude

Documento operacional completo:
- `docs/cash_fraud_operational_truth.md`

Comandos canônicos por tenant:

```bash
TENANT_ID=1 make operational-truth-diagnose
TENANT_ID=1 SCOPE=cash-fraud make operational-truth-purge
TENANT_ID=1 REF_DATE=2026-03-25 make operational-truth-rebuild
TENANT_ID=1 DT_INI=2026-03-01 DT_FIM=2026-03-25 make operational-truth-validate
```

Notas:
- `operational-truth-purge` limpa apenas o domínio selecionado e reseta os watermarks necessários do tenant.
- `INCLUDE_STAGING=1` deve ser usado apenas quando o staging do tenant estiver corrompido e a reingestão da fonte for necessária.
- `WITH_RISK=1` no rebuild adiciona o trilho modelado de risco depois do rebuild operacional.

---

## Reset do banco (dev/homolog)

Você pode rodar o alvo:

```bash
make resetdb
```

Ou executar diretamente o script:

```bash
psql -v ON_ERROR_STOP=1 -U postgres -d TORQMIND -f sql/torqmind_reset_db_v2.sql
```

O reset agora derruba os schemas e reexecuta a cadeia oficial `001..030`, mantendo o banco alinhado com as migrations de runtime.
Depois do reset, rode o bootstrap:

```bash
docker compose exec api python -m app.cli.seed
```

> **Atenção:** ele faz `DROP SCHEMA ... CASCADE` (não use em produção).

---

## Troubleshooting

### Tabelas vazias no pgAdmin
Na maioria dos casos, API/CLI e pgAdmin estão apontando para bancos diferentes.

Cheque:
1) `DATABASE_URL` e `PG_*` no container `api`
2) `docker compose exec api curl -s http://localhost:8000/debug/db`
3) conexão do pgAdmin (host/porta/db/usuário)

O endpoint `/debug/db` deve bater com o mesmo banco que você abriu no pgAdmin.

---

## Release e validação final

- Release notes operacionais: `docs/release_notes.md`
- Proof pack técnico (comandos, tempos ETL, contagens, endpoints): `docs/proof_pack.md`

### Login falhando com 422 / erro estranho no front
O frontend agora converte erros da API em texto; verifique resposta em:
- `http://localhost:8000/docs` (endpoint `/auth/login`)
- valor atual de `SEED_PASSWORD` no seu `.env` local

### Frontend remoto, LAN e Radmin VPN
O frontend não deve montar host/porta da API no browser. A estratégia canônica agora é:

- `NEXT_PUBLIC_API_BASE_URL`: base pública usada no browser. O valor correto é sempre `/api`.
- `API_INTERNAL_URL`: URL interna usada pelo container do Next.js em chamadas server-side. Em Docker, o default correto é `http://api:8000`.
- `APP_CORS_ORIGINS`: origens explícitas permitidas, por padrão `http://localhost:3000,http://127.0.0.1:3000`.
- `APP_CORS_ORIGIN_REGEX`: regex para permitir acesso por hostname/IP na porta `3000`, cobrindo LAN e Radmin VPN sem hardcode de IP.

Regra obrigatória:
- browser usa somente `/api`
- server-side do Next usa `API_INTERNAL_URL`
- o browser nunca deve conhecer `:8000`

Exemplos:

- Desenvolvimento local na mesma máquina: acesse `http://localhost:3000`
- Outra máquina na LAN: acesse `http://192.168.x.y:3000`
- Outra máquina via Radmin VPN: acesse `http://IP_RADMIN:3000`

Configuração mínima recomendada no `.env`:

```bash
NEXT_PUBLIC_API_BASE_URL=/api
API_INTERNAL_URL=http://api:8000
```

Portas que precisam estar acessíveis na máquina servidora:
- `3000/tcp` para o frontend
- `8000/tcp` apenas para tráfego interno entre containers
