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
- `deploy/scripts/prod-logs.sh`
- `deploy/scripts/prod-seed.sh`

Passo a passo no Linux:

1. Clonar o repositório no servidor.
2. Criar o arquivo `.env` a partir do exemplo seguro:

```bash
cp .env.production.example .env
```

3. Preencher no `.env` pelo menos:
- `POSTGRES_PASSWORD`
- `API_JWT_SECRET`
- `SEED_PASSWORD`
- `OPENAI_API_KEY` se quiser Jarvis IA ativo
- `TELEGRAM_BOT_TOKEN` se quiser notificações Telegram

4. Subir a stack:

```bash
docker compose -f docker-compose.prod.yml --env-file .env up -d --build
```

Ou usar o script:

```bash
./deploy/scripts/prod-up.sh
```

5. Rodar seed inicial:

```bash
./deploy/scripts/prod-seed.sh
```

Em produção, esse seed cria/atualiza apenas o usuário `platform_master`.
Ele não cria tenant nem filial demo.

6. Validar no navegador:
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
make migrate   # aplica todas as migrations em ordem
make resetdb   # recria o banco via cadeia oficial de migrations (DEV/HOMOLOG)
make lint   # valida build do web + compilação Python
make down   # derruba os serviços
make platform-billing-daily   # gera receivables / atualiza overdue do backoffice
```

### Backfill de snapshots executivos

Os snapshots históricos de `churn`, `health score` e `aging financeiro` são persistidos por `dt_ref`.
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

---

## Seed de usuários e tenant

Depois de subir, rode:

```bash
docker compose exec api python -m app.cli.seed
```

No modo padrão local/dev, cria/atualiza:
- **MASTER**  → `master@torqmind.com` / valor definido em `SEED_PASSWORD`
- **OWNER**   → `owner@empresa1.com` / valor definido em `SEED_PASSWORD`  (Empresa 1)
- **MANAGER** → `manager@empresa1.com` / valor definido em `SEED_PASSWORD` (Empresa 1, Filial 1)

E imprime o `ingest_key` da Empresa 1 (útil para o Agent).

No script de produção `./deploy/scripts/prod-seed.sh`, o seed roda em modo `master-only`:
- cria/atualiza apenas `master@torqmind.com`
- não cria tenant demo
- não cria filial demo

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

A tela `/scope` define `dt_ini`, `dt_fim`, `id_empresa` (MASTER) e `id_filial` (opcional).

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
- `channel_admin`: acesso apenas à própria carteira, sem financeiro global.
- `tenant_admin`, `tenant_manager`, `tenant_viewer`: continuam no produto do cliente com validação reforçada de vigência e escopo.

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
- O backoffice não cria nem edita filial manualmente.

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
- `/finance` → Financeiro
- `/pricing` → Preço da Concorrência (input manual + simulação 10 dias)
- `/goals` → Metas & Equipe

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

O reset agora derruba os schemas e reexecuta a cadeia oficial `001..021`, mantendo o banco alinhado com as migrations de runtime.
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
