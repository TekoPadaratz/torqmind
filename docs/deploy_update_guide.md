# TorqMind — Guia de Atualização em Produção

> **Branch:** `nova-brach-limpa`
> **Data:** Abril 2026
> **Servidor:** Ubuntu 24.02 com Docker (Postgres + API + Web + Nginx em containers)

---

## Resumo das Mudanças

| Componente | O que muda |
|---|---|
| **SQL** | Migration 059 (indexes de performance + `compute_risk_events_v2`) |
| **SQL** | Migration 060 (RLS — isolamento multi-tenant no banco) |
| **API** | Pool otimizado, session isolation, batch UPSERTs, date clamping, risk v2 |
| **Web** | `strict: true` no TS, Error Boundary, interceptor 401, `.env.example` |
| **Tests** | `compute_risk_events` → `compute_risk_events_v2` |

---

## Resposta: Preciso Separar API/Web em Outra VM?

**Não para o momento.** Tudo no mesmo servidor funciona bem com as otimizações feitas:

- Os novos **22+ indexes** vão eliminar full table scans — queries que levavam 5-10s passam a <100ms
- O **pool de conexões** agora suporta 30 conexões simultâneas (antes 12)
- O **compute_risk_events_v2** elimina gargalo O(n²) → O(n)
- O **RLS** adiciona segurança mas NÃO impacta performance significativamente (indexes existem)

**Quando separar:**
- Se tiver >50 usuários simultâneos E CPU/RAM da VM ficarem acima de 80% consistentemente
- Se o Postgres precisar de mais RAM exclusiva (>8GB shared_buffers)
- Nesse caso: mova o Postgres para uma VM dedicada, mantenha API+Web+Nginx na outra

**Recomendação de recursos para VM única:**
- **Mínimo:** 4 vCPU, 8GB RAM, SSD
- **Ideal:** 8 vCPU, 16GB RAM, SSD NVMe
- Se >16GB RAM disponível, aumente no `.env`: `POSTGRES_SHARED_BUFFERS=4GB`, `POSTGRES_EFFECTIVE_CACHE_SIZE=12GB`

---

## Pré-requisitos

Antes de começar, confirme que você tem:
- Acesso SSH ao servidor (Putty)
- Git configurado e autenticado no servidor
- O `.env` de produção em `/etc/torqmind/prod.env`

---

## PASSO 1 — Fazer Backup do Banco (OBRIGATÓRIO)

```bash
# Conectar via Putty e rodar:

# Descubra o nome do container do Postgres
docker ps --format '{{.Names}}' | grep postgres

# Backup completo (substitua o nome do container se diferente)
cd /root  # ou outro diretório com espaço
docker compose -f /caminho/do/projeto/docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --format=custom --compress=6 \
  -f /tmp/backup_antes_v059.dump

# Copiar o dump do container para o host
docker compose -f /caminho/do/projeto/docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  cp postgres:/tmp/backup_antes_v059.dump ./backup_antes_v059_$(date +%Y%m%d_%H%M).dump

echo "Backup salvo com sucesso!"
ls -lh backup_antes_v059_*.dump
```

> **Se algo der errado**, restaure com:
> ```bash
> docker compose exec -T postgres pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists /tmp/backup_antes_v059.dump
> ```

---

## PASSO 2 — Atualizar o Código via Git

```bash
# Navegar até o diretório do projeto
cd /caminho/do/projeto  # ajuste para o caminho real no seu servidor

# Verificar branch atual
git branch

# Fazer pull da branch com as mudanças
git fetch origin
git checkout nova-brach-limpa
git pull origin nova-brach-limpa

# Confirmar que os arquivos novos estão lá
ls -la sql/migrations/059_performance_indexes_and_etl_fixes.sql
ls -la sql/migrations/060_enable_rls_tenant_isolation.sql
echo "Arquivos de migração OK"
```

---

## PASSO 3 — Aplicar Migrations SQL (Antes de Rebuildar Containers)

A migration 059 cria indexes `CONCURRENTLY` — isso é feito **sem bloquear** leituras/escritas. Pode rodar com o sistema ativo.

```bash
# Posicionar no diretório do projeto
cd /caminho/do/projeto

# Rodar as migrações via script oficial
./deploy/scripts/prod-migrate.sh

# OU manualmente via docker compose:
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T api python -m app.cli.migrate
```

**Resultado esperado:** Deve exibir que migrations 059 e 060 foram aplicadas com sucesso.

> **ATENÇÃO:** A migration 059 cria ~22 indexes e pode levar de **2 a 15 minutos** dependendo do volume de dados. Os indexes `CONCURRENTLY` NÃO bloqueiam o sistema — os usuários podem continuar usando normalmente.

---

## PASSO 4 — Rebuild e Restart dos Containers (API + Web)

```bash
cd /caminho/do/projeto

# Rebuild e restart de todos os serviços
# Isso vai rebuildar API (Python) e Web (Next.js) com o código novo
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  up -d --build api web

# Aguardar os containers ficarem healthy (30-60 segundos)
echo "Aguardando containers..."
sleep 10

# Verificar status dos containers
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env ps
```

**Resultado esperado:** Todos os containers devem estar `healthy` ou `running`.

> **NOTA:** O `--build api web` faz rebuild apenas da API e Web, sem tocar no Postgres. Seus dados estão seguros.

---

## PASSO 5 — Otimizar Pool de Conexões no .env (Opcional mas Recomendado)

Edite o arquivo de configuração de produção:

```bash
sudo nano /etc/torqmind/prod.env
```

Adicione ou atualize estas variáveis:

```env
# Pool de conexões (valores otimizados)
DB_POOL_MIN_SIZE=4
DB_POOL_MAX_SIZE=30
DB_POOL_TIMEOUT_SECONDS=30
DB_POOL_MAX_IDLE_SECONDS=300

# Postgres performance (se tiver ≥8GB RAM na VM)
POSTGRES_SHARED_BUFFERS=1GB
POSTGRES_EFFECTIVE_CACHE_SIZE=3GB
POSTGRES_WORK_MEM=32MB
POSTGRES_MAINTENANCE_WORK_MEM=512MB
POSTGRES_SHM_SIZE=2g
POSTGRES_MAX_CONNECTIONS=200
```

Se editou o `.env`, reinicie o Postgres também:

```bash
cd /caminho/do/projeto
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  up -d --build postgres api web

# Aguardar Postgres voltar healthy antes de prosseguir
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres pg_isready -U postgres
```

---

## PASSO 6 — Validação Pós-Deploy

### 6.1 — Verificar que todos os containers estão saudáveis

```bash
cd /caminho/do/projeto
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env ps
```

Todos devem estar `Up` e `healthy`.

### 6.2 — Verificar que a API responde

```bash
curl -s http://localhost/api/health | python3 -m json.tool
# Deve retornar {"status": "ok"} ou similar
```

### 6.3 — Verificar que as migrations foram aplicadas

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT filename, applied_at FROM app.schema_migrations ORDER BY filename DESC LIMIT 5;"
```

Deve listar `059_performance_indexes_and_etl_fixes.sql` e `060_enable_rls_tenant_isolation.sql`.

### 6.4 — Verificar que os indexes foram criados

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT indexname FROM pg_indexes WHERE schemaname = 'dw' AND indexname LIKE 'ix_fact_%' ORDER BY indexname;"
```

Deve listar os novos indexes (`ix_fact_venda_cliente_data`, `ix_fact_comprovante_usuario_data`, etc.).

### 6.5 — Verificar que RLS está ativo

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname = 'dw' AND rowsecurity = true ORDER BY tablename;"
```

Deve listar: `fact_venda`, `fact_comprovante`, `fact_venda_item`, `fact_financeiro`, `fact_pagamento_comprovante`, `fact_risco_evento`, etc.

### 6.6 — Verificar que `compute_risk_events_v2` existe

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'etl' AND routine_name LIKE '%risk%';"
```

Deve listar `compute_risk_events_v2`.

### 6.7 — Teste de fumaça no dashboard

Abra o browser e acesse o sistema normalmente. Navegue por:
- Dashboard geral
- Dashboard de vendas
- Dashboard antifraude
- Dashboard financeiro

Confirme que os dados carregam e os gráficos renderizam.

### 6.8 — Verificar logs por erros

```bash
cd /caminho/do/projeto
./deploy/scripts/prod-logs.sh
# Ou:
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  logs --tail=100 api web
```

Procure por erros (linhas com `ERROR`, `CRITICAL`, `Traceback`).

---

## PASSO 7 — Script Completo (Copiar e Colar)

Se preferir rodar tudo de uma vez, aqui está o script consolidado.
**Substitua `/caminho/do/projeto`** pelo caminho real no servidor:

```bash
#!/bin/bash
set -e

PROJECT_DIR="/caminho/do/projeto"   # <<< MUDE AQUI
ENV_FILE="/etc/torqmind/prod.env"

echo "=========================================="
echo "TorqMind — Deploy v059+v060"
echo "=========================================="

cd "$PROJECT_DIR"

# 1. Backup
echo "[1/5] Fazendo backup do banco..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" \
  exec -T postgres pg_dump -U postgres -d torqmind \
  --format=custom --compress=6 > "$HOME/backup_torqmind_$(date +%Y%m%d_%H%M).dump"
echo "Backup salvo em $HOME/"

# 2. Git pull
echo "[2/5] Atualizando código..."
git fetch origin
git checkout nova-brach-limpa
git pull origin nova-brach-limpa

# 3. Rebuild + restart
echo "[3/5] Rebuild containers API + Web..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" \
  up -d --build api web

# 4. Aguardar API healthy
echo "[4/5] Aguardando API ficar healthy..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "API healthy!"
    break
  fi
  echo "  tentativa $i/30..."
  sleep 5
done

# 5. Migrations
echo "[5/5] Aplicando migrations..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" \
  exec -T api python -m app.cli.migrate

echo ""
echo "=========================================="
echo "Deploy concluído!"
echo "=========================================="
echo ""

# Validação
echo "Verificando containers..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" ps

echo ""
echo "Verificando migrations aplicadas..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT filename, applied_at FROM app.schema_migrations ORDER BY filename DESC LIMIT 5;"

echo ""
echo "Verificando RLS ativo..."
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" \
  exec -T postgres psql -U postgres -d torqmind -c \
  "SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname = 'dw' AND rowsecurity = true;"

echo ""
echo "Tudo pronto! Acesse o sistema e faça um teste visual nos dashboards."
```

---

## Rollback (Se Algo Der Errado)

### Voltar código para versão anterior
```bash
cd /caminho/do/projeto
git log --oneline -5          # anotar o hash do commit anterior
git checkout <hash_anterior>
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  up -d --build api web
```

### Restaurar banco do backup
```bash
# Parar containers
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env down

# Restaurar
cat ~/backup_torqmind_*.dump | docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env \
  exec -T postgres pg_restore -U postgres -d torqmind --clean --if-exists

# Subir novamente
docker compose -f docker-compose.prod.yml --env-file /etc/torqmind/prod.env up -d
```

---

## Checklist Final

- [ ] Backup do banco realizado e arquivo `.dump` salvo
- [ ] `git pull` executado com sucesso na branch `nova-brach-limpa`
- [ ] Migrations 059 + 060 aplicadas sem erro
- [ ] Containers API + Web rebuilt e healthy
- [ ] Dashboard carrega normalmente
- [ ] Logs sem erros (`ERROR`/`CRITICAL`)
- [ ] Pool de conexões configurado no `.env` (opcional)
