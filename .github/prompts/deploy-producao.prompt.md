---
name: deploy-producao
description: "Executar checklist de deploy seguro em produção TorqMind."
agent: "TorqMind SSH Produção"
---

# Deploy produção TorqMind

Alteração: `${input:alteracao}`

## Antes

```bash
cd /home/tm/apps/torqmind
git status -sb
git branch --show-current
git log -3 --oneline
```

Não prosseguir se branch errada, working tree sujo sem explicação, testes não rodados, migrations perigosas ou segredos no diff.

## Deploy API/Web

```bash
cd /home/tm/apps/torqmind
docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env build --no-cache api web
docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env up -d --force-recreate api web nginx
docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env ps
curl -fsS http://127.0.0.1/api/health
curl -fsS -o /dev/null -w '%{http_code}\n' https://www.torqmind.com.br
```

Antes do recreate, conferir no env de Prod (sem commitar): `WEB_PUBLIC_URL=https://www.torqmind.com.br` e `APP_CORS_ORIGINS` com `https://www.torqmind.com.br` + `https://torqmind.com.br` (allowlist explícita, sem wildcard). Hom permanece em `https://hom.torqmind.com.br`.

## Deploy Analytics

Somente se CDC/MartBuilder/ClickHouse foi alterado.

```bash
ssh tm@172.30.0.9 'cd /home/tm/apps/torqmind && docker compose -f docker-compose.analytics.yml --env-file /etc/torqmind/prod.analytics.env build --no-cache && docker compose -f docker-compose.analytics.yml --env-file /etc/torqmind/prod.analytics.env up -d --force-recreate'
```

## Smoke

```bash
cd /home/tm/apps/torqmind
ENV_FILE=/etc/torqmind/prod.app.env PUBLIC_URL=https://www.torqmind.com.br ./deploy/scripts/realtime-product-screen-smoke.sh
```

Relatório: deploy executado, containers, health, smoke, pendências e handoff Git.
