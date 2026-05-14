---
name: debug-producao
description: "Diagnosticar problema de produção TorqMind com comandos seguros."
agent: "TorqMind SSH Produção"
---

# Debug produção TorqMind

Diagnostique o problema informado sem executar ação destrutiva.

Problema: `${input:problema}`

## Procedimento

1. Confirmar branch/containers.
2. Validar health público.
3. Identificar camada: Web, API, PostgreSQL, ClickHouse, CDC/Debezium, Mart, permissão/role ou dados ausentes.
4. Coletar logs mínimos.
5. Medir endpoint se houver lentidão.
6. Propor correção mínima.
7. Não fazer deploy sem confirmação.

```bash
cd /home/tm/apps/torqmind
git status -sb
docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env ps
curl -I http://redevr.ddns.me:14023
curl -I http://redevr.ddns.me:14023/api/health
ssh tm@172.30.0.9 'cd /home/tm/apps/torqmind && docker compose -f docker-compose.analytics.yml --env-file /etc/torqmind/prod.analytics.env ps'
```

Responder com causa provável, evidência, camada afetada, comandos executados e próximo passo seguro.
