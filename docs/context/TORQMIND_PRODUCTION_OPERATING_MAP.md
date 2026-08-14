# TorqMind — Mapa Operacional de Produção

Atualizado em: **2026-08-14 — America/Sao_Paulo**

Mapa mestre: `CODEX_TORQMIND_MAP.md`

Este documento é um índice operacional curto. Regras de segurança, dados, permissões e critérios de PASS estão em `AGENTS.md`; o retrato completo está no mapa mestre.

## Topologia

- PostgreSQL / STG / DW: `172.30.0.8`.
- Analytics / ClickHouse / Redpanda / Debezium / CDC: `172.30.0.9`.
- App / API / Web / Nginx: `172.30.0.10`.
- Checkout esperado: `/home/tm/torqmind`.
- URL pública: `https://www.torqmind.com.br`.
- Homologação: `https://hom.torqmind.com.br`.
- URL NAT legada para diagnóstico: `http://redevr.ddns.me:14023`.
- SSH externo documentado: `ssh -p 14022 tm@redevr.ddns.me`.
- Timezone: `America/Sao_Paulo`.

Produção e homologação compartilham analytics/CDC. DDL ClickHouse, alteração slim e rebuild do consumer são mudanças de produção.

## Fluxo

```text
Xpert → Agent 2.0.4 → API ingest → PG STG
                                  ├→ ETL/DW/apoio
                                  └→ Debezium → Redpanda → CDC Consumer
                                                              ↓
                                                  ClickHouse current/slim
                                                              ↓
                                                   torqmind_mart_rt
                                                              ↓
                                                        API → Web
```

## Entrada pública

- Nginx produção: HTTP local `:80`.
- Nginx homolog: HTTP local `127.0.0.1:81`.
- Tunnel esperado:
  - `www.torqmind.com.br` → `http://127.0.0.1:80`;
  - `hom.torqmind.com.br` → `http://127.0.0.1:81`.
- HTTPS termina na borda Cloudflare; o serviço local pode permanecer HTTP.
- Ativar `Always Use HTTPS` na borda.
- Não usar `www.hom.torqmind.com.br` sem certificado avançado específico; preferir `hom.torqmind.com.br`.

## Composes e envs

- `docker-compose.app.yml`
- `docker-compose.pg.yml`
- `docker-compose.analytics.yml`
- `docker-compose.homolog.yml`
- `/etc/torqmind/prod.app.env`
- `/etc/torqmind/prod.pg.env`
- `/etc/torqmind/prod.analytics.env`
- `/etc/torqmind/cluster.env`

## Provas mínimas

```bash
curl -I https://www.torqmind.com.br
curl -sS https://www.torqmind.com.br/api/health
curl -I https://hom.torqmind.com.br
curl -sS https://hom.torqmind.com.br/api/health

ENV_FILE=/etc/torqmind/prod.app.env PUBLIC_URL=https://www.torqmind.com.br ./deploy/scripts/realtime-product-screen-smoke.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh
```

Para realtime, container healthy não basta. Provar connector, consumer membership, `TOTAL-LAG=0`, logs sem erro de schema e publicação recente em `mart_publication_log`.

## Estado público em 2026-08-14

- Produção HTTPS e health: 200.
- Homologação HTTPS e health: 200.
- HTTP ainda responde 200 sem redirect.
- `www.hom` falha no handshake TLS.
- SSH externo não respondeu durante a revisão; estado interno não foi revalidado.

## Proibições

- Não apagar STG.
- Não resetar volumes.
- Não regenerar Ingest Key.
- Não executar `docker compose down -v`.
- Não executar DROP/TRUNCATE em produção sem plano, backup e confirmação explícita.
- Não fazer deploy sem teste e health check.
- Não declarar PASS sem prova.
