---
description: Validar um número/grid da tela contra a fonte (Xpert → STG → ClickHouse → API → tela)
---

# Validar dado: fonte → tela

Use quando um número, KPI ou grid da tela não bate com a realidade do cliente
(ex.: cliente duplicado, valor errado, total que não fecha).

Regra de ouro: **bug de dado não se corrige na tela.** Concilie a cadeia toda
com amostra real (filial real + data de referência real) antes de declarar PASS.

## Passos

1. **Identificar o caminho ativo**
   - Conferir flags em `/etc/torqmind/prod.app.env`: `USE_REALTIME_MARTS`,
     `USE_CLICKHOUSE`, `REALTIME_MARTS_FALLBACK`, `REALTIME_MARTS_SOURCE`.
   - Na facade `repos_analytics`, ver se a função está em `REALTIME_FUNCTIONS`
     (→ `repos_mart_realtime`), senão `repos_mart_clickhouse` ou `repos_mart` (PG).

2. **Fonte canônica — SQL Server Xpert**
   - `tools/xpert_source_explorer.py query --env config/source-explorer.env --sql-file F --out DIR`
     (a SQL precisa começar com `SELECT`/`WITH`; sem comentário no topo).
   - Mapear chaves reais do título/registro e a regra de negócio
     (ex.: recebível aberto = `DTAPGTO IS NULL`; saldo = `VALOR - GREATEST(VLRPAGO, SUM(VALORBAIXA))`).

3. **PostgreSQL STG** (`stg.*`) — contagem e soma batem com Xpert?
4. **PostgreSQL DW/mart** (`dw.*`, `mart.*`) — grão correto? baixas consideradas?
5. **ClickHouse** `torqmind_current` e `torqmind_mart_rt` — tabela existe, populada,
   grão certo? (cuidado com JOIN que multiplica por filial).
6. **API** — chamar a função/rota real com a filial do cliente e checar
   duplicidade de chave (ex.: `id_cliente` repetido) e valores.
7. **Frontend** — confirmar que só pagina/ordena o payload; nunca deduplicar aqui.

## Conexões (servidor de produção)

```bash
set -a; source /etc/torqmind/prod.app.env; set +a
# PostgreSQL (STG/DW/mart) em 172.30.0.8
PGPASSWORD="$POSTGRES_PASSWORD" psql -h 172.30.0.8 -U "${POSTGRES_USER:-torqmind}" -d "${POSTGRES_DB:-torqmind}" -P pager=off
# ClickHouse em 172.30.0.9
curl -s "http://172.30.0.9:8123/?user=${CLICKHOUSE_USER:-default}&password=$CLICKHOUSE_PASSWORD" --data-binary "SELECT ..."
```

## PASS

- Xpert = STG = ClickHouse/mart = API conciliados na amostra real.
- Grid sem chave duplicada, valores corretos por entidade.
- Tests/build OK, health check OK.
- `CODEX_TORQMIND_MAP.md` atualizado com fonte/mart/contrato/regra.
- Sem segredo no diff.
