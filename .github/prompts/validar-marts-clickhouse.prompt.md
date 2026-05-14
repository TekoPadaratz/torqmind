---
name: validar-marts-clickhouse
description: "Validar slims, dimensões e marts ClickHouse do TorqMind."
agent: "TorqMind SSH Produção"
---

# Validar Marts ClickHouse

Tela/Mart: `${input:mart_ou_tela}`

Não aceitar consistência interna sem fonte. Comparar STG PostgreSQL quando necessário.

```bash
ssh tm@172.30.0.9 "clickhouse-client --query 'SHOW TABLES FROM torqmind_current'"
ssh tm@172.30.0.9 "clickhouse-client --query 'SHOW TABLES FROM torqmind_mart_rt'"
ssh tm@172.30.0.9 "clickhouse-client --query 'SELECT * FROM torqmind_mart_rt.source_freshness ORDER BY updated_at DESC LIMIT 20 FORMAT Vertical'"
ssh tm@172.30.0.9 "clickhouse-client --query 'SELECT * FROM torqmind_mart_rt.mart_publication_log ORDER BY published_at DESC LIMIT 20 FORMAT Vertical'"
```

Para Antifraude/Caixa/Metas validar filial, operador, turno, caixa, data real, id_db, id_comprovante e data_quality_status se não resolver.

Relatório: fonte, Mart, API, Web, gaps e correção recomendada.
