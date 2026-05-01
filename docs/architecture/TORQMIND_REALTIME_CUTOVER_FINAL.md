# TorqMind Realtime Cutover — Final Implementation

## Status: IMPLEMENTED (Awaiting Production Activation)

## Decisão Arquitetural: Origem Realtime = DW (Opção B)

### Justificativa

O hot path realtime começa no **DW** (schema `dw.*`), não no STG.

**Por quê:**
1. STG contém dados crus e não-normalizados (comprovantes, itenscomprovantes, formas_pgto_comprovantes). A transformação STG→DW é onde se resolve: joins, deduplicação, tipagem, cancelamento flag, CFOP, etc.
2. O DW já é o modelo canônico para BI — dimensões e fatos normalizados.
3. Debezium captura `dw.*` facts e dims — exatamente as tabelas que os marts precisam.
4. O ETL cron (2min) apenas mantém DW atualizado. Não é o motor do BI; é o normalizador.
5. Latência real: Agent→STG→(cron 2min)→DW→(Debezium <1s)→Redpanda→CDC Consumer→ClickHouse→mart_rt→API. Total: ~2 min.

### Transparência sobre dependência do ETL

- O cron ETL (STG→DW) **ainda existe** como normalizador.
- Se o cron parar, novos dados param de chegar ao DW, logo param de chegar ao realtime.
- Porém, o BI dashboard mostra imediatamente qualquer dado que chegue ao DW — sem esperar sync batch ao ClickHouse mart.
- O MartBuilder atualiza marts em <1s após o CDC flush.

### Bloqueador real para Opção A (STG direto)

Para eliminar completamente a dependência do cron:
- CDC Consumer precisaria fazer a transformação STG→current diretamente (resolver joins, cancelamentos, CFOP).
- Isso duplicaria lógica do ETL SQL no Python.
- Risco: divergência entre os dois caminhos.
- Decisão: não implementar agora. Documentar como futuro.

---

## Componentes Implementados

### 1. ClickHouse DDLs (sql/clickhouse/streaming/)
- `040_mart_rt_database.sql` — Database + mart_publication_log
- `041_mart_rt_tables.sql` — 12 tabelas mart_rt:
  - sales_daily_rt (grain: empresa/filial/data_key)
  - sales_hourly_rt (grain: empresa/filial/data_key/hora)
  - sales_products_rt (grain: empresa/filial/data_key/id_produto)
  - sales_groups_rt (grain: empresa/filial/data_key/id_grupo_produto)
  - payments_by_type_rt (grain: empresa/filial/data_key/tipo_forma)
  - cash_overview_rt (grain: empresa/filial/id_turno)
  - fraud_daily_rt (grain: empresa/filial/data_key/event_type)
  - risk_recent_events_rt (grain: empresa/filial/id)
  - finance_overview_rt (grain: empresa/filial/tipo_titulo/faixa)
  - dashboard_home_rt (grain: empresa/filial/data_key)
  - customers_churn_rt (grain: empresa/filial/id_cliente)
  - source_freshness (grain: empresa/domain)

Todas usam **ReplacingMergeTree(published_at)** — idempotência garantida via FINAL.

### 2. CDC Consumer — MartBuilder
- `apps/cdc_consumer/torqmind_cdc_consumer/mart_builder.py`
- Após cada CDC flush, identifica tabelas e data_keys afetados
- Executa INSERT SELECT com FINAL para atualizar apenas marts relevantes
- Suporta backfill completo via CLI

### 3. API — repos_mart_realtime.py
- 14 funções com assinaturas IDÊNTICAS a repos_mart_clickhouse.py
- Pattern: `(role, id_empresa, id_filial, dt_ini, dt_fim, ...)`
- Testado via test_realtime_mart.py::TestSignatureParity

### 4. Feature Flags (repos_analytics.py)
- `USE_REALTIME_MARTS=true` — ativa routing para mart_rt
- `REALTIME_MARTS_DOMAINS=dashboard,sales,cash,fraud,finance,payments`
- `REALTIME_MARTS_FALLBACK=true/false` — fallback para legacy batch mart
- Cutover valida com **FALLBACK=false**

### 5. Deploy Scripts
- `prod-realtime-cutover-apply.sh` — 13 steps, guard-based
- `streaming-init-mart-rt.sh` — aplica DDLs com credenciais
- `realtime-validate-cutover.sh` — bloqueante, exit 1 em divergência
- `realtime-e2e-smoke.sh` — teste ponta a ponta completo

---

## Como Operar o Cutover

### Pre-requisitos
```bash
# Stack rodando
make prod-up
make streaming-up

# Debezium registrado
make streaming-register-debezium

# mart_rt inicializado
make streaming-init-mart-rt
```

### Cutover completo
```bash
ENV_FILE=/etc/torqmind/prod.env make realtime-cutover
```

Isso executa:
1. Preflight checks
2. Validate compose
3. Build services
4. Migrate PostgreSQL
5. Init ClickHouse streaming (raw/current/ops/mart_rt)
6. Prepare PG publication/slot
7. Start streaming stack
8. Register Debezium connector
9. Backfill mart_rt (condition-based wait, not sleep)
10. **Validate parity** (bloqueante — exit 1 se divergente)
11. Set USE_REALTIME_MARTS=true + FALLBACK=false
12. Restart API
13. Smoke test com fallback=false

### Validação isolada
```bash
make realtime-validate
```

### Rollback
```bash
make realtime-rollback
```
Sets `USE_REALTIME_MARTS=false` + restart API.

### E2E Smoke
```bash
make realtime-e2e-smoke
```
Insere venda sintética → confirma CDC → confirma mart_rt → confirma API.

---

## Garantias de Segurança

1. **Cutover não ativa se marts estão vazias** — step_backfill verifica rows > 0
2. **Cutover não ativa se validação falha** — step_validate_parity é bloqueante
3. **Fallback=false durante aceite** — garante que o realtime funciona sozinho
4. **Idempotência** — ReplacingMergeTree + FINAL = re-run seguro
5. **Debezium condition wait** — não usa sleep, espera connector RUNNING + dados
6. **Rollback instantâneo** — flag no .env + restart API

---

## Papel Residual do Cron ETL

| Cron Job | Função | Ainda necessário? |
|----------|--------|-------------------|
| ETL STG→DW (2min) | Normaliza dados crus em modelo canônico | **SIM** — alimenta o DW que Debezium captura |
| Sync DW→ClickHouse batch | Populava torqmind_mart (legacy) | **NÃO** — substituído por CDC |
| Refresh MVs | Atualizava materialized views batch | **NÃO** — substituído por mart_rt |

---

## Testes Executados

- 63 testes CDC consumer (incluindo parity tests) ✅
- npm run build (web) ✅
- bash -n (4 scripts) ✅
- docker compose config (prod + streaming) ✅
- API facade routing com USE_REALTIME_MARTS=true fallback=false ✅
- Signature parity tests vs repos_mart_clickhouse ✅
