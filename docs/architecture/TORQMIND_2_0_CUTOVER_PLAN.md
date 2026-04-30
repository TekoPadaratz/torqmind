# TorqMind 2.0 — Cutover Plan: Batch → Streaming

**Data:** 2026-04-30  
**Status:** Fase 1 completa (fundação CDC paralela)  
**Branch:** nova-branch-limpa

---

## 1. Estado Atual

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PRODUÇÃO ATUAL                                    │
│                                                                           │
│  Agent Windows → NDJSON → FastAPI Ingest → stg.*                         │
│                                                                           │
│  Cron 2min → ETL (stg → dw) → refresh_marts → ClickHouse sync DW        │
│           → refresh marts CH → API BI → Next.js dashboards               │
│                                                                           │
│  Latência: 2-5 min (best case) / 10+ min (falha + retry)                │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    STREAMING 2.0 (PARALELO, AINDA NÃO CORTA)             │
│                                                                           │
│  PostgreSQL (dw.*) → Debezium → Redpanda → CDC Consumer                 │
│                                    → ClickHouse raw/current/ops          │
│                                                                           │
│  Status: fundação criada, não validada em produção.                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Componentes existentes

| Componente | Status | Notas |
|-----------|--------|-------|
| docker-compose.streaming.yml | ✅ Criado | Rede compartilhada com main compose |
| CDC Consumer (Python) | ✅ Criado | 29 testes passando |
| Debezium connector config | ✅ Criado | Template com substituição Python |
| ClickHouse schemas (raw/current/ops) | ✅ Criado | DDL alinhado com writer |
| Scripts operacionais (7) | ✅ Criado | Auth CH, env standardizado |
| Makefile targets (9) | ✅ Criado | streaming-* |
| Documentação arquitetural | ✅ Criada | TORQMIND_EVENT_DRIVEN_2_0.md |

---

## 2. Estado Alvo

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PRODUÇÃO ALVO (STREAMING-FIRST)                   │
│                                                                           │
│  Agent → NDJSON → FastAPI Ingest → stg.* → ETL → dw.*                   │
│                                                                           │
│  dw.* → Debezium CDC → Redpanda → CDC Consumer                          │
│       → ClickHouse raw → current → streaming marts → API BI → Next.js   │
│                                                                           │
│  Latência: < 10 segundos (CDC + consumer + query)                        │
│                                                                           │
│  Agent/Jarvis: consome eventos para alertas reativos sub-segundo         │
│                                                                           │
│  Batch ETL: mantido como safety net com intervalo relaxado (30min)       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Meta de latência por camada

| Camada | Latência target | Medição |
|--------|----------------|---------|
| PostgreSQL → Redpanda | < 2s | Debezium source.ts_ms vs Kafka timestamp |
| Redpanda → ClickHouse raw | < 3s | consumer lag |
| raw → current | < 3s | (mesma operação) |
| current → mart streaming | < 5s | MV refresh ou consumer |
| mart → API response | < 500ms | P99 endpoint |
| **E2E total** | **< 10s** | Mudança em PG → dado na tela |

---

## 3. Fase 1: CDC Paralelo (✅ COMPLETA)

### Entregáveis
- [x] docker-compose.streaming.yml com rede compartilhada
- [x] CDC Consumer com 15 tabelas mapeadas
- [x] ClickHouse schemas: raw (ReplacingMergeTree), current (ReplacingMergeTree), ops
- [x] Scripts com auth ClickHouse, env POSTGRES_* padronizado
- [x] Debezium config com Python JSON generation (seguro)
- [x] Testes unitários (29 + DDL alignment)
- [x] Documentação completa

### Validação
```bash
make streaming-up              # sobe stack
make streaming-init-clickhouse # cria schemas
make streaming-register-debezium  # registra connector
make streaming-status          # verifica saúde
make streaming-validate-cdc    # valida fluxo end-to-end
make test-cdc-consumer         # 29 testes passam
```

---

## 4. Fase 2: Raw/Current Validado

### Objetivo
Validar que `torqmind_current` reflete fielmente o estado de `dw.*` no PostgreSQL.

### Tarefas

| # | Tarefa | Critério de aceite |
|---|--------|-------------------|
| 1 | Deploy streaming em staging/homolog | Stack rodando 48h sem crash |
| 2 | Snapshot completo (Debezium initial) | Count de cada tabela current = count DW PG |
| 3 | CDC contínuo 48h | Lag < 5s sustentado |
| 4 | Reconciliação automática | Script compara PG vs CH current por tenant/tabela |
| 5 | Stress test com ingest simultâneo | ETL batch + CDC simultâneo não gera inconsistência |
| 6 | Monitor de erros ops | Zero erros não tratados em 48h |

### Critério de saída
- Divergência max 0.01% (window de seconds entre PG commit e CH write)
- Zero erros de tipo/schema em 48h contínuas
- Lag P99 < 5 segundos

### Comandos
```bash
# Reconciliação
./deploy/scripts/streaming-validate-cdc.sh

# Monitor contínuo
./deploy/scripts/streaming-status.sh

# Verificar lag
docker compose -f docker-compose.streaming.yml exec -T redpanda \
  rpk group describe torqmind-cdc-consumer
```

---

## 5. Fase 3: Mart Piloto Streaming

### Objetivo
Criar primeira mart streaming alimentada por `torqmind_current` (não por batch sync).

### Marts candidatas (ordem de prioridade)

| # | Mart | Justificativa | Complexidade |
|---|------|---------------|-------------|
| 1 | `agg_vendas_diaria_streaming` | Mais usada, fácil de validar | Média |
| 2 | `agg_vendas_hora_streaming` | Alta cardinalidade, bom teste | Média |
| 3 | `fraude_cancelamentos_streaming` | Alto valor de latência baixa | Média |
| 4 | `alerta_caixa_aberto_streaming` | Alerta em tempo real | Baixa |

### Implementação
```sql
-- Opção A: Materialized View ClickHouse (simples)
CREATE MATERIALIZED VIEW torqmind_mart.agg_vendas_diaria_streaming
TO torqmind_mart.agg_vendas_diaria_streaming_target
AS SELECT
    id_empresa, id_filial, data_key,
    sum(total_venda) as faturamento,
    ...
FROM torqmind_current.fact_venda FINAL
WHERE is_deleted = 0
GROUP BY id_empresa, id_filial, data_key;

-- Opção B: Consumer streaming (mais controle)
-- CDC Consumer gera marts no mesmo ciclo de flush
```

### Validação
- Comparar mart streaming vs mart batch para mesma janela
- Divergência < 1% por 7 dias
- Query performance < 200ms P99

---

## 6. Fase 4: API Feature Flag

### Objetivo
API pode ler de `torqmind_current` ou `torqmind_mart_streaming` sem quebrar clientes.

### Feature flags propostas

```python
# config.py
class Settings:
    # Existing
    use_clickhouse: bool = True
    
    # New - Streaming 2.0
    streaming_read_enabled: bool = False  # Master switch
    streaming_marts: list[str] = []       # Marts migradas ["agg_vendas_diaria"]
    streaming_fallback_on_lag: bool = True # Se lag > threshold, usa batch
    streaming_lag_threshold_seconds: int = 30
```

### Implementação no facade

```python
# repos_analytics.py
def _resolve_source(func_name: str) -> str:
    if not settings.streaming_read_enabled:
        return "batch"
    if func_name in settings.streaming_marts:
        lag = _check_streaming_lag()
        if lag > settings.streaming_lag_threshold_seconds and settings.streaming_fallback_on_lag:
            logger.warning("streaming_lag_exceeded", func=func_name, lag=lag)
            return "batch"
        return "streaming"
    return "batch"
```

### Rollback
- Desligar `STREAMING_READ_ENABLED=false` no env
- Zero código de streaming no hot path (flag off = código antigo)

---

## 7. Fase 5: Dashboards Migrados

### Objetivo
Dashboards leem de marts streaming com latência < 10s.

### Migração gradual (por tela)

| Ordem | Tela | Marts envolvidas | Risco |
|-------|------|-----------------|-------|
| 1 | Caixa (caixa agora) | alerta_caixa_aberto | Baixo |
| 2 | Vendas (série hora) | agg_vendas_hora | Baixo |
| 3 | Dashboard Geral | agg_vendas_diaria, risco | Médio |
| 4 | Antifraude | fraude_cancelamentos | Médio |
| 5 | Financeiro | financeiro_vencimentos | Médio |
| 6 | Clientes | churn, RFM | Alto |

### Critério de saída por tela
- Dados iguais ao batch (reconciliação automática)
- Latência < 10s end-to-end
- Zero regressão de funcionalidade
- Testes de contrato passando

---

## 8. Fase 6: Alertas

### Objetivo
Alertas push em tempo real baseados em eventos CDC.

### Implementação

```
CDC Consumer → detecta condição → insere em torqmind_ops.alerts_pending
API polling (ou WebSocket) → entrega ao frontend / push notification
```

### Alertas fase 1

| Alerta | Condição | Ação |
|--------|---------|------|
| Caixa aberto > 4h | turno sem fechamento_ts por > 4h | Push + badge |
| Cancelamento alto valor | cancel + valor > P95 da filial | Push + badge |
| Operador outlier detectado | FUNCIONARIO_OUTLIER com score > 80 | Badge em Antifraude |
| Meta em risco | projeção < 70% após dia 20 | Notificação equipe |

### Tecnologia
- **Fase 6a:** Polling via endpoint `/bi/notifications/alerts` a cada 30s
- **Fase 6b:** WebSocket ou SSE para push real-time
- **Fase 6c:** Push notification (mobile PWA)

---

## 9. Fase 7: Agent/Jarvis

### Objetivo
Assistente inteligente que responde perguntas operacionais em português.

### Pré-requisitos
- [x] Marts consolidadas
- [x] Jarvis briefing endpoint
- [ ] Streaming com latência < 10s
- [ ] Alertas implementados
- [ ] Embeddings de contexto operacional

### Capacidades progressivas

| Versão | Capacidade | Exemplo |
|--------|-----------|---------|
| 0.1 (atual) | Briefing passivo | "Resumo do dia" |
| 0.2 | Perguntas diretas | "Quanto vendeu filial X ontem?" |
| 0.3 | Alertas narrados | "Caixa 3 está aberto há 6h. Ligar pro operador?" |
| 0.4 | Sugestões proativas | "Diesel caiu 15% esta semana. Verificar?" |
| 0.5 | Ações executivas | "Fechar turno X automaticamente" |

---

## 10. Rollback

### Por fase

| Fase | Rollback | Tempo |
|------|----------|-------|
| 1 (CDC paralelo) | `make streaming-down` | 5 segundos |
| 2 (validação) | Ignorar current — é paralelo | Imediato |
| 3 (marts streaming) | Não expõe para API — dropar tables | 1 minuto |
| 4 (feature flag) | `STREAMING_READ_ENABLED=false` | Restart API (10s) |
| 5 (dashboards) | Flag off = volta para batch marts | 10 segundos |
| 6 (alertas) | Desabilitar polling/WebSocket | Deploy |
| 7 (Agent) | Feature flag off | Deploy |

### Rollback total
```bash
# Parar streaming inteiro
make streaming-down

# Desabilitar no env
STREAMING_READ_ENABLED=false

# API volta a usar batch marts automaticamente
docker compose restart api
```

---

## 11. Checklists

### Pré-deploy streaming (qualquer fase)

- [ ] `make streaming-config-check` passa
- [ ] `make test-cdc-consumer` passa (29 testes)
- [ ] Variáveis `POSTGRES_*` e `CLICKHOUSE_*` configuradas no env de produção
- [ ] PostgreSQL com `wal_level=logical` confirmado
- [ ] `max_replication_slots >= 10` confirmado
- [ ] Rede Docker compartilhada existe (`torqmind_default`)
- [ ] ClickHouse acessível na mesma rede
- [ ] Disco: >20GB livre para Redpanda + raw events

### Pós-deploy (validação)

- [ ] `make streaming-status` mostra todos containers healthy
- [ ] Debezium connector status = RUNNING
- [ ] `rpk topic list` mostra tópicos `torqmind.dw.*`
- [ ] Consumer group lag < 100 mensagens
- [ ] `streaming-validate-cdc.sh` mostra raw_count > 0
- [ ] `streaming-validate-cdc.sh` mostra current tables com dados
- [ ] Comparação PG vs CH current: divergência < 0.01%

### Migração de mart

- [ ] Mart streaming criada com MV ou consumer
- [ ] Reconciliação automática rodando (batch vs streaming)
- [ ] Feature flag ativada para mart específica
- [ ] Monitoramento de lag com threshold configurado
- [ ] Fallback automático para batch se lag > threshold
- [ ] Testes de contrato API passando
- [ ] Performance < 500ms P99

---

## 12. Comandos

### Operação diária

```bash
# Status geral
make streaming-status

# Verificar lag
docker compose -f docker-compose.streaming.yml exec -T redpanda \
  rpk group describe torqmind-cdc-consumer

# Logs do consumer
make streaming-logs

# Validar dados
make streaming-validate-cdc

# Reiniciar consumer (sem perda — offset committed)
docker compose -f docker-compose.streaming.yml restart cdc-consumer
```

### Bootstrap / Reset

```bash
# Subir tudo do zero
make streaming-up
make streaming-init-clickhouse
make streaming-register-debezium

# Forçar re-snapshot (cuidado: reprocessa tudo)
curl -X POST http://localhost:18083/connectors/torqmind-postgres-cdc/restart

# Reset completo (dev only)
make streaming-down
docker volume rm torqmind_redpanda_data
make streaming-up
make streaming-init-clickhouse
make streaming-register-debezium
```

### Troubleshooting

```bash
# Connector com erro
curl -s http://localhost:18083/connectors/torqmind-postgres-cdc/status | python3 -m json.tool

# Consumer errors no ClickHouse
docker exec torqmind-clickhouse-1 clickhouse-client \
  --user torqmind --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT * FROM torqmind_ops.cdc_errors ORDER BY created_at DESC LIMIT 10"

# Raw events count
docker exec torqmind-clickhouse-1 clickhouse-client \
  --user torqmind --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT count() FROM torqmind_raw.cdc_events"
```

---

## Timeline Estimada

| Fase | Duração | Dependência |
|------|---------|-------------|
| 1 - CDC Paralelo | ✅ Completa | — |
| 2 - Validação | 1-2 semanas | Staging environment |
| 3 - Mart Piloto | 1 semana | Fase 2 green |
| 4 - Feature Flag | 2-3 dias | Fase 3 validada |
| 5 - Dashboards | 2-4 semanas | Fase 4 + por tela |
| 6 - Alertas | 1-2 semanas | Fase 5 parcial |
| 7 - Agent | Contínuo | Fase 6 |

**Total estimado: 8-12 semanas para streaming-first completo.**
