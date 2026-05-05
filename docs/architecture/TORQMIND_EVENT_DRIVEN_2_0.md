# TorqMind 2.0 — Event-Driven Architecture

## 1. Objetivo

Transformar o TorqMind de um sistema batch-centric (cron/shell ETL) para uma plataforma event-driven capaz de:
- Latência sub-minuto para alertas operacionais
- Processamento contínuo de vendas, caixa, fraude e risco
- Base para Agent/Jarvis reativo
- Escalabilidade para centenas de postos

## 2. Por que sair de cron/shell como motor principal

| Problema | Impacto |
|---|---|
| Cron 2min é o menor intervalo prático | Alertas atrasam 2-4 min no melhor caso |
| Full refresh de marts é pesado | Custa CPU e impede escala |
| Lock/timeout em pipeline shell | Uma falha trava próximo ciclo |
| Sem replay/backfill nativo | Rebuild exige scripts manuais |
| Sem semântica de evento | Agent não sabe "o que mudou" |

O pipeline cron/shell continua operando como fallback confiável. A migração para streaming é incremental.

## 3. Arquitetura Alvo

```
┌──────────────┐     CDC (logical replication)     ┌──────────────┐
│  PostgreSQL  │ ─────────────────────────────────► │   Redpanda   │
│  (canônico)  │          Debezium                  │   (broker)   │
└──────────────┘                                    └──────┬───────┘
                                                           │ events
                                                    ┌──────▼───────┐
                                                    │  TorqMind    │
                                                    │ CDC Consumer │
                                                    └──────┬───────┘
                                                           │ inserts/upserts
                                              ┌────────────┼────────────┐
                                              ▼            ▼            ▼
                                    ┌─────────────┐ ┌────────────┐ ┌─────────┐
                                    │torqmind_raw │ │torqmind_   │ │torqmind_│
                                    │(append-only)│ │current     │ │ops      │
                                    └─────────────┘ │(state)     │ │(monitor)│
                                                    └────────────┘ └─────────┘
                                                           │
                                                    ┌──────▼───────┐
                                                    │torqmind_mart │
                                                    │(streaming)   │
                                                    └──────────────┘
                                                           │
                                              ┌────────────┼────────────┐
                                              ▼            ▼            ▼
                                        ┌─────────┐ ┌──────────┐ ┌─────────┐
                                        │ FastAPI │ │  Next.js  │ │  Agent  │
                                        └─────────┘ └──────────┘ └─────────┘
```

## 4. Por que Redpanda

- **Kafka-compatible**: ecossistema completo (Debezium, Flink, etc.)
- **Single binary**: sem ZooKeeper, operação simplificada
- **Baixo consumo de memória**: cabe no servidor 8 GB
- **Performance superior**: escrita em C++, otimizado para NVMe/SSD
- **Console incluso**: UI de monitoramento sem custo
- **Self-hosted**: sem dependência de cloud

Alternativas descartadas:
- Kafka puro: requer ZooKeeper ou KRaft, mais RAM
- NATS JetStream: não tem ecossistema Debezium nativo
- RabbitMQ Streams: limitado para replay/compaction

## 5. Por que Debezium

- **Padrão de mercado** para CDC PostgreSQL
- **Snapshot + streaming** em um único fluxo
- **Logical replication** sem trigger/polling
- **Idempotente** com replication slot
- **Monitorável** via REST API
- **Extensível** com transforms/SMTs

Alternativas descartadas:
- Trigger-based CDC: overhead no PostgreSQL, acoplamento
- pg_logical + custom: reinventar a roda
- AWS DMS: serviço pago

## 6. Por que Consumer próprio (vs Kafka Connect Sink)

- **Controle total** sobre mapeamento Debezium → ClickHouse
- **Idempotência customizada** por chave natural da tabela
- **Soft-delete** handling (is_deleted flag)
- **Raw + Current + Ops** em uma mesma passagem
- **Observabilidade** integrada ao domínio TorqMind
- **Python stack** consistente com backend

## 7. Por que ClickHouse raw/current/marts

### torqmind_raw
- Log imutável de todos os eventos CDC
- Auditoria, replay, debug
- TTL 90 dias (ajustável)

### torqmind_current
- Estado mais recente de cada registro
- ReplacingMergeTree com version = source_ts_ms
- Consultas de estado rápidas sem JOIN pesado
- Base para marts streaming futuras

### torqmind_ops
- Offsets, lag, erros, estado por tabela
- Alimenta aba "Plataforma" no frontend
- Alertas de saúde do pipeline

### torqmind_mart (existente)
- Marts agregadas para dashboards
- Pilot: streaming_vendas_diaria, streaming_pagamentos_diaria
- Futuro: todas as marts migram para cálculo incremental

## 8. Como funciona backfill + streaming

```
Fase 1: Snapshot
  - Debezium faz snapshot com snapshot.mode=initial
  - Todas as linhas existentes viram eventos op='r' (read)
  - Consumer escreve em raw + current

Fase 2: Streaming
  - Após snapshot, Debezium entra em modo streaming
  - Novos inserts/updates/deletes geram eventos c/u/d
  - Consumer mantém current atualizado

Fase 3: Validação
  - Script streaming-validate-cdc.sh compara counts PG vs CH
  - Divergências durante snapshot são esperadas
  - Após convergência, CDC é fonte confiável de freshness
```

O pipeline cron/shell antigo continua rodando em paralelo. Nenhum cutover é feito nesta fase.

## 9. Como evitar duplicidade

### Raw layer
- MergeTree com projeção dedup por (topic, partition, offset)
- Idempotente: mesmo offset processado N vezes → mesma linha

### Current layer
- ReplacingMergeTree com version = source_ts_ms
- Inserção de mesma PK com mesmo ou menor ts_ms → descartada no merge
- FINAL em queries garante dedup lógica

### Consumer
- Commit de offset somente após flush com sucesso
- at-least-once: pode reprocessar, mas resultado é idempotente
- Batch insert no ClickHouse é atômico por batch

## 10. Como lidar com deletes

- Evento Debezium `op='d'` carrega `before` com dados da linha
- Consumer escreve no current com `is_deleted=1`
- ReplacingMergeTree mantém versão mais recente (incluindo delete marker)
- Queries usam `WHERE is_deleted = 0` para estado limpo
- Raw mantém evento completo para auditoria/replay
- Não há DELETE físico no ClickHouse

## 11. Como validar

```bash
# Comparar counts PostgreSQL vs ClickHouse current
ENV_FILE=.env ID_EMPRESA=1 ./deploy/scripts/streaming-validate-cdc.sh

# Status geral (containers, lag, erros)
./deploy/scripts/streaming-status.sh

# Queries manuais
docker exec torqmind-clickhouse clickhouse-client --query "
  SELECT table_name, events_total, last_event_at
  FROM torqmind_ops.cdc_table_state FINAL
  ORDER BY last_event_at DESC"
```

## 12. Como operar no Ubuntu 4 CPU / 8 GB

Configuração prod-lite:
- Redpanda: 512 MB (--memory 512M, --smp 1)
- Debezium Connect: 768 MB (Xms256/Xmx512)
- CDC Consumer: 256 MB
- Total streaming extra: ~1.5 GB
- ClickHouse já existente: compartilhado

```bash
# Perfil prod-lite (sem Console UI)
STREAMING_PROFILE=prod-lite ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-up.sh
```

Resource limits no compose:
```yaml
deploy:
  resources:
    limits:
      memory: 768M  # Redpanda
      memory: 768M  # Debezium
      memory: 256M  # Consumer
```

## 13. Como operar local (PC forte)

Dev machine: i9 14900KF, 64 GB RAM, RTX 4070

```bash
# Perfil full com Redpanda Console
ENV_FILE=.env STREAMING_PROFILE=local-full ./deploy/scripts/streaming-up.sh

# Console disponível em http://localhost:18080
# Debezium REST em http://localhost:18083
```

Sem restrições de memória significativas em dev.

## 14. Fase 2: Flink / Temporal / Observabilidade

### Apache Flink (futuro)
- CEP (Complex Event Processing) para fraude em tempo real
- Aggregation windows para marts streaming
- Enrichment com dimensões em memória
- Watermarks para late data handling

### Temporal (futuro)
- Workflows de onboarding de novo tenant
- Backfill orquestrado com retry/checkpoint
- Replay de janela específica
- Scheduled reconciliation jobs

### Observabilidade Fase 2
- Prometheus metrics (consumer lag, throughput, errors)
- Grafana dashboards
- Alertmanager para SLA de latência
- Dead letter queue para eventos irrecuperáveis

## 15. Agent no futuro

```
Redpanda (events)  →  torqmind_mart (features)  →  TorqMind Agent
                                                         │
                                                    ┌────┼────┐
                                                    ▼    ▼    ▼
                                                Alertas  Recs  Briefings
```

O Agent consumirá:
- Eventos brutos para triggers (ex: abertura de caixa)
- Features de mart para contexto (ex: vendas da filial)
- Alertas calculados para priorização
- Histórico de decisões para aprendizado

## 16. Plano de cutover em fases

### Fase 0 (base) ✓
- Stack paralela: Redpanda + Debezium + Consumer + ClickHouse schemas
- Sem impacto no sistema atual
- Validação de ponta a ponta

### Fase 1 (STG-direto)
- CDC rodando com snapshot completo das tabelas STG canonicas
- MartBuilder com `source=stg`
- Validacao STG vs mart_rt e smoke E2E sem ETL STG->DW

### Fase 2
- API consulta torqmind_current para endpoints selecionados (ex: caixa aberto)
- Feature flag por endpoint
- Fallback para pipeline atual se latência CDC > threshold

### Fase 3
- Marts streaming substituem marts batch
- Pipeline cron reduzido a reconciliação periódica
- Agent começa a consumir eventos

### Fase 4
- Pipeline cron desligado (mantido como disaster recovery)
- Todas as marts são streaming-driven
- Temporal orquestra backfill/replay
- Agent operacional pleno

## 17. Riscos

| Risco | Mitigação |
|---|---|
| Replication slot cresce sem Consumer ativo | Monitorar pg_replication_slots; alerta se slot > 1 GB |
| ClickHouse ReplacingMergeTree não deduplica imediatamente | Usar FINAL em queries; aceitar delay de merge |
| Debezium perde offset após crash | Snapshot initial reconstrói; slot preserva posição |
| Servidor 8 GB não suporta carga total | Perfil prod-lite com limits; escalar se necessário |
| Consumer Python single-threaded é gargalo | Batch inserts + flush interval; scale out se necessário |
| Schema change em PostgreSQL quebra connector | Monitorar; Debezium schema evolution; reconfigure se necessário |
| Duplicatas durante snapshot se Consumer reinicia | Raw aceita; Current deduplica por ReplacingMergeTree |
| Heartbeat falha e slot é dropado | heartbeat.interval.ms=30s; monitorar slot age |

---

## Referência Rápida

### Estrutura de diretórios

```
apps/cdc_consumer/                  # CDC Consumer service
  Dockerfile
  pyproject.toml
  requirements.txt
  torqmind_cdc_consumer/
    main.py                         # Entry point
    config.py                       # Settings from env
    debezium.py                     # Event parsing
    clickhouse_writer.py            # Write to CH layers
    mappings.py                     # Table → CH mappings
    state.py                        # Consumer state
    logging.py                      # Structured logging
  tests/

deploy/debezium/connectors/         # Debezium connector config
deploy/scripts/streaming-*.sh       # Operations scripts

sql/clickhouse/streaming/           # ClickHouse DDL
  001_databases.sql
  010_raw_events.sql
  020_current_tables.sql
  030_ops_tables.sql
  040_pilot_marts.sql

docker-compose.streaming.yml        # Streaming stack compose
```

### Environment Variables (CDC Consumer)

| Variable | Default | Description |
|---|---|---|
| REDPANDA_BROKERS | redpanda:9092 | Kafka bootstrap servers |
| CLICKHOUSE_HOST | clickhouse | ClickHouse host |
| CLICKHOUSE_PORT | 8123 | ClickHouse HTTP port |
| CLICKHOUSE_USER | torqmind | ClickHouse user |
| CLICKHOUSE_PASSWORD | | ClickHouse password |
| CDC_CONSUMER_GROUP | torqmind-cdc-consumer | Kafka consumer group |
| CDC_TOPICS | | Explicit topic list (comma-sep) |
| CDC_TOPIC_PATTERN | ^torqmind\\..* | Regex pattern for auto-subscribe |
| CDC_BATCH_SIZE | 500 | Flush after N events |
| CDC_FLUSH_INTERVAL_SECONDS | 5 | Flush every N seconds |
| REALTIME_MARTS_SOURCE | stg | MartBuilder source; `stg` is final hot path, `dw` is compatibility |
| LOG_LEVEL | INFO | Log verbosity |

### Debezium Topics (generated)

| Topic | Source Table |
|---|---|
| torqmind.stg.comprovantes | stg.comprovantes |
| torqmind.stg.itenscomprovantes | stg.itenscomprovantes |
| torqmind.stg.formas_pgto_comprovantes | stg.formas_pgto_comprovantes |
| torqmind.stg.turnos | stg.turnos |
| torqmind.stg.entidades | stg.entidades (clientes no schema atual) |
| torqmind.stg.produtos | stg.produtos |
| torqmind.stg.grupoprodutos | stg.grupoprodutos |
| torqmind.stg.funcionarios | stg.funcionarios |
| torqmind.stg.usuarios | stg.usuarios |
| torqmind.stg.localvendas | stg.localvendas |
| torqmind.stg.contaspagar | stg.contaspagar |
| torqmind.stg.contasreceber | stg.contasreceber |
| torqmind.app.payment_type_map | app.payment_type_map |

DW topics can still exist for reconciliation and rollback, but they are not the accepted realtime BI hot path.

### Natural Keys (idempotency)

| Table | Primary Key |
|---|---|
| fact_venda | (id_empresa, id_filial, id_db, id_movprodutos) |
| fact_venda_item | (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) |
| fact_pagamento_comprovante | (id_empresa, id_filial, referencia, tipo_forma) |
| fact_caixa_turno | (id_empresa, id_filial, id_turno) |
| fact_comprovante | (id_empresa, id_filial, id_db, id_comprovante) |
| fact_financeiro | (id_empresa, id_filial, id_db, tipo_titulo, id_titulo) |
| fact_risco_evento | (id_empresa, id_filial, id) |
| dim_filial | (id_empresa, id_filial) |
| dim_produto | (id_empresa, id_filial, id_produto) |
| dim_grupo_produto | (id_empresa, id_filial, id_grupo_produto) |
| dim_funcionario | (id_empresa, id_filial, id_funcionario) |
| dim_usuario_caixa | (id_empresa, id_filial, id_usuario) |
| dim_local_venda | (id_empresa, id_filial, id_local_venda) |
| dim_cliente | (id_empresa, id_filial, id_cliente) |
| payment_type_map | (id) |

### Operations Commands

```bash
# Start streaming stack (local)
make streaming-up

# Initialize ClickHouse schemas
make streaming-init-clickhouse

# Register Debezium connector
make streaming-register-debezium

# Check status
make streaming-status

# Validate CDC data
make streaming-validate-cdc

# View logs
make streaming-logs

# Stop streaming stack
make streaming-down
```
