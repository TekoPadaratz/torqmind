# ADR: isolamento do analytics de homologação

- Status: **proposto** — não executar sem aprovação explícita
- Data: 2026-08-14
- Contexto: Homolog e Prod compartilham ClickHouse, Redpanda, Debezium e CDC Consumer em `172.30.0.9`

## Problema

Um `ALTER` slim ou rebuild do `cdc-consumer` “só para Hom” quebra produção (`NUMBER_OF_COLUMNS_DOESNT_MATCH`, marts congeladas). Homolog não é isolado no analytics e não é descartável.

## Opção A — isolamento lógico na VM analytics atual

- Bancos ClickHouse separados (`torqmind_*_hom`)
- Tópicos, connectors e consumer groups separados
- Containers Hom com cgroup/limits
- Envs separados

**Blast radius residual:** mesmo kernel, disco, rede e daemon Docker. Um `down -v`, disco cheio ou Debezium mal configurado ainda atinge Prod. Operação mais complexa (dois connectors no mesmo cluster).

**Custo:** baixo (sem VM nova). **Risco residual:** médio-alto.

## Opção B — VM analytics exclusiva para Hom (preferencial)

VM nova na LAN `172.30.0.0/24` (ex.: `.11`) com ClickHouse, Redpanda, Debezium e CDC Consumer próprios.

- Sizing inicial: 4 vCPU, 16–32 GB RAM, SSD ≥ 500 GB (slim+marts), snapshot diário
- Firewall: Hom App `.10` → Hom analytics; Prod `.10`/`.8` **não** escrevem nessa VM
- Observabilidade: logs e `mart_publication_log` independentes
- Cutover: connector Hom aponta para PG Hom; Prod permanece em `.9`
- Rollback: desligar connector Hom; Prod intocado

**Custo:** uma VM extra + storage. **Risco residual:** baixo (falha Hom não congela `sales_daily_rt` de Prod).

## Recomendação

**Opção B** quando capacidade e custo permitirem. Opção A só como etapa intermediária, nunca como “Hom isolado”.

Não aplicar DDL, não provisionar VM e não mover tópicos neste ADR.
