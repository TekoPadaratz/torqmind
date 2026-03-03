# TorqMind Diagnostics Report (Fase 0)

Data: 2026-03-03
Ambiente: Docker local (`TORQMIND`)

## 0.1 Diagnostico de execucao (provas)

### `docker compose ps`

- `torqmind-postgres-1`: Up (healthy)
- `torqmind-api-1`: Up
- `torqmind-web-1`: Up

### `docker compose logs --tail=200 api`

- API iniciou com sucesso em `0.0.0.0:8000`
- Endpoints BI responderam `200` em chamadas recentes

### `\dn`

Schemas presentes:

- `app`
- `auth`
- `dw`
- `etl`
- `mart`
- `public`
- `stg`

### `\dt stg.*`

13 tabelas detectadas:

- `stg.comprovantes`, `stg.movprodutos`, `stg.itensmovprodutos`
- `stg.produtos`, `stg.grupoprodutos`, `stg.entidades`, `stg.funcionarios`
- `stg.filiais`, `stg.localvendas`, `stg.turnos`
- `stg.contaspagar`, `stg.contasreceber`, `stg.financeiro`

### `\dt dw.*`

11 tabelas detectadas:

- dimensoes: `dw.dim_filial`, `dw.dim_produto`, `dw.dim_grupo_produto`, `dw.dim_cliente`, `dw.dim_funcionario`, `dw.dim_local_venda`
- fatos: `dw.fact_comprovante`, `dw.fact_venda`, `dw.fact_venda_item`, `dw.fact_financeiro`, `dw.fact_risco_evento`

### `\dt mart.*`

- Sem tabelas fisicas em `mart` (esperado no modelo atual com `VIEW` + `MATERIALIZED VIEW`)

### `pg_matviews` em `mart`

13 MVs detectadas, incluindo:

- `mart.agg_vendas_diaria`
- `mart.agg_vendas_hora`
- `mart.agg_risco_diaria`
- `mart.risco_top_funcionarios_diaria`
- `mart.clientes_churn_risco`
- `mart.financeiro_vencimentos_diaria`

## 0.2 Contagens base

### STG

| tabela | total |
|---|---:|
| `stg.comprovantes` | 10,924,419 |
| `stg.movprodutos` | 10,909,620 |
| `stg.itensmovprodutos` | 18,546,845 |

### DW

| tabela | total |
|---|---:|
| `dw.fact_comprovante` | 10,924,419 |
| `dw.fact_venda` | 10,909,620 |
| `dw.fact_venda_item` | 18,546,845 |

### MART

| tabela | total |
|---|---:|
| `mart.agg_vendas_diaria` | 17,645 |
| `mart.agg_vendas_hora` | 335,407 |

## 0.3 Problemas de join/dimensao identificados

### Evidencias de quebra de nomenclatura (causa de "Sem ...")

- `dw.fact_venda` possui `23` filiais distintas
- `dw.dim_cliente`, `dw.dim_funcionario`, `dw.dim_grupo_produto` possuem apenas `3` filiais

Orfandade por join estrito (`id_empresa + id_filial + id`):

| metrica | total |
|---|---:|
| `fact_venda_sem_cliente` | 838,905 |
| `fact_venda_item_sem_grupo` | 18,544,041 |
| `fact_venda_item_sem_func` | 9,327,818 |

## Acoes corretivas aplicadas (Fase 0)

1. **API/consulta de clientes endurecida** em `apps/api/app/repos_mart.py`:
   - `customers_top`: troca de join estrito por `LEFT JOIN LATERAL` em `dim_cliente` por `id_empresa + id_cliente`, priorizando a mesma filial quando existir.
   - fallback de nome para `#ID <id_cliente>` quando dim nao existir/nao tiver nome.
2. **Churn endpoint** (`customers_churn_risk`) com fallback aditivo:
   - de `'(Sem cliente)'` para `#ID <id_cliente>` quando nome ausente.
3. **Correção de credibilidade ETL SQL**:
   - removida ambiguidade da função `etl.set_watermark` (assinatura duplicada de 3 args) para evitar erro `AmbiguousFunction` durante `etl.run_all`.

## ETL (1a/2a execucao), watermarks e endpoint

### ETL incremental medido

- Tenant de smoke (`id_empresa=999`), 2 execucoes consecutivas:
  - `run_all` #1: `duration_ms = 0`
  - `run_all` #2: `duration_ms = 0`
  - status: `ok` nas duas

Observacao operacional:
- Tentativa de medir `etl.run_all(1,false,false)` com volume real excedeu varios minutos e foi interrompida administrativamente para nao bloquear a sessao.
- Isso confirma gargalo de 1a execucao em base grande e reforca prioridade da Fase 3 (incremental/hot-window otimizado de ponta a ponta).

### Watermarks

`etl.watermark` atualizado e consistente para datasets principais (`comprovantes`, `movprodutos`, `itensmovprodutos`, `risk_events`) em `id_empresa=1`.

### Endpoint BI com dados

- Login: `POST /auth/login` -> `200`
- Overview: `GET /bi/dashboard/overview?dt_ini=2025-08-01&dt_fim=2025-08-31&id_empresa=1` -> `200`
- Chaves confirmadas no payload:
  - `kpis`: `faturamento`, `margem`, `ticket_medio`, `itens`
  - `risk`: `kpis`, `by_day`

## Gates de qualidade executados

- `make test`: **OK** (`Ran 3 tests`)
- `make lint`: **OK** (Next.js build + typecheck/lint + compile do backend)

## Arquivos tocados nesta fase

- `apps/api/app/repos_mart.py`
- `apps/api/app/test_smoke_api.py`
- `sql/migrations/005_etl_incremental_scalable.sql`
- `docs/diagnostics_report.md`

