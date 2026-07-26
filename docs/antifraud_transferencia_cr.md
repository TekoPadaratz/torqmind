# Antifraude — Transferência de contas a receber (descoberta)

## Objetivo

Detectar quando um título de `CONTASRECEBER` muda de entidade (ex.: João → José),
sinal operacional relevante para o dono do posto.

## O que existe hoje no TorqMind

| Camada | Achado |
|--------|--------|
| Agent | `contasreceber`, `contasreceberbaixa`, `entidades`, `movcreditoentidades` |
| STG | `stg.contasreceber` guarda só o **estado atual** de `ID_ENTIDADE` |
| DW | `dw.fact_financeiro` sobrescreve `id_entidade` no UPSERT — **sem histórico** |
| CH | `stg_contasreceber` idem — sem mart de eventos de transferência |
| Auditoria | Não há dataset/migração de log de troca de entidade em CR |

`stg.controle_troca_pgto` é troca de **forma de pagamento**, não de entidade do título.
`stg.movcreditoentidades` é movimento de crédito do cliente (entrada/saída), não transferência de título.

## Busca no Xpert (pendência operacional)

Garimpar no SQL Server (`ATXDADOS`) tabelas/views com nomes do tipo:

- `*TRANSF*CONTAS*`, `*TRANSFER*RECEBER*`, `*AUDITORIA*CONTAS*`
- `*HIST*ENTIDADE*`, `*ALTERA*ENTIDADE*`, `LOG_*`

Query sugerida (no host do agent / SSMS):

```sql
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.name LIKE '%TRANSF%' OR t.name LIKE '%TRANSFER%'
   OR t.name LIKE '%AUDITORIA%' OR t.name LIKE '%LOG_%'
ORDER BY 1, 2;
```

Se existir log com `ID_CONTASRECEBER`, `ID_ENTIDADE` de/para, data e usuário →
criar agent dataset + STG + mart_rt + grid (Filial | Data | Documento/título | De | Para | Valor | Usuário).

## Fonte confirmada (homolog)

O Xpert grava no `CONTASRECEBER.HISTORICO` o texto:

`Transferência de Conta do cliente {id_de} para o {id_para}`

O título permanece com `ID_ENTIDADE = id_para` (estado atual). Não há tabela de
auditoria separada no STG; o histórico textual **é** a evidência operacional.

## Implementação

- Leitura CH: `torqmind_current.stg_contasreceber` (payload HISTORICO)
- Parse dos IDs de/para + nomes via `dim_cliente` / `stg_entidades`
- API: `fraud_transferencia_cr` (realtime)
- UI Antifraude → Risco financeiro: grid Filial | Data | Título | De | Para | Valor

