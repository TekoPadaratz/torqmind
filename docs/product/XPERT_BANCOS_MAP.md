# Mapeamento Xpert — Contas bancárias e saldos (canônico 2026-07-17)

## Achado principal

O saldo **não** mora em uma tabela `BANCOS`. Mora em **conta corrente**
(`CONTASBANCARIA`) + movimentos (`MOVBANCOS`). `SALDOSBANCARIOS` existe no schema
mas está **vazia (0 linhas)** neste cliente — descartada.

## Objetos Xpert (ATXDADOS)

| Objeto | Tipo | Papel | Volume (cliente) |
|--------|------|--------|------------------|
| `dbo.CONTASBANCARIA` | tabela | Cadastro da conta (agência, C/C, banco, ativo) | 1.320 (100 ativas) |
| `dbo.BANCOSPADRAO` | tabela | Domínio FEBRABAN (`CODIGOBANCOSPADRAO` → nome) | 4.312 |
| `dbo.MOVBANCOS` | tabela | Lançamentos (depósito, PIX, pagamento, cheque…) | ~656k / ~585k vivos |
| `dbo.SALDOSBANCARIOS` | tabela | Snapshot `SALDOATUAL` | **0 linhas** |
| `dbo.CONCILIACAOBANCARIA` | tabela | Conciliação OFX ↔ movimento | auxiliar |
| `dbo.CONTAS_BANCARIAS_API` | tabela | Credenciais API PIX/boleto | **não usar** (segredo) |
| `SX_LCTOBANCO`, `SX_DEMONSTRATIVOFINANCEIROBANCOS`, `SX_CADASTROCONTASBANCARIA` | procs | UI Xpert | **criptografadas** (sem definition) |

### PKs reais

- `CONTASBANCARIA`: `(ID_CONTASBANCARIAS, ID_FILIAL)` — o ID da conta **não é global**
  (mesmo `ID_CONTASBANCARIAS` aparece em ~22 filiais com a mesma descrição).
- `MOVBANCOS`: `(ID_FILIAL, ID_MOVBANCOS, ID_DB)`.

### Colunas úteis — `CONTASBANCARIA`

`ID_CONTASBANCARIAS`, `ID_FILIAL`, `CODIGOBANCOSPADRAO`, `NROCONTA`, `AGENCIA`,
`DESCRICAO`, `ATIVO`, `ID_FILIAL_VINC`, `LIMITE`, `DATAREPL`.  
**Não existe** coluna `DELETAR` nesta tabela.

### Colunas úteis — `MOVBANCOS`

`ID_MOVBANCOS`, `ID_FILIAL`, `ID_DB`, `ID_CONTASBANCARIAS`, `VALOR`, `TIPO`,
`OPERACAO`, `TIPOLCTO`, `DTACONTA`, `DTAPGTO`, `NRODOCUMENTO`, `NOMINAL`,
`CONCILIADO`, `DELETAR`, `DATAREPL`.

Semântica observada nos textos:

| TIPO | Exemplos | Sinal natural |
|------|----------|---------------|
| 1, 8 | Venda/NFC-e creditada, PIX TEF, crédito cliente | **+** (entrada) |
| 3, 5 | Pagamento fornecedor, baixa fatura, débito | **−** (saída) |
| `OPERACAO=1` | Estorno | **inverte** o sinal |
| `OPERACAO=2` | raro (TIPO 3) | ignorar no saldo (sinal 0) até validação operacional |

## Regra canônica de saldo (Solvência)

Contrato TorqMind: **posição na abertura do mês** = meia-noite do dia 1
(`America/Sao_Paulo`), movimentos com `DTACONTA < dia_1`.

```sql
-- Filtros obrigatórios
DELETAR = 0
ID_DB = ID_FILIAL          -- CRÍTICO: sem isso a réplica cross-DB gera saldo negativo absurdo
DTACONTA < make_date(ano, mes, 1)
JOIN CONTASBANCARIA ON (ID_CONTASBANCARIAS, ID_FILIAL)

-- Sinal
TIPO IN (1,8) → +VALOR ; OPERACAO=1 → −VALOR
TIPO IN (3,5) → −VALOR ; OPERACAO=1 → +VALOR

-- Total operacional da filial
SUM(saldo) WHERE ATIVO = 1
```

### Prova VR01 (filial 14458, as-of 2026-07-01)

| Filtro | Total |
|--------|-------|
| Sem `ID_DB=ID_FILIAL` | **−R$ 116.212.506** (lixo) |
| Com `ID_DB=ID_FILIAL` + ativas | **+R$ 28.630.079** |

Contas ativas (mesma prova):

| Conta | Banco | C/C | Saldo |
|-------|-------|-----|-------|
| 2 | Itaú | 36370-0 | 24.919.471 |
| 35 | Sicoob | 95032-7 | 2.023.953 |
| 7 | Bradesco | 16116-0 | 1.533.822 |
| 11 | Banrisul | 06.012539.0-4 | 152.833 |

## TorqMind

| Camada | Objeto |
|--------|--------|
| Agent | `movbancos` (já), `contasbancaria`, `bancospadrao` (full refresh) |
| STG | `stg.movbancos`, `stg.contasbancaria`, `stg.bancospadrao` |
| ETL | `etl.movbancos_sinal`, `etl.refresh_liquidez_banco` (migration 112+113) |
| Mart | `mart.liquidez_solvencia.ativo_banco` + `mart.solvencia_banco_conta` |
| UI | Solvência → seção **Bancos** (auto as-of + hint por conta; override manual opcional) |
| Bootstrap | `tools/bootstrap_bancos_from_xpert.py` (histórico completo desde 2019) |

**Importante:** saldo as-of é cumulativo. Bootstrap só desde 2025-01-01 **subestima**
o saldo (ex.: VR01 13,2M vs 28,6M com histórico completo). Coletar `MOVBANCOS` desde
a origem dos lançamentos (2019+ neste cliente).

## Não fazer

- Não usar `SALDOSBANCARIOS` neste cliente.
- Não somar movimentos de todos os `ID_DB`.
- Não juntar conta só por `ID_CONTASBANCARIAS` (sem `ID_FILIAL`).
- Não ingerir `CONTAS_BANCARIAS_API` (segredos).
- Não truncar o histórico de `MOVBANCOS` se o objetivo é saldo de abertura do mês.