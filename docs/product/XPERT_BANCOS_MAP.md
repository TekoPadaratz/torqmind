# Mapeamento Xpert — Contas bancárias e saldos (canônico 2026-07-24)

## Achado principal

O saldo **não** mora em uma tabela `BANCOS`. Mora em **conta corrente**
(`CONTASBANCARIA`) + movimentos (`MOVBANCOS`) **+ ajustes de plano**
(`MOVLCTOS` com documentos de ajuste de saldo/empréstimo ligados a
`CONTASBANCARIA.ID_PLANODECONTAS`).

### O que NÃO compõe saldo CC (varredura 2026-07-24)

| Objeto | Motivo |
|--------|--------|
| `SALDOSBANCARIOS` | **0 linhas** neste cliente |
| `SALDOS` | Entrada/saída por turno/plano — não é saldo CC cumulativo geral |
| `OFXIMPORTADOS` / `ITENSOFX` | Extrato/conciliação; `VLRSALDO` é snapshot do arquivo, não CC Xpert |
| `LCTOENTRECONTASBCO` | Só ponteiro (`REFERENCIA` → `MOVLCTOS` diverso), sem valor próprio |
| `NEGATIVACONTABANCARIA` | Config de negativação (sem valor) |
| `CONCILIACAOBANCARIA` | Link OFX↔movimento |
| `MOVLCTOS` genérico no plano | Empréstimos/transf/taxas sem filtro destroem Itaú (~centenas de M) |
| `MOVBANCOS` com `ID_DB≠ID_FILIAL` | Réplica matriz (ex. R$ 1,50) — inclui ≈ −R$ 230M na conta 2 |

## Prova Banrisul VR01 (conta 11 / filial 14458) — 2026-07-24

| Fonte | Valor |
|-------|------:|
| `MOVBANCOS` vivos `ID_DB=ID_FILIAL` (só TIPO=8 PIX TEF, 29/03–11/04/2025) | **152.833,35** |
| `MOVLCTOS` `DOCUMENTO='TRANSF AJUSTE PIX'` plano **414** TIPO=1 em 31/03/2025 | **−24.869,45** |
| Contrapartida mesma `REFERENCIA` no plano **815** (conta 35 Sicoob) TIPO=0 | **+24.869,45** |
| **Saldo TorqMind / alvo tela** | **127.963,90** |

O ajuste **não** aparece em `MOVBANCOS`. Por isso somar só movimentos
bancários inflava a Banrisul em R$ 24.869,45. O “corte 01/04/2025” que
coincidia com o alvo era efeito colateral de excluir os PIX de março —
a regra canônica é o **TRANSF AJUSTE** no plano, não uma data mágica.

Outras contas ativas (Itaú 2, Bradesco 7) **não** têm `TRANSF AJUSTE*` no
plano ligado → saldo permanece = só `MOVBANCOS`. A contrapartida Sicoob
recebe +24.869,45 (partida dobrada contábil; total da filial inalterado).

## R$ 1,50 “sumiu” da tabela

Débitos tipo `Bx.Fatura` (ex.: R$ 1,50) **existem** em `MOVBANCOS`, mas com
`ID_FILIAL=14458` e **`ID_DB=14126`** (matriz). O ETL de saldo exige
`ID_DB=ID_FILIAL` — incluir o DB da matriz na conta 2 gera ≈ **−R$ 230M**
de lixo. A tela Xpert lista por filial; o demonstrativo de saldo filtra
diferente. **Não** misturar os dois DBs no saldo.

## Compensado (`CONCILIADO`) — prova 2026-07-21

Filtro “Compensado” da tela Xpert mapeia para `MOVBANCOS.CONCILIADO=1`.

| Conta VR01 (as-of < 2026-06-30) | Só `CONCILIADO=1` | Todos vivos |
|---|---:|---:|
| 11 Banrisul 06.012539.0-4 | **R$ 0,00** | R$ 152.833,35 (antes do ajuste) |
| 2 Itaú | R$ 2.593.453 | R$ 24.904.741 |

A Banrisul da referência (PIX TEF, TIPOLCTO=4) tem **100% dos lançamentos com
`CONCILIADO=0`**. Filtrar só compensados zera a conta — **não** usar.

## Objetos Xpert (ATXDADOS)

| Objeto | Tipo | Papel | Volume (cliente) |
|--------|------|--------|------------------|
| `dbo.CONTASBANCARIA` | tabela | Cadastro da conta (agência, C/C, banco, ativo, `ID_PLANODECONTAS`, `SALDOUNIFICADO`) | 1.320 (100 ativas) |
| `dbo.BANCOSPADRAO` | tabela | Domínio FEBRABAN (`CODIGOBANCOSPADRAO` → nome) | 4.312 |
| `dbo.MOVBANCOS` | tabela | Lançamentos (depósito, PIX, pagamento, cheque…) | ~656k / ~585k vivos |
| `dbo.MOVLCTOS` | tabela | Lançamentos de plano; `TRANSF AJUSTE*` ajusta saldo bancário | ~23M (filtro estreito no agent) |
| `dbo.SALDOS` | tabela | Entradas/saídas por turno/plano — **não** é saldo CC cumulativo geral | auxiliar |
| `dbo.SALDOSBANCARIOS` | tabela | Snapshot `SALDOATUAL` | **0 linhas** |
| `dbo.OFXIMPORTADOS` / `ITENSOFX` | tabela | Extrato OFX / conciliação | auxiliar |
| `SX_LCTOBANCO`, `SX_DEMONSTRATIVOFINANCEIROBANCOS`, … | procs | UI Xpert | **criptografadas** |

### PKs reais

- `CONTASBANCARIA`: `(ID_CONTASBANCARIAS, ID_FILIAL)` — o ID da conta **não é global**.
- `MOVBANCOS`: `(ID_FILIAL, ID_MOVBANCOS, ID_DB)`.
- `MOVLCTOS`: `(ID_FILIAL, ID_MOVLCTOS, ID_DB)`.

### Colunas úteis — `MOVBANCOS`

`ID_MOVBANCOS`, `ID_FILIAL`, `ID_DB`, `ID_CONTASBANCARIAS`, `VALOR`, `TIPO`,
`OPERACAO`, `TIPOLCTO`, `DTACONTA`, `DTAPGTO`, `NRODOCUMENTO`, `NOMINAL`,
`CONCILIADO`, `DELETAR`, `DATAREPL`.

| TIPO | Exemplos | Sinal natural |
|------|----------|---------------|
| 1, 8 | Venda/NFC-e creditada, PIX TEF, crédito cliente | **+** (entrada) |
| 3, 5 | Pagamento fornecedor, baixa fatura, débito | **−** (saída) |
| `OPERACAO=1` | Estorno | **inverte** o sinal |
| `OPERACAO=2` | raro (TIPO 3) | ignorar no saldo (sinal 0) |

### Ajuste de plano — documentos aceitos (`etl.movbancos_ajuste_plano_documento_ok`)

| Prefixo `DOCUMENTO` (upper/trim) | Exemplo |
|----------------------------------|---------|
| `TRANSF AJUSTE%` | `TRANSF AJUSTE PIX` (Banrisul→Sicoob) |
| `AJUSTE-SALDO%` | `AJUSTE-SALDO CREDOR VR05` |
| `AJUSTE SALDO%` | `AJUSTE SALDO`, `ajuste saldo TVR` |
| `AJUSTE DE SALDOS%` | `Ajuste de Saldos` |
| `AJUSTE EMPRESTIMO%` | `AJUSTE EMPRESTIMO VR05` |

Excluir de propósito: `ajuste a taxa…`, cartões, `TRANSF VR01 P/…` genérico.

| Campo | Uso |
|-------|-----|
| `ID_PLANODECONTAS` | = `CONTASBANCARIA.ID_PLANODECONTAS` da conta |
| `TIPO` | 0 = +VALOR; 1 = −VALOR (no saldo bancário) |
| `ESTORNO` | ignorar se verdadeiro |
| `ID_DB` | **não** filtrar `= ID_FILIAL` (ajuste costuma vir da matriz) |

## Regra canônica de saldo (Solvência)

Contrato TorqMind: **posição na abertura do mês** = meia-noite do dia 1
(`America/Sao_Paulo`), movimentos/ajustes com data **&lt; dia_1**.

```sql
-- MOVBANCOS
DELETAR = 0
ID_DB = ID_FILIAL
DTACONTA < make_date(ano, mes, 1)
sinal via etl.movbancos_sinal

-- + ajuste plano
stg.movbancos_ajuste_plano / MOVLCTOS
etl.movbancos_ajuste_plano_documento_ok(DOCUMENTO)
ESTORNO = 0
join CONTASBANCARIA.ID_PLANODECONTAS
sinal via etl.movbancos_ajuste_plano_sinal
DTACONTA < make_date(ano, mes, 1)

-- Total operacional da filial
SUM(saldo) WHERE ATIVO = 1
```

### Prova VR01 (filial 14458, as-of 2026-07-01) — após ajuste

| Conta | Banco | C/C | Saldo |
|-------|-------|-----|-------|
| 2 | Itaú | 36370-0 | 24.919.471 |
| 35 | Sicoob | 95032-7 | 2.023.953 **+ 24.869,45** |
| 7 | Bradesco | 16116-0 | 1.533.822 |
| 11 | Banrisul | 06.012539.0-4 | **127.963,90** |

## TorqMind

| Camada | Objeto |
|--------|--------|
| Agent | `movbancos`, `contasbancaria`, `bancospadrao`, **`movbancos_ajuste_plano`** (full refresh) |
| STG | `stg.movbancos`, `stg.contasbancaria`, `stg.bancospadrao`, **`stg.movbancos_ajuste_plano`** |
| ETL | `etl.movbancos_sinal`, `etl.movbancos_ajuste_plano_sinal`, `etl.movbancos_ajuste_plano_documento_ok`, `etl.refresh_liquidez_banco` (mig 112/113/**126**/ **127**) |
| Mart | `mart.liquidez_solvencia.ativo_banco` + `mart.solvencia_banco_conta` |
| UI | Solvência → seção **Bancos** |
| Bootstrap | `tools/bootstrap_bancos_from_xpert.py` (histórico MOVBANCOS + TRANSF AJUSTE*) |

**Importante:** saldo as-of é cumulativo. Bootstrap só desde 2025-01-01 **subestima**
o saldo (ex.: VR01 13,2M vs 28,6M com histórico completo). Coletar `MOVBANCOS` desde
a origem dos lançamentos (2019+ neste cliente).

## Não fazer

- Não usar `SALDOSBANCARIOS` neste cliente.
- Não somar movimentos de todos os `ID_DB` de `MOVBANCOS`.
- Não juntar conta só por `ID_CONTASBANCARIAS` (sem `ID_FILIAL`).
- Não ingerir `CONTAS_BANCARIAS_API` (segredos).
- Não truncar o histórico de `MOVBANCOS` se o objetivo é saldo de abertura do mês.
- Não hardcodar corte `2025-04-01` — usar ajustes de plano documentados.
- Não usar `SALDOS` (turno/plano) como saldo CC de todas as contas.
- Não ingerir todo `MOVLCTOS` do plano bancário como ajuste.
