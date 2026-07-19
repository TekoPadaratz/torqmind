# Xpert → TorqMind — Despesas Operacionais (DRE / Solvência)

**Status:** canônico (homolog · VR01 jun/2026 vs `dre_referencia_xpert.md`)  
**Ignorar no relatório Xpert:** receitas (TorqMind usa vendas canônicas).  
**Usar:** totalizadores **nível 3** do plano (`3.2.01`, `3.2.02`, …).

## Fonte canônica no SQL Server (ATXDADOS)

| Objeto | Papel |
|--------|--------|
| `dbo.MOVLCTOS` | **Fonte do Demonstrativo de Resultado** — `DTACONTA`, `TIPO`, `VALOR`, `ID_PLANODECONTAS` |
| `dbo.PLANODECONTAS` | Cadastro / hierarquia (`CODIGOPLANODECONTAS`, `CONTAMAE`) |
| `dbo.CONTASPAGAR` | Contas a pagar (passivo/caixa) — **não** é a base do DRE de despesas |

`TIPO`: `0`/`2` = débito (soma +); `1` = crédito (soma −).  
Lançamentos com `ESTORNO=1` **entram** no DRE Xpert (não filtrar).

## Nível 3 (o que a tela agrega)

```
3.2.01  DESPESAS COM FUNCIONÁRIOS  → pessoal
3.2.02  DESPESAS COMERCIAIS        → comercial  (taxas cartão 3.2.02.07*)
3.2.03  DESPESAS ADMINISTRATIVAS   → administrativo
3.2.04  DESPESAS FINANCEIRAS       → financeiro
3.2.05  DESPESAS TRIBUTÁRIAS       → tributos
3.2.07  MATERIAL USO/CONSUMO       → administrativo
3.2.08  BRINDE/BONIFICAÇÃO         → comercial
3.2.09  PERDA/ROUBO/DETERIORAÇÃO   → excepcional
3.3     DESPESAS NÃO OPERACIONAIS  → excepcional
```

Folhas (`3.2.01.01`, `3.2.02.07.64`, …) são detalhe — não linhas do DRE resumido.

## Regime temporal

| Campo Xpert | Uso TorqMind |
|-------------|--------------|
| `MOVLCTOS.DTACONTA` | Competência (`ano_mes_competencia`) — **igual ao relatório** |
| `CONTASPAGAR.DTAVCTO` | **Não** usar para DRE de despesas |

## Pipeline

```
Xpert MOVLCTOS (3.2*/3.3*) + PLANODECONTAS
  → Agent / bootstrap_movlctos_despesas_from_xpert
  → stg.movlctos + stg.planodecontas
  → etl.load_dim_plano_contas_gerencial
  → etl.load_fact_despesa_operacional   (migration 115)
  → DRE / Solvência / Orçamento (PG)
```

## Prova VR01 · jun/2026 (`id_filial=14458`) vs referência

| Nível 3 | Referência Xpert | Fonte MOVLCTOS |
|---------|------------------|----------------|
| 3.2.01 pessoal | 136.603,47 | 136.603,47 |
| 3.2.02 comercial | 174.588,09 | ~174.692,75 (±0,06% taxas cartão) |
| 3.2.03 administrativo | 98.633,63 | 98.633,63 |
| 3.2.04 financeiro | 16.534,79 | ~16.601,06 (±0,4%) |
| 3.2.05 tributos | 11.913,33 | 11.913,33 |
| 3.2.09 excepcional | 60,00 | 60,00 |
| 3.3 excepcional | 477,94 | 477,94 |

Resíduo em taxas de cartão / ajustes financeiros: ordem de centenas de reais vs centenas de milhares — aceitável; investigar só se o cliente exigir batimento folha a folha.

## Regras absolutas

1. DRE de despesas = `MOVLCTOS` + `DTACONTA`, nunca `CONTASPAGAR`/`DTAVCTO`.
2. Agregar no **nível 3**; não espalhar folhas no DRE resumido.
3. Sem `stg.planodecontas` a dim fica vazia — bootstrap obrigatório.
4. Homolog: `-p torqmind-homolog` + `homolog.app.env`.
