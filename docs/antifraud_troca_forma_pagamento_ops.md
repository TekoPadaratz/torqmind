# Antifraude — Troca de forma de pagamento (ops)

## Sintoma (jul/2026)

Grid vazio no último mês (homolog/prod) apesar de trocas existirem no CH.

## Causa raiz

1. Watermark de `movlctoscancelados` usava `MAX(DTACONTA, DATAREPL)`.
2. `DTACONTA` de cheque/pré pode ser **futura** (ex.: 2027-04-29).
3. Watermark avançou para o futuro → incremental parou de capturar cancelamentos novos.
4. Mart `mart_troca_forma_pgto_rt` ficou com `forma_de=''` e `valor=0` (join movlcto miss).
5. API filtrava `valor > 0 AND forma_de != ''` + UI default **Só suspeitas** → empty state.

Última suspeita materializada corretamente: **2026-06-03**. Gap de IDs movlcto: max STG **324525** vs trocas até **325283**.

## Correções no código

- Agent: watermark = `DATAREPL` (+ `DTACONTA` só se ≤ amanhã) + `revisit_open_clause` 60d / ID ≥ 324000.
- MartBuilder: join movlcto por `(id_empresa, id_db, id)` — sem exigir mesmo `id_filial`.
- API: filtro por `dt` (SP); re-resolve forma/valor no read; qualidade mais permissiva.
- UI: default **Todas** (não só suspeitas).

## Ação operacional (obrigatória)

No agent Windows, após deploy desta branch:

```text
agent reset-watermark movlctoscancelados
agent run --dataset movlctoscancelados
```

Depois rebuild da mart de troca (CDC MartBuilder ou refresh full do dia).

Prova: `countIf(forma_de != '' AND valor > 0)` em `mart_troca_forma_pgto_rt` para `dt >= today()-40` > 0.
