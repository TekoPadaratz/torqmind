# Antifraude — Troca de Forma de Pagamento (Xpert SQL Server)

Status: MAPEADO (descoberta validada na base real). Ainda NÃO ingerido/implementado.
Data da investigação: 2026-06-03. Base: SQL Server `172.30.0.12` / `ATXDADOS`.

## Resumo executivo

A Xpert **guarda histórico completo das trocas de forma de pagamento** de uma venda,
incluindo **quem trocou, quando, o valor, o turno e a forma de origem (DE)**. A forma
de destino (PARA) é o lançamento financeiro vivo atual da mesma referência.

Isso é um sinal forte de fraude quando uma forma **já recebida** (dinheiro, cartão, pix)
é trocada por uma forma **ainda não recebida** (a prazo, cheque a receber) após a venda.

## Tabelas envolvidas

| Tabela | Papel | Campos-chave |
|---|---|---|
| `dbo.CONTROLE_TROCA_PGTO` | Auditoria da troca (quem/quando) | `ID`, `ID_FILIAL`, `ID_DB`, `ID_MOVLCTOSCANCELADOS`, `USUARIO`, `DATA`, `DATAREPL` |
| `dbo.MOVLCTOSCANCELADOS` | Lançamento financeiro ANTIGO cancelado (forma DE) | `ID_MOVLCTOSCANCELADOS`, `ID_FILIAL`, `ID_DB`, `ID_PLANODECONTAS`, `REFERENCIA`, `TIPO` (0/1 = débito/crédito), `VALOR`, `DTACONTA`, `REF_OPERACAO`, `ID_TURNOS`, `DOCUMENTO` |
| `dbo.MOVLCTOS` | Razão financeiro VIVO (forma PARA atual) | mesmos campos + `ID_USUARIOS`, `DATA`, `ESTORNO`, `CONCILIADO` |
| `dbo.PLANODECONTAS` | Nome da conta/forma | `ID_PLANODECONTAS`, `ID_FILIAL`, `NOMEPLANODECONTAS`, `CODIGOPLANODECONTAS` |
| `dbo.FORMAS_PGTO_COMPROVANTES` | Forma de pagamento atual da venda | `ID_FILIAL`, `ID_DB`, `ID_REFERENCIA`, `TIPO_FORMA`, `VALOR_PAGO` |

Observações:
- `MOVLCTOS`/`MOVLCTOSCANCELADOS` são razão financeiro com partida dobrada
  (`TIPO` 0/1 = débito/crédito). A "forma" de pagamento é representada pela CONTA
  (`ID_PLANODECONTAS` → `NOMEPLANODECONTAS`), ex.: `Caixa Turno` = dinheiro,
  `Vendas a Prazo a Receber` = a prazo, `VISA ELECTRON (POS)`, `CONVCARD`, etc.
- `MOVLCTOSCANCELADOS.TIPO` aqui é o lado contábil (débito/crédito), **não** o
  `tipo_forma` da venda. Não confundir com `FORMAS_PGTO_COMPROVANTES.TIPO_FORMA`.
- `CONTROLE_TROCA_PGTO` tinha 130.036 linhas; `MOVLCTOSCANCELADOS` 457.257 linhas
  (toda a base, várias filiais).

## Como reconstruir a trilha DE → PARA

```sql
-- Forma DE (cancelada) + quem/quando trocou
SELECT t.ID AS troca_id, t.USUARIO, t.DATA AS data_troca,
       c.REFERENCIA, c.ID_TURNOS, c.VALOR,
       c.ID_PLANODECONTAS AS plano_de, p.NOMEPLANODECONTAS AS forma_de,
       c.DOCUMENTO
FROM dbo.CONTROLE_TROCA_PGTO t
INNER JOIN dbo.MOVLCTOSCANCELADOS c
        ON c.ID_MOVLCTOSCANCELADOS = t.ID_MOVLCTOSCANCELADOS
LEFT JOIN dbo.PLANODECONTAS p
        ON p.ID_PLANODECONTAS = c.ID_PLANODECONTAS
       AND p.ID_FILIAL = c.ID_FILIAL
WHERE t.ID_FILIAL = :id_filial AND c.VALOR > 0
ORDER BY t.DATA DESC;

-- Forma PARA (atual) da mesma REFERENCIA no razão vivo
SELECT m.ID_PLANODECONTAS, p.NOMEPLANODECONTAS AS forma_para,
       m.TIPO, m.VALOR, m.ID_TURNOS, m.ID_USUARIOS, m.DATA
FROM dbo.MOVLCTOS m
LEFT JOIN dbo.PLANODECONTAS p
       ON p.ID_PLANODECONTAS = m.ID_PLANODECONTAS AND p.ID_FILIAL = m.ID_FILIAL
WHERE m.ID_FILIAL = :id_filial AND m.REFERENCIA = :referencia
ORDER BY m.ID_MOVLCTOS;
```

## Caso real provado (filial 14122, 2026-06-03)

| REFERENCIA | DE (cancelado) | PARA (vivo) | Valor | Usuário |
|---|---|---|---|---|
| 2771590 | `Caixa Turno` (DINHEIRO) | `Vendas a Prazo a Receber` (À PRAZO) | R$1.569,07 | 38 |
| 2771328 | `Caixa Turno` (DINHEIRO) | `Vendas a Prazo a Receber` (À PRAZO) | R$2.837,16 | 38 |

`FORMAS_PGTO_COMPROVANTES` da venda já reflete `TIPO_FORMA=1` (PRAZO) — confirmando
que a forma foi efetivamente trocada de dinheiro para a prazo após a venda.

Esta é a MESMA causa raiz da duplicação corrigida em
`torqmind_current.stg_formas_pgto_slim` (o ERP insere a nova forma sem marcar a
antiga `is_deleted`).

## Sinal de fraude proposto (a definir solução)

Classificar formas em dois grupos:
- **Recebidas** (caixa imediato): `Caixa Turno`/Dinheiro, cartões (débito/crédito/POS),
  Pix, vouchers já liquidados.
- **A receber** (risco): `Vendas a Prazo a Receber`, `Cheque a Receber`, convênios não
  liquidados.

Gatilho de alerta: troca onde `forma_de ∈ Recebidas` e `forma_para ∈ A receber`,
especialmente DINHEIRO → A PRAZO, agrupando por `USUARIO`, `ID_TURNOS`, filial e dia.
Métricas úteis: valor total trocado por operador/turno, nº de trocas, defasagem entre
`venda` e `data_troca`.

## Próximos passos sugeridos (quando priorizado)

1. Ingerir `CONTROLE_TROCA_PGTO` + `MOVLCTOSCANCELADOS` (+ join com `MOVLCTOS` vivo)
   para STG via Agent/extractor (mesmo padrão das demais tabelas, append/idempotente).
2. Mapear `ID_PLANODECONTAS` → categoria de forma (recebida/a-receber) por filial.
3. Materializar mart de antifraude `troca_forma_pgto` (DE→PARA, usuário, turno, valor, dia).
4. Expor apenas para `platform_master`/`owner` (nunca gerente/vendedor — é dado sensível).

> Acesso de leitura usado: `tools/xpert_source_explorer.py` (somente leitura, sem
> INSERT/UPDATE/DELETE). Credenciais em `config/source-explorer.env` (gitignored).
