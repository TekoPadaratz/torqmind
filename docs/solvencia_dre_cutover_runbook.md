# Runbook de Cutover — DRE Afinado: Solvência / Capital de Giro

Status (2026-07-09):
- **PASSIVO (contas a pagar, com baixas parciais)**: validado e **já aplicado em
  produção** (mart `mart.liquidez_solvencia` criada e populada via
  `etl.refresh_liquidez_solvencia`). Alteração aditiva, não muda front nem
  funcionamento — liberada pelo dono. Falta só o **deploy da imagem** (endpoint
  + aba) para a tela aparecer.
- **BAIXAS PARCIAIS**: o valor em aberto desconta as baixas de
  `stg.contaspagarbaixa` (validado na fonte: `VALOR = VLRPAGO + SUM(VALORBAIXA)`).
  Join por `(id_empresa, id_db, ID_CONTASPAGAR)` — `ID_CONTASPAGAR` não é único
  global. Sem isso, o passivo ficava superestimado.
- **ESTOQUE (ativo)**: **BLOQUEADO** — o pipeline está inconsistente. `stg.estoque`
  tem schema dedicado (`quantidade`/`custo_medio`) mas o ingest
  (`routes_ingest.py`) grava colunas *shadow* (`qtd_atual_shadow`, etc.) que não
  existem na tabela; o pipeline "canônico" (`dw.fact_estoque_atual`,
  `mart.agg_estoque_posicao_atual`) é referenciado no código mas o DDL foi
  perdido (não existe em migration nem em produção). **Não ligar o estoque** até
  reconstruir o pipeline e validar a coleta real (senão vira "tabela vazia").

## O que é

Nova aba **"Solvência"** dentro do DRE Gerencial (`profit-management`) que
responde: *"meus ativos cobrem as contas a pagar do mês?"*. Tem **filtro de mês**
e cruza:

- **Passivo circulante** = contas a pagar em aberto vencendo no mês-alvo.
- **Ativo circulante** = disponível (caixa + banco) + recebíveis de curto prazo
  (cartões + cheques) + estoque a custo.
- **Índice de Liquidez Corrente** = Ativo Circulante ÷ Passivo Circulante.
- **Capital de Giro Líquido** = Ativo Circulante − Passivo Circulante.

Não altera o DRE existente. Padrão contábil BR. Não expõe margem/lucro/custo além
do que a tela `profit_management` já protege (`require_screen` + `redact_sensitive`).

## Arquivos entregues

| Arquivo | O quê |
|---|---|
| `sql/migrations/098_liquidez_solvencia.sql` | mart `mart.liquidez_solvencia` (índices como colunas GENERATED) + `etl.refresh_liquidez_solvencia` (passivo) |
| `sql/migrations/099_liquidez_estoque.sql` | `etl.refresh_liquidez_estoque` (ativo estoque valorizado a custo) |
| `apps/api/app/repos_mart.py` | `solvencia_overview()` + `_month_label_ptbr()` |
| `apps/api/app/routes_profit.py` | endpoint `GET /bi/profit-management/solvencia?ano_mes=YYYYMM` |
| `apps/web/app/profit-management/page.tsx` | aba "Solvência" + filtro de mês + veredito/cards/tabela |

## Estado validado (homologação, empresa 1, 2026-07-09)

- Passivo jul/2026 = R$ 457.314,91 (119 títulos); ago = R$ 368.849,01 — batem com `stg.contaspagar`.
- Estoque total = R$ 16.061.701,84 — bate com `dbo.ESTOQUE × PRODUTOS.CUSTOMEDIO`.
- Consolidado jul/2026: liquidez corrente **35,12**, capital de giro R$ 15,6M, **cobre = true**.
- `pytest -k "profit or solvencia"` = 20 passed · `npm run build` OK · endpoint e2e OK.

## Fontes de dado

| Componente | Fonte Xpert | STG hoje (prod) | Precisa Agent? |
|---|---|---|---|
| Passivo (contas a pagar) | `dbo.CONTASPAGAR` | ✅ `stg.contaspagar` (233k) | **Não** — já coletado |
| Estoque | `dbo.ESTOQUE` × `PRODUTOS.CUSTOMEDIO` | ❌ `stg.estoque` vazio | **Sim** — ligar dataset |
| Cheques | `dbo.CHEQUESRECEBIDOS` | ❌ | Sim (migration 096 já tem mart) |
| Banco | `dbo.MOVBANCOS` | ❌ | Sim (validar TIPO/OPERACAO) |
| Caixa / cartões | derivar de `stg.formas_pgto_comprovantes` | ⚠️ regra a definir | Não (só ETL) |

> `dbo.SALDOSPRODUTOS` foi **descartado** para estoque: `QTDETOTAL` vem zerado
> nesta base. `dw.dim_produto.custo_medio` também está zerado — usar sempre
> `stg.produtos.payload->>'CUSTOMEDIO'`.

## Cutover para produção (dois níveis)

### Nível 1 — Passivo (NÃO toca o Agent, baixo risco)

Já mostra o total de contas a pagar por mês na aba, com a estrutura de ativos
sinalizando "aguardando dados".

1. Deploy da imagem já validada em homolog (API com o endpoint + web com a aba).
   Migrations `098` e `099` rodam no startup da API (idempotentes, aditivas).
2. Rodar o refresh do passivo em produção:
   ```bash
   PW="$(grep -E '^POSTGRES_PASSWORD=' /etc/torqmind/prod.app.env | cut -d= -f2-)"
   PGPASSWORD="$PW" psql -h 172.30.0.8 -U torqmind -d torqmind -c "SELECT etl.refresh_liquidez_solvencia(1)"
   ```
3. Health check:
   ```bash
   curl -I http://redevr.ddns.me:14023
   curl -I http://redevr.ddns.me:14023/api/health
   ```
4. Validar fonte→tela: abrir a aba Solvência e conferir o passivo do mês contra
   o Xpert (contas a pagar em aberto por vencimento).

### Nível 2 — Estoque (toca o Agent — exige confirmação do dono)

1. Habilitar o dataset de estoque no Agent (produção). Origem sugerida:
   ```sql
   SELECT e.ID_ESTOQUE, e.ID_FILIAL, e.ID_PRODUTOS, e.QTDEATUAL,
          CAST(e.??? AS datetime2) AS TORQMIND_DT_EVENTO   -- definir watermark
   FROM dbo.ESTOQUE e
   ```
   `dbo.ESTOQUE` não tem custo nem timestamp próprio: o custo vem do join com
   `stg.produtos` no ETL; a watermark precisa ser resolvida (snapshot completo
   periódico é aceitável — a tabela é pequena por filial).
2. Confirmar `stg.estoque` populando (CDC consumer → mapeamento de colunas
   `quantidade`/`custo_medio`).
3. Rodar o refresh do estoque:
   ```bash
   PGPASSWORD="$PW" psql -h 172.30.0.8 -U torqmind -d torqmind -c "SELECT etl.refresh_liquidez_estoque(1)"
   ```
4. Validar fonte→tela: o estoque valorizado por posto deve bater com
   `SUM(GREATEST(QTDEATUAL,0) × CUSTOMEDIO)` no Xpert.

### Agendamento do refresh

Incluir os dois refreshes no orquestrador de marts (junto do
`prod-profit-marts-refresh.sh` ou no scheduler que já roda os refreshes de
mart), para manter passivo e estoque atualizados.

## Próximos ativos (fases seguintes)

- **Cheques a receber**: somar `mart.cheques_pendentes` (migration 096) não
  compensados → `ativo_cheques`. Só ligar o dataset `cheques` no Agent.
- **Banco**: `dbo.MOVBANCOS` — validar semântica `TIPO`/`OPERACAO` (crédito vs
  débito) antes de somar saldo.
- **Caixa / cartões**: derivar de `stg.formas_pgto_comprovantes` (dinheiro/PIX
  recebido não depositado; cartões a compensar) — regra gerencial a definir.

Cada componente novo é preenchido por um ETL próprio que faz `UPDATE` do seu
campo na `mart.liquidez_solvencia` e marca `tem_ativo_dados=true`; os índices
(colunas GENERATED) se recalculam sozinhos.

## Rollback

- Reverter a imagem para a tag anterior (API/web).
- As tabelas/funções novas são aditivas; para remover:
  ```sql
  DROP FUNCTION IF EXISTS etl.refresh_liquidez_estoque(integer);
  DROP FUNCTION IF EXISTS etl.refresh_liquidez_solvencia(integer);
  DROP TABLE IF EXISTS mart.liquidez_solvencia;
  ```
  Nada em `stg` é alterado; nenhum dado de origem é tocado.
