# Antifraude — Crédito / Vale de Funcionário

## Discovery (Xpert)

| Papel | Tabela / campo | Uso |
|-------|----------------|-----|
| Limite | `dbo.FUNCIONARIOS.LIMITEVALE` | Teto de vale/crédito a prazo |
| Snapshot consumo | `dbo.FUNCIONARIOS.VALES` | Cruzamento com o cadastro |
| Cliente espelho | `dbo.ENTIDADES.CNPJCPF` | Vínculo funcionário↔cliente pelo CPF |
| Uso (título) | `dbo.CONTASRECEBER` | Vendas a prazo; `HISTORICO` traz Cupom/NFC-e |
| Operador | `dbo.COMPROVANTES.ID_USUARIOS` → `USUARIOS.NOMEUSUARIOS` | Quem liberou no caixa |
| Data/hora real | `COMPROVANTES.DATA` via cupom | Preferida a `DTACONTA` do título |
| **Documento (tela)** | `stg.nfe` / `stg_nfe_slim` (+ parse HISTORICO NFC-e) | Número da NF-e/NFC-e — **nunca** exibir `id_comprovante` como documento |

`VALECOMBUSTIVEL` / `INSVALECOMBUSTIVEL` estão zerados — controle operacional usa **LIMITEVALE + CONTASRECEBER**.

## Contrato de documento (UI)

1. Preferência: número NF-e/NFC-e (`documento_source=nota_fiscal`)
2. Fallback: `NROCOMPROVANTE`
3. Último recurso: `id_comprovante` (rastreio técnico)
4. Label = só o número (sem prefixo "Cupom"/"Nota")
5. Filtro de período: seletor **Mês/Ano** (`profitScopeMonthSelect`), igual DRE/Solvência

## Regras Suspeito (OR)

1. **Limite Extrapolado** — `usado_mes > LIMITEVALE` (ou `VALES > LIMITEVALE`)
2. **Frequência Anômala** — ≥ 2 usos no mesmo dia (America/Sao_Paulo)
3. **Valor Atípico** — valor ≥ max(2.5×mediana, mediana+2σ) no histórico 90d

## Artefatos

- PG mash: `118_fraud_credito_funcionario.sql` → `mart.fraud_credito_funcionario_*` + ETL
- **ClickHouse (leitura API):** `052_fraud_credito_funcionario.sql` → `torqmind_mart_rt.mart_fraud_credito_funcionario_*`
- Publish: `repos_mart.publish_fraud_credito_funcionario_to_ch` (após refresh)
- API: `GET /bi/fraud/credito-funcionario` via `repos_mart_realtime` (`source: clickhouse`)
- ACL `fraud.credito_funcionario` · UI Antifraude · Agent `funcionarios` enabled

## Operação

```sql
SELECT etl.refresh_fraud_credito_funcionario(:id_empresa, :ano_mes);  -- mash PG ~50s
-- API ?refresh=true também publica no CH
```

Homolog: precisa `stg.funcionarios` populado.
