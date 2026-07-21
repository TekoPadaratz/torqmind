# Antifraude — Crédito / Vale de Funcionário

## Discovery (Xpert)

| Papel | Tabela / campo | Uso |
|-------|----------------|-----|
| **Limite a prazo** | `dbo.ENTIDADES.LIMITE` | Teto a prazo do cliente-espelho |
| **Limite vale** | `dbo.ENTIDADES.LIMITE_VALE` | Teto de vale do cliente-espelho |
| Join | `FUNCIONARIOS.CPF` = `ENTIDADES.CNPJCPF` (11 dígitos) | Amarra colaborador ↔ entidade |
| Snapshot consumo | `dbo.FUNCIONARIOS.VALES` | Cruzamento opcional com o cadastro |
| Uso (título) | `dbo.CONTASRECEBER` | Títulos do cliente; `HISTORICO` ~ `%vale%` → tipo vale, senão a prazo |
| Operador | `stg.nfe` → `stg.comprovantes.ID_USUARIOS` → `USUARIOS` | Quem liberou no caixa (via NFC-e quando HISTORICO não tem Cupom) |
| Data/hora real | `COMPROVANTES.DATA` via cupom | Preferida a `DTACONTA` do título |
| **Documento (tela)** | `stg.nfe` / `stg_nfe_slim` (+ parse HISTORICO NFC-e) | Número da NF-e/NFC-e — **nunca** `NROCOMPROVANTE` / `id_comprovante` |

**Não usar** `FUNCIONARIOS.LIMITEVALE` como fonte de limite (cliente opera pelos campos da entidade).
`VALECOMBUSTIVEL` / `INSVALECOMBUSTIVEL` estão zerados no levantamento.

## Contrato de tela

Grid: Limite a prazo | Limite vale | Limite total | Usado a prazo | Usado vale | Usado total | Saldo | Usos | Status.

Drill-down: Data/Hora | NF-e/NFC-e | Tipo (A prazo / Vale) | Operador | Valor.

**DOCUMENTO = NF-e/NFC-e.** Sem NF → `—`. Ver `.cursor/rules/07-documento-nota-fiscal.mdc`.

## Regras Suspeito (OR)

1. Limite a prazo / vale / total extrapolado
2. Frequência anômala — ≥ 2 usos no mesmo dia (America/Sao_Paulo)
3. Valor atípico — ≥ max(2.5×mediana, mediana+2σ) no histórico 90d

## Artefatos

- PG: `118` (base) + `120_fraud_credito_funcionario_limites_entidade.sql` (limites entidade)
- CH: `052_fraud_credito_funcionario.sql` → `torqmind_mart_rt.mart_fraud_credito_funcionario_*`
- Publish: `repos_mart.publish_fraud_credito_funcionario_to_ch`
- API: `GET /bi/fraud/credito-funcionario` via `repos_mart_realtime`
- ACL `fraud.credito_funcionario` · UI Antifraude

## Operação

```sql
SELECT etl.refresh_fraud_credito_funcionario(:id_empresa, :ano_mes);
-- API ?refresh=true também publica no CH
```

Homolog: precisa `stg.funcionarios` + `stg.entidades` com `LIMITE`/`LIMITE_VALE` no payload.
