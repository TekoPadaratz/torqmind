# Antifraude — Crédito / Vale de Funcionário

## Discovery (Xpert)

| Papel | Tabela / campo | Uso |
|-------|----------------|-----|
| **Universo** | `dbo.ENTIDADES` com `ID_GRUPOENTIDADES = 12` (Funcionários) | Base do relatório — **não** usar `FUNCIONARIOS` |
| **Limite a prazo** | `ENTIDADES.LIMITE` | Teto a prazo |
| **Limite vale** | `ENTIDADES.LIMITE_VALE` | Teto de vale |
| Escopo | `id_empresa` (entidade compartilhada entre filiais) | Limite/uso empresa-wide |
| Uso (título) | `dbo.CONTASRECEBER` por `ID_ENTIDADE` | Toda a empresa; `HISTORICO` ~ `%vale%` → vale, senão a prazo |
| Filial do gasto | `CONTASRECEBER.id_filial` | Só no detalhe do uso (nome reduzido) |
| Operador | `stg.nfe` → `stg.comprovantes.ID_USUARIOS` → `USUARIOS` | Quem liberou no caixa |
| Cliente (venda) | `comprovante.ID_ENTIDADE` / `id_cliente` → `dim_cliente` / `ENTIDADES` | Só se distinto do titular do crédito; senão `—` |
| Data/hora real | `COMPROVANTES.DATA` via cupom/NF | Preferida a `DTACONTA` do título |
| **Documento (tela)** | `stg.nfe` / `stg_nfe_slim` (+ parse HISTORICO NFC-e) | Número da NF-e/NFC-e — **nunca** comprovante |

**Não** fazer INNER JOIN por CPF com `FUNCIONARIOS` (cadastro incompleto → faltam colaboradores).
**Lista** filtra pela filial selecionada (cópia ATIVA da entidade naquele posto). **Usos** não filtrar — gasto em qualquer filial da rede.

## Contrato de tela

Grid: funcionários ativos na **filial selecionada** (ENTIDADES grupo 12 + ATIVO). Limites e uso são da entidade; o **uso** soma gastos em **toda a empresa**.

Drill-down: **Filial** | Data | NF-e/NFC-e | Cliente | Tipo | Operador | Valor — ordenado por filial, data (mais recente), valor.

Ordenação da lista: nome.

## Regras Suspeito (OR)

1. Limite a prazo / vale / total extrapolado
2. Frequência anômala — ≥ 2 usos no mesmo dia (America/Sao_Paulo)
3. Valor atípico — ≥ max(2.5×mediana, mediana+2σ) no histórico 90d

## Artefatos

- PG: `118`…`122` + `123_fraud_credito_funcionario_grupo_entidade.sql` (grupo 12, empresa-wide)
- CH: `052_fraud_credito_funcionario.sql` → `torqmind_mart_rt.mart_fraud_credito_funcionario_*`
- Publish: `repos_mart.publish_fraud_credito_funcionario_to_ch`
- API: `GET /bi/fraud/credito-funcionario` via `repos_mart_realtime` (lista por filial ATIVA; usos empresa-wide)
- ACL `fraud.credito_funcionario` · UI Antifraude

## Operação

```sql
SELECT etl.refresh_fraud_credito_funcionario(:id_empresa, :ano_mes);
-- API ?refresh=true também publica no CH
```

Homolog/prod: `stg.entidades` com `ID_GRUPOENTIDADES=12` e `LIMITE`/`LIMITE_VALE` no payload.

## Agent (obrigatório)

Dataset **`entidades`** (`dbo.ENTIDADES`) deve estar **enabled** no agent.
Sem ele, limites/vale ficam stale no STG (ex.: Ilson 22175 — STG de maio com
`LIMITE=400`/`LIMITE_VALE=0` enquanto o Xpert já tinha vale atualizado).

Após ligar o dataset (ou se `ULTALTERACAO` não sobe em mudança de limite):

```powershell
torqmind-agent.exe reset-watermark --dataset entidades --config config.enc
torqmind-agent.exe run --once --config config.enc
```

Depois: `SELECT etl.refresh_fraud_credito_funcionario(1, :ano_mes);` e/ou
`GET /bi/fraud/credito-funcionario?refresh=true`.

Não habilitar o alias `clientes` junto (mesma tabela → ingest duplicado).
Mash preferencial: `mash_fraud_credito_funcionario_ch` (ClickHouse STG→mart_rt, ~2–3s).
