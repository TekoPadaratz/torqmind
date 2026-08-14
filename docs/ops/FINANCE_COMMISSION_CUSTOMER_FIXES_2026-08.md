# Prova — Finance / Comissões / Clientes (2026-08)

## 1) Despesas 3.2.02.23 (VR01 jul/2026)

- **Causa:** tela lia CAP (`CONTASPAGAR`/`DTAVCTO`) e marcava “Pago”; Razão Xpert é `MOVLCTOS`/`DTACONTA`.
- **Xpert:** 101 lançamentos, Entradas R$ 3.688,64, Saídas R$ 0,00.
- **10/07:** 6 linhas / R$ 279,99 (DOCUMENTO = texto do Razão). STG Hom curado via bootstrap; Prod precisa bootstrap + republish.
- **Saídas:** crédito contábil (TIPO=1), não baixa CAP.

## 2) PDF comissões

- Botão `Imprimir / PDF` em `CommissionsTab` (iframe + HTML dedicado, A4 landscape).

## 3) Descontos / preço fixo

- Seção sob o grid; tipos `venda` (VLRDESCONTO) e `preco_fixo` (mart). Sem alterar fórmula de comissão. Sem custo/margem.

## 4) Inatividade preço fixo

- `GET /bi/customers/preco-fixo/inativos?days_without=15|30|60`.

## 5) Nome reduzido

- Troca forma PGTO: API aplica `_filial_label` (apelido Plataforma).

## 6) Alcioni Felipe dos Santos

- ENTIDADE `7343` grupo 12 ATIVA no Xpert na maioria das filiais; FUNCIONARIO ATIVO=false em algumas, true em 14122.
- Títulos CR reais em ago/2026 (NFC-e) na filial 14122 — histórico legítimo de venda a prazo, não fantasma de mash.
- Ação de negócio: se desligado, inativar ENTIDADE e zerar limites no Xpert.

## Containers

- Hom/Prod: apenas `torqmind-api(-homolog)` e `torqmind-web(-homolog)` após merge/deploy.
- TorqMind-Ops: não tocado.
