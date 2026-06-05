# TorqMind — Próximos Passos (Roadmap Oficial)

> Documento **oficial** de próximos passos do TorqMind. Consolida o que já foi
> blindado, o que está pronto e a priorização por fase das próximas evoluções.
> Para o detalhamento dor-a-dor (fonte de dados, gap técnico, tabelas Xpert
> reais), ver o documento de referência [ROADMAP_DORES_POSTO.md](./ROADMAP_DORES_POSTO.md).
>
> Arquitetura fixa: `SQL Server Xpert -> Agent -> API ingest -> PostgreSQL STG ->
> Debezium/Redpanda -> CDC Consumer -> ClickHouse current/mart_rt -> API -> Web`.
>
> Última atualização: 2026-06-04.

---

## 0. Blindagem atual — FINALIZADA

Rodadas recentes de hardening, todas com prova fonte→tela e deploy validado:

- **Prioridade de cobrança de clientes** — dedupe no grão da mart PG reconciliada
  com a fonte Xpert (sem duplicação por filial). FEITO.
- **Comissão (3 modos)** — `team_total`, `equal_split`, `individual_sales`;
  `ensure_default_config` corrigido; rota respeita `payment_mode`. FEITO.
- **Totalizadores por forma de pagamento** — conciliação explícita
  (`total_vendas`, `total_pagamentos_conciliado`, `diferenca_conciliacao` +
  linha "Não conciliado (operacional)") no Caixa e no Financeiro, sem esconder
  diferença. FEITO.
- **2FA TOTP** — backend + tela "Minha Segurança" + fluxo `totp_required`. FEITO
  (ver §1).
- **SMTP env-driven** — remetente nunca `@torqmind.com` por padrão; exemplos de
  env atualizados. FEITO (ver §1).
- **Agents/regras** — ritual de início, fluxo fonte→tela, armadilhas de grão
  realtime. FEITO.

---

## 1. Segurança / 2FA — PRONTO (manutenção)

- **TOTP** (RFC 6238, stdlib + Fernet) compatível com Google/Microsoft
  Authenticator, Authy, Bitwarden, 1Password, Proton. Opt-in, default desligado.
- **Tela "Minha Segurança"** (`/security`): status, ativar com QR + chave manual,
  recovery codes (uma vez), desativar com código.
- **Fluxo `totp_required`**: login de conta marcada como obrigatória emite token
  de setup e força a configuração antes de liberar a sessão.
- **Admin**: resetar 2FA e exigir/dispensar 2FA no cadastro de usuário.
- **SMTP**: `SMTP_FROM_EMAIL` (domínio controlado) + SPF/DKIM/DMARC antes de usar
  domínio próprio. Sem `@torqmind.com` sem domínio comprado.
- Próximos (futuro): WebAuthn/passkey; verificação de e-mail; trilha de auditoria
  de login/2FA por usuário.

---

## 2. Comissão em Metas & Equipe — evoluir

Base pronta (3 modos). Próximos incrementos:
- Visual premium do resultado por vendedor e por equipe na aba Comissões.
- Histórico mês a mês e exportação.
- Regras por grupo de produto com simulação ("e se").
- Comissão de gerente já existe; revisar política de combustível/CMV oculto.

---

## 3. Exportação PDF/Excel premium

- Exportar telas-chave (Caixa, Financeiro, Clientes, Metas) em PDF com identidade
  visual e em Excel com dados tabulares.
- Geração server-side para não pesar no cliente; cache por escopo.
- Respeitar permissões (gerente/vendedor nunca recebem margem/lucro/custo).

---

## 4. Saúde dos dados / Admin técnico

- Tela interna de saúde: freshness por domínio (comprovantes, itens, formas,
  caixa, financeiro), lag de CDC, status de marts, última publicação.
- Alertas quando uma mart fica obsoleta ou o pipeline atrasa.
- Painel de reconciliação fonte→tela (amostra por filial/data) para auditoria.

---

## 5. Estoque de combustível (PRIORIDADE de negócio)

- Pensar em **litros**, não só R$. Entrada (nota) − saída (venda L) = saldo
  esperado vs. medição física (sonda) → perda/sobra/evaporação/furto.
- Trilha de dados: encerrante (`TURNOS.ENCERRANTEFECHAMENTO`, já ingerido) +
  tanque/sonda + LMC. Ver §6 e ROADMAP_DORES_POSTO.md §1.
- KPI de estoque no dashboard geral.

---

## 6. Tanques / bicos / aferição + LMC

- **Tanques/bicos**: posição por tanque, vínculo bico↔tanque↔produto.
- **Aferição/INMETRO**: tabela `AFERICAO` (Xpert) viva e atual — ingerir em STG e
  notificar via Telegram quando vencer/proximo. Validade legal (lacre) em
  `BICOS.DATALACRE`.
- **LMC (Livro de Movimentação de Combustíveis)**: volumetria viva canônica
  (não usar `ENCERRANTESTURNOS` congelado). Base para o estoque volumétrico.

---

## 7. Conciliação de cartões (Financeiro)

- Vendas por bandeira/adquirente (bruto) já viável com dados de movimento.
- **Bloqueio atual**: `TAXASADMINISTRADORA` (cadastro de taxas) vazio → sem taxa
  não há líquido nem detecção de divergência. Opções: criar cadastro de taxa no
  schema `app` do TorqMind, ou extrair taxa efetiva do EDI
  (`CONCILIACAOVENDASCARTOES_EDI`). Confirmar com o cliente antes de codar.

---

## 8. Preço concorrente avançado

- Base de precificação concorrente já existe. Evoluir para: histórico, alerta de
  movimento do concorrente, sugestão de preço por margem-alvo, comparativo por
  praça.

---

## 9. Alertas / Telegram — evoluir

- Infra madura (`services/telegram.py`, dedupe diário, retry). Já dispara
  cancelamento e caixa>24h.
- Próximos alertas: aferição vencendo, divergência de estoque/tanque, anomalia
  de pagamento, meta em risco. Centralizar catálogo de alertas e preferências por
  usuário.

---

## 10. Gestão de lucro evoluída

- Evoluir margem/lucro/CMV/markup/rentabilidade **somente para owner/master**
  (gerente/vendedor nunca veem). DRE gerencial, lucro por produto/grupo/filial,
  evolução temporal.

---

## 11. Priorização sugerida por fase

| Fase | Foco | Itens |
|------|------|-------|
| **A — concluída** | Blindagem | Cobrança, comissão, pagamentos, 2FA, SMTP, agents |
| **B — curto prazo** | Confiança operacional | Saúde dos dados/admin técnico (§4); alertas evoluídos (§9); exportação PDF/Excel (§3) |
| **C — valor de negócio** | Combustível | Estoque (§5); tanques/bicos/aferição + LMC (§6) |
| **D — financeiro** | Conciliação | Cartões com taxa (§7); gestão de lucro (§10) |
| **E — diferenciação** | Inteligência | Preço concorrente avançado (§8); comissão premium (§2) |
| **Futuro** | Pesquisa | WebAuthn/passkey; validação de assinatura/cheque por IA |

> Regra de execução: medir antes de otimizar; provar fonte→tela antes de PASS;
> nunca expor margem/lucro/custo a gerente/vendedor; mart rápida e reconciliada
> em vez de query pesada no endpoint.
