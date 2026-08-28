# TorqMind — Mapa de modularização e precificação (proposta)

Documento de estudo para decisão de produto. **Não implementado** — aguarda OK do dono.

## Princípios (mercado B2B vertical)

Referências: Syntphony Stations (módulos cloud fuel retail), Vertex Pulse (core + módulos compliance/pricing), TARGIT PDI (aceleradores Finance / Wholesale / Site), padrão SaaS híbrido (base + expansão por uso/módulo).

1. **Pacotes**, não dezena de SKUs — 3–4 tiers + 2 add-ons máximo na venda inicial.
2. **Base obrigatória** — ingestão, multi-filial, segurança, branding, usuários.
3. **Add-ons** com valor claro para o dono do posto (dinheiro, risco, pessoas).
4. **Intelligence (Chat)** como premium — diferencial de IA determinística + dados ao vivo.
5. Precificação **por rede** (filiais ativas) + faixa de faturamento, não por feature isolada.

---

## Módulos candidatos (mapa técnico → produto)

| Módulo produto | Inclui (telas/API/marts) | Dependências | Esforço / inteligência |
|----------------|--------------------------|--------------|-------------------------|
| **Core Operacional** | Dashboard vendas, escopo, ACL, agent ingest, TV kiosk básico | Agent, CDC, CH realtime vendas | Base da plataforma |
| **Financeiro Básico** | CAP, CAR, overview financeiro, grids títulos CH | `mart_finance_titles_rt`, `finance_overview_rt`, publish títulos | Alto (reconciliação CR/CAP, baixas) |
| **Financeiro Plus** | Despesas (CAP×plano), Gestão de lucro, DRE/solvência | PG mash + CH publish despesas, margem | Muito alto (sensível, CMV) |
| **Clientes & Cobrança** | CRM clientes, inadimplência priorizada, contas a receber operacional | PG delinquency mart + CAR | Alto (reconciliação títulos) |
| **Antifraude** | Crédito cliente, crédito funcionário, devoluções, eventos risco | Marts fraud CH, CR cruzado | Alto (regras negócio) |
| **Caixa & Turno** | Caixa, turnos, operadores | STG turnos/comprovantes | Médio |
| **Metas** | Metas rede/filial, projeção mês | OLTP metas + CH vendas | Médio |
| **Comissões** | Comissão vendedor, extrato | PG comissões + vendas CH | Alto (custom por cliente) |
| **Equipe** | Ranking, custo equipe (se Plus) | CH + cadastro | Médio |
| **Precificação** | Concorrente, ANP (se aplicável) | OLTP + CH | Médio-alto |
| **Estoque** | Loja + combustível, aferições | Agent movprodutos, CH inventory | Alto |
| **TorqMind Intelligence** | Assistente conversacional, capabilities | Parser + tools CH + RLS | Diferencial premium |
| **Plataforma / Backoffice** | Multi-empresa, branding, usuários, billing ops | `app.*`, `auth.*` | Incluído no core SaaS |

---

## Pacotes sugeridos (venda)

### 1. TorqMind Essencial — **R$ 890–1.290 / filial / mês**

- Core Operacional (vendas, produtos, hora, ranking básico)
- Agent + 1 VM app
- Usuários: owner + 3 gerentes
- **Sem** margem/lucro, **sem** antifraude, **sem** chat

**ICP:** posto único ou rede pequena que só quer “ver o dia”.

### 2. TorqMind Profissional — **R$ 1.490–2.190 / filial / mês**

- Essencial +
- **Financeiro Básico** (CAP + CAR)
- Clientes & Cobrança (inadimplência)
- Caixa & Turno
- Metas (somente leitura de progresso)

**ICP:** rede regional — dono + gerente operando financeiro básico.

### 3. TorqMind Gestão — **R$ 2.490–3.490 / filial / mês**

- Profissional +
- **Financeiro Plus** (despesas + gestão de lucro)
- Antifraude completo
- Equipe + ranking avançado
- Estoque combustível/loja

**ICP:** rede que compra “gestão do dono”.

### 4. TorqMind Intelligence Add-on — **+R$ 390–690 / filial / mês** (ou +25% no tier Gestão)

- Assistente TorqMind (chat determinístico)
- Perguntas operacionais + deep links
- Prioridade de novas intents

**ICP:** quem já paga BI e quer “copiloto” sem consultor.

### Add-ons opcionais (máx. 2 na proposta comercial)

| Add-on | Preço sugerido | Nota |
|--------|----------------|------|
| **Comissões** | +R$ 290–490 / filial / mês | Motor de regras + extrato |
| **Metas Pro** (edição + comissão ligada) | +R$ 190–290 / filial / mês | Se não incluso no Gestão |
| **Precificação concorrente** | +R$ 390–590 / filial / mês | Dados ANP + concorrente |
| **Rede 15+ filiais** | desconto −15% no tier | Volume |

**Mínimo de rede:** 2 filiais ou R$ 2.500/mês — evita micro-SaaS inviável.

---

## Comparativo esforço → preço (interno)

| Componente | Complexidade | Por quê pesa no preço |
|------------|--------------|------------------------|
| Pipeline Agent→CH | Muito alta | Único no segmento posto |
| Multi-tenant + ACL granular | Alta | Segurança por tela/aba |
| CAP/CAR realtime + reconciliação | Alta | Baixas, fantasmas, id_db |
| Antifraude CR×limite | Alta | Regras + cruzamento |
| Intelligence (sem LLM externo) | Alta | Parser, evidências, escopo |
| Gestão lucro / CMV | Muito alta | Dado sensível, qualidade custo |
| Comissões | Alta | Customização por rede |

---

## Roadmap de implementação (após OK)

1. **Catálogo de módulos** em `app.company_modules` + enforcement API (`require_module`) + FE gate menus.
2. **Billing Ops** — espelhar pacotes no TorqMind-Ops (fora deste repo).
3. **Feature flags** por `id_empresa` com fallback homolog.
4. **Onboarding** — wizard “escolha seu pacote” + upsell in-app (Intelligence, Antifraude).

---

## Decisões para o OK

- [ ] Quantos pacotes na vitrine (3 ou 4)?
- [ ] Intelligence incluso no Gestão ou só add-on?
- [ ] Comissões sempre add-on ou incluso em Gestão para redes >N filiais?
- [ ] Precificação por filial ativa vs faturamento da rede?

---

*Versão 1.0 — 2026-08-27. Revisar após piloto com 2–3 redes.*
