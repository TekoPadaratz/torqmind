# TorqMind Intelligence — Assistente determinístico (pt-BR)

Versão do catálogo: `1.0.0`  
Branch de entrega: `codex/torqmind-intelligence-deterministic-ptbr-2026-08-27`

## Terminologia (não misturar)

| Nome | Estado | O que é |
|---|---|---|
| **TorqMind Intelligence** | **Atual** | Assistente determinístico (sem LLM) no produto |
| **Jarvis** | **Atual (opcional)** | `/bi/jarvis/*` + `services/jarvis_ai.py` — OpenAI quando configurado, com fallback local |
| Capacidades agentic futuras | **Planejado** | Não confundir com Intelligence/Jarvis atuais |
| **Phase 3 (jornada investigativa)** | **Integrada (código)** | Investigar → aprofundar → explicar com evidências → recomendar; vendas + carteira; LLM opcional |


## Fechamento pré-Phase 3 (2026-09-11)

Rodada de segurança/desempenho/gráficos/publicação financeira considerada **suficiente para avançar**. Pós-deploy `a97aa13`: `finance_titles_publish` publicou 57053 linhas e confirmou `covered_through` em `etl.watermark`; pendência posterior = novas chegadas STG (não exigir `pending=False` permanente). Pendências externas/otimizações residuais **não bloqueiam** Phase 3.

> `PHASE3_*.md` / `README_PHASE3_START_HERE.md` na raiz tratam de entrega histórica de **marts ClickHouse** — não são a lei da Phase 3 investigativa atual.

## Phase 3 — jornada investigativa integrada

**Status:** implementada no código (vendas + carteira CAP/CAR + continuidade no Assistente).

### Onde acessar
- Tela `/sales` → card **Investigar variação de vendas** (sob demanda).
- Assistente TorqMind (bolha) → chips **Investigar vendas** / **Investigar carteira** ou linguagem natural.
- APIs: `GET /bi/sales/investigate-variation`, `GET /bi/finance/investigate-portfolio`.

### Contratos
- **Núcleo determinístico** permanece em Intelligence (`process_message` + tools allowlisted).
- Decomposições filial/grupo/hora são **visões alternativas** da mesma variação — não somar entre dimensões (`additive_warning` + `dimension_views`).
- 2º domínio: snapshot CAP/CAR em `mart_finance_titles_rt` (sem série histórica inventada).
- Follow-ups (“qual filial…”, “e o grupo…”, títulos vencidos) reexecutam capacidade com **escopo vigente**; mudança de filial/permissão/período invalida contexto.
- Jarvis opcional: narrativa via OpenAI Responses **somente** a partir do pack de evidências; números não sustentados são rejeitados. Sem chave → modo determinístico explícito.
- Proibido: SQL livre do modelo, mutações, agents em background, histórico como prova de autorização.

### Validação Jarvis
- Hom/Prod: `OPENAI_API_KEY` **vazio** nos env atuais → narrativa permanece em modo determinístico (`openai_not_configured`). Integração de código pronta; **LLM real não validada** sem chave autorizada.
- Não contratar/habilitar provedor nesta entrega.

## Phase 3 — primeira jornada (base `937688a`)

**Investigar variação de vendas** (somente leitura):

- Entrada: tela `/sales` → “Investigar agora” (sob demanda; não no hot path do overview).
- API: `GET /bi/sales/investigate-variation` (escopo reautorizado; marts `sales_daily_rt` / `sales_groups_rt` / `sales_hourly_rt`).
- Compara o período selecionado ao período **imediatamente anterior de mesma duração**.
- Decompõe contribuições (filial, grupo, hora) vs hipóteses do playbook `revenue_drop`.
- Não atribui causa comprovada; período incompleto e ausência de dados são avisos honestos (não viram R$ 0).
- Intelligence: `action.plan_revenue_drop` pode anexar `investigation` quantitativa quando houver `dt_ini`/`dt_fim` (sem LLM obrigatório).

## O que é

Bolha conversacional no produto autenticado. Responde perguntas de negócio em português brasileiro **sem LLM**: parser, sinônimos, tools tipadas, playbooks e templates.

Somente leitura. Não altera metas, comissões, preços, títulos, usuários ou configurações.

## Arquitetura

```
UI (IntelligenceHost)
  → POST /ai/conversations/{id}/messages
  → process_message (guards → parser → authz → tools → templates)
  → repos_analytics / handlers allowlisted (ClickHouse-first)
  → evidências + deep link PRODUCT_LINKS
```

Jarvis (`/bi/jarvis/*`, `services/jarvis_ai.py`) permanece como superfície própria.
A investigação Phase 3 pode usar narrativa opcional (httpx → OpenAI) **sem** importar o pacote `openai` no núcleo Intelligence; falha → texto determinístico.

## Feature flag

- `AI_CHAT_ENABLED` (default `false`)
- Homolog: ligar após migration 140 + rebuild api/web
- Rollback: `AI_CHAT_ENABLED=false` (migration aditiva permanece)

## Persistência

Migration `140_torqmind_intelligence.sql` — tabelas `app.ai_*` com RLS por `id_empresa`.

## Cobertura (v1)

Ver `apps/api/app/intelligence/data/coverage_summary_v1.json`.

- 47 intents / tools allowlisted
- ≥500 perguntas-semente
- ≥1.500 formulações
- ≥5.000 casos de regressão gerados
- ≥200 adversariais
- ≥100 multi-turno

## Lacunas honestas (unsupported)

- Estoque de produtos de loja (`inventory.products`) — sem mart/tela dedicada
- Algumas consultas de lucro podem cair em `navigate_only` se a função analytics não estiver exposta no facade
- Comissões: orientação + deep link (sem mutação)

## Operação

1. Aplicar migration 140 no PG alvo (homolog primeiro)
2. Rebuild `api` + `web` do projeto compose correto
3. `AI_CHAT_ENABLED=true` só em homolog até prova
4. Validar casos-ouro com owner e manager
5. Produção somente com Hom PASS + autorização explícita
