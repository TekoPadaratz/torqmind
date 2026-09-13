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
- Follow-ups só com ação implementada: filial/grupo/hora (vendas), filial/vencidos/tipo/restringir filial (carteira). Troca de assunto (“investigar carteira” após vendas) não reutiliza o acompanhamento anterior.
- Carteira: receber e pagar são distintos; pergunta ambígua pede esclarecimento curto. Sem série histórica — “mês passado” explica a posição atual.
- Linguagem: números/datas em pt-BR (vendas e carteira), apelido de filial, situação “A vencer”; avisos de dado incompleto, permissão e hipótese permanecem. “Dessa filial” usa a filial em foco do último resultado (título ou concentração).
- Follow-ups reexecutam capacidade com **escopo vigente**; mudança de filial/permissão/período invalida contexto.
- Jarvis opcional: narrativa via OpenAI Responses **somente** a partir do pack de evidências; números não sustentados são rejeitados. Sem chave → o usuário vê só o texto de negócio (números e avisos). Código interno (`openai_not_configured`) não vai à interface.
- Textos gerados seguem o contrato de copy do produto (pt-BR claro; hipótese ≠ causa; sem jargão de pipeline).
- Proibido: SQL livre do modelo, mutações, agents em background, histórico como prova de autorização.

### Validação Jarvis
- Hom/Prod: `OPENAI_API_KEY` **vazio** nos env atuais → sem chamada paga. Integração de código pronta; **LLM real não validada** sem chave autorizada.
- Não contratar/habilitar provedor nesta entrega. Avaliação de custo abaixo; ativação depende de aprovação explícita.

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

## Avaliação de custos de IA (antes de ativação paga)

**Data da consulta:** 13/09/2026.  
**Fontes oficiais (USD por 1 milhão de tokens, Responses API):**

| Modelo | Entrada | Entrada em cache | Saída | Fonte |
|---|---:|---:|---:|---|
| `gpt-4.1-mini` | 0,40 | 0,10 | 1,60 | [modelo](https://developers.openai.com/api/docs/models/gpt-4.1-mini) · [preços](https://developers.openai.com/api/docs/pricing) |
| `gpt-4.1` | 2,00 | 0,50 | 8,00 | [modelo](https://developers.openai.com/api/docs/models/gpt-4.1) · [anúncio](https://openai.com/index/gpt-4-1/) |
| `gpt-4o-mini` | 0,15 | 0,075 | 0,60 | [modelo](https://developers.openai.com/api/docs/models/gpt-4o-mini) |

Assinatura do Cursor ou do ChatGPT **não** paga a API do TorqMind. Só conta `OPENAI_API_KEY` na API.

Valores em **dólar**. Conversão para reais e tributos/IOF/spread **não** incluídos.

### O que a integração realmente chama

**Narração da investigação** (`maybe_narrate_with_jarvis`) — hot path do Assistente quando a chave existir:

- 1 chamada `POST https://api.openai.com/v1/responses` por investigação com status ok
- 1 chamada adicional por pergunta de acompanhamento com status ok
- Modelo: `JARVIS_MODEL_FAST` (default `gpt-4.1-mini`)
- Conteúdo: system + pack de evidências (headline, totais, comparação, avisos, até 5 itens por dimensão, títulos sem nome de pessoa). JSON truncado a 6.000 caracteres
- Histórico da conversa **não** vai ao provedor
- `max_output_tokens`: 500
- Sem retentativa; falha → texto de negócio permanece
- Sem chave → zero chamada (`openai_not_configured` só no diagnóstico interno)

**Planos Jarvis** (`generate_jarvis_ai_plans`) — lote de alertas, **não** é a conversa:

- Até `JARVIS_AI_TOP_N` (10) chamadas por execução
- Até 3 retentativas em 429/5xx
- Cache em `app.insight_ai_cache`; `ai_usage_summary` agrega tokens e custo estimado
- Preços configuráveis: `JARVIS_AI_INPUT_COST_PER_1M` / `JARVIS_AI_OUTPUT_COST_PER_1M` (defaults 0,40 / 1,60, alinhados ao `gpt-4.1-mini`)

O chat determinístico (`AI_CHAT_ENABLED`) responde sem LLM. A chave só liga a narração e os planos.

### Estimativa de tokens (sem envio ao provedor)

Classificação: **aproximação** (≈ 1 token / 4 caracteres em JSON+pt-BR; tokenizador oficial da OpenAI não foi executado neste host). **Não** é consumo medido nem valor faturado.

Premissa da jornada representativa (1 investigação + 2 acompanhamentos, pack típico de vendas):

| Etapa | Entrada (aprox.) | Saída (aprox.) | Teto de saída |
|---|---:|---:|---:|
| Investigação | 1.400 | 280 | 500 |
| Acompanhamento | 1.200 | 220 | 500 |
| Conversa (1+2) | 3.800 | 720 | 1.500 |

Custo estimado com `gpt-4.1-mini` (entrada × 0,40 + saída × 1,60 / 1M):

| Jornada | Custo estimado |
|---|---:|
| 1 investigação | **USD 0,00101** |
| 1 acompanhamento | **USD 0,00083** |
| 1 conversa (1+2) | **USD 0,00267** |

Cenários mensais — premissas explícitas: 100% das conversas com LLM, 1 investigação + 2 acompanhamentos, sem cache, sem retentativa, 1 empresa, só narração (sem lote de planos):

| Conversas / mês | Estimativa (`gpt-4.1-mini`) | Pessimista (pack e saída no teto, ≈ USD 0,0048 / conversa) |
|---|---:|---:|
| 1.000 | USD 2,67 | USD 4,80 |
| 10.000 | USD 26,70 | USD 48,00 |
| 50.000 | USD 133,50 | USD 240,00 |

Efeitos que mudam o custo:

- Histórico da conversa: **não** entra na narração Phase 3 (não aumenta)
- Resposta no teto de 500 tokens: cerca de +USD 0,00035 por chamada vs. 280
- Pack no teto de 6.000 caracteres: cerca de +USD 0,00024 de entrada vs. 1.400
- Retentativas: só nos planos Jarvis (até 3× aquela chamada)
- Lote de 10 planos sem cache: ordem de USD 0,01 por execução (não está no cenário mensal acima)

Custo **efetivamente faturado** só aparece depois de uso real na fatura OpenAI.

### Controles já existentes

- Chave vazia = zero consumo
- `max_output_tokens` 500 (narração e planos)
- Cache + `ai_usage_summary` **somente** nos planos
- `AI_CHAT_ENABLED` liga o chat; o chat funciona sem LLM

### Lacunas

- Sem teto de gasto nem alerta de orçamento na narração
- Sem persistência de tokens da narração
- Sem retentativa (e sem multiplicador de custo) na narração
- Preços em env podem ficar defasados em relação à tabela oficial
- Sem limite por empresa/mês

### Comparação e recomendação de piloto

| Opção | Prós | Contras |
|---|---|---|
| **`gpt-4.1-mini` (recomendado)** | Já é o default; bom pt-BR e tools; Responses API já integrada; preço alinhado ao config | Não é o mais barato |
| `gpt-4o-mini` | ~60% mais barato na entrada | Qualidade e aderência à linha 4.1 menores; exigiria validação nova |
| `gpt-4.1` | Mais forte | 5× mais caro na entrada; desnecessário para redigir evidências curtas |

Recomendação: piloto com **`gpt-4.1-mini`**. Não escolher só pelo menor preço — a integração, o português e o uso de evidências já foram desenhados nesse modelo.

### Decisão ainda necessária (não executar)

Não definir orçamento em nome do usuário. Proposta para um piloto **limitado**, só após aprovação:

- Ambiente: Homologação
- Modelo: `gpt-4.1-mini`
- Superfície: só narração de investigação (não ligar lote de planos)
- Duração sugerida: 14 dias
- **Teto máximo proposto: USD 30** — cobre a ordem de 11 mil conversas no cenário-base ou ~6 mil no pessimista; suficiente para julgar qualidade sem risco material
- Configurar `OPENAI_API_KEY` **somente** depois dessa aprovação
- Revisar `JARVIS_AI_*_COST_PER_1M` na data da ativação
- Sem esta aprovação, a chave permanece vazia e o consumo pago continua **zero**
