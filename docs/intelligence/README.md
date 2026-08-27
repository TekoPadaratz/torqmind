# TorqMind Intelligence — Assistente determinístico (pt-BR)

Versão do catálogo: `1.0.0`  
Branch de entrega: `codex/torqmind-intelligence-deterministic-ptbr-2026-08-27`

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

Jarvis (`/bi/jarvis/*`, `services/jarvis_ai.py`) permanece intacto e **não** é chamado pelo assistente.

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
