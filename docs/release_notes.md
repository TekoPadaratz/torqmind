# TorqMind ULTIMATE DIAMOND - Release Notes

Data: 2026-03-03
Escopo: Fases 0 a 8 concluídas (diagnóstico, agent, ingest, ETL incremental, money leak OS, IA com custo controlado, frontend premium, alertas in-app, testes/CI).

## 1) Resumo executivo

- Pipeline STG->DW->MART incremental validado com watermark + hot window.
- Radares operacionais entregues: Fraude, Churn, Caixa, Health Score realista.
- Jarvis IA em modo custo-controlado com cache por hash, fallback determinístico e uso/tokens auditáveis.
- Alertas in-app entregues com fluxo unread/read funcional.
- Front acima da dobra orientado a ação: HERO monetário, top ações, radares e evidências.
- Gate de qualidade consolidado em `make ci` + workflow CI.
- Rotas BI críticas agora priorizam leitura ao vivo com `dt_ref` preservado na URL, eliminando reuso de snapshot stale entre filtros.

## 2) Migrations desta release

Aplicar em ordem:

1. `sql/migrations/006_ingest_shadow_columns.sql`
2. `sql/migrations/007_etl_incremental_hot_received.sql`
3. `sql/migrations/008_run_all_skip_risk_when_no_changes.sql`
4. `sql/migrations/009_phase4_moneyleak_health.sql`
5. `sql/migrations/010_phase5_ai_engine.sql`
6. `sql/migrations/011_phase7_notifications.sql`

## 3) Novos componentes de produto

Backend/API:
- `/bi/clients/churn`
- `/bi/jarvis/generate`
- `/bi/admin/ai-usage`
- `/bi/notifications`
- `/bi/notifications/{id}/read`
- `/bi/notifications/unread-count`
- `/bi/pricing/competitor/overview`
- `/bi/pricing/competitor/prices`

Tabelas/app:
- `app.insight_ai_cache`
- `app.notifications`
- `app.competitor_fuel_prices`
- colunas aditivas em `app.insights_gerados` (`ai_plan`, `ai_model`, tokens, cache_hit, erro, timestamps)

Frontend:
- Dashboard premium com Hero + Top 3 ações + radares + alertas
- Dashboard de preço da concorrência (`/pricing`) com input manual por combustível e simulação 10 dias
- Componentes UI reutilizáveis (`HeroMoneyCard`, `ActionCard`, `RadarPanel`, `EvidenceChips`, `RiskBadge`, `Skeleton`)

## 4) Compatibilidade e contratos

- Mudanças aditivas (sem remoção de campos existentes).
- Payloads existentes preservados.
- Multi-tenant mantido via escopo JWT/RLS (`id_empresa`, `id_filial` conforme contexto).

## 5) Variáveis de ambiente críticas

Obrigatórias para produção:
- `DATABASE_URL` (ou `PG_*` equivalente)
- `API_JWT_SECRET`
- `INGEST_REQUIRE_KEY=true`

Jarvis IA:
- `OPENAI_API_KEY`
- `JARVIS_MODEL_FAST` (default: `gpt-4.1-mini`)
- `JARVIS_MODEL_STRONG` (default: `gpt-4.1`)
- `JARVIS_AI_TOP_N` (default: `10`)
- `JARVIS_AI_MAX_OUTPUT_TOKENS` (default: `500`)
- `JARVIS_AI_INPUT_COST_PER_1M`
- `JARVIS_AI_OUTPUT_COST_PER_1M`

## 6) Sequência de deploy recomendada

1. Backup lógico do banco.
2. Aplicar migrations da seção 2.
3. Publicar containers `api` e `web`.
4. Validar saúde:
   - `GET /health`
   - login OWNER
   - `GET /bi/dashboard/overview`
5. Executar `etl.run_all(id_empresa,false,true)` para tenants piloto.
6. Validar radares + jarvis + notificações no front.

## 7) Smoke pós-deploy (obrigatório)

1. `make ci`
2. Ingest de 1 linha NDJSON em `produtos`.
3. Rodar ETL 2x e registrar tempos.
4. Conferir contagens STG/DW/MART.
5. Verificar endpoints:
   - `/bi/dashboard/overview`
   - `/bi/clients/churn`
   - `/bi/finance/overview`
   - `/bi/jarvis/generate`
   - `/bi/admin/ai-usage`
   - `/bi/notifications`

## 8) Plano de rollback

Rollback técnico:
1. Reverter imagem `api` e `web` para tag anterior estável.
2. Desabilitar geração IA (`OPENAI_API_KEY` vazio) para fallback determinístico.
3. Manter ingest ativo; pausar apenas `/etl/run` se necessário.

Rollback de dados:
- As migrations são majoritariamente aditivas; rollback preferencial é por aplicação (versão de app) e não por drop de schema em produção.
- Em incidente severo, restaurar backup pré-release.

## 9) Monitoração pós-release (24h)

KPIs de estabilidade:
- taxa de erro API (>=500)
- latência de `/bi/dashboard/overview`
- duração ETL e `etl.run_log.status`
- volume de `app.notifications` e unread acumulado
- uso/custo IA (`/bi/admin/ai-usage`)

KPIs de valor:
- impacto estimado em insights
- churn `revenue_at_risk_30d`
- aging financeiro vencido
- variação de health score

## 10) Evidências

- Evidências técnicas completas em: `docs/proof_pack.md`
- Diagnóstico inicial e correções: `docs/diagnostics_report.md`
- Runbook do agent: `docs/agent_runbook.md`
