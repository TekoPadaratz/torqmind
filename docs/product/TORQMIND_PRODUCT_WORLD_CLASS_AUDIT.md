# TorqMind — Auditoria de Produto World-Class

**Data:** 2026-04-30
**Autor:** Principal Product & Architecture Team
**Versão:** 1.0
**Branch:** nova-branch-limpa

---

## 1. Visão de Produto

### Missão
TorqMind transforma dados operacionais de postos de combustível em decisões rápidas, corretas e lucrativas.

### Proposta de Valor
Um único ponto de verdade para toda a rede: vendas, caixa, fraude, financeiro, equipe, clientes e concorrência — com alertas inteligentes e linguagem que o dono do posto entende.

### Estado-alvo (world-class)
- Latência sub-minuto para alertas críticos (caixa aberto >4h, cancelamento atípico)
- Dashboard que responde em <2s com dados atualizados a cada 2 min
- Zero jargão técnico visível ao cliente
- Inteligência acionável: cada número leva a uma ação
- Agent/Jarvis proativo em português natural

---

## 2. Personas

| Persona | Papel | Necessidade principal | Frequência |
|---------|-------|----------------------|------------|
| **Dono de Rede** | CEO/Diretor | Visão executiva, impacto financeiro, fraude, desvios | 2-3x/dia |
| **Gerente Operacional** | Gerência | Problemas do dia, turnos, equipe, metas | Contínuo |
| **Supervisor de Pista** | Operação | Caixa agora, turnos abertos, divergências | Em tempo real |
| **Financeiro** | Contas | Vencidos, fluxo, inadimplência, DRE | Diário |
| **Auditor Antifraude** | Compliance | Cancelamentos suspeitos, desvios, padrões | Diário/semanal |
| **Analista de Dados** | BI interno | Consultas ad-hoc, tendências, comparativos | Semanal |
| **Suporte TorqMind** | Interno | Saúde técnica, CDC, ETL, erros | Contínuo |

---

## 3. Auditoria Tela por Tela

### 3.1 Dashboard Geral

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Radar executivo: o que exige ação agora |
| **Usuário-alvo** | Dono/Gerente |
| **Perguntas respondidas** | Fraude? Caixa sob pressão? Clientes em risco? Vencidos? |
| **Dados necessários** | KPIs agregados de vendas, risco, caixa, financeiro, churn |
| **Origem** | `torqmind_mart` (agg_vendas_diaria, agg_risco_diaria, etc.) ✅ |
| **Problemas** | (1) Usa "recorte" em empty states — jargão técnico; (2) Health score sem explicação do cálculo para o leigo |
| **Melhorias obrigatórias** | Trocar "recorte" → "período selecionado" |
| **Melhorias desejáveis** | Tooltip explicativo no health score; CTA direto para a tela-problema |
| **Testes** | ✅ Testes guardam ausência de "frescor" e "Saídas normais" |

### 3.2 Vendas

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Visão comercial: quanto vendeu, margem, produtos, equipe |
| **Usuário-alvo** | Gerente/Dono |
| **Perguntas respondidas** | Faturamento? Margem? Quem vendeu mais? O que vendeu mais? |
| **Dados necessários** | Vendas diárias, por hora, produtos, grupos, funcionários |
| **Origem** | `torqmind_mart` ✅ |
| **Problemas** | Nenhum crítico encontrado. Linguagem correta. |
| **Melhorias desejáveis** | Comparativo mês anterior no topo; meta embutida no gráfico |
| **Testes** | ✅ Teste garante "Vendas normais" (não "Saídas normais") |

### 3.3 Caixa

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Caixa operacional agora e no período |
| **Usuário-alvo** | Supervisor/Gerente |
| **Perguntas respondidas** | Turnos abertos? Formas de pagamento? Divergências? DRE? |
| **Dados necessários** | Turnos, pagamentos, alertas caixa aberto, DRE |
| **Origem** | `torqmind_mart` ✅ |
| **Problemas** | (1) "recorte" em 3 empty states |
| **Melhorias obrigatórias** | Trocar "recorte" → "período" |
| **Melhorias desejáveis** | Alerta sonoro/push para caixa aberto >4h |

### 3.4 Antifraude

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Identificar e priorizar eventos de risco operacional |
| **Usuário-alvo** | Auditor/Dono |
| **Perguntas respondidas** | Quem? Onde? Quanto? Padrão? |
| **Dados necessários** | Eventos de risco, top usuários, turnos, impacto |
| **Origem** | `torqmind_mart` (fraude_cancelamentos_*, risco_*) ✅ |
| **Problemas** | (1) "não identificado" em 5+ labels; (2) "recorte" em empty states |
| **Melhorias obrigatórias** | (1) Quando operador/turno não tem cadastro: "Cadastro pendente" ou "Sem registro" (não "não identificado"); (2) Trocar "recorte" |
| **Melhorias desejáveis** | Score visual (semáforo); timeline de eventos; drill para comprovante |

### 3.5 Clientes

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Saúde da base de clientes, churn, inadimplência |
| **Usuário-alvo** | Gerente/Financeiro |
| **Perguntas respondidas** | Quantos clientes? Risco de perda? Inadimplência? Recorrência? |
| **Dados necessários** | RFM, churn, retenção anônima, inadimplência |
| **Origem** | `torqmind_mart` (customer_rfm_daily, churn_risk) ✅ |
| **Problemas** | (1) "recorte" em 1 empty state; (2) Falta mart de inadimplência por cliente (debt explícita) |
| **Melhorias obrigatórias** | Trocar "recorte"; criar mart de inadimplência |
| **Melhorias desejáveis** | Ação de WhatsApp/SMS para clientes em risco |

### 3.6 Financeiro

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Pressão de caixa, vencidos, pagamentos, DRE |
| **Usuário-alvo** | Financeiro/Dono |
| **Perguntas respondidas** | Quanto vencido? A vencer? Concentração? Forma líder? Anomalias? |
| **Dados necessários** | Aging, vencimentos, pagamentos, anomalias |
| **Origem** | `torqmind_mart` ✅ |
| **Problemas** | (1) ~8x "recorte"; (2) "Pagamentos não identificados" como label fixo |
| **Melhorias obrigatórias** | Trocar "recorte" → "período"; Trocar "Pagamentos não identificados" → "Pagamentos sem classificação" |
| **Melhorias desejáveis** | Projeção de fluxo 30/60/90 dias; alerta de inadimplência crescente |

### 3.7 Preço Concorrente

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Monitorar e simular preços vs concorrência |
| **Usuário-alvo** | Gerente/Dono |
| **Perguntas respondidas** | Meu preço está competitivo? Impacto de ajuste? |
| **Dados necessários** | Preços registrados manualmente no app |
| **Origem** | PostgreSQL `app.competitor_fuel_prices` ✅ (OLTP, não mart) |
| **Problemas** | Nenhum crítico. |
| **Melhorias desejáveis** | Integração com ANP/feeds automáticos; alerta de mudança significativa |

### 3.8 Metas & Equipe

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Acompanhar desempenho individual e de equipe |
| **Usuário-alvo** | Gerente/Dono |
| **Perguntas respondidas** | Quem está acima/abaixo? Projeção? Ranking? |
| **Dados necessários** | Vendas por funcionário, metas definidas |
| **Origem** | `torqmind_mart.agg_funcionarios_diaria` + `app.goals` ✅ |
| **Problemas** | Projeção mensal mistura app+mart (debt documentada) |
| **Melhorias desejáveis** | Gamificação visual; histórico de desempenho; notificação de queda |

### 3.9 Plataforma (Admin)

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Gestão técnica interna |
| **Usuário-alvo** | Suporte TorqMind |
| **Problemas** | Não tem painel de CDC lag, Redpanda status, ClickHouse health |
| **Melhorias obrigatórias** | Após streaming 2.0: adicionar seção de health técnica |

### 3.10 Login/Autenticação

| Item | Avaliação |
|------|-----------|
| **Objetivo** | Autenticar e redirecionar |
| **Problemas** | Default JWT secret em config.py (CRITICAL para produção) |
| **Melhorias obrigatórias** | Fail-fast se JWT secret = default em produção |

### 3.11 Navegação (AppNav)

| Item | Avaliação |
|------|-----------|
| **Problemas** | Nenhum. Labels em pt-BR. "Plataforma" correto. Sem frescor técnico. |
| **Melhorias desejáveis** | Badge de alertas por seção (não apenas total) |

### 3.12 Componentes Compartilhados

| Item | Avaliação |
|------|-----------|
| **format.ts** | Usa "Turno não identificado" como fallback (L99) |
| **ScopeTransitionState** | Bem implementado |
| **EmptyState** | Padrão consistente, mas usa "recorte" |

---

## 4. Auditoria Domínio por Domínio

### 4.1 Vendas

| Aspecto | Status | Notas |
|---------|--------|-------|
| Origem canônica | ✅ | `stg.comprovantes/itenscomprovantes` → `dw.fact_venda/item` |
| Cancelamento | ✅ | Flag `cancelado` + evento de risco |
| CFOP/Entradas | ✅ | `saidas_entradas` diferencia venda de entrada |
| Produtos/Grupos | ✅ | Marts com nome humano desnormalizado |
| Funcionários | ✅ | Nome humano na mart |
| Multi-tenant | ✅ | `id_empresa` em todas as queries |
| Hora | ✅ | `agg_vendas_hora` por hora local |

### 4.2 Caixa

| Aspecto | Status | Notas |
|---------|--------|-------|
| Turnos | ✅ | `fact_caixa_turno` com abertura/fechamento |
| Operador | ✅ | Nome via dim_usuario_caixa |
| Pagamentos | ✅ | `payment_type_map` resolve labels |
| Formas reais | ✅ | `dim_forma_pagamento` no CH |
| Divergências | ⚠️ | Não há mart de divergência automática turno vs sistema |
| Caixa aberto | ✅ | `alerta_caixa_aberto` com severity |

### 4.3 Antifraude

| Aspecto | Status | Notas |
|---------|--------|-------|
| Cancelamentos | ✅ | `CANCELAMENTO` event_type |
| Funcionário outlier | ✅ | `FUNCIONARIO_OUTLIER` com IDs sintéticos |
| Score de risco | ✅ | score_level: ALTO/SUSPEITO/ATENÇÃO/NORMAL |
| Impacto financeiro | ✅ | `impacto_estimado` em cada evento |
| Labels humanos | ✅ | Marts com filial_nome, operador_nome |
| Razões | ✅ | Campo `reasons` JSON |

### 4.4 Financeiro

| Aspecto | Status | Notas |
|---------|--------|-------|
| Contas a pagar/receber | ✅ | `financeiro_vencimentos_diaria` |
| Vencidos | ✅ | Bucket aging |
| DRE/Fluxo | ✅ | `cash_dre_summary` via finance_aging_daily |
| Inadimplência cliente | ⚠️ | Falta mart customer-level (debt) |

### 4.5 Clientes

| Aspecto | Status | Notas |
|---------|--------|-------|
| Churn | ✅ | `customer_churn_risk_daily` |
| Recorrência | ✅ | `anonymous_retention_daily` |
| RFM | ✅ | `customer_rfm_daily` |
| Inadimplência | ⚠️ | Sem drilldown por cliente (debt) |

### 4.6 Metas

| Aspecto | Status | Notas |
|---------|--------|-------|
| Metas | ✅ | `app.goals` OLTP |
| Ranking | ✅ | `agg_funcionarios_diaria` |
| Projeção | ⚠️ | Mistura app + mart (debt) |

### 4.7 Preço Concorrente

| Aspecto | Status | Notas |
|---------|--------|-------|
| Escrita | ✅ | PostgreSQL OLTP |
| Leitura | ✅ | Bypassa cache |
| Histórico | ⚠️ | Não há mart de evolução de preço |

### 4.8 Plataforma / Operacional

| Aspecto | Status | Notas |
|---------|--------|-------|
| CDC lag | ❌ | Não implementado na UI |
| Health ClickHouse | ❌ | Não implementado |
| Erros ETL | ⚠️ | Visível apenas em logs |
| Saúde por tenant | ❌ | Não implementado |

---

## 5. Linguagem Recomendada

### Substituições obrigatórias (UI)

| De | Para | Razão |
|----|------|-------|
| "recorte" | "período" ou "período selecionado" | Cliente não sabe o que é recorte de dados |
| "não identificado" (operador/turno) | "Cadastro pendente" ou "Sem registro" | Mais preciso e menos alarmante |
| "Pagamentos não identificados" | "Pagamentos sem classificação" | Problema é falta de mapeamento, não identidade |

### Termos banidos de UI

| Termo | Onde pode existir | Onde NÃO pode |
|-------|-------------------|---------------|
| mart | código/SQL/docs técnicos | Qualquer texto visível ao cliente |
| snapshot | variáveis internas | UI labels |
| recorte | NUNCA | NUNCA em UI |
| frescor operacional | NUNCA | NUNCA em UI |
| trilho operacional | código interno | UI |
| publicação analítica | código interno | UI |
| Platform | nomes de componentes/roles internos | Labels visíveis (usar "Plataforma") |
| FORMA_X | NUNCA no hot path | NUNCA — resolver via payment_type_map |
| 1970-01-01 | NUNCA | NUNCA — retornar null/indisponível |

---

## 6. Problemas Críticos (Top 10 Produto)

| # | Problema | Impacto | Esforço |
|---|---------|---------|---------|
| 1 | "recorte" em 32 ocorrências na UI | Cliente confuso | Baixo (find-replace) |
| 2 | "não identificado" em 8 ocorrências | Parece bug para o cliente | Baixo |
| 3 | Falta mart de inadimplência por cliente | Gap funcional | Médio |
| 4 | Plataforma sem health técnico (CDC/CH) | Suporte cego | Médio |
| 5 | Projeção mensal mistura app+mart | Dado pode estar stale | Médio |
| 6 | Sem mart de divergência caixa vs sistema | Gap operacional crítico | Alto |
| 7 | Sem alerta push/sonoro para caixa aberto | Supervisor não é notificado em tempo real | Médio |
| 8 | Sem histórico de preço concorrente analítico | Sem tendência de mercado | Médio |
| 9 | Sem gamificação/notificação em metas | Equipe não engaja | Médio |
| 10 | Sem Agent/Jarvis acessível no mobile | Perde valor pra supervisor de pista | Alto |

---

## 7. Problemas Críticos (Top 10 Técnicos)

| # | Problema | Severidade | Onde |
|---|---------|-----------|------|
| 1 | JWT secret default "CHANGE_ME_SUPER_SECRET" | CRITICAL | config.py |
| 2 | `product_global` role pode acessar qualquer tenant | HIGH | scope.py |
| 3 | Endpoints BI sem response_model tipado | HIGH | routes_bi.py |
| 4 | PG password default "1234" | CRITICAL (dev only) | config.py |
| 5 | ClickHouse default user sem senha | MEDIUM | config.py |
| 6 | Ingest endpoint aberto por default | MEDIUM | config.py |
| 7 | F-string SQL em _branch_clause | HIGH (arquitetural) | repos_mart_clickhouse.py |
| 8 | Fact estoque desabilitado sem timeline | MEDIUM | etl_orchestrator.py |
| 9 | Streaming compose usava rede isolada | FIXED nesta sessão | docker-compose.streaming.yml |
| 10 | SQL DDLs streaming não trackeados pelo git | FIXED nesta sessão | sql/clickhouse/streaming/ |

---

## 8. Quick Wins (implementáveis em <1 dia cada)

| # | Ação | Impacto | Risco |
|---|------|---------|-------|
| 1 | Replace "recorte" → "período" em 32 locais do frontend | UX | Zero |
| 2 | Replace "não identificado" → "Cadastro pendente" em 8 locais | UX | Zero |
| 3 | Fail-fast em config.py se JWT secret = default em production | Segurança | Zero |
| 4 | Validar `id_empresa_q` em product_global role | Segurança | Baixo |
| 5 | Adicionar response_model nos 5 endpoints mais usados | Qualidade | Baixo |
| 6 | Criar seção "Saúde Técnica" vazia na Plataforma | Preparação | Zero |
| 7 | Adicionar `INGEST_REQUIRE_KEY=true` no .env.production.example | Segurança | Zero |
| 8 | Remover 040_pilot_marts.sql do CODEX (arquivo não existe mais) | Documentação | Zero |

---

## 9. Refactors Estruturais

| # | Refactor | Motivação | Prazo sugerido |
|---|---------|-----------|---------------|
| 1 | Criar mart `caixa_divergencia_turno` | Divergência calculada (esperado vs observado por forma) | Sprint 2 |
| 2 | Criar mart `cliente_inadimplencia_detail` | Drilldown por cliente | Sprint 2 |
| 3 | Criar mart `preco_concorrente_historico` | Evolução de preço competitivo | Sprint 3 |
| 4 | Criar mart `projecao_meta_mensal` | Projeção pura ClickHouse | Sprint 3 |
| 5 | Migrar endpoints BI para response_model Pydantic | Contrato tipado | Contínuo |
| 6 | Parametrizar SQL com bind vars em repos_mart_clickhouse | Segurança | Sprint 1 |
| 7 | Implementar rate limiting em /bi/jarvis/generate | Custo | Sprint 1 |
| 8 | Implementar mart de estoque | Feature completa | Sprint 4 |

---

## 10. Futuras Telas e Alertas

### Telas novas

| Tela | Persona | Descrição |
|------|---------|-----------|
| **Divergências de Caixa** | Supervisor | Turnos com diferença > threshold entre esperado e observado |
| **Alertas Ativos** | Todos | Feed de alertas com prioridade e ação sugerida |
| **Histórico de Preços** | Gerente | Gráfico de evolução de preços vs concorrentes |
| **Relatório Executivo** | Dono | PDF/export semanal/mensal com KPIs top-level |
| **Agent/Jarvis** | Todos | Chat/briefing em português natural |

### Alertas prioritários

| Alerta | Trigger | Canal | Urgência |
|--------|---------|-------|----------|
| Caixa aberto > 4h | `alerta_caixa_aberto.hours_open > 4` | Push + UI | Alta |
| Cancelamento atípico | Evento risco score > 80 | Push + UI | Alta |
| Vencido crescente | aging bucket "Vencido" > threshold | UI | Média |
| Meta em risco | Projeção < 70% no dia 20 | UI + WhatsApp | Média |
| Operador outlier | FUNCIONARIO_OUTLIER + score alto | UI | Alta |

---

## 11. Como o Agent/Jarvis Entra

### Pré-requisitos
1. ✅ Marts consolidadas com dados limpos
2. ✅ Health score e insights base
3. 🔄 Streaming 2.0 com latência sub-minuto
4. ❌ Semântica de evento para "o que mudou"
5. ❌ Mapa de ações por tipo de alerta

### Fases

| Fase | Capacidade | Dependência |
|------|-----------|-------------|
| 1 - Briefing passivo | Resumo diário/semanal baseado em marts | ✅ Já implementado (`jarvis_briefing`) |
| 2 - Alertas reativos | "Caixa X está aberto há 5h" | Streaming 2.0 + alertas |
| 3 - Sugestões proativas | "Produto Y caiu 30% esta semana, considere promoção" | Streaming + signals |
| 4 - Ação executiva | "Quer que eu feche o turno automaticamente?" | Workflow engine + permissões |
| 5 - Conversação | Chat livre com contexto operacional | LLM + embeddings |

---

## 12. Critérios de Excelência

### Para cada tela do TorqMind ser world-class:

1. **Carrega em < 2 segundos** (cold start incluído)
2. **Dados atualizados** (< 5 minutos para batch, < 1 minuto para streaming)
3. **Zero jargão técnico** visível ao cliente
4. **Cada número leva a uma ação** (CTA ou drill)
5. **Empty states explicativos** (não "sem dados")
6. **Erros amigáveis** com sugestão de resolução
7. **Multi-filial funcional** sem ambiguidade
8. **Mobile-friendly** para supervisor de pista
9. **Dados comparativos** (ontem, semana passada, mês anterior)
10. **Alertas visuais** para desvios críticos (cor, ícone, badge)

### Para o backend ser world-class:

1. **Multi-tenant enforced** em 100% das queries (id_empresa obrigatório)
2. **Response models tipados** em todos os endpoints públicos
3. **Sem secrets em defaults** — fail-fast em produção
4. **ClickHouse-first** sem fallback silencioso
5. **Latência P99 < 500ms** para endpoints BI
6. **Zero SQL injection** — parametrizado ou int-cast
7. **Observabilidade** — structured logs com correlation_id
8. **Testes de contrato** para cada endpoint
9. **Feature flags** para migração gradual
10. **Rate limiting** em endpoints custosos (Jarvis, exports)

---

## Apêndice A — Contagem de Termos Proibidos

| Termo | Ocorrências em UI | Ação |
|-------|-------------------|------|
| "recorte" | 32 | Substituir por "período" |
| "não identificado" | 8 | Substituir por "Cadastro pendente" / "Sem classificação" |
| "Platform" (texto visível) | 0 | OK |
| "frescor operacional" | 0 | OK (removido anteriormente) |
| "FORMA_" | 0 | OK (resolvido via payment_type_map) |
| "1970" | 0 | OK (guards implementados) |
| "snapshot" (texto UI) | 0 | OK (apenas em variáveis internas) |
| "mart" (texto UI) | 0 | OK |
| "Saídas normais" | 0 | OK (corrigido para "Vendas normais") |

---

## Apêndice B — Testes Executados nesta Auditoria

| Teste | Resultado |
|-------|-----------|
| CDC Consumer (pytest) | 29 passed, 15 subtests |
| Frontend (node --test) | 80 passed |
| bash -n streaming scripts | OK (7 scripts) |
| bash -n prod scripts | OK |
| docker compose streaming config | OK |
| docker compose main config | OK |
| docker compose prod config | OK |
| grep termos proibidos | 32 "recorte", 8 "não identificado" |
