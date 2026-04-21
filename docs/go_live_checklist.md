# TorqMind Go-Live Checklist

Data base: 2026-03-03
Escopo: release ULTIMATE DIAMOND (Fases 0-8)

## 1) T-24h (preparação)

### Infra e acesso
- [ ] Confirmar acesso Docker/DB/API/Web em produção.
- [ ] Confirmar credenciais e segredos em vault (`API_JWT_SECRET`, `OPENAI_API_KEY`, `DATABASE_URL`, `INGEST_REQUIRE_KEY=true`).
- [ ] Confirmar janela de deploy aprovada e responsáveis on-call.

### Banco e dados
- [ ] Executar backup lógico completo do banco.
- [ ] Validar espaço em disco e crescimento esperado de STG/DW.
- [ ] Revisar ordem de migrations em `docs/release_notes.md`.

### Qualidade
- [ ] Executar `make ci` no ambiente de homolog idêntico ao prod.
- [ ] Validar smoke manual de login + dashboard + ETL.

Critério de saída T-24h:
- `make ci` verde e backup verificado.

## 2) T-1h (pré-deploy imediato)

### Congelamento
- [ ] Congelar mudanças fora do escopo (code freeze).
- [ ] Confirmar commit/tag de release.

### Sanidade antes da troca
- [ ] `docker compose ps` sem degradação.
- [ ] `GET /health` retornando `ok=true`.
- [ ] Capturar baseline:
  - latência dashboard
  - duração ETL média
  - unread notifications

Critério de saída T-1h:
- Ambiente estável e baseline registrado.

## 3) T-15min (execução do deploy)

### Banco
- [ ] Aplicar migrations:
  1. `006_ingest_shadow_columns.sql`
  2. `007_etl_incremental_hot_received.sql`
  3. `008_run_all_skip_risk_when_no_changes.sql`
  4. `009_phase4_moneyleak_health.sql`
  5. `010_phase5_ai_engine.sql`
  6. `011_phase7_notifications.sql`

### Aplicação
- [ ] Subir nova imagem API.
- [ ] Subir nova imagem Web.
- [ ] Confirmar containers `Up` e `healthy`.

### Smoke rápido de release
- [ ] Login OWNER.
- [ ] `GET /bi/dashboard/overview` -> 200 com `health_score`.
- [ ] `GET /bi/clients/churn` -> 200.
- [ ] `GET /bi/pricing/competitor/overview` -> 200.
- [ ] `GET /bi/finance/overview` -> 200 com `aging`.
- [ ] `POST /bi/jarvis/generate` -> 200.
- [ ] `GET /bi/notifications` -> 200.

Critério de saída T-15min:
- Todos endpoints críticos 200.

## 4) T+1h (estabilização)

### Operação assistida
- [ ] Rodar ETL para tenants piloto (duas execuções) e registrar tempos.
- [ ] Conferir contagens STG/DW/MART.
- [ ] Validar geração de insights e ações top 3 no dashboard.
- [ ] Validar criação e leitura de notificação in-app.

### IA e custo
- [ ] Checar `/bi/admin/ai-usage` (tokens/custo/calls).
- [ ] Confirmar que fallback determinístico funciona sem erro se IA indisponível.

Critério de saída T+1h:
- ETL estável + dashboards funcionais + custo IA sob controle.

## 5) T+24h (aceite operacional)

### KPIs técnicos
- [ ] Erro API 5xx dentro do limite acordado.
- [ ] Latência dashboard dentro do baseline.
- [ ] ETL sem falhas em `etl.run_log`.

### KPIs de negócio
- [ ] Hero exibindo valor monetário recuperável.
- [ ] Top 3 ações com checklist/evidência.
- [ ] Radares fraude/churn/caixa com dados coerentes.
- [ ] Health score com explicação (sem fake signal).

Critério de aceite final:
- Produto vendável operacionalmente para dono de rede.

## 6) Rollback rápido (se incidente)

- [ ] Reverter imagem API/Web para tag estável anterior.
- [ ] Manter ingest ativo; pausar apenas ETL se necessário.
- [ ] Desligar IA (`OPENAI_API_KEY` vazio) para fallback determinístico.
- [ ] Se necessário, restaurar backup pré-release.
- [ ] Registrar incidente e linha do tempo.

## 7) Responsáveis sugeridos

- Release Captain: coordenação e go/no-go.
- Data Lead: migrations, ETL, contagens.
- API Lead: endpoints e auth/tenant safety.
- Front Lead: dashboard/UX e regressões.
- SRE/DevOps: observabilidade, rollback, estabilidade.
