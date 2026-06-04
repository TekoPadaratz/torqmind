---
description: Ritual de início de sessão TorqMind (ler mapas antes de mexer)
---

# Iniciar sessão TorqMind

Antes de qualquer alteração, faça o ritual de início. O objetivo é não
redescobrir o projeto (economia de tokens) e não quebrar padrão.

## 1. Ler os mapas (sempre)

- `CODEX_TORQMIND_MAP.md` — mapa canônico (funções, marts, rotas, flags).
- `AGENTS.md` e `.github/copilot-instructions.md`.
- Docs de produção/realtime **só quando o tema exigir**
  (`docs/REALTIME_OPERATIONS_RUNBOOK.md`, `docs/PRODUCTION_MULTI_VM_RUNBOOK.md`).

## 2. Confirmar em poucas linhas

- Arquitetura atual e hot path (ClickHouse-first quando `USE_REALTIME_MARTS=true`).
- Onde a tela/feature afetada busca o dado (fonte → mart → API → tela).
- Quais arquivos serão alterados (liste antes de editar).

## 3. Produção (servidor real)

- App/API/Web/Nginx: `172.30.0.10`; PostgreSQL/STG/DW: `172.30.0.8`;
  Analytics/ClickHouse/Redpanda/Debezium/CDC: `172.30.0.9`.
- Env de produção: `/etc/torqmind/prod.app.env`
  (`set -a; source /etc/torqmind/prod.app.env; set +a`).
- SQL Server Xpert disponível via `tools/xpert_source_explorer.py`.
- URL pública: `http://redevr.ddns.me:14023` (e `/api`).

## 4. Disciplina

- Mudança cirúrgica; não reescrever componente inteiro por bug pequeno.
- Bug de dado: validar fonte → tela (ver `validar-dado-fonte-a-tela`).
- Nunca: apagar STG, `down -v`, `reset --hard`, `push --force`, expor segredo,
  deploy sem health check, esconder permissão só no frontend, expor
  margem/lucro/custo a gerente/vendedor.

## 5. PASS

Causa raiz + arquivos + testes + validação real + health check + deploy/prova
(se prod mudou) + `CODEX_TORQMIND_MAP.md` atualizado + sem segredo no diff.
