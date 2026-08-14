# Scripts e documentos destrutivos — aposentados

Estado: **não executar em produção nem homologação.**

Homologação TorqMind contém dados reais de cliente e **não é descartável**.

| Artefato | Papel histórico | Estado |
|----------|-----------------|--------|
| `sql/torqmind_reset_db_v2.sql` | Reset total PG | Aposentado Hom/Prod. Só Postgres local efêmero. |
| `make resetdb` / `hard-resetdb` | Wrapper do reset | Recusa Hom/Prod. Exige `TM_EPHEMERAL_LOCAL=1`. |
| `sql/migrations/003_mart_demo.sql` | `DROP SCHEMA` em cadeia bootstrap | Fora do trilho incremental managed. |
| `docs/go_live_runbook.md` (`DROP DATABASE IF EXISTS TORQMIND`) | Go-live antigo | Histórico. Não copiar para o servidor. |
| `docs/REALTIME_OPERATIONS_RUNBOOK.md` TRUNCATE slim | Operação CH | Não executar sem autorização e plano. |
| `deploy/scripts/realtime-reset-mart-rt.sh` | TRUNCATE mart_rt | Ferramenta controlada; não é reset PG; ainda exige confirmação. |
| `deploy/scripts/prod-clickhouse-init.sh` `DROP DATABASE` | Bootstrap CH | Não rerodar em analytics compartilhado. |
| `.github/workflows/ci.yml` `docker compose down -v` | Runner GitHub efêmero | **Somente CI**. Nunca no host TorqMind. |

Autoridade: `AGENTS.md` > `CODEX_TORQMIND_MAP.md` > este arquivo.
