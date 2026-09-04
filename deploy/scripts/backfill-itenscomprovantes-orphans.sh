#!/usr/bin/env bash
# Backfill de ITENSCOMPROVANTE para comprovantes órfãos (header sem itens).
# Roda no posto (Windows/PowerShell via agent CLI) ou documenta o comando.
#
# Exemplo Eldely (emp 1 / VR01) — comprovantes 3694867 e 3694873:
#   torqmind-agent.exe backfill --dataset itenscomprovantes \
#     --from 2026-08-20 --to 2026-08-21
#
# Ou janela ampla recente:
#   torqmind-agent.exe backfill --dataset itenscomprovantes --from 2026-08-01 --to 2026-09-04
#
# Depois do backfill: conferir no CH
#   SELECT id_comprovante, count() FROM torqmind_current.stg_itenscomprovantes_slim
#   WHERE id_empresa=1 AND id_comprovante IN (3694867,3694873) GROUP BY id_comprovante
set -euo pipefail
echo "Use o agent no posto (não este host):"
echo "  torqmind-agent.exe backfill --dataset itenscomprovantes --from YYYY-MM-DD --to YYYY-MM-DD"
echo "Overlaps parent/filho aumentados no agent 2.0.10 (6h) para reduzir órfãos novos."
