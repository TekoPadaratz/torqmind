#!/usr/bin/env bash
# Backfill de ITENSCOMPROVANTE para comprovantes órfãos (header sem itens).
# Preferir --ids (agent ≥ 2.0.11): cirúrgico e NÃO avança watermark.
#
# No posto (recomendado):
#   torqmind-agent.exe backfill --dataset itenscomprovantes --ids 3694867,3694873 --config config.enc
#
# Equivalente via wrapper:
#   ./deploy/scripts/backfill-itenscomprovantes-by-ids.sh 3694867,3694873
#
# Fallback por janela de datas (avança só se for backfill com --from/--to;
# watermark NÃO é commitado em janela manual — ver AgentRunner.commit_cursor):
#   torqmind-agent.exe backfill --dataset itenscomprovantes --from 2026-08-20 --to 2026-08-21
#
# Depois do backfill: conferir no CH
#   SELECT id_comprovante, count() FROM torqmind_current.stg_itenscomprovantes_slim
#   WHERE id_empresa=1 AND id_comprovante IN (3694867,3694873) GROUP BY id_comprovante
#
# Mitigação contínua (agent 2.0.11): itensmovprodutos revisita 14d pelo DATA do
# header + overlap 6h parent/filho (2.0.10) para reduzir órfãos novos de estoque.
set -euo pipefail

IDS="${1:-${IDS:-}}"
if [[ -n "${IDS}" ]]; then
  exec "$(dirname "$0")/backfill-itenscomprovantes-by-ids.sh" "${IDS}"
fi

echo "Use o agent no posto (não este host):"
echo "  torqmind-agent.exe backfill --dataset itenscomprovantes --ids 3694867,3694873 --config config.enc"
echo "  (ou --from/--to se a data do comprovante for conhecida)"
echo "Agent 2.0.11: --ids + revisit 14d itensmovprodutos; 2.0.10: overlap 6h anti-órfão."
