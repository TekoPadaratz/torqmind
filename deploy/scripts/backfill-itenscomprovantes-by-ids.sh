#!/usr/bin/env bash
# backfill-itenscomprovantes-by-ids.sh
#
# Backfill cirúrgico de ITENSCOMPROVANTE no posto (agent Windows), para
# comprovantes com header em STG mas sem itens (órfãos).
#
# NÃO roda nada destrutivo em produção daqui. Preferir executar no posto:
#
#   torqmind-agent.exe backfill --dataset itenscomprovantes --ids 3694867,3694873 --config config.enc
#
# Equivalente Python (repo):
#   python -m agent backfill --dataset itenscomprovantes --ids 3694867,3694873 --config /path/config.enc
#
# Notas de segurança:
# - --ids NÃO avança watermark (cursor restaurado após o envio).
# - Não usa --from/--to; só os IDs listados.
# - Para movprodutos/itensmovprodutos use a mesma flag com --id-column ID_MOVPRODUTOS
#   (default automático nesses datasets).
# - Depois do backfill: confira stg.itenscomprovantes no PG e, se CDC ativo,
#   aguarde slim/mart. Não truncate/delete.
#
# Exemplos (emp 1 — comprovantes 3694867 / 3694873):
#   ./deploy/scripts/backfill-itenscomprovantes-by-ids.sh 3694867,3694873
#   IDS=3694867,3694873 CONFIG=/path/config.enc ./deploy/scripts/backfill-itenscomprovantes-by-ids.sh
#
set -euo pipefail

IDS="${1:-${IDS:-}}"
CONFIG="${CONFIG:-config.enc}"
DATASET="${DATASET:-itenscomprovantes}"
ID_COLUMN="${ID_COLUMN:-}"

if [[ -z "${IDS}" ]]; then
  echo "Uso: $0 <id1,id2,...>" >&2
  echo "  ou IDS=3694867,3694873 CONFIG=config.enc $0" >&2
  echo "" >&2
  echo "No posto (recomendado):" >&2
  echo "  torqmind-agent.exe backfill --dataset itenscomprovantes --ids 3694867,3694873 --config config.enc" >&2
  exit 2
fi

CMD=(python -m agent backfill --dataset "${DATASET}" --ids "${IDS}" --config "${CONFIG}")
if [[ -n "${ID_COLUMN}" ]]; then
  CMD+=(--id-column "${ID_COLUMN}")
fi

echo "== dry command =="
printf ' %q' "${CMD[@]}"
echo
echo
echo "Executando backfill cirúrgico (não destrutivo; watermark preservado)..."
exec "${CMD[@]}"
