#!/usr/bin/env bash
# Cura CONTASPAGAR/BAIXA no STG a partir do Xpert e republica mart de títulos.
#
# Pré-requisito: rota ao SQL Server do posto (CENTRALVR / 172.30.0.12:1433).
# A App VM (172.30.0.10) normalmente NÃO alcança o Xpert — rode no posto/jump.
#
# Uso:
#   ID_FILIAL=14458 ./deploy/scripts/heal-and-publish-contaspagar.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ID_EMPRESA="${ID_EMPRESA:-1}"
ID_FILIAL="${ID_FILIAL:-14458}"
PAID_DAYS="${PAID_DAYS:-180}"
PG_DATABASE="${PG_DATABASE:-torqmind}"
SOURCE_ENV="${SOURCE_ENV:-/home/tm/torqmind/config/source-explorer.env}"
ENV_APP="${ENV_FILE:-/etc/torqmind/prod.app.env}"

cd "$ROOT"

set -a
# shellcheck disable=SC1090
source "$ENV_APP"
set +a

echo "=== 1) Heal Xpert → STG (filial=${ID_FILIAL}) ==="
.venv/bin/python tools/heal_contaspagar_from_xpert.py \
  --id-empresa "$ID_EMPRESA" \
  --pg-database "$PG_DATABASE" \
  --id-filial "$ID_FILIAL" \
  --paid-days "$PAID_DAYS" \
  --source-env "$SOURCE_ENV"

echo "=== 2) Publish mart_finance_titles_rt ==="
ENV_FILE="$ENV_APP" ID_EMPRESA="$ID_EMPRESA" DAYS="$PAID_DAYS" \
  ./deploy/scripts/publish-finance-titles.sh

echo "=== 3) Refresh finance_overview_rt ==="
if docker ps --format '{{.Names}}' | grep -qx torqmind-cdc-consumer; then
  docker exec torqmind-cdc-consumer python - <<'PY'
from torqmind_cdc_consumer.mart_builder import MartBuilder
mb = MartBuilder()
# Rebuild overview for all (small volume) — cures KPI after STG heal.
res = mb._refresh_finance_overview_stg(mb._client(), id_empresa=1, id_filial=None)
print({"ok": True, "result": str(res)})
PY
else
  echo "WARN: container torqmind-cdc-consumer ausente — overview sobe no próximo CDC."
fi

echo "=== 4) Prova STG vencido filial ${ID_FILIAL} ==="
.venv/bin/python - <<PY
from app.db import get_conn
id_empresa, id_filial = ${ID_EMPRESA}, ${ID_FILIAL}
with get_conn() as c:
    r = c.execute(
        """
        WITH bp AS (
          SELECT id_db, etl.safe_int(payload->>'ID_CONTASPAGAR') id_t,
                 sum(etl.safe_numeric(payload->>'VALORBAIXA')) vb
          FROM stg.contaspagarbaixa
          WHERE id_empresa=%s AND id_filial=%s
          GROUP BY 1,2
        )
        SELECT coalesce(sum(greatest(0,
          etl.safe_numeric(cp.payload->>'VALOR')
          - etl.safe_numeric(cp.payload->>'VLRPAGO')
          - coalesce(bp.vb,0)
        )),0) AS vencido
        FROM stg.contaspagar cp
        LEFT JOIN bp ON bp.id_db=cp.id_db AND bp.id_t=cp.id_contaspagar
        WHERE cp.id_empresa=%s AND cp.id_filial=%s
          AND NULLIF(trim(cp.payload->>'DTAPGTO'),'') IS NULL
          AND (etl.safe_timestamp(cp.payload->>'DTAVCTO'))::date
              < (now() AT TIME ZONE 'America/Sao_Paulo')::date
          AND greatest(0,
            etl.safe_numeric(cp.payload->>'VALOR')
            - etl.safe_numeric(cp.payload->>'VLRPAGO')
            - coalesce(bp.vb,0)
          ) > 0.01
        """,
        [id_empresa, id_filial, id_empresa, id_filial],
    ).fetchone()
    print({"stg_vencido": float(r["vencido"]), "alvo_xpert_vr01": 666712.62})
PY

echo "=== DONE ==="
