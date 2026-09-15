#!/usr/bin/env bash
# list-commission-orphan-comprovantes.sh
#
# Lista comprovantes comerciais com header no ClickHouse slim e ZERO itens
# (órfãos). Uso típico antes do backfill no posto:
#
#   ENV_FILE=/etc/torqmind/prod.app.env \
#     ID_EMPRESA=1 ID_FILIAL=11621 \
#     DT_INI=2026-08-20 DT_FIM=2026-09-13 \
#     ./deploy/scripts/list-commission-orphan-comprovantes.sh
#
# Saída: CSV de id_comprovante + comando sugerido de backfill do Agent.
#
# Não altera dados. Não avança watermark.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
ID_EMPRESA="${ID_EMPRESA:?ID_EMPRESA obrigatório}"
ID_FILIAL="${ID_FILIAL:?ID_FILIAL obrigatório}"
DT_INI="${DT_INI:?DT_INI YYYY-MM-DD}"
DT_FIM="${DT_FIM:?DT_FIM YYYY-MM-DD}"
API_CONTAINER="${API_CONTAINER:-torqmind-api}"
MAX_VALOR="${MAX_VALOR:-5000}"

cat > /tmp/tm_list_orphans.py <<'PY'
import os
from datetime import date
from app.db_clickhouse import query_dict
from app.commission_period import data_key_bounds_half_open

id_empresa = int(os.environ["ID_EMPRESA"])
id_filial = int(os.environ["ID_FILIAL"])
dt_ini = date.fromisoformat(os.environ["DT_INI"])
dt_fim = date.fromisoformat(os.environ["DT_FIM"])
max_valor = float(os.environ.get("MAX_VALOR", "5000"))
dk_ini, dk_fim = data_key_bounds_half_open(dt_ini, dt_fim)

rows = query_dict(
    f"""
    SELECT
      c.id_comprovante AS id_comprovante,
      c.data_key AS data_key,
      c.id_usuario AS id_usuario,
      round(c.valor_total, 2) AS valor_total
    FROM torqmind_current.stg_comprovantes_slim AS c FINAL
    LEFT JOIN (
      SELECT id_empresa, id_filial, id_db, id_comprovante, count() AS n_items
      FROM torqmind_current.stg_itenscomprovantes_slim FINAL
      WHERE id_empresa = {{e:Int32}} AND id_filial = {{f:Int32}} AND is_deleted = 0
      GROUP BY id_empresa, id_filial, id_db, id_comprovante
    ) AS it
      ON it.id_empresa = c.id_empresa AND it.id_filial = c.id_filial
     AND it.id_db = c.id_db AND it.id_comprovante = c.id_comprovante
    WHERE c.id_empresa = {{e:Int32}} AND c.id_filial = {{f:Int32}}
      AND c.data_key >= {{a:Int32}} AND c.data_key < {{b:Int32}}
      AND c.is_deleted = 0 AND c.cancelado = 0
      AND c.situacao NOT IN (2, 3, 14)
      AND coalesce(c.commercial_eligible, 1) = 1
      AND (it.n_items = 0 OR isNull(it.n_items))
      AND c.valor_total < {{m:Float64}}
    ORDER BY c.data_key, c.id_comprovante
    """,
    parameters={
        "e": id_empresa,
        "f": id_filial,
        "a": dk_ini,
        "b": dk_fim,
        "m": max_valor,
    },
)
ids = [str(int(r["id_comprovante"])) for r in rows]
print(f"orphan_headers={len(ids)} period={dt_ini.isoformat()}..{dt_fim.isoformat()} filial={id_filial}")
print("id_comprovante,data_key,id_usuario,valor_total")
for r in rows:
    print(
        f"{int(r['id_comprovante'])},{int(r['data_key'])},{int(r['id_usuario'] or 0)},"
        f"{float(r['valor_total'])}"
    )
if ids:
    print("")
    print("# No posto (Agent Windows):")
    # Chunk to keep command line reasonable
    chunk = 80
    for i in range(0, len(ids), chunk):
        part = ",".join(ids[i : i + chunk])
        print(
            f"torqmind-agent.exe backfill --dataset itenscomprovantes --ids {part} --config config.enc"
        )
PY

docker cp /tmp/tm_list_orphans.py "${API_CONTAINER}:/tmp/tm_list_orphans.py"
docker exec \
  -e "ID_EMPRESA=${ID_EMPRESA}" \
  -e "ID_FILIAL=${ID_FILIAL}" \
  -e "DT_INI=${DT_INI}" \
  -e "DT_FIM=${DT_FIM}" \
  -e "MAX_VALOR=${MAX_VALOR}" \
  "${API_CONTAINER}" python /tmp/tm_list_orphans.py
