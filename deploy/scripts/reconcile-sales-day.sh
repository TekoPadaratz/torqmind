#!/usr/bin/env bash
# =============================================================================
# reconcile-sales-day.sh — Reconciliação completa de um dia de vendas
# Percorre: STG PG → ClickHouse Slim → Mart RT → API
# =============================================================================
set -uo pipefail

ID_EMPRESA="${ID_EMPRESA:-1}"
ID_FILIAL="${ID_FILIAL:?Usage: ID_FILIAL=14458 DATE=2026-05-11 $0}"
DATE="${DATE:?Usage: ID_FILIAL=14458 DATE=2026-05-11 $0}"
DATA_KEY="${DATE//-/}"

PG_HOST="${PG_HOST:-172.30.0.8}"
CH_HOST="${CH_HOST:-172.30.0.9}"
APP_HOST="${APP_HOST:-172.30.0.10}"
CH_USER="${CH_USER:-torqmind}"
CH_PASS="${CH_PASS:-ZO3G3srS8bCdwWnJjaCtMhlx2wHEWhxZp12ZaGlPI}"

echo "================================================================="
echo " Reconciliação: empresa=$ID_EMPRESA filial=$ID_FILIAL data=$DATE"
echo "================================================================="
echo ""

# ── Layer 1: STG PostgreSQL ──────────────────────────────────────────
PG_CONTAINER="${PG_CONTAINER:-torqmind-postgres}"
echo "── LAYER 1: STG PostgreSQL (${PG_HOST}) ──"
ssh "tm@${PG_HOST}" "docker exec ${PG_CONTAINER} psql -U torqmind -d torqmind -c \"
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE cancelado = 1) AS cancelados,
  COUNT(*) FILTER (WHERE situacao = 3) AS sit3,
  COALESCE(SUM(valortotal),0)::numeric(18,2) AS valor_total,
  COALESCE(SUM(valortotal) FILTER (WHERE cancelado=0 AND situacao<>3),0)::numeric(18,2) AS valor_elegivel
FROM stg.comprovantes
WHERE id_empresa=$ID_EMPRESA AND id_filial=$ID_FILIAL
  AND dt_evento AT TIME ZONE 'America/Sao_Paulo' >= '${DATE}'::date
  AND dt_evento AT TIME ZONE 'America/Sao_Paulo' < '${DATE}'::date + 1;
\"" 2>/dev/null || true
echo ""

# ── Layer 2: ClickHouse Slim ─────────────────────────────────────────
echo "── LAYER 2: ClickHouse Slim (${CH_HOST}) ──"
ssh "tm@${CH_HOST}" "docker exec torqmind-clickhouse clickhouse-client \
  --user $CH_USER --password $CH_PASS -q \"
SELECT
  count() AS total,
  countIf(cancelado=1) AS cancelados,
  countIf(ignored_business=1) AS sit3,
  countIf(commercial_eligible=1) AS elegiveis,
  sum(valor_total) AS valor_total,
  sumIf(valor_total, commercial_eligible=1) AS valor_elegivel
FROM torqmind_current.stg_comprovantes_slim
WHERE id_empresa=$ID_EMPRESA AND id_filial=$ID_FILIAL AND data_key=$DATA_KEY
FORMAT TabSeparatedWithNames
\"" 2>/dev/null | column -t -s$'\t' || true
echo ""

# ── Layer 2b: Faturamento Slim (soma itens cfop>5000) ────────────────
echo "── LAYER 2b: Faturamento via Itens Slim ──"
ssh "tm@${CH_HOST}" "docker exec torqmind-clickhouse clickhouse-client \
  --user $CH_USER --password $CH_PASS -q \"
SELECT
  sum(i.total) AS faturamento_itens,
  count() AS qtd_itens
FROM torqmind_current.stg_itenscomprovantes_slim i
INNER JOIN torqmind_current.stg_comprovantes_slim c
  ON c.id_empresa=i.id_empresa AND c.id_filial=i.id_filial
     AND c.id_comprovante=i.id_comprovante AND c.data_key=i.data_key
WHERE i.id_empresa=$ID_EMPRESA AND i.id_filial=$ID_FILIAL AND i.data_key=$DATA_KEY
  AND c.commercial_eligible=1 AND i.cfop > 5000
FORMAT TabSeparatedWithNames
\"" 2>/dev/null | column -t -s$'\t' || true
echo ""

# ── Layer 3: Mart RT ─────────────────────────────────────────────────
echo "── LAYER 3: Mart RT (${CH_HOST}) ──"
ssh "tm@${CH_HOST}" "docker exec torqmind-clickhouse clickhouse-client \
  --user $CH_USER --password $CH_PASS -q \"
SELECT 'sales_daily_rt' AS mart, faturamento, qtd_vendas, qtd_itens, published_at
FROM torqmind_mart_rt.sales_daily_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND id_filial=$ID_FILIAL AND data_key=$DATA_KEY
UNION ALL
SELECT 'dashboard_home_rt', faturamento, qtd_vendas, 0, published_at
FROM torqmind_mart_rt.dashboard_home_rt FINAL
WHERE id_empresa=$ID_EMPRESA AND id_filial=$ID_FILIAL AND data_key=$DATA_KEY
FORMAT TabSeparatedWithNames
\"" 2>/dev/null | column -t -s$'\t' || true
echo ""

# ── Layer 4: API ─────────────────────────────────────────────────────
echo "── LAYER 4: API (${APP_HOST}) ──"
ssh "tm@${APP_HOST}" "docker exec torqmind-api python3 -c \"
import urllib.request, json
data = json.dumps({'identifier':'teko94@gmail.com','password':'@Crmjr105'}).encode()
req = urllib.request.Request('http://localhost:8000/auth/login', data=data, headers={'Content-Type':'application/json'})
token = json.loads(urllib.request.urlopen(req, timeout=10).read())['access_token']
for ep, label in [('bi/sales/overview','SALES'), ('bi/dashboard/home','DASHBOARD'), ('bi/cash/overview','CASH')]:
    try:
        req2 = urllib.request.Request('http://localhost:8000/' + ep + '?id_empresa=$ID_EMPRESA&id_filial=$ID_FILIAL&dt_ini=$DATE&dt_fim=$DATE', headers={'Authorization': 'Bearer ' + token})
        r = json.loads(urllib.request.urlopen(req2, timeout=30).read())
        if 'commercial_kpis' in r:
            ck = r['commercial_kpis']
            print(label + ': faturamento=' + str(ck.get('saidas','N/A')) + ' qtd=' + str(ck.get('qtd_vendas','N/A')))
        elif 'kpis' in r:
            k = r['kpis']
            print(label + ': faturamento=' + str(k.get('faturamento','N/A')) + ' qtd=' + str(k.get('qtd_vendas','N/A')))
    except Exception as e:
        print(label + ': ERROR ' + str(e))
\"" 2>/dev/null || true
echo ""

echo "================================================================="
echo " Reconciliação completa"
echo "================================================================="
