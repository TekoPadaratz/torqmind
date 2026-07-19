#!/usr/bin/env bash
# deploy/scripts/prod-profit-marts-refresh.sh
# Refreshes the profit management ClickHouse marts from PostgreSQL DW.
# Usage: ENV_FILE=/etc/torqmind/prod.app.env ./deploy/scripts/prod-profit-marts-refresh.sh [--full]
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

CH_HOST="${CLICKHOUSE_HOST:-localhost}"
CH_PORT="${CLICKHOUSE_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASS="${CLICKHOUSE_PASSWORD:-}"
PG_H="${CLICKHOUSE_PG_HOST:-${PG_HOST:-172.30.0.8}}"
PG_P="${CLICKHOUSE_PG_PORT:-${PG_PORT:-5432}}"
PG_DB="${PG_DATABASE:-${POSTGRES_DB:-torqmind}}"
PG_U="${PG_USER:-${POSTGRES_USER:-torqmind}}"
PG_W="${PG_PASSWORD:-${POSTGRES_PASSWORD:-}}"

# Fail fast with a clear message instead of letting psql/clickhouse block on a
# tty password prompt (root cause of past "hangs" when credentials were empty).
# This script needs BOTH Postgres creds (reads dw on 172.30.0.8) and ClickHouse
# creds (writes marts on 172.30.0.9). Run it where both are available (analytics
# VM with CLICKHOUSE_PASSWORD, providing POSTGRES_PASSWORD) — not with a single
# env that only has one of them.
if [[ -z "$PG_W" ]]; then
  echo "ERRO: senha do Postgres ausente. Defina POSTGRES_PASSWORD (ou PG_PASSWORD) no ENV_FILE=$ENV_FILE." >&2
  exit 1
fi
if [[ -z "$CH_PASS" ]]; then
  echo "ERRO: senha do ClickHouse ausente. Defina CLICKHOUSE_PASSWORD no ENV_FILE=$ENV_FILE (fica em prod.analytics.env)." >&2
  exit 1
fi

MODE="${1:-incremental}"
ID_EMPRESA="${ID_EMPRESA:-1}"

echo "=== Profit Marts Refresh (mode=$MODE, empresa=$ID_EMPRESA) ==="
echo "ClickHouse: $CH_HOST:$CH_PORT"
echo "PostgreSQL: $PG_H:$PG_P/$PG_DB"

ch() {
  clickhouse-client --host="$CH_HOST" --port="${CLICKHOUSE_NATIVE_PORT:-9000}" \
    --user="$CH_USER" --password="$CH_PASS" --query="$1"
}

ch_http() {
  curl -sS "http://${CH_HOST}:${CH_PORT}/?user=${CH_USER}&password=${CH_PASS}" \
    --data-binary "$1"
}

# pg_to_ch: Run PG query, pipe TSV into ClickHouse INSERT
# Usage: pg_to_ch "INSERT INTO target FORMAT TabSeparated" "SELECT ... FROM dw.table ..."
pg_to_ch() {
  local ch_insert="$1"
  local pg_query="$2"
  local encoded_insert
  encoded_insert=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$ch_insert")
  PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" \
    --no-align -t -F $'\t' -c "$pg_query" \
    | sed '/^$/d' \
    | curl -sS "http://${CH_HOST}:${CH_PORT}/?user=${CH_USER}&password=${CH_PASS}&query=${encoded_insert}" \
        --data-binary @-
}

# ─── Step 1: Determine reference months ───────────────────────────────
echo ""
echo "--- Step 1: Determining reference months ---"

# Get available months from PostgreSQL fact_despesa_operacional
MONTHS=$(PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" \
  --no-align -t -c "
  SELECT DISTINCT ano_mes_competencia
  FROM dw.fact_despesa_operacional
  WHERE id_empresa = $ID_EMPRESA AND ano_mes_competencia >= 202401
  ORDER BY ano_mes_competencia DESC
  LIMIT 12
")

if [[ -z "$MONTHS" ]]; then
  echo "WARN: No expense data found. Will refresh sales-only marts."
fi

echo "Months with expense data: $(echo $MONTHS | tr '\n' ' ')"

# ─── Step 2: Refresh profit_dre_mensal ────────────────────────────────
echo ""
echo "--- Step 2: Refreshing profit_dre_mensal ---"

if [[ "$MODE" == "--full" ]] || [[ "$MODE" == "full" ]]; then
  ch_http "ALTER TABLE torqmind_mart_rt.profit_dre_mensal DELETE WHERE id_empresa = $ID_EMPRESA" || true
  ch_http "ALTER TABLE torqmind_mart_rt.profit_despesas_mensal DELETE WHERE id_empresa = $ID_EMPRESA" || true
  ch_http "ALTER TABLE torqmind_mart_rt.profit_produto_mensal DELETE WHERE id_empresa = $ID_EMPRESA" || true
  ch_http "ALTER TABLE torqmind_mart_rt.profit_resumo_filial DELETE WHERE id_empresa = $ID_EMPRESA" || true
  echo "Full mode: cleared existing data for empresa $ID_EMPRESA"
fi

# Insert DRE from PostgreSQL (pipe TSV via pg_to_ch)
pg_to_ch "INSERT INTO torqmind_mart_rt.profit_dre_mensal FORMAT TabSeparated" "
SELECT
    v.id_empresa,
    v.id_filial,
    v.ano_mes,
    v.receita_bruta_total,
    v.receita_conveniencia,
    v.receita_pista,
    v.receita_automotivo,
    v.receita_cigarro,
    v.receita_servico,
    0 AS impostos_sobre_vendas,
    v.receita_bruta_total AS receita_liquida_gerencial,
    v.cmv_total,
    v.cmv_conveniencia,
    v.cmv_pista,
    v.receita_bruta_total - v.cmv_total AS margem_bruta,
    CASE WHEN v.receita_bruta_total > 0
         THEN (v.receita_bruta_total - v.cmv_total) / v.receita_bruta_total
         ELSE 0 END AS margem_bruta_pct,
    COALESCE(d.desp_pessoal, 0),
    COALESCE(d.desp_comercial, 0),
    COALESCE(d.desp_administrativa, 0),
    COALESCE(d.desp_financeira, 0),
    COALESCE(d.desp_tributaria_operacional, 0),
    COALESCE(d.desp_excepcional, 0),
    COALESCE(d.desp_rateavel, 0),
    COALESCE(d.desp_total, 0),
    v.receita_bruta_total - v.cmv_total - COALESCE(d.desp_total, 0) AS resultado_operacional,
    v.receita_bruta_total - v.cmv_total - COALESCE(d.desp_total, 0) AS lucro_gerencial_estimado,
    CASE WHEN v.receita_bruta_total > 0
         THEN (v.receita_bruta_total - v.cmv_total - COALESCE(d.desp_total, 0)) / v.receita_bruta_total
         ELSE 0 END AS lucro_gerencial_pct,
    COALESCE(d.qtd_lancamentos, 0),
    v.qtd_produtos,
    now()::timestamp(0)
FROM (
  SELECT
    fvi.id_empresa,
    fvi.id_filial,
    (fvi.data_key / 100) AS ano_mes,
    SUM(fvi.total)::numeric(18,2) AS receita_bruta_total,
    SUM(CASE WHEN dp.id_grupo_produto IN (14,15,18,12,13,11,17,21,37,40,41,19,20) THEN fvi.total ELSE 0 END)::numeric(18,2) AS receita_conveniencia,
    SUM(CASE WHEN dp.id_grupo_produto = 1 THEN fvi.total ELSE 0 END)::numeric(18,2) AS receita_pista,
    SUM(CASE WHEN dp.id_grupo_produto IN (2,4,7,8,9,16,39) THEN fvi.total ELSE 0 END)::numeric(18,2) AS receita_automotivo,
    SUM(CASE WHEN dp.id_grupo_produto = 10 THEN fvi.total ELSE 0 END)::numeric(18,2) AS receita_cigarro,
    SUM(CASE WHEN dp.id_grupo_produto IN (5,35) THEN fvi.total ELSE 0 END)::numeric(18,2) AS receita_servico,
    SUM(fvi.custo_total)::numeric(18,2) AS cmv_total,
    SUM(CASE WHEN dp.id_grupo_produto IN (14,15,18,12,13,11,17,21,37,40,41,19,20) THEN fvi.custo_total ELSE 0 END)::numeric(18,2) AS cmv_conveniencia,
    SUM(CASE WHEN dp.id_grupo_produto = 1 THEN fvi.custo_total ELSE 0 END)::numeric(18,2) AS cmv_pista,
    COUNT(DISTINCT fvi.id_produto)::int AS qtd_produtos
  FROM dw.fact_venda_item fvi
  LEFT JOIN dw.dim_produto dp ON dp.id_empresa = fvi.id_empresa AND dp.id_filial = fvi.id_filial AND dp.id_produto = fvi.id_produto
  WHERE fvi.id_empresa = ${ID_EMPRESA}
    AND fvi.data_key >= 20240101
    AND fvi.qtd > 0
  GROUP BY fvi.id_empresa, fvi.id_filial, (fvi.data_key / 100)
) v
LEFT JOIN (
  SELECT
    id_empresa, id_filial, ano_mes_competencia AS ano_mes,
    SUM(CASE WHEN classificacao_gerencial = 'pessoal' THEN valor ELSE 0 END)::numeric(18,2) AS desp_pessoal,
    SUM(CASE WHEN classificacao_gerencial = 'comercial' THEN valor ELSE 0 END)::numeric(18,2) AS desp_comercial,
    SUM(CASE WHEN classificacao_gerencial = 'administrativo' THEN valor ELSE 0 END)::numeric(18,2) AS desp_administrativa,
    SUM(CASE WHEN classificacao_gerencial = 'financeiro' THEN valor ELSE 0 END)::numeric(18,2) AS desp_financeira,
    SUM(CASE WHEN is_tributo_operacional THEN valor ELSE 0 END)::numeric(18,2) AS desp_tributaria_operacional,
    SUM(CASE WHEN is_excepcional THEN valor ELSE 0 END)::numeric(18,2) AS desp_excepcional,
    SUM(CASE WHEN entra_rateio_produto THEN valor ELSE 0 END)::numeric(18,2) AS desp_rateavel,
    SUM(valor)::numeric(18,2) AS desp_total,
    COUNT(*)::int AS qtd_lancamentos
  FROM dw.fact_despesa_operacional
  WHERE id_empresa = ${ID_EMPRESA} AND ano_mes_competencia >= 202401
  GROUP BY id_empresa, id_filial, ano_mes_competencia
) d ON d.id_empresa = v.id_empresa AND d.id_filial = v.id_filial AND d.ano_mes = v.ano_mes
"

DRE_COUNT=$(ch_http "SELECT count() FROM torqmind_mart_rt.profit_dre_mensal WHERE id_empresa = $ID_EMPRESA")
echo "profit_dre_mensal rows: $DRE_COUNT"

# ─── Step 3: Refresh profit_despesas_mensal ───────────────────────────
echo ""
echo "--- Step 3: Refreshing profit_despesas_mensal ---"

pg_to_ch "INSERT INTO torqmind_mart_rt.profit_despesas_mensal FORMAT TabSeparated" "
SELECT
    id_empresa,
    id_filial,
    ano_mes_competencia AS ano_mes,
    classificacao_gerencial,
    codigo_plano,
    nome_plano,
    centro_custo_gerencial,
    SUM(valor)::numeric(18,2) AS valor_total,
    COUNT(*)::int AS qtd_lancamentos,
    SUM(CASE WHEN tipo_conta = 0 THEN valor ELSE 0 END)::numeric(18,2) AS valor_tipo_0,
    SUM(CASE WHEN tipo_conta = 1 THEN valor ELSE 0 END)::numeric(18,2) AS valor_tipo_1,
    SUM(CASE WHEN entra_rateio_produto THEN valor ELSE 0 END)::numeric(18,2) AS valor_rateavel,
    SUM(CASE WHEN NOT entra_rateio_produto THEN valor ELSE 0 END)::numeric(18,2) AS valor_nao_rateavel,
    0 AS percentual_sobre_receita,
    0 AS percentual_sobre_despesa_total,
    max(CASE WHEN entra_rateio_produto THEN 1 ELSE 0 END)::int AS entra_rateio_produto,
    max(CASE WHEN is_excepcional THEN 1 ELSE 0 END)::int AS is_excepcional,
    max(CASE WHEN is_financeiro THEN 1 ELSE 0 END)::int AS is_financeiro,
    now()::timestamp(0)
FROM dw.fact_despesa_operacional
WHERE id_empresa = $ID_EMPRESA AND ano_mes_competencia >= 202401
GROUP BY id_empresa, id_filial, ano_mes_competencia, classificacao_gerencial, codigo_plano, nome_plano, centro_custo_gerencial
"

DESP_COUNT=$(ch_http "SELECT count() FROM torqmind_mart_rt.profit_despesas_mensal WHERE id_empresa = $ID_EMPRESA")
echo "profit_despesas_mensal rows: $DESP_COUNT"

# ─── Step 4: Refresh profit_produto_mensal ────────────────────────────
echo ""
echo "--- Step 4: Refreshing profit_produto_mensal ---"

pg_to_ch "INSERT INTO torqmind_mart_rt.profit_produto_mensal FORMAT TabSeparated" "
WITH desp_filial AS (
  SELECT id_empresa, id_filial, ano_mes_competencia AS ano_mes, SUM(valor)::numeric(18,2) AS desp_rateavel
  FROM dw.fact_despesa_operacional
  WHERE id_empresa = ${ID_EMPRESA} AND entra_rateio_produto = true AND ano_mes_competencia >= 202401
  GROUP BY id_empresa, id_filial, ano_mes_competencia
),
base AS (
  SELECT
    fvi.id_empresa,
    fvi.id_filial,
    (fvi.data_key / 100) AS ano_mes,
    fvi.id_produto,
    COALESCE(dp.nome, 'Produto '||fvi.id_produto) AS nome_produto,
    COALESCE(dp.id_grupo_produto, 0) AS id_grupo_produto,
    COALESCE(gp.nome, 'Grupo '||COALESCE(dp.id_grupo_produto,0)) AS nome_grupo_produto,
    COALESCE(
      CASE
        WHEN dp.id_grupo_produto = 1 THEN 'combustivel'
        WHEN dp.id_grupo_produto IN (2,4,7,8,9,16,39) THEN 'automotivo'
        WHEN dp.id_grupo_produto = 10 THEN 'cigarro'
        WHEN dp.id_grupo_produto IN (5,35) THEN 'servico'
        WHEN dp.id_grupo_produto IN (6,28,32,38,42) THEN 'interno'
        WHEN dp.id_grupo_produto IN (14,15,18,12,13,11,17,21,37,40,41,19,20) THEN 'conveniencia'
        ELSE 'outros'
      END, 'outros'
    ) AS setor_gerencial,
    SUM(fvi.qtd)::numeric(18,3) AS qtd_vendida,
    SUM(fvi.total)::numeric(18,2) AS receita,
    COALESCE((ARRAY_AGG((fvi.total / fvi.qtd)::numeric(18,4) ORDER BY fvi.data_key DESC, fvi.id_comprovante DESC))[1], 0) AS preco_medio,
    COALESCE((ARRAY_AGG(CASE WHEN fvi.custo_total > 0 THEN (fvi.custo_total / fvi.qtd)::numeric(18,4) END ORDER BY fvi.data_key DESC, fvi.id_comprovante DESC))[1], 0) AS custo_medio,
    SUM(fvi.custo_total)::numeric(18,2) AS cmv,
    (SUM(fvi.total) - SUM(fvi.custo_total))::numeric(18,2) AS margem_bruta,
    CASE WHEN SUM(fvi.total) > 0 THEN ((SUM(fvi.total) - SUM(fvi.custo_total)) / SUM(fvi.total))::numeric(8,4) ELSE 0 END AS margem_bruta_pct,
    SUM(SUM(fvi.total)) OVER (PARTITION BY fvi.id_empresa, fvi.id_filial, (fvi.data_key / 100),
      CASE
        WHEN dp.id_grupo_produto = 1 THEN 'combustivel'
        WHEN dp.id_grupo_produto IN (2,4,7,8,9,16,39) THEN 'automotivo'
        WHEN dp.id_grupo_produto = 10 THEN 'cigarro'
        WHEN dp.id_grupo_produto IN (5,35) THEN 'servico'
        WHEN dp.id_grupo_produto IN (6,28,32,38,42) THEN 'interno'
        WHEN dp.id_grupo_produto IN (14,15,18,12,13,11,17,21,37,40,41,19,20) THEN 'conveniencia'
        ELSE 'outros'
      END)::numeric(18,2) AS receita_setor,
    SUM(SUM(fvi.total)) OVER (PARTITION BY fvi.id_empresa, fvi.id_filial, (fvi.data_key / 100))::numeric(18,2) AS receita_total_filial,
    COALESCE(df.desp_rateavel, 0) AS desp_rateavel_filial
  FROM dw.fact_venda_item fvi
  LEFT JOIN dw.dim_produto dp ON dp.id_empresa = fvi.id_empresa AND dp.id_filial = fvi.id_filial AND dp.id_produto = fvi.id_produto
  LEFT JOIN dw.dim_grupo_produto gp ON gp.id_empresa = fvi.id_empresa AND gp.id_grupo_produto = dp.id_grupo_produto
  LEFT JOIN desp_filial df ON df.id_empresa = fvi.id_empresa AND df.id_filial = fvi.id_filial AND df.ano_mes = (fvi.data_key / 100)
  WHERE fvi.id_empresa = ${ID_EMPRESA}
    AND fvi.data_key >= 20240101
    AND fvi.qtd > 0
    AND COALESCE(
      CASE WHEN dp.id_grupo_produto IN (6,28,32,38,42) THEN 'interno' ELSE 'ok' END, 'ok') != 'interno'
  GROUP BY fvi.id_empresa, fvi.id_filial, (fvi.data_key / 100), fvi.id_produto, dp.nome, dp.id_grupo_produto, gp.nome, df.desp_rateavel,
    CASE
      WHEN dp.id_grupo_produto = 1 THEN 'combustivel'
      WHEN dp.id_grupo_produto IN (2,4,7,8,9,16,39) THEN 'automotivo'
      WHEN dp.id_grupo_produto = 10 THEN 'cigarro'
      WHEN dp.id_grupo_produto IN (5,35) THEN 'servico'
      WHEN dp.id_grupo_produto IN (6,28,32,38,42) THEN 'interno'
      WHEN dp.id_grupo_produto IN (14,15,18,12,13,11,17,21,37,40,41,19,20) THEN 'conveniencia'
      ELSE 'outros'
    END
)
SELECT
    p.id_empresa,
    p.id_filial,
    p.ano_mes,
    p.id_produto,
    p.nome_produto,
    p.id_grupo_produto,
    p.nome_grupo_produto,
    p.setor_gerencial,
    CASE WHEN p.setor_gerencial IN ('conveniencia','automotivo','cigarro') THEN 1 ELSE 0 END AS entra_simulador_reajuste,
    p.qtd_vendida,
    p.receita,
    p.preco_medio,
    p.custo_medio,
    p.cmv,
    p.margem_bruta,
    p.margem_bruta_pct,
    CASE WHEN p.receita_setor > 0 THEN (p.receita / p.receita_setor)::numeric(8,4) ELSE 0 END AS participacao_receita_setor,
    CASE WHEN p.receita_total_filial > 0
         THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial))::numeric(18,2)
         ELSE 0 END AS desp_operacional_rateada,
    CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0
         THEN ((p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida)::numeric(18,4)
         ELSE 0 END AS desp_operacional_unitaria,
    (p.receita - p.cmv - (CASE WHEN p.receita_total_filial > 0 THEN p.desp_rateavel_filial * (p.receita / p.receita_total_filial) ELSE 0 END))::numeric(18,2) AS lucro_gerencial_estimado,
    CASE WHEN p.receita > 0
         THEN ((p.receita - p.cmv - (CASE WHEN p.receita_total_filial > 0 THEN p.desp_rateavel_filial * (p.receita / p.receita_total_filial) ELSE 0 END)) / p.receita)::numeric(8,4)
         ELSE 0 END AS margem_gerencial_pct,
    CASE WHEN (p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END)) > 0
         THEN (p.preco_medio / (p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END)))::numeric(8,4)
         ELSE 0 END AS markup_real,
    (p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))::numeric(18,4) AS preco_minimo_saudavel,
    ((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
        / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30
                     WHEN p.setor_gerencial = 'automotivo' THEN 0.30
                     WHEN p.setor_gerencial = 'cigarro' THEN 0.12
                     WHEN p.setor_gerencial = 'combustivel' THEN 0.08
                     ELSE 0.25 END))::numeric(18,4) AS preco_ideal_sugerido,
    greatest(0,
        ((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
        / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30
                     WHEN p.setor_gerencial = 'automotivo' THEN 0.30
                     WHEN p.setor_gerencial = 'cigarro' THEN 0.12
                     WHEN p.setor_gerencial = 'combustivel' THEN 0.08
                     ELSE 0.25 END))
        - p.preco_medio
    )::numeric(18,4) AS reajuste_sugerido_valor,
    CASE WHEN p.preco_medio > 0 THEN greatest(0,
        (((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
        / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30
                     WHEN p.setor_gerencial = 'automotivo' THEN 0.30
                     WHEN p.setor_gerencial = 'cigarro' THEN 0.12
                     WHEN p.setor_gerencial = 'combustivel' THEN 0.08
                     ELSE 0.25 END))
        - p.preco_medio) / p.preco_medio
    )::numeric(8,4) ELSE 0 END AS reajuste_sugerido_pct,
    p.qtd_vendida AS qtd_mes_anterior,
    (greatest(0,
        ((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
        / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30
                     WHEN p.setor_gerencial = 'automotivo' THEN 0.30
                     WHEN p.setor_gerencial = 'cigarro' THEN 0.12
                     WHEN p.setor_gerencial = 'combustivel' THEN 0.08
                     ELSE 0.25 END))
        - p.preco_medio
    ) * p.qtd_vendida * 2)::numeric(18,2) AS impacto_estimado_60d,
    CASE
        WHEN p.custo_medio = 0 THEN 'sem_custo'
        WHEN p.preco_medio < (p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
            THEN 'abaixo_minimo'
        WHEN p.preco_medio < ((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
            / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30
                         WHEN p.setor_gerencial = 'automotivo' THEN 0.30
                         WHEN p.setor_gerencial = 'cigarro' THEN 0.12
                         WHEN p.setor_gerencial = 'combustivel' THEN 0.08
                         ELSE 0.25 END))
            THEN 'abaixo_ideal'
        ELSE 'saudavel'
    END AS status_preco,
    CASE
        WHEN p.custo_medio = 0 THEN 'Custo indisponivel'
        WHEN p.preco_medio < (p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
            THEN 'Preco abaixo do custo operacional'
        WHEN p.preco_medio < ((p.custo_medio + (CASE WHEN p.qtd_vendida > 0 AND p.receita_total_filial > 0 THEN (p.desp_rateavel_filial * (p.receita / p.receita_total_filial)) / p.qtd_vendida ELSE 0 END))
            / (1 - CASE WHEN p.setor_gerencial = 'conveniencia' THEN 0.30 WHEN p.setor_gerencial = 'automotivo' THEN 0.30 WHEN p.setor_gerencial = 'cigarro' THEN 0.12 WHEN p.setor_gerencial = 'combustivel' THEN 0.08 ELSE 0.25 END))
            THEN 'Reajuste sugerido para atingir margem ideal'
        ELSE 'Preco saudavel'
    END AS recomendacao_curta,
    now()::timestamp(0)
FROM base p
"

PROD_COUNT=$(ch_http "SELECT count() FROM torqmind_mart_rt.profit_produto_mensal WHERE id_empresa = $ID_EMPRESA")
echo "profit_produto_mensal rows: $PROD_COUNT"

# ─── Step 5: Refresh profit_resumo_filial ─────────────────────────────
echo ""
echo "--- Step 5: Refreshing profit_resumo_filial ---"

ch_http "
INSERT INTO torqmind_mart_rt.profit_resumo_filial
SELECT
    id_empresa,
    id_filial,
    ano_mes AS ano_mes_referencia,
    receita_bruta_total AS receita_total,
    lucro_gerencial_estimado,
    lucro_gerencial_pct AS margem_gerencial_pct,
    desp_operacional_total,
    CASE WHEN receita_bruta_total > 0 THEN desp_operacional_total / receita_bruta_total ELSE 0 END AS desp_sobre_receita_pct,
    qtd_produtos_vendidos AS produtos_analisados,
    0 AS produtos_abaixo_minimo,
    0 AS produtos_com_reajuste,
    0 AS impacto_positivo_60d,
    '' AS setor_mais_lucrativo,
    now()
FROM torqmind_mart_rt.profit_dre_mensal FINAL
WHERE id_empresa = $ID_EMPRESA
"

# Update resumo with product-level stats
ch_http "
INSERT INTO torqmind_mart_rt.profit_resumo_filial
SELECT
    id_empresa,
    id_filial,
    ano_mes AS ano_mes_referencia,
    0 AS receita_total,
    0 AS lucro_gerencial_estimado,
    0 AS margem_gerencial_pct,
    0 AS desp_operacional_total,
    0 AS desp_sobre_receita_pct,
    count() AS produtos_analisados,
    countIf(status_preco = 'abaixo_minimo') AS produtos_abaixo_minimo,
    countIf(status_preco IN ('abaixo_minimo', 'abaixo_ideal')) AS produtos_com_reajuste,
    sumIf(impacto_estimado_60d, impacto_estimado_60d > 0) AS impacto_positivo_60d,
    '' AS setor_mais_lucrativo,
    now()
FROM torqmind_mart_rt.profit_produto_mensal FINAL
WHERE id_empresa = $ID_EMPRESA
GROUP BY id_empresa, id_filial, ano_mes
"

RESUMO_COUNT=$(ch_http "SELECT count() FROM torqmind_mart_rt.profit_resumo_filial WHERE id_empresa = $ID_EMPRESA")
echo "profit_resumo_filial rows: $RESUMO_COUNT"

# ─── Step 6: Refresh mart de Solvencia (PG-only: passivo + estoque) ────
# Aba "Solvencia" do DRE. Passivo = contas a pagar em aberto (descontando
# baixas parciais). Estoque = sensor de tanque (combustivel) + loja curada.
echo ""
echo "--- Step 6: Refreshing mart.liquidez_solvencia (passivo + estoque) ---"
PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" -v ON_ERROR_STOP=1 -c "
  SELECT etl.refresh_liquidez_solvencia(${ID_EMPRESA});
  SELECT etl.refresh_liquidez_estoque(${ID_EMPRESA});
  SELECT etl.refresh_liquidez_banco(${ID_EMPRESA});
" >/dev/null
LIQ_COUNT=$(PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" --no-align -t -c "SELECT count(*) FROM mart.liquidez_solvencia WHERE id_empresa = ${ID_EMPRESA}")
echo "liquidez_solvencia rows: $LIQ_COUNT"

# ─── Step 7: Refresh marts de Gestao Orcamentaria (PG-only) ────
# Tela "Gestao Orcamentaria": despesa realizada por conta/mes + catalogo de
# contas gerenciais, a partir de dw.fact_despesa_operacional (competencia).
echo ""
echo "--- Step 7: Refreshing mart.despesa_conta_mensal + plano_contas_gerencial ---"
PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" -v ON_ERROR_STOP=1 -c "
  SELECT etl.refresh_gestao_orcamentaria(${ID_EMPRESA});
" >/dev/null
ORC_COUNT=$(PGPASSWORD="$PG_W" psql -h "$PG_H" -p "$PG_P" -U "$PG_U" -d "$PG_DB" --no-align -t -c "SELECT count(*) FROM mart.despesa_conta_mensal WHERE id_empresa = ${ID_EMPRESA}")
echo "despesa_conta_mensal rows: $ORC_COUNT"

echo ""
echo "=== Profit Marts Refresh COMPLETE ==="
echo "  DRE:       $DRE_COUNT rows"
echo "  Despesas:  $DESP_COUNT rows"
echo "  Produtos:  $PROD_COUNT rows"
echo "  Resumo:    $RESUMO_COUNT rows"
echo "  Solvencia: $LIQ_COUNT rows"
echo "  Orcamento: $ORC_COUNT rows"
