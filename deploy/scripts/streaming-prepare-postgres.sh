#!/usr/bin/env bash
# TorqMind Streaming: Prepare PostgreSQL for Debezium CDC
# Creates required publication, heartbeat table and signal table.
# Usage: ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-prepare-postgres.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

# Source env
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

: "${POSTGRES_USER:=${PG_USER:-postgres}}"
: "${POSTGRES_DB:=${PG_DATABASE:-torqmind}}"

echo "=== TorqMind Streaming: Prepare PostgreSQL ==="
echo "  Database: $POSTGRES_DB"
echo ""

PG_CONTAINER=$(docker compose -f "$REPO_ROOT/$COMPOSE_FILE" --env-file "$ENV_FILE" ps -q postgres 2>/dev/null || true)

if [[ -z "$PG_CONTAINER" ]]; then
    echo "ERROR: PostgreSQL container not found"
    exit 1
fi

echo "Checking wal_level..."
WAL_LEVEL=$(docker exec "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SHOW wal_level;" 2>/dev/null || echo "unknown")
echo "  wal_level=$WAL_LEVEL"
if [[ "$WAL_LEVEL" != "logical" ]]; then
    echo "  ERROR: wal_level must be 'logical'. Update postgresql.conf and restart."
    exit 1
fi

echo ""
echo "Creating Debezium support objects..."

docker exec -i "$PG_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 <<'SQL'
-- Heartbeat table for Debezium
CREATE TABLE IF NOT EXISTS app.debezium_heartbeat (
    id integer PRIMARY KEY DEFAULT 1,
    ts timestamptz NOT NULL DEFAULT now()
);
INSERT INTO app.debezium_heartbeat (id, ts) VALUES (1, now())
ON CONFLICT (id) DO UPDATE SET ts = now();

-- Signal table for Debezium ad-hoc signals
CREATE TABLE IF NOT EXISTS app.debezium_signal (
    id varchar(42) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data varchar(2048) NULL
);

-- Create/update publication for CDC (idempotent).
-- STG tables are mandatory for the final realtime hot path. DW tables remain
-- published only for reconciliation and emergency rollback.
DO $$
DECLARE
    required_tables text[] := ARRAY[
        'stg.comprovantes',
        'stg.itenscomprovantes',
        'stg.formas_pgto_comprovantes',
        'stg.turnos',
        'stg.entidades',
        'stg.produtos',
        'stg.grupoprodutos',
        'stg.funcionarios',
        'stg.usuarios',
        'stg.localvendas',
        'stg.contaspagar',
        'stg.contasreceber',
        'app.payment_type_map'
    ];
    optional_tables text[] := ARRAY[
        'stg.clientes',
        'stg.filiais',
        'app.competitor_fuel_prices',
        'app.goals',
        'dw.fact_venda',
        'dw.fact_venda_item',
        'dw.fact_pagamento_comprovante',
        'dw.fact_caixa_turno',
        'dw.fact_comprovante',
        'dw.fact_financeiro',
        'dw.fact_risco_evento',
        'dw.dim_filial',
        'dw.dim_produto',
        'dw.dim_grupo_produto',
        'dw.dim_funcionario',
        'dw.dim_usuario_caixa',
        'dw.dim_local_venda',
        'dw.dim_cliente'
    ];
    table_name text;
    publication_tables text := '';
    missing_required text[];
BEGIN
    SELECT array_agg(t)
      INTO missing_required
    FROM unnest(required_tables) AS t
    WHERE to_regclass(t) IS NULL;

    IF missing_required IS NOT NULL THEN
        RAISE EXCEPTION 'Cannot prepare STG-direct CDC publication. Missing required table(s): %', missing_required;
    END IF;

    FOR table_name IN
        SELECT t FROM unnest(required_tables || optional_tables) AS t
        WHERE to_regclass(t) IS NOT NULL
        ORDER BY t
    LOOP
        publication_tables := publication_tables
            || CASE WHEN publication_tables = '' THEN '' ELSE ', ' END
            || to_regclass(table_name)::text;

        -- Required for update/delete events on tables without a stable PK in STG.
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', to_regclass(table_name));
    END LOOP;

    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'torqmind_cdc_publication') THEN
        EXECUTE 'CREATE PUBLICATION torqmind_cdc_publication FOR TABLE ' || publication_tables;
        RAISE NOTICE 'Publication torqmind_cdc_publication created';
    ELSE
        EXECUTE 'ALTER PUBLICATION torqmind_cdc_publication SET TABLE ' || publication_tables;
        RAISE NOTICE 'Publication torqmind_cdc_publication updated';
    END IF;
END $$;

-- Verify
SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'torqmind_cdc_publication';
SQL

echo ""
echo "PostgreSQL prepared for CDC."
echo "  - wal_level=logical: OK"
echo "  - app.debezium_heartbeat: OK"
echo "  - app.debezium_signal: OK"
echo "  - torqmind_cdc_publication: OK"
echo ""
echo "Next: start streaming stack and register Debezium connector."
