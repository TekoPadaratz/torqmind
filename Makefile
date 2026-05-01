SHELL := /bin/bash

COMPOSE ?= docker compose
COMPOSE_PROD ?= docker compose -f docker-compose.prod.yml
ENV_FILE ?= .env
PROD_ENV_FILE ?= /etc/torqmind/prod.env
ENV_EXAMPLE ?= .envexemple
RESET_TMP_DIR ?= /tmp/torqmind-reset
DB_NAME ?=
CLICKHOUSE_PG_HOST ?= postgres
CLICKHOUSE_PG_PORT ?= 5432
CLICKHOUSE_PG_DATABASE ?= $(or $(PG_DATABASE),TORQMIND)
CLICKHOUSE_PG_USER ?= $(or $(PG_USER),postgres)
CLICKHOUSE_PG_PASSWORD ?= $(or $(PG_PASSWORD),postgres)

ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

.PHONY: setup up down logs migrate resetdb hard-resetdb backfill-snapshots backfill-snapshots-resume etl-incremental etl-operational etl-risk purge-sales-history analyze-hot-tables reconcile-sales operational-truth-diagnose operational-truth-preflight operational-truth-purge operational-truth-rebuild operational-truth-validate platform-billing-daily clickhouse-sync-dw clickhouse-dw-init clickhouse-wait-dw clickhouse-marts-init clickhouse-init clickhouse-mvs clickhouse-backfill clickhouse-native-backfill clickhouse-smoke analytics-smoke test test-agent lint ci prod-up prod-down prod-logs prod-migrate prod-seed prod-clickhouse-sync-dw prod-clickhouse-sync-dw-full prod-clickhouse-sync-dw-incremental prod-clickhouse-refresh-marts-full prod-clickhouse-refresh-marts-incremental prod-clickhouse-init prod-data-reconcile prod-semantic-marts-audit prod-history-coverage-audit prod-sales-orphans-report prod-etl-pipeline prod-etl-incremental prod-etl-operational prod-etl-risk prod-purge-sales-history prod-rebuild-derived-from-stg prod-reconcile-sales prod-platform-billing-daily prod-install-cron prod-post-boot-check prod-homologation-apply prod-homologation-apply-streaming prod-homologation-apply-full-stg streaming-up streaming-down streaming-init-clickhouse streaming-init-mart-rt streaming-register-debezium streaming-status streaming-validate-cdc streaming-logs streaming-config-check test-cdc-consumer realtime-cutover realtime-validate realtime-backfill realtime-rollback realtime-e2e-smoke

setup:
	@command -v docker >/dev/null || (echo "docker nao encontrado no PATH" && exit 1)
	@$(COMPOSE) version >/dev/null
	@if [ ! -f "$(ENV_FILE)" ]; then \
		if [ -f "$(ENV_EXAMPLE)" ]; then \
			cp "$(ENV_EXAMPLE)" "$(ENV_FILE)"; \
			echo "$(ENV_FILE) criado a partir de $(ENV_EXAMPLE)"; \
		else \
			echo "Arquivo $(ENV_FILE) nao encontrado e $(ENV_EXAMPLE) indisponivel"; \
			exit 1; \
		fi; \
	else \
		echo "$(ENV_FILE) ja existe"; \
	fi
	@$(COMPOSE) build --pull

up:
	@$(COMPOSE) up -d --build

down:
	@$(COMPOSE) down

logs:
	@$(COMPOSE) logs -f --tail=200

migrate:
	@$(COMPOSE) exec -T api python -m app.cli.migrate

resetdb:
	@$(MAKE) hard-resetdb

hard-resetdb:
	@if [ "$${RESET_CONFIRM:-}" != "1" ]; then \
		echo "Destructive reset requires RESET_CONFIRM=1"; \
		exit 1; \
	fi
	@if [ "$${RESET_ENV:-}" != "dev" ] && [ "$${RESET_ENV:-}" != "homolog" ]; then \
		echo "Destructive reset requires RESET_ENV=dev or RESET_ENV=homolog"; \
		exit 1; \
	fi
	@if [ "$${APP_ENV:-}" = "prod" ] || [ "$${APP_ENV:-}" = "production" ]; then \
		echo "Refusing destructive reset with APP_ENV=$${APP_ENV}"; \
		exit 1; \
	fi
	@$(COMPOSE) exec -T postgres sh -lc 'until pg_isready -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" >/dev/null 2>&1; do sleep 1; done'
	@$(COMPOSE) exec -T postgres sh -lc 'rm -rf "$(RESET_TMP_DIR)" && mkdir -p "$(RESET_TMP_DIR)/migrations"'
	@$(COMPOSE) cp sql/torqmind_reset_db_v2.sql postgres:$(RESET_TMP_DIR)/torqmind_reset_db_v2.sql
	@$(COMPOSE) cp sql/migrations/. postgres:$(RESET_TMP_DIR)/migrations/
	@$(COMPOSE) exec -T postgres sh -lc 'db_name="$(DB_NAME)"; if [ -z "$$db_name" ]; then db_name="$${POSTGRES_DB:-torqmind}"; fi; cd "$(RESET_TMP_DIR)" && psql -v ON_ERROR_STOP=1 -v TM_ALLOW_RESET=1 -v TM_RESET_ENV="$(RESET_ENV)" -U "$${POSTGRES_USER:-postgres}" -d "$$db_name" -f torqmind_reset_db_v2.sql'

backfill-snapshots:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "CALL etl.run_operational_snapshot_backfill($${ID_EMPRESA:-1}::int, '\''$${START_DT:?missing START_DT}'\''::date, '\''$${END_DT:?missing END_DT}'\''::date, $${STEP_DAYS:-7}::int, false, false);"'

backfill-snapshots-resume:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "CALL etl.run_operational_snapshot_backfill($${ID_EMPRESA:-1}::int, '\''$${START_DT:?missing START_DT}'\''::date, '\''$${END_DT:?missing END_DT}'\''::date, $${STEP_DAYS:-7}::int, true, false);"'

etl-incremental:
	@$(COMPOSE) exec -T api python -m app.cli.etl_incremental --track "$${TRACK:-full}" $${TENANT_ID:+--tenant-id "$${TENANT_ID}"} $${REF_DATE:+--ref-date "$${REF_DATE}"} $${FAIL_FAST:+--fail-fast} $${SKIP_BUSY_TENANTS:+--skip-busy-tenants}

etl-operational:
	@TRACK=operational SKIP_BUSY_TENANTS=1 $(MAKE) etl-incremental

etl-risk:
	@TRACK=risk SKIP_BUSY_TENANTS=1 $(MAKE) etl-incremental

purge-sales-history:
	@$(COMPOSE) exec -T api python -m app.cli.purge_sales_history $${TENANT_ID:+--tenant-id "$${TENANT_ID}"} $${REF_DATE:+--ref-date "$${REF_DATE}"}

analyze-hot-tables:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "SELECT etl.analyze_hot_tables();"'

reconcile-sales:
	@$(COMPOSE) exec -T api python -m app.cli.reconcile_sales --tenant-id "$${TENANT_ID:?missing TENANT_ID}" --date "$${DATE:?missing DATE}" $${BRANCH_ID:+--branch-id "$${BRANCH_ID}"} $${GROUP:+--group "$${GROUP}"} $${DETAIL_LIMIT:+--detail-limit "$${DETAIL_LIMIT}"}

operational-truth-diagnose:
	@$(COMPOSE) exec -T api python -m app.cli.operational_truth diagnose --tenant-id "$${TENANT_ID:?missing TENANT_ID}" $${BRANCH_ID:+--branch-id "$${BRANCH_ID}"} $${DT_INI:+--dt-ini "$${DT_INI}"} $${DT_FIM:+--dt-fim "$${DT_FIM}"}

operational-truth-preflight:
	@$(COMPOSE) exec -T api python -m app.cli.operational_truth preflight --tenant-id "$${TENANT_ID:?missing TENANT_ID}" $${WITH_RISK:+--with-risk} $${OPERATIONAL_ONLY:+--operational-only}

operational-truth-purge:
	@$(COMPOSE) exec -T api python -m app.cli.operational_truth purge --tenant-id "$${TENANT_ID:?missing TENANT_ID}" $${BRANCH_ID:+--branch-id "$${BRANCH_ID}"} $${SCOPE:+--scope "$${SCOPE}"} $${INCLUDE_STAGING:+--include-staging} $${REF_DATE:+--ref-date "$${REF_DATE}"} $${DRY_RUN:+--dry-run}

operational-truth-rebuild:
	@$(COMPOSE) exec -T api python -m app.cli.operational_truth rebuild --tenant-id "$${TENANT_ID:?missing TENANT_ID}" $${REF_DATE:+--ref-date "$${REF_DATE}"} $${WITH_RISK:+--with-risk} $${OPERATIONAL_ONLY:+--operational-only}

operational-truth-validate:
	@$(COMPOSE) exec -T api python -m app.cli.operational_truth validate --tenant-id "$${TENANT_ID:?missing TENANT_ID}" $${BRANCH_ID:+--branch-id "$${BRANCH_ID}"} $${DT_INI:+--dt-ini "$${DT_INI}"} $${DT_FIM:+--dt-fim "$${DT_FIM}"}

platform-billing-daily:
	@$(COMPOSE) exec -T api python -m app.cli.platform_billing daily --as-of "$${AS_OF:-}" --competence-month "$${COMPETENCE_MONTH:-}" --months-ahead "$${MONTHS_AHEAD:-0}" $${TENANT_ID:+--tenant-id "$${TENANT_ID}"}

clickhouse-sync-dw:
	@ALLOW_INSECURE_ENV=1 ENV_FILE=$(ENV_FILE) COMPOSE_FILE=docker-compose.yml MODE=full ./deploy/scripts/prod-clickhouse-sync-dw.sh

clickhouse-dw-init: clickhouse-sync-dw

clickhouse-wait-dw:
	@for attempt in {1..120}; do \
		count="$$( $(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'torqmind_dw' AND name IN ('dim_cliente','dim_filial','dim_funcionario','dim_grupo_produto','dim_local_venda','dim_produto','dim_usuario_caixa','fact_caixa_turno','fact_comprovante','fact_financeiro','fact_pagamento_comprovante','fact_risco_evento','fact_venda','fact_venda_item')" )"; \
		if [ "$$count" -ge 14 ]; then \
			echo "torqmind_dw ready with $$count required tables"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "Timed out waiting for native torqmind_dw tables"; \
	exit 1

clickhouse-marts-init:
	@$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < sql/clickhouse/phase2_mvs_design.sql

clickhouse-init: clickhouse-dw-init clickhouse-wait-dw clickhouse-marts-init

clickhouse-mvs:
	@$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < sql/clickhouse/phase2_mvs_streaming_triggers.sql

clickhouse-backfill:
	@bash deploy/scripts/load_clickhouse_historical.sh

clickhouse-native-backfill:
	@$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < sql/clickhouse/phase3_native_backfill.sql

clickhouse-smoke:
	@$(COMPOSE) exec -T clickhouse sh -lc 'wget -q -O - http://127.0.0.1:8123/ping'
	@$(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT database, count() AS tables FROM system.tables WHERE database IN ('torqmind_dw', 'torqmind_mart') GROUP BY database ORDER BY database"

analytics-smoke:
	@$(COMPOSE) exec -T api python -c "from app.config import settings; from app.repos_analytics import analytics_backend_inventory; inv=analytics_backend_inventory(); print({'use_clickhouse': settings.use_clickhouse, 'dual_read_mode': settings.dual_read_mode, 'functions': len(inv['functions'])})"

test:
	@$(COMPOSE) exec -T api python -m unittest discover -s app -p 'test*.py'
	@$(COMPOSE) exec -T web npm test

test-agent:
	@PYTHONPATH=apps/agent python3 -m unittest discover -s apps/agent/tests -v

lint:
	@$(COMPOSE) exec -T api python -m compileall -q app
	@$(COMPOSE) exec -T web npm run build

ci: test test-agent lint

prod-up:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-up.sh

prod-down:
	@$(COMPOSE_PROD) --env-file $(PROD_ENV_FILE) down

prod-logs:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-logs.sh

prod-migrate:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-migrate.sh

prod-seed:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-seed.sh

prod-clickhouse-init:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-clickhouse-init.sh

prod-clickhouse-sync-dw:
	@ENV_FILE=$(PROD_ENV_FILE) MODE=incremental ./deploy/scripts/prod-clickhouse-sync-dw.sh

prod-clickhouse-sync-dw-full:
	@ENV_FILE=$(PROD_ENV_FILE) MODE=full ./deploy/scripts/prod-clickhouse-sync-dw.sh

prod-clickhouse-sync-dw-incremental:
	@ENV_FILE=$(PROD_ENV_FILE) MODE=incremental ./deploy/scripts/prod-clickhouse-sync-dw.sh

prod-clickhouse-refresh-marts-full:
	@ENV_FILE=$(PROD_ENV_FILE) MODE=full ./deploy/scripts/prod-clickhouse-refresh-marts.sh

prod-clickhouse-refresh-marts-incremental:
	@ENV_FILE=$(PROD_ENV_FILE) MODE=incremental ./deploy/scripts/prod-clickhouse-refresh-marts.sh

prod-data-reconcile:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-data-reconcile.sh

prod-semantic-marts-audit:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-semantic-marts-audit.sh

prod-history-coverage-audit:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-history-coverage-audit.sh

prod-sales-orphans-report:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-sales-orphans-report.sh

prod-etl-pipeline:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-etl-pipeline.sh

prod-etl-incremental:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-etl-incremental.sh

prod-etl-operational:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-etl-operational.sh

prod-etl-risk:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-etl-risk.sh

prod-purge-sales-history:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-purge-sales-history.sh

prod-rebuild-derived-from-stg:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes $${ID_EMPRESA:+--id-empresa "$${ID_EMPRESA}"} $${ID_FILIAL:+--id-filial "$${ID_FILIAL}"} $${FROM_DATE:+--from-date "$${FROM_DATE}"} $${TO_DATE:+--to-date "$${TO_DATE}"} $${INCLUDE_DIMENSIONS:+--include-dimensions}

prod-install-cron:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-install-cron.sh

prod-post-boot-check:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-post-boot-check.sh

prod-homologation-apply:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse

prod-homologation-apply-streaming:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --with-streaming

prod-homologation-apply-full-stg:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01 --full-clickhouse

prod-reconcile-sales:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-check-sales-reconciliation.sh

prod-platform-billing-daily:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/platform-billing-daily.sh

# ============================================================
# Streaming / Event-Driven (CDC)
# ============================================================

STREAMING_COMPOSE ?= docker-compose.streaming.yml
STREAMING_PROFILE ?= local-full

streaming-up:
	@ENV_FILE=$(ENV_FILE) STREAMING_PROFILE=$(STREAMING_PROFILE) ./deploy/scripts/streaming-up.sh

streaming-down:
	@ENV_FILE=$(ENV_FILE) ./deploy/scripts/streaming-down.sh

streaming-init-clickhouse:
	@ENV_FILE=$(ENV_FILE) COMPOSE_FILE=docker-compose.yml ./deploy/scripts/streaming-init-clickhouse.sh

streaming-register-debezium:
	@ENV_FILE=$(ENV_FILE) ./deploy/scripts/streaming-register-debezium.sh

streaming-status:
	@ENV_FILE=$(ENV_FILE) ./deploy/scripts/streaming-status.sh

streaming-validate-cdc:
	@ENV_FILE=$(ENV_FILE) COMPOSE_FILE=docker-compose.yml ./deploy/scripts/streaming-validate-cdc.sh

streaming-logs:
	@ENV_FILE=$(ENV_FILE) ./deploy/scripts/streaming-tail.sh

streaming-config-check:
	@docker compose -f $(STREAMING_COMPOSE) --env-file $(ENV_FILE) --profile local-full config --quiet && echo "Streaming compose config: OK"

test-cdc-consumer:
	@cd apps/cdc_consumer && python -m pytest tests/ -v

# ============================================================
# Realtime Cutover
# ============================================================

streaming-init-mart-rt:
	@ENV_FILE=$(PROD_ENV_FILE) COMPOSE_FILE=docker-compose.prod.yml ./deploy/scripts/streaming-init-mart-rt.sh

realtime-cutover:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-realtime-cutover-apply.sh --yes --with-backfill

realtime-validate:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" COMPOSE_FILE=docker-compose.prod.yml ./deploy/scripts/realtime-validate-cutover.sh

realtime-backfill:
	@docker compose -f $(STREAMING_COMPOSE) --env-file $(ENV_FILE) exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill --from-date 2025-01-01 --id-empresa 1

realtime-rollback:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy

realtime-e2e-smoke:
	@ENV_FILE="$${ENV_FILE:-$(PROD_ENV_FILE)}" ./deploy/scripts/realtime-e2e-smoke.sh
