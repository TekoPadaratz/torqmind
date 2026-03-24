SHELL := /bin/bash

COMPOSE ?= docker compose
COMPOSE_PROD ?= docker compose -f docker-compose.prod.yml
ENV_FILE ?= .env
PROD_ENV_FILE ?= /etc/torqmind/prod.env
ENV_EXAMPLE ?= .envexemple
RESET_TMP_DIR ?= /tmp/torqmind-reset
DB_NAME ?=

.PHONY: setup up down logs migrate resetdb backfill-snapshots backfill-snapshots-resume etl-incremental purge-sales-history analyze-hot-tables platform-billing-daily test test-agent lint ci prod-up prod-down prod-logs prod-migrate prod-seed prod-etl-incremental prod-purge-sales-history prod-platform-billing-daily

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
	@$(COMPOSE) exec -T postgres sh -lc 'until pg_isready -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" >/dev/null 2>&1; do sleep 1; done'
	@$(COMPOSE) exec -T postgres sh -lc 'rm -rf "$(RESET_TMP_DIR)" && mkdir -p "$(RESET_TMP_DIR)/migrations"'
	@$(COMPOSE) cp sql/torqmind_reset_db_v2.sql postgres:$(RESET_TMP_DIR)/torqmind_reset_db_v2.sql
	@$(COMPOSE) cp sql/migrations/. postgres:$(RESET_TMP_DIR)/migrations/
	@$(COMPOSE) exec -T postgres sh -lc 'db_name="$(DB_NAME)"; if [ -z "$$db_name" ]; then db_name="$${POSTGRES_DB:-torqmind}"; fi; cd "$(RESET_TMP_DIR)" && psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$$db_name" -f torqmind_reset_db_v2.sql'

backfill-snapshots:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "CALL etl.run_operational_snapshot_backfill($${ID_EMPRESA:-1}::int, '\''$${START_DT:?missing START_DT}'\''::date, '\''$${END_DT:?missing END_DT}'\''::date, $${STEP_DAYS:-7}::int, false, false);"'

backfill-snapshots-resume:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "CALL etl.run_operational_snapshot_backfill($${ID_EMPRESA:-1}::int, '\''$${START_DT:?missing START_DT}'\''::date, '\''$${END_DT:?missing END_DT}'\''::date, $${STEP_DAYS:-7}::int, true, false);"'

etl-incremental:
	@$(COMPOSE) exec -T api python -m app.cli.etl_incremental $${TENANT_ID:+--tenant-id "$${TENANT_ID}"} $${REF_DATE:+--ref-date "$${REF_DATE}"} $${FAIL_FAST:+--fail-fast}

purge-sales-history:
	@$(COMPOSE) exec -T api python -m app.cli.purge_sales_history $${TENANT_ID:+--tenant-id "$${TENANT_ID}"} $${REF_DATE:+--ref-date "$${REF_DATE}"}

analyze-hot-tables:
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -c "SELECT etl.analyze_hot_tables();"'

platform-billing-daily:
	@$(COMPOSE) exec -T api python -m app.cli.platform_billing daily --as-of "$${AS_OF:-}" --competence-month "$${COMPETENCE_MONTH:-}" --months-ahead "$${MONTHS_AHEAD:-0}" $${TENANT_ID:+--tenant-id "$${TENANT_ID}"}

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

prod-etl-incremental:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-etl-incremental.sh

prod-purge-sales-history:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/prod-purge-sales-history.sh

prod-platform-billing-daily:
	@ENV_FILE=$(PROD_ENV_FILE) ./deploy/scripts/platform-billing-daily.sh
