SHELL := /bin/bash

COMPOSE ?= docker compose
ENV_FILE ?= .env
ENV_EXAMPLE ?= .envexemple

.PHONY: setup up down logs migrate test lint

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
	@$(COMPOSE) exec -T postgres sh -lc 'until pg_isready -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" >/dev/null 2>&1; do sleep 1; done'
	@$(COMPOSE) exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_DB:-torqmind}" -f /docker-entrypoint-initdb.d/003_mart_demo.sql'

test:
	@$(COMPOSE) exec -T api python -m unittest discover -s app -p 'test*.py'

lint:
	@$(COMPOSE) exec -T api python -m compileall -q app
	@$(COMPOSE) exec -T web npm run build
