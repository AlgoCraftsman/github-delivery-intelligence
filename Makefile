UV ?= uv
COMPOSE_FILE := infra/docker-compose.yml

.PHONY: install lock format format-check lint typecheck test check compose-config up down ps logs webhook

install:
	$(UV) sync --frozen

lock:
	$(UV) lock

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

format-check:
	$(UV) run ruff format --check .

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest

check: format-check lint typecheck test compose-config

compose-config:
	docker compose -f $(COMPOSE_FILE) config --quiet

up:
	docker compose -f $(COMPOSE_FILE) up -d --wait

down:
	docker compose -f $(COMPOSE_FILE) down

ps:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs --follow

webhook:
	$(UV) run uvicorn github_analytics.webhook:create_runtime_app --factory --env-file .env
