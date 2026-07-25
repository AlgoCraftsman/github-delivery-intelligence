UV ?= uv
COMPOSE_FILE := infra/docker-compose.yml

.PHONY: install lock format format-check lint typecheck test check compose-config up down ps logs topics migrate webhook warehouse pr-monitor

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
	$(MAKE) topics

down:
	docker compose -f $(COMPOSE_FILE) down

ps:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs --follow

topics:
	docker compose -f $(COMPOSE_FILE) exec -T kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic github.events.raw.v1 \
		--partitions 3 \
		--replication-factor 1
	docker compose -f $(COMPOSE_FILE) exec -T kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic github.events.dlq.v1 \
		--partitions 1 \
		--replication-factor 1

migrate:
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/002_create_raw_github_events.sql"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/003_create_pr_monitor.sql"

webhook:
	$(UV) run uvicorn github_analytics.webhook:create_runtime_app --factory --env-file .env

warehouse:
	$(UV) run --env-file .env warehouse-writer

pr-monitor:
	$(UV) run --env-file .env pr-monitor
