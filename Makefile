UV ?= uv
COMPOSE_FILE := infra/docker-compose.yml
AIRFLOW_IMAGE ?= github-delivery-intelligence-airflow:3.3.0
KAFKA_INTERNAL_BOOTSTRAP_SERVER ?= localhost:29092

.PHONY: install lock format format-check lint typecheck test check compose-config up down ps logs topics migrate metabase-access dashboard-up dashboard-down webhook warehouse pr-monitor backfill dbt-debug dbt-parse dbt-freshness dbt-build dashboard-sql-check demo airflow-image airflow-dag-check airflow-analytics-check analytics-refresh day13-evidence

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
		--bootstrap-server $(KAFKA_INTERNAL_BOOTSTRAP_SERVER) \
		--create --if-not-exists \
		--topic github.events.raw.v1 \
		--partitions 3 \
		--replication-factor 1
	docker compose -f $(COMPOSE_FILE) exec -T kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server $(KAFKA_INTERNAL_BOOTSTRAP_SERVER) \
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
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/004_create_backfill_checkpoints.sql"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/005_create_metabase_reader.sql"
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/006_create_analytics_refresh_runs.sql"

metabase-access:
	docker compose -f $(COMPOSE_FILE) exec -T postgres \
		psql --username "$${POSTGRES_USER:-github_analytics}" \
		--dbname "$${POSTGRES_DB:-github_analytics}" \
		--set ON_ERROR_STOP=1 \
		--command "\i /docker-entrypoint-initdb.d/005_create_metabase_reader.sql"

dashboard-up:
	docker compose -f $(COMPOSE_FILE) up -d --wait postgres
	$(MAKE) metabase-access
	docker compose -f $(COMPOSE_FILE) --profile dashboards up -d --wait metabase

dashboard-down:
	docker compose -f $(COMPOSE_FILE) --profile dashboards stop metabase

webhook:
	$(UV) run uvicorn github_analytics.webhook:create_runtime_app --factory --env-file .env

warehouse:
	$(UV) run --env-file .env warehouse-writer

pr-monitor:
	$(UV) run --env-file .env pr-monitor

backfill:
	$(UV) run --env-file .env github-backfill \
		--start "$(BACKFILL_START)" \
		--end "$(BACKFILL_END)"

dbt-debug:
	$(UV) run dbt debug \
		--project-dir dbt/github_analytics \
		--profiles-dir dbt/github_analytics

dbt-parse:
	$(UV) run dbt parse \
		--project-dir dbt/github_analytics \
		--profiles-dir dbt/github_analytics

dbt-freshness:
	$(UV) run dbt source freshness \
		--project-dir dbt/github_analytics \
		--profiles-dir dbt/github_analytics

dbt-build:
	$(UV) run dbt build \
		--project-dir dbt/github_analytics \
		--profiles-dir dbt/github_analytics

dashboard-sql-check:
	$(UV) run python tools/validate_dashboard_sql.py

demo:
	docker compose -f $(COMPOSE_FILE) up -d --wait kafka postgres
	$(MAKE) topics migrate
	$(UV) run python -m github_analytics.demo

airflow-image:
	docker build \
		--file airflow/Dockerfile \
		--tag $(AIRFLOW_IMAGE) \
		.

airflow-dag-check: airflow-image
	docker run --rm \
		--env AIRFLOW__CORE__LOAD_EXAMPLES=False \
		$(AIRFLOW_IMAGE) \
		python /opt/airflow/check_dag.py

airflow-analytics-check: airflow-image
	docker run --rm \
		--network github-delivery-intelligence_default \
		--env AIRFLOW__CORE__LOAD_EXAMPLES=False \
		--env ANALYTICS_REFRESH_DATABASE_URL=postgresql://github_analytics:local_only_change_me@postgres:5432/github_analytics \
		--env ANALYTICS_REFRESH_SOURCE_IDENTIFIER=github_events_fixture \
		--env ANALYTICS_REFRESH_DBT_PROJECT_DIR=/opt/airflow/dbt/github_analytics \
		--env ANALYTICS_REFRESH_DBT_PROFILES_DIR=/opt/airflow/dbt/github_analytics \
		--env ANALYTICS_REFRESH_DBT_TARGET_DIR=/opt/airflow/dbt/github_analytics/target/analytics_refresh \
		--env DBT_POSTGRES_HOST=postgres \
		--env DBT_POSTGRES_PORT=5432 \
		--env DBT_POSTGRES_DB=github_analytics \
		--env DBT_POSTGRES_USER=github_analytics \
		--env DBT_POSTGRES_PASSWORD=local_only_change_me \
		--env DBT_POSTGRES_SCHEMA=analytics \
		$(AIRFLOW_IMAGE) \
		python /opt/airflow/run_analytics_refresh_smoke.py

analytics-refresh: airflow-analytics-check

day13-evidence:
	docker compose -f $(COMPOSE_FILE) up -d --wait kafka postgres
	$(MAKE) topics migrate
	$(UV) run python tools/run_day13_evidence.py \
		--event-count 500 \
		--concurrency 25 \
		--json-output .artifacts/day-13-evidence.json \
		--markdown-output docs/day-13-evidence.md
