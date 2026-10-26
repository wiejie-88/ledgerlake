COMPOSE := docker compose
MC_ALIAS := mc alias set local http://minio:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs smoke seed tick bronze silver silver-report gold dq run dq-gate-demo reset-lake test-container test lint

help: ## List available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'

.env:
	cp .env.example .env
	@echo "Created .env from .env.example (placeholder values; edit before sharing anything)."

up: .env ## Build and start all services; returns when all are healthy
	$(COMPOSE) up -d --build --wait

down: ## Stop all services (data volumes are kept)
	$(COMPOSE) down

clean: ## Stop all services and DELETE all data volumes
	@read -p "This deletes all Postgres, MongoDB and MinIO data. Type 'yes' to continue: " ans && [ "$$ans" = yes ]
	$(COMPOSE) down -v

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Follow logs (one service: make logs s=airflow)
	$(COMPOSE) logs -f $(s)

smoke: ## Iceberg smoke test, then show the catalog row in Postgres and the files in MinIO
	$(COMPOSE) exec airflow python -m jobs.common.smoke
	@echo "--- Iceberg JDBC catalog (Postgres iceberg_catalog.iceberg_tables)"
	@$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d iceberg_catalog -c "SELECT catalog_name, table_namespace, table_name, metadata_location FROM iceberg_tables;"'
	@echo "--- Objects in MinIO (s3://warehouse/ops/smoke_test/)"
	@$(COMPOSE) run --rm --no-deps minio-init '$(MC_ALIAS) && mc ls --recursive local/warehouse/ops/smoke_test/'

seed: ## Rebuild the 60-day synthetic source history in Postgres and MongoDB (wipes source data)
	$(COMPOSE) exec airflow python -m generator seed

tick: ## Add one day of source data: make tick date=YYYY-MM-DD
	@test -n "$(date)" || (echo "usage: make tick date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow python -m generator tick --date $(date)

bronze: ## Extract sources into Iceberg bronze for one logical date: make bronze date=YYYY-MM-DD
	@test -n "$(date)" || (echo "usage: make bronze date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow python -m jobs.bronze.run --date $(date)

silver: ## Build silver and quarantine from one bronze partition: make silver date=YYYY-MM-DD
	@test -n "$(date)" || (echo "usage: make silver date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow python -m jobs.silver.run --date $(date)

silver-report: ## Quarantine reasons, silver counts, PII columns and schema evolution evidence
	$(COMPOSE) exec airflow python -m jobs.silver.report

gold: ## Build gold (dims + fact via MERGE, marts rebuilt) from current silver: make gold date=YYYY-MM-DD
	@test -n "$(date)" || (echo "usage: make gold date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow python -m jobs.gold.run --date $(date)

dq: ## Run a DQ suite and store results in ops.dq_results: make dq layer=silver date=YYYY-MM-DD
	@test -n "$(layer)" -a -n "$(date)" || (echo "usage: make dq layer=silver date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow python -m quality.run --layer $(layer) --date $(date)

run: ## Run the full ledgerlake_daily DAG for one business date: make run date=YYYY-MM-DD
	@test -n "$(date)" || (echo "usage: make run date=YYYY-MM-DD" && exit 2)
	$(COMPOSE) exec airflow airflow dags test ledgerlake_daily -c '{"business_date": "$(date)"}'

dq-gate-demo: ## Run ledgerlake_daily with a simulated DQ defect: dq_silver fails, downstream upstream_failed
	@test -n "$(date)" || (echo "usage: make dq-gate-demo date=YYYY-MM-DD" && exit 2)
	-$(COMPOSE) exec airflow airflow dags test ledgerlake_daily \
		-c '{"business_date": "$(date)", "simulate_defect": "null_amount"}'

reset-lake: ## DROP all bronze/silver/quarantine/gold/ops tables (asks first)
	@read -p "This drops every lake table and deletes its files. Type 'yes' to continue: " ans && [ "$$ans" = yes ]
	$(COMPOSE) exec airflow python -m jobs.common.reset_lake

test-container: ## Run the Spark and DAG tests inside the Airflow container
	$(COMPOSE) exec airflow python -m pytest -q tests/spark tests/dags

test: ## Run unit tests locally (needs: pip install -r requirements-dev.txt)
	python -m pytest -q

lint: ## Run ruff locally (needs: pip install -r requirements-dev.txt)
	ruff check . && ruff format --check .
