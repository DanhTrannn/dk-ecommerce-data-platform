.PHONY: up down ps logs seed generate generate-verify source-reader test help superset-sync

WAIT_TIMEOUT ?= 180

up: .env
	@docker compose up -d --build --wait --wait-timeout $(WAIT_TIMEOUT) $(SERVICES)

down:
	@docker compose down

ps:
	@docker compose ps

logs:
	@docker compose logs -f

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo ""
	@echo "Daily commands:"
	@echo "  make up                        Start all services, including Superset"
	@echo "  make down                      Remove containers and retain data volumes"
	@echo "  make ps                        Show service status"
	@echo "  make logs                      Follow logs from all services"
	@echo "  make help                      Show this guide"
	@echo ""
	@echo "For first-time setup, run make up SERVICES=platform-app and configure credentials in Streamlit."
	@echo ""
	@echo "Advanced commands:"
	@echo "  make up SERVICES=trino         Start selected services and their dependencies"
	@echo "  make source-reader             Configure the PostgreSQL batch reader"
	@echo "  make seed                      Seed the source catalog explicitly"
	@echo "  make generate PRESET=1m        Load synthetic OLTP history"
	@echo "  make generate-verify PRESET=1m Check the generated history"
	@echo "  make test                      Run every test suite (pipeline tests in the Airflow scheduler)"
	@echo "  make superset-sync             Create the sales dashboard over current Gold marts"

.env:
	@cp .env.example .env

seed:
	@uv run --locked --package platform-app -- python -m database.seeds.seed_catalog

generate: .env
	@uv run --locked --env-file .env --package data-generator -- datagen generate --preset $(PRESET)

generate-verify: .env
	@uv run --locked --env-file .env --package data-generator -- datagen verify --preset $(PRESET)

source-reader:
	@docker compose exec -T postgres sh /opt/project/configure-source-reader.sh

test:
	@docker compose exec -T -w /tmp -e PYTHONDONTWRITEBYTECODE=1 \
		-e PYTHONPATH=/opt/project/airflow:/opt/project/airflow/tests airflow-scheduler \
		python -m unittest discover -s /opt/project/airflow/tests -t /opt/project/airflow/tests
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=services/platform-app/tests uv run --locked --package platform-app -- \
		python -m unittest discover -s services/platform-app/tests -t services/platform-app/tests
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=generator/tests uv run --locked --package data-generator -- \
		python -m unittest discover -s generator/tests -t generator/tests
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=infrastructure/superset uv run --locked --package platform-app -- \
		python -m unittest discover -s infrastructure/superset/tests -t infrastructure/superset/tests

superset-sync:
	@docker compose run --rm --no-deps -T superset-sync
