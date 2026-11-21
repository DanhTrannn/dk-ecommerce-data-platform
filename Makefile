.PHONY: all up start core stream bi up-all build build-all down stop restart ps logs status seed generate generate-verify source-reader help superset-db superset-init superset-sync

PROFILES ?= core
SERVICES ?= platform-app
WAIT_TIMEOUT ?= 180
export COMPOSE_PROFILES := $(PROFILES)

all: up

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo ""
	@echo "Daily commands:"
	@echo "  make start                     Start the console/API and management dependencies"
	@echo "  make core                      Start the full core, including Airflow and Trino"
	@echo "  make stream                    Start core + Kafka/Flink; submit the Flink job separately"
	@echo "  make bi                        Start core + Superset; initialize BI metadata first"
	@echo "  make stop                      Stop/remove all containers; retain data volumes"
	@echo "  make status                    Show all container groups"
	@echo "  make logs                      Follow logs from all groups"
	@echo "  make help                      Show this guide"
	@echo ""
	@echo "Create workload access/credentials in Streamlit before starting core, stream or bi."
	@echo ""
	@echo "Advanced commands:"
	@echo "  make build                     Build the shared platform-app image"
	@echo "  make build-all                 Build all project images"
	@echo "  make up SERVICES='trino'        Start selected services and their dependencies"
	@echo "  make up-all PROFILES=core,streaming,bi  Start a custom profile combination"
	@echo "  make restart                   Restart the existing console/API; use start to apply config"
	@echo "  make source-reader             Configure the PostgreSQL batch reader"
	@echo "  make seed                      Seed the source catalog explicitly"
	@echo "  make generate PRESET=1m        Load synthetic OLTP history"
	@echo "  make generate-verify PRESET=1m Check the generated history"
	@echo "  make superset-db               Create dedicated Superset metadata storage"
	@echo "  make superset-init             Initialize BI metadata and its first admin"
	@echo "  make superset-sync             Create the sales dashboard over current Gold marts"
	@echo ""
	@echo "Aliases: make / make up = make start; make down = make stop; make ps = make status"

up: start

start: .env
	@docker compose up -d --build --wait --wait-timeout $(WAIT_TIMEOUT) $(SERVICES)

core:
	@$(MAKE) --no-print-directory up-all PROFILES=core

stream:
	@$(MAKE) --no-print-directory up-all PROFILES=core,streaming

bi:
	@$(MAKE) --no-print-directory up-all PROFILES=core,bi

up-all: .env
	@docker compose up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

build: .env
	@docker compose build platform-app

build-all: .env
	@docker compose --profile '*' build

.env:
	@cp .env.example .env

down: stop

stop:
	@docker compose --profile '*' down

restart: .env
	@docker compose restart $(SERVICES)

ps:
	@docker compose --profile '*' ps

status: ps

logs:
	@docker compose --profile '*' logs -f

seed:
	@uv run --locked --package platform-app -- python -m database.seeds.seed_catalog

generate: .env
	@uv run --locked --env-file .env --package data-generator -- datagen generate --preset $(PRESET)

generate-verify: .env
	@uv run --locked --env-file .env --package data-generator -- datagen verify --preset $(PRESET)

source-reader:
	@docker compose exec -T postgres sh /opt/project/configure-source-reader.sh

superset-db:
	@docker compose exec -T postgres sh /opt/project/configure-superset.sh

superset-init:
	@docker compose --profile bi run --rm --no-deps -T superset-init

superset-sync:
	@docker compose --profile bi run --rm --no-deps -T superset-sync
