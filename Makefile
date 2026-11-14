.PHONY: all up start up-all build build-all down stop restart ps logs status seed generate generate-verify source-reader provision-minio provision-polaris provision-lakehouse typecheck help superset-db superset-init superset-sync

PROFILES ?= core
SERVICES ?= platform-app lakehouse-access
WAIT_TIMEOUT ?= 180
export COMPOSE_PROFILES := $(PROFILES)

all: up

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo "  make up / make start           Build and start the admin console and its dependencies"
	@echo "                                 Fresh source: schema + one admin; manage workload access in Streamlit"
	@echo "  make build                     Build the shared platform-app/admin image"
	@echo "  make build-all                 Build all project images, including streaming and BI"
	@echo "  make up-all                    Build/start the full selected profiles after configuring workload credentials"
	@echo "  make up-all PROFILES=core,streaming  Include Kafka/Flink (job submission remains manual)"
	@echo "  make down                      Stop all groups; retain volumes"
	@echo "  make restart                   Restart the existing console/backend; use make up to apply config changes"
	@echo "  make up SERVICES='trino'        Start selected services and their dependencies"
	@echo "  make ps / make logs            Inspect all groups"
	@echo "  make provision-lakehouse        Optional preset workload provisioning; skip for manual Streamlit setup"
	@echo "  make source-reader             Configure the dedicated PostgreSQL batch reader"
	@echo "  make seed                      Run source seeds (business schema required)"
	@echo "  make generate PRESET=1m        Load synthetic OLTP history (GENERATOR_DATABASE_URL)"
	@echo "  make generate-verify PRESET=1m Compare generated data with the preset rules"
	@echo "  make superset-db               Create dedicated Superset metadata storage"
	@echo "  make superset-init             Initialize BI metadata and first admin explicitly"
	@echo "  make superset-sync             Create a sales dashboard pinned to a publication"
	@echo "  make typecheck                 Check frontend TypeScript"

up: start

start: .env
	@docker compose up -d --build --wait --wait-timeout $(WAIT_TIMEOUT) $(SERVICES)

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

provision-minio:
	@docker compose exec -T --user 0:0 minio sh /opt/project/minio/provision.sh

provision-polaris:
	@docker compose run --rm --no-deps -T lakehouse-provision

provision-lakehouse:
	@$(MAKE) provision-minio
	@$(MAKE) provision-polaris

superset-db:
	@docker compose exec -T postgres sh /opt/project/configure-superset.sh

superset-init:
	@docker compose --profile bi run --rm --no-deps -T superset-init

superset-sync:
	@docker compose --profile bi run --rm --no-deps -T superset-sync

typecheck:
	@npm --prefix services/storefront run typecheck
