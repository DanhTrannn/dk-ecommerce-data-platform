.PHONY: all up start build down stop restart ps logs status seed source-reader provision-minio provision-polaris provision-lakehouse typecheck help superset-db superset-init superset-sync

PROFILES ?= core
export COMPOSE_PROFILES := $(PROFILES)

all: up

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo "  make up                        Start core services and wait for readiness"
	@echo "  make up PROFILES=core,batch     Add native batch client"
	@echo "  make up PROFILES=core,batch,streaming  Start data profiles"
	@echo "  make build                     Build all project images"
	@echo "  make down                      Stop all groups; retain volumes"
	@echo "  make restart PROFILES=core     Restart selected existing containers"
	@echo "  make ps / make logs            Inspect all groups"
	@echo "  make provision-lakehouse        Initialize missing MinIO/Polaris resources explicitly"
	@echo "  make source-reader             Configure the dedicated PostgreSQL batch reader"
	@echo "  make seed                      Run source seeds (business schema required)"
	@echo "  make superset-db               Create dedicated Superset metadata storage"
	@echo "  make superset-init             Initialize BI metadata and first admin explicitly"
	@echo "  make superset-sync             Create a sales dashboard pinned to a publication"
	@echo "  make typecheck                 Check frontend TypeScript"

up: start

start: .env
	@docker compose up -d --wait --wait-timeout 180

build: .env
	@docker compose --profile '*' build

.env:
	@cp .env.example .env

down: stop

stop:
	@docker compose --profile '*' down

restart:
	@docker compose restart

ps:
	@docker compose --profile '*' ps

status: ps

logs:
	@docker compose --profile '*' logs -f

seed:
	@uv run --locked --package platform-app -- python -m database.seeds.seed_catalog

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
