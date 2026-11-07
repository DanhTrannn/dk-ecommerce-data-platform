.PHONY: all up start build down stop restart ps logs status seed typecheck help

PROFILES ?= core
export COMPOSE_PROFILES := $(PROFILES)

all: up

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo "  make up                        Start core services and wait for readiness"
	@echo "  make up PROFILES=core,batch     Add local Spark client"
	@echo "  make up PROFILES=core,batch,streaming  Start all groups"
	@echo "  make build                     Build all project images"
	@echo "  make down                      Stop all groups; retain volumes"
	@echo "  make restart PROFILES=core     Restart selected existing containers"
	@echo "  make ps / make logs            Inspect all groups"
	@echo "  make seed                      Run source seeds (business schema required)"
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
	@uv run --locked --package platform-app -- python -m database.seeds.0010_multi_city_stores

typecheck:
	@npm --prefix services/storefront run typecheck
