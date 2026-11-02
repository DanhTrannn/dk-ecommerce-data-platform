.PHONY: all up start down stop restart ps logs status seed typecheck help

PROFILES ?= core
START_FLAGS ?=

all: up

help:
	@echo "D&K E-Commerce Lakehouse Platform"
	@echo "  make up                        Start core (MySQL, API, storefront)"
	@echo "  make up PROFILES=core,batch     Add the batch/query platform"
	@echo "  make up PROFILES=core,batch,streaming,tools  Start all groups"
	@echo "  make up START_FLAGS=--build    Rebuild selected images"
	@echo "  make down                      Stop all groups; retain volumes"
	@echo "  make restart PROFILES=core     Restart selected existing containers"
	@echo "  make ps / make logs            Inspect all groups"
	@echo "  make seed                      Initialize catalog/admin in MySQL"
	@echo "  make typecheck                 Check frontend TypeScript"

up: start

start:
	@./scripts/start_all.sh --profiles "$(PROFILES)" $(START_FLAGS)

down: stop

stop:
	@./scripts/stop_all.sh

restart:
	@COMPOSE_PROFILES="$(PROFILES)" docker compose restart

ps:
	@docker compose --profile '*' ps

status: ps

logs:
	@docker compose --profile '*' logs -f

seed:
	@uv run --locked --package ecommerce-api -- python database/seeds/seed_catalog.py

typecheck:
	@npm --prefix apps/storefront run typecheck
