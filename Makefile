.DEFAULT_GOAL := help

.PHONY: help restore build up down restart status logs config smoke clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "OrderFlow commands:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

restore: ## Restore pinned .NET dependencies
	dotnet restore OrderFlow.sln

build: ## Compile the .NET 10 backend
	dotnet build OrderFlow.sln -c Release --no-restore

up: ## Build and start the complete stack
	docker compose up --build -d

down: ## Stop the stack and retain its data
	docker compose down

restart: ## Restart application containers
	docker compose restart frontend api worker

status: ## Show container and health status
	docker compose ps

logs: ## Follow application logs
	docker compose logs --follow frontend api worker

config: ## Validate and render the Compose configuration
	docker compose config --quiet

smoke: ## Run the local smoke test
	node ./scripts/smoke-test.mjs

clean: ## Remove containers and project volumes
	docker compose down --volumes --remove-orphans
