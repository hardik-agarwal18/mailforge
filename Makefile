GO ?= go
DOCKER_COMPOSE ?= docker compose
COMPOSE_FILE := -f deployments/docker-compose.yml

.PHONY: help fmt fmt-check vet test test-race test-integration build ci deps-up deps-down deps-reset

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' Makefile | awk 'BEGIN {FS = ":.*## "} {printf "%-18s %s\n", $$1, $$2}'

fmt: ## Format all Go source files
	@gofmt -w $$(find cmd internal -type f -name '*.go')

fmt-check: ## Fail if any Go source file is not gofmt-formatted
	@files="$$(gofmt -l $$(find cmd internal -type f -name '*.go'))"; \
	if [ -n "$$files" ]; then printf 'Files need gofmt:\n%s\n' "$$files"; exit 1; fi

vet: ## Run go vet across all packages
	$(GO) vet ./...

test: ## Run all tests (integration tests require their environment variables)
	$(GO) test ./... -count=1

test-race: ## Run race detector on delivery, benchmark, and SMTP packages
	$(GO) test -race -count=1 ./internal/delivery ./internal/deliverybench ./internal/smtp

test-integration: ## Run PostgreSQL/Redis/SMTP integration tests (requires test env vars)
	@test -n "$$MAILFORGE_TEST_DATABASE_URL" || (echo 'MAILFORGE_TEST_DATABASE_URL is required' >&2; exit 1)
	@test -n "$$MAILFORGE_TEST_REDIS_ADDR" || (echo 'MAILFORGE_TEST_REDIS_ADDR is required' >&2; exit 1)
	@test -n "$$REDIS_TEST_ADDR" || (echo 'REDIS_TEST_ADDR is required' >&2; exit 1)
	$(GO) test ./internal/delivery ./internal/deliverybench -run '^TestIntegration|^TestMockSMTP' -count=1 -v
	$(GO) test ./internal/smtp -run '^TestRedisAuthAttemptLimiterIntegration$$' -count=1 -v

build: ## Build all command packages
	$(GO) build ./cmd/...

ci: fmt-check vet test test-race build ## Run the local CI-equivalent checks

deps-up: ## Start local PostgreSQL, Redis, Mailpit, Prometheus, Grafana, and Alertmanager
	$(DOCKER_COMPOSE) --env-file .env $(COMPOSE_FILE) up -d postgres redis mailpit prometheus grafana alertmanager

deps-down: ## Stop local dependencies and retain volumes
	$(DOCKER_COMPOSE) --env-file .env $(COMPOSE_FILE) down

deps-reset: ## Stop local dependencies and DELETE their named data volumes
	$(DOCKER_COMPOSE) --env-file .env $(COMPOSE_FILE) down -v
