GO ?= go
DATABASE_URL ?= postgres://app:app@localhost:5432/wagering?sslmode=disable
MIGRATE_IMAGE ?= migrate/migrate:v4.18.1

.PHONY: up down logs ps clean
.PHONY: build test test-race test-integration test-e2e vet fmt tidy
.PHONY: migrate-up migrate-down migrate-create

## Infra
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v

## Go
build:
	$(GO) build ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test -tags=integration -race ./test/integration/...

test-e2e:
	$(GO) test -tags='integration faultinject' -race ./test/e2e/...

vet:
	$(GO) vet ./...

fmt:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "Arquivos fora do padrão:"; echo "$$out"; exit 1; else echo "gofmt ok"; fi

tidy:
	$(GO) mod tidy

## Migrations
migrate-up:
	docker run --rm -v "$(CURDIR)/migrations:/migrations" $(MIGRATE_IMAGE) \
		-path=/migrations -database="$(DATABASE_URL)" up

migrate-down:
	docker run --rm -v "$(CURDIR)/migrations:/migrations" $(MIGRATE_IMAGE) \
		-path=/migrations -database="$(DATABASE_URL)" down 1

migrate-create:
	@test -n "$(name)" || (echo "Uso: make migrate-create name=<descricao>"; exit 1)
	docker run --rm -v "$(CURDIR)/migrations:/migrations" $(MIGRATE_IMAGE) \
		create -ext sql -dir /migrations -seq "$(name)"