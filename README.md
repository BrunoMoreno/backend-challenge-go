# Desafio Backend — Processamento Distribuído de Apostas em Go

Serviço em **Go + Uber Fx** que processa operações financeiras de provedores de jogos
(`BET/WIN/LOSS/REFUND/ROLLBACK`) por **API HTTP** e **consumidor SQS**, com PostgreSQL,
ledger append-only, idempotência persistente, inbox/outbox e autenticação OIDC via Keycloak.
Correto com múltiplas instâncias e falhas entre etapas.

> Documentação: `docs/` contém `PRD.md` (requisitos), `ARCHITECTURE.md` (decisões),
> `API.md` (contrato HTTP), `MESSAGING.md` (filas) e `TESTING.md` (testes). O plano de
> execução e o progresso estão em `docs/CONTEXT.md`.

## Stack

| Responsabilidade | Tecnologia |
|---|---|
| Linguagem | Go 1.27 (`net/http` stdlib) |
| Composição | Uber Fx |
| Persistência | PostgreSQL 17 + `pgx/v5` + `golang-migrate` |
| Mensageria | AWS SQS (LocalStack) via `aws-sdk-go-v2` |
| Autenticação | Keycloak (OIDC, `client_credentials`) |

## Pré-requisitos

- Go 1.27+
- Docker + Docker Compose
- Make (opcional; os comandos equivalentes estão documentados no Makefile)

## Início rápido

```sh
cp .env.example .env
make up          # sobe postgres, keycloak e localstack
make migrate-up  # aplica migrations
make kc-token CLIENT=provider-a   # access token OIDC de um provedor de teste
```

> **Roles do PostgreSQL:** migrations rodam como `app` (dona do schema, via
> `DATABASE_URL` no Makefile); a aplicação conecta como `wager_app`, role sem
> escrita no ledger (append-only) e sem DDL. Ver `migrations/000006_create_roles.up.sql`.

> **Keycloak (auth):** o realm `wagering` é importado no boot do container a
> partir de `deploy/keycloak/` (roles `wagering:provider`, `wagering:internal`,
> `wallet:internal`; clients `provider-a`, `provider-b`, `wagering-internal` e
> `wager-api`). `make kc-token CLIENT=<client>` emite um token de teste por
> `client_credentials`. A API valida `Bearer` JWT via JWKS (assinatura RS256,
> `iss`, `aud`, `exp`) e aplica a matriz de `docs/API.md` §2. Detalhes em
> `deploy/keycloak/README.md` e `docs/API.md`.

## Testes

```sh
make test          # go test ./...
make test-race     # go test -race ./...
make test-integration
make test-e2e
make vet && make fmt
```

> Os testes de integração (e e2e) assumem infraestrutura up
> (`make up`: PostgreSQL, LocalStack, Keycloak), mas o serviço `app` do
> compose deve estar **parado** (`docker compose stop app`): os loops de
> outbox/consumidor/worker de referências do container compartilham o mesmo
> banco e as mesmas filas SQS, e instâncias externas ativas deixam os testes
> de lease/claim (ex.: `TestOutboxCrashAfterClaimPublisherRecovers`) e as
> pendências do `TestReferenceWorker*` não determinísticos.
> O `TestMain` já purga as filas e zera as tabelas no início da suíte.

## Comandos úteis

```sh
make down          # derruba os containers
make logs          # logs em streaming
make migrate-down  # reverte a última migration
```

## Status de implementação

**M6 (worker de referências) e M8 (Fx, shutdown e observabilidade) completos** — worker em
`internal/infra/referenceworker` (papel `APP_ROLES=reference-worker`): resolução tardia de
`PENDING_REFERENCE` com `FOR UPDATE SKIP LOCKED`, retry com backoff exponencial
(`APP_REFERENCE_WORKER_*`), TTL/limite de tentativas → `REFERENCE_NOT_FOUND` e varredor de
`PENDING` órfãos. Grafo Fx final em `internal/app/bootstrap`, papéis por `APP_ROLES`, shutdown
ordenado (`APP_SHUTDOWN_TIMEOUT`), logs JSON com correlação (`X-Correlation-Id` no HTTP,
`messageId` no SQS) e métricas Prometheus em `APP_METRICS_ADDR` (default `:9090`, `/metrics`,
inclui `reference_resolutions_total`). Envelope e contratos das filas em `docs/MESSAGING.md`.
Próximo marco: **M9 — multi-instância e falhas**. Detalhes em `docs/CONTEXT.md`.

<!-- Preencher no M10: env completo, filas, migrations detalhadas, exemplos de curl autenticados,
     procedimentos de integração/e2e e validação em clone limpo. -->