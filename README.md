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

## Documentação da API

Com o serviço em execução, a especificação OpenAPI está em
`http://localhost:8080/openapi.yaml` e a interface Swagger UI em
`http://localhost:8080/swagger/`. A UI carrega seus assets do CDN oficial do
Swagger; a especificação segue acessível para importação mesmo sem internet.

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
> O `TestMain` já purga as filas, recria a fila do cenário de crash, mata
> órfãos de execuções abortadas e zera as tabelas no início da suíte.
> Use `-count=1` nas suítes e2e/integração para não reutilizar cache.

## Comandos úteis

```sh
make down          # derruba os containers
make logs          # logs em streaming
make migrate-down  # reverte a última migration
```

## Status de implementação

**M0–M9 completos** — domínio puro (`Money`, ledger, máquina de estados), PostgreSQL (ledger
append-only com constraint-trigger deferida no commit, inbox/outbox, roles de banco), casos de uso
com idempotência persistente (`idempotentReplay`), HTTP+Keycloak (JWT/JWKS), outbox com lease
`FOR UPDATE SKIP LOCKED`, worker de referências com resolução tardia e backoff exponencial, consumidor
SQS com inbox transacional/retry/DLQ, grafo Fx com papéis por `APP_ROLES` e shutdown ordenado, e
**harness multi-instância** (`test/e2e`, build tag `faultinject`): 3 processos independentes cobrindo
disputa 100.00 × 2×80.00, 50 envios idênticos, crash do consumidor pós-commit pré-delete com
reentrega sem duplicação e conferência final saldo × ledger + reconciliação. Suíte verde e
determinística com `-race` (`-count=1`). Ao longo da validação, corrigidos 3 bugs em
`internal/infra/postgres/ledger.go` (500 no `GET /wallets/{id}/ledger`): scan de named type via `pgx`,
parâmetro SQL não usado e sentinela de cursor incompatível com o zero do Go.

Convenção de commits `ADD`/`TEST`/`FIX`; progresso e pendências (4.7, 9.5) em `docs/CONTEXT.md`.

Principais variáveis de ambiente em `.env.example`; filas provisionadas por `deploy/localstack/init/queues.sh`
e detalhes de contrato em `docs/MESSAGING.md`. Matriz de autorização, rotas e códigos de erro em `docs/API.md`.
