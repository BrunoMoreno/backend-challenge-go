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
```

> **Roles do PostgreSQL:** migrations rodam como `app` (dona do schema, via
> `DATABASE_URL` no Makefile); a aplicação conecta como `wager_app`, role sem
> escrita no ledger (append-only) e sem DDL. Ver `migrations/000006_create_roles.up.sql`.

## Testes

```sh
make test          # go test ./...
make test-race     # go test -race ./...
make test-integration
make test-e2e
make vet && make fmt
```

## Comandos úteis

```sh
make down          # derruba os containers
make logs          # logs em streaming
make migrate-down  # reverte a última migration
```

## Status de implementação

M2 (persistência) em andamento — veja `docs/CONTEXT.md` para a fase atual e próximas tarefas.

<!-- Preencher no M10: env completo, filas, migrations detalhadas, exemplos de curl autenticados,
     procedimentos de integração/e2e e validação em clone limpo. -->