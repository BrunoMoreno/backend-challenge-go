# Status de implementação

> Status detalhado, fase a fase, com pendências e bloqueios em `docs/CONTEXT.md` (§6).

**M0–M9 completos** — domínio puro (`Money`, ledger, máquina de estados), PostgreSQL (ledger
append-only com constraint-trigger deferida no commit, inbox/outbox, roles de banco), casos de uso
com idempotência persistente (`idempotentReplay`), HTTP+Keycloak (JWT/JWKS), outbox com lease
`FOR UPDATE SKIP LOCKED`, worker de referências com resolução tardia e backoff exponencial, consumidor
SQS com inbox transacional/retry/DLQ, grafo Fx com papéis por `APP_ROLES` e shutdown ordenado, e
**harness multi-instância** (`test/e2e`, build tag `faultinject`): 3 processos independentes cobrindo
disputa 100.00 × 2×80.00, 50 envios idênticos, crash do consumidor pós-commit pré-delete com
reentrega sem duplicação e conferência final saldo × ledger + reconciliação. Suíte **verde e
determinística** com `-race -count=1` e listener HTTP ligado sincronamente no `OnStart` (readiness
real, RF-11). Suíte de integração RF-11 (`-tags=integration -race`) e unit `-race` verdes no
estado commitado e em clone limpo; PR de entrega aberto.

Durante a validação, corrigidos 3 bugs em `internal/infra/postgres/ledger.go` (500 no
`GET /wallets/{id}/ledger`): scan de named type via `pgx`, parâmetro SQL não usado e sentinela de
cursor incompatível com o zero do Go.

Convenção de commits `ADD`/`TEST`/`FIX`/`DOC`; progresso e pendências (4.7, 9.5) em
`docs/CONTEXT.md`. Matriz de autorização, rotas e códigos de erro em `docs/API.md`.
