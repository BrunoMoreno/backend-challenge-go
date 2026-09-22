# Status de implementação

> Status detalhado, fase a fase, com pendências e bloqueios em `docs/CONTEXT.md` (§6).

**M0–M9 completos e 4.7 concluído** — domínio puro (`Money`, ledger, máquina de estados), PostgreSQL
(ledger append-only com constraint-trigger deferida no commit, inbox/outbox, roles de banco), casos de
uso com idempotência persistente (`idempotentReplay`), HTTP+Keycloak (JWT/JWKS), outbox com lease
`FOR UPDATE SKIP LOCKED`, worker de referências com resolução tardia e backoff exponencial, consumidor
SQS com inbox transacional/retry/DLQ, grafo Fx com papéis por `APP_ROLES` e shutdown ordenado, e
**harness multi-instância** (`test/e2e`, build tag `faultinject`): 3 processos independentes cobrindo
disputa 100.00 × 2×80.00, 50 envios idênticos, crash do consumidor pós-commit pré-delete com
reentrega sem duplicação e conferência final saldo × ledger + reconciliação.

**4.7 — Auth real + isolamento** (`test/integration/auth_integration_test.go`, 14 testes):
token JWT real do Keycloak (`client_credentials`), sem token → 401 em todas as rotas protegidas,
token inválido/tampering → 401, bearer malformado → 401, role errada → 403, `CanSubmitAsProvider`
(provider-a como provider-b → 403), `RequireProviderPath` (path cruzado → 403), isolamento por ID
(outro provedor → 404 anti-enumeração), replay com JWT real sem duplicação de saldo,
401/403 sem efeito financeiro. Suíte `-tags=integration -race -count=1` **verde** (54s).

Durante a validação, corrigidos 3 bugs em `internal/infra/postgres/ledger.go` (500 no
`GET /wallets/{id}/ledger`): scan de named type via `pgx`, parâmetro SQL não usado e sentinela de
cursor incompatível com o zero do Go.

Convenção de commits `ADD`/`TEST`/`FIX`/`DOC`; progresso e pendências (4.7, 9.5) em
`docs/CONTEXT.md`. Matriz de autorização, rotas e códigos de erro em `docs/API.md`.
