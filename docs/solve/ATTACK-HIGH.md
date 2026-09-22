# Plano de ataque — itens de prioridade alta (IMPROVEMENTS.md A1–A4)

Fonte de análise: `docs/solve/IMPROVEMENTS.md`. O ataque cobre os 4 itens de
prioridade alta. Cada item traz: causa raiz, desenho do fix, arquivos, testes
novos e validação. Status atual no fim.

## A1 — JWKS envenena-se na primeira falha

**Causa:** `ensureKeys` (`internal/interfaces/httpapi/jwks.go:165-168`) usa
`sync.Once` que memoiza também o **erro**. Keycloak fora do ar na 1ª verificação
→ `initErr` cacheado → todo request vira `ErrUnauthenticated` até o restart.

**Fix (desenho):** trocar `initOnce/sync.Once` por mutex + flag `initialized`
que só é setada no sucesso; em falha, registrar `nextInitAttempt = now+backoff`
e retornar `ErrUnauthenticated` sem rede até lá (evita martelar o Keycloak
morto a cada request). `refresh` continua reutilizado por `publicKey`/`verifySignature`.

**Arquivo:** `internal/interfaces/httpapi/jwks.go` (struct + `ensureKeys`).

**Testes novos:** `jwks_test.go` — servidor JWKS httptest que responde 500 nas
N primeiras chamadas e depois serve a chave; verifier lazy:
1. 1ª `Verify` com o servidor 500 → `ErrUnauthenticated`.
2. Transcorrido o backoff (encurtado no teste / configurável), servidor OK →
   `Verify` válido passa (prova recuperação sem restart).

**Validação:** `go test ./internal/interfaces/httpapi/...`, suite unit completa.

## A2 — Backoff do consumidor SQS truncado → visibility 0

**Causa:** `consumer.go:537-558` subtrai jitter com piso 1s sobre `BackoffBase=1s`
→ `delay` chega a 0; `VisibilityTimeout: int32(delay/time.Second)` (`:445`) vira
0 → reentrega imediata, `ApproximateReceiveCount` dispara, falha transitória
estoura `MaxReceiveCount` e vai para a DLQ sem merecer (+ busy-wait).

**Fix (desenho):** piso de **1 segundo** no `delay` dentro de `backoff()` e
conversão p/ segundos com arredondamento para cima na `retry()` (ceil), para o
truncamento nunca gerar 0.

**Arquivo:** `internal/infra/sqsconsumer/consumer.go`.

**Testes novos:** primeiro unit do pacote (hoje só tem integração): `consumer_test.go`
construindo `Consumer{c.cfg.BackoffBase=1s, BackoffMax=X}` e exercitando
`backoff(count)` p/ vários counts: assert `delay >= 1s` sempre. (Jitter é
aleatório; o invariante do piso é o que importa.)

**Validação:** unit `./internal/infra/sqsconsumer/...` + suite de integração
(comporta redrive).

## A3 — `MarkFailed` da outbox sem guarda de estado

**Causa:** `outbox.go:90-103` faz `UPDATE ... WHERE event_id = $1` sem
`AND published_at IS NULL` (assimétrico com `MarkPublished`, `:74-87`). Um
publisher com lease vencido (late mark) incrementa `attempts` e sobrescreve
`next_attempt_at` de evento já publicado por outra instância.

**Fix (desenho):** adicionar `AND published_at IS NULL`. Com 0 linhas →
`ErrNotFound` (que `publisher.go` já trata como "evento publicado entre claim e
mark" — conferir no mark). Ownership estável de instância (`locked_by`) fica
fora do corte; nota registrada.

**Arquivo:** `internal/infra/postgres/outbox.go`.

**Testes novos:** `test/integration/postgres_repos_test.go` — insert de evento,
claim, `MarkPublished`, depois `MarkFailed` → espera `ErrNotFound` e
`attempts`/`next_attempt_at` inalterados.

**Validação:** suite de integração (`-tags=integration -race`).

## A4 — Credenciais SQS hardcoded

**Causa:** `sender.go:35-46` força `Optional credenciais estáticas "test"/"test"`
+ `WithBaseEndpoint` sempre → anula a cadeia default (env/IRSA/IAM); envs
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (compose/.env) são mortos. Sem
caminho de produção para AWS real.

**Fix (desenho):** cadeia default (região + endpoint opcional). Credencial
estática `test/test` **apenas** quando aponta p/ emulador (endpoint setado) e
não há credenciais explícitas no ambiente (`AWS_ACCESS_KEY_ID`/`AWS_PROFILE`).
Em produção (sem endpoint) → IRSA/IAM/env normal.

**Arquivo:** `internal/infra/sqs/sender.go`.

**Testes novos:** coberto pela suite de integração (sobe contra LocalStack);
não há fake para creds — valida comportamento real.

**Validação:** build + suite de integração com LocalStack up.

## Status

- [x] Doc de análise: `docs/solve/IMPROVEMENTS.md`
- [x] A1 JWKS — implementado (`ensureKeys` com mutex + backoff, sem memo de erro), teste `TestJWKSVerifierLazyRecoversAfterInitialFailure`
- [x] A2 backoff — piso 1s + ceil na visibilidade, unit `TestBackoffNeverBelowOneSecond`/`TestBackoffRespectsCeiling` (primeiros tests do pacote)
- [x] A3 MarkFailed — `AND published_at IS NULL`, integração `TestOutboxLateMarkFailedDoesNotRegressPublished`
- [x] A4 creds SQS — cadeia default + fallback "test/test" só p/ emulador sem creds explícitas
- [x] Validação final: gofmt limpo, vet OK, unit `-race` verde, integração `-race` verde (54s), teste A3 isolado OK