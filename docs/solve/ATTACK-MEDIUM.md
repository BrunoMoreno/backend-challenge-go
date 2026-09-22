# Ataque de prioridade média — M1–M12

Plano e status dos itens de prioridade média da análise (`docs/solve/IMPROVEMENTS.md`), implementados na branch `fix/medium-priority-improvements` a partir de `main` (`b151aa9`).

## Status

Todos os 12 itens implementados, com testes e suíte validada:

- Unit `-race` verde; `gofmt`/`go vet`/`go build` limpos.
- Integração `-tags=integration -race -count=1` verde (53s).
- Migração `000009` aplicada (`make migrate-up`); `make migrate-down` reverte.

## Itens

### M1. Heartbeat SQS sem timeout (goroutine leak / shutdown travado)
- **Fix:** `consumer.go` — `heartbeat` passou a usar `changeVisibility(receipt, timeout)`, helper com `context.WithTimeout` (janela = `min(VisibilityTimeout/2, 5s)`). As duas chamadas de `ChangeMessageVisibility` (release no shutdown e extensão periódica) nunca mais podem segurar o dreno contra um broker pendurado; a falha do release no shutdown agora é logada.

### M2. Roles engolem erro de setup
- **Fix:** `bootstrap.go` — `runOutboxPublisher` e `runSQSConsumer` agora retornam `error`; falha ao criar `sqs.NewSender`/`sqs.NewClient` **aborta o boot** (fx) em vez de subir com o papel morto e `/ready` verde. A readiness foi estendida (`provideReady`): confirmada a fila de **eventos** quando o papel `outbox-publisher` está ativo (além da fila de entrada com `sqs-consumer`), via `sqsQueueReachable`.

### M3. Reversão concorrente → 422 em vez de 500
- **Fix:** `httpapi/errors.go` — `isDuplicateOf` detecta a duplicata do índice `wager_resolved_reference_uniq` (por substring do constraint, já que o `mapError` envolve `ErrDuplicate` com a string sem preservar o `*pgconn.PgError`) e `failSubmit` responde **422 `ALREADY_REVERSED`**. A serialização pelo lock da carteira cobre a maioria dos casos (RFC de `processwager.go:287`); esta é a rede de segurança do caso de corrida entre processadores.

### M4. Idempotência burlável por strings brutas
- **Fix:** campos livres (`providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `referenceExternalTransactionId`, `Idempotency-Key`) são normalizados com `TrimSpace` e validados com limite de 400 (`handlers.go` `parseSubmit` e `consumer.go` `parseRequested`). Sem o trim, `" key-1 "` fabricava chave de idempotência e hash distintos.

### M5. `isRetryable` manda corrida legítima para a DLQ
- **Fix:** `consumer.go` — `postgres.ErrOptimisticLock` (RowsAffected==0) e `postgres.ErrDuplicate` agora são retryable (transitórios por natureza; o commit do concorrente torna a reentrega um replay). Teste unit `TestIsRetryableClassifiesTransientFailures`.

### M6. Sem `statement_timeout`/`lock_timeout` no Postgres
- **Fix:** `postgres/pool.go` — `NewPool` usa `pgxpool.ParseConfig` + `applyDefaultRuntimeParams` que injeta, **se ausentes** (não sobrescreve operator/DSN), `statement_timeout=30s`, `lock_timeout=5s`, `idle_in_transaction_session_timeout=30s`. Um cliente travado não segura lock de carteira/conexão para sempre.

### M7. Lease da outbox < pior caso do batch
- **Fix:** `outboxpublisher.New` garante `Lease ≥ BatchSize×SendTimeout` (clamp com warn), evitando double-publish/ordem FIFO quebrada por estouro de lease no meio do lote. Teste unit `TestNewClampsLeaseToBatchCoverage`.

### M8. HTTP server sem timeouts completos
- **Fix:** `NewServerWithDeps` ganhou `ReadTimeout=15s`, `WriteTimeout=30s`, `IdleTimeout=60s` e `MaxHeaderBytes=1MiB`; o servidor de `/metrics` no bootstrap mesma política.

### M9. JWKS: refresh por `kid` desconhecido (DoS)
- **Fix:** `jwks.go` — `publicKey` agora só dispara refresh para `kid` desconhecido com o cache **fresco** dentro de um throttle (`missingKeyRefreshGrace=5s`, `unknownRefreshAllowed`). Cache vencido continua sempre fazendo refresh (rotação/recuperação). Teste unit `TestJWKSVerifierUnknownKidThrottled` prova que um kid inventado não gera GET por request.

### M10. Readiness vaza detalhe interno e não indica draining
- **Fix:** `handleReady` responde 503 com código genérico (`DRAINING`/`NOT_READY`) e o detalhe do erro fica **no log** do servidor. `Deps.Draining` (`atomic.Bool`, provido via `provideDraining`) marca o desligamento no início do `OnStop` do HTTP — o LB para de rotear antes do listener fechar e é limpo no fim.

### M11. Outbox: índice da claim não serve o sort
- **Fix:** migração `000009` cria `outbox_claim_ready_idx ON (occurred_at, next_attempt_at) WHERE published_at IS NULL` — caminho quente do claim (filter + ORDER BY + SKIP LOCKED) lê o índice. `TestSchemaMigrationsAtLatest` atualizado para 9.

### M12. Log/resultado ausente em caminhos quentes
- **Fix:** `consumer.go` — o `Result` de `ProcessOn` deixa de ser descartado (`_ = res`) e vira um debug estruturado (transactionId/state/failureCode); nova métrica `sqs_messages_total{status="redrive"}` no gateway `maxReceiveCount` e `{status="dlq_failed"}` quando a publicação na DLQ falha (a original não é apagada e volta na reentrega). Texto de help do counter atualizado.

## Fora do escopo deste ataque (registrados em IMPROVEMENTS.md)

- L1–L7 (prioridade baixa). Nota M3/L3: o índice `NULLS FIRST` do worker de referências (`L3`) permanece.