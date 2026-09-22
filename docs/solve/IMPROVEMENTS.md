# Análise de melhorias — backend-challenge-go

Levantamento feito em 2026-09-21 por exploração dirigida do código (4 recortes: HTTP/domínio, infra postgres/SQS, platform/bootstrap, e suíte de testes). Cada item foi conferido contra o fonte com `arquivo:linha`. Alvos são cumulativos: corrigir primeiro a **Prioridade alta** (bugs de disponibilidade/corretude), depois média e baixa.

## Prioridade alta — bugs de disponibilidade/corretude

> **Resolvido** (mesma branch): os quatro itens A1–A4 foram implementados e
> validados — unit `-race` verde, suíte de integração `-race` verde (54s),
> vet/gofmt limpos. Detalhes e testes novos em `docs/solve/ATTACK-HIGH.md`.

### A1. JWKS envenena-se na primeira falha
- `internal/interfaces/httpapi/jwks.go:165-168`
- `ensureKeys` usa `sync.Once` para memoizar também o **erro**. Se a primeira verificação do processo ocorrer com o Keycloak fora do ar, `initErr` fica cacheado para sempre e toda requisição vira `ErrUnauthenticated` até reiniciar.
- **Fix:** memoizar só o sucesso; em falha, liberar a tentativa para a próxima requisição (com backoff curto para não martelar o Keycloak morto).

### A2. Backoff do consumidor SQS truncado para 0 → visibility 0
- `internal/infra/sqsconsumer/consumer.go:549-557` + `:445`
- Com `BackoffBase=1s`, `delay -= jitter(maxJitter)` (jitter mínimo de 1s) leva `delay` a 0; `VisibilityTimeout: int32(delay/time.Second)` vira 0 → a mensagem é reentregue imediatamente. `ApproximateReceiveCount` sobe a cada ciclo → falha transitória estoura `maxReceiveCount=5` e vai para a DLQ sem merecer, além de busy-wait no worker.
- **Fix:** piso de 1s no `delay` e arredondar para cima na conversão p/ segundos.

### A3. `MarkFailed` da outbox sem guarda de estado — corrida de lease vencido
- `internal/infra/postgres/outbox.go:90-103`
- `MarkFailed` faz `UPDATE ... SET attempts = attempts+1, next_attempt_at = $2 WHERE event_id = $1` **sem** `AND published_at IS NULL` (assimetria com `MarkPublished`, `:74-87`, que tem a guarda). Um publisher com lease expirado (late mark de ciclo anterior) corrompe `attempts`/`next_attempt_at` de evento já publicado.
- **Fix:** adicionar `AND published_at IS NULL` (mínimo). Melhor ainda: guarda de ownership com token de instância (`locked_by` está escrito mas nunca lido, `:46`).

### A4. Credenciais SQS hardcoded — sem caminho de produção na AWS
- `internal/infra/sqs/sender.go:36-41`
- `NewClient` força `credentials.NewStaticCredentialsProvider("test","test","")` e `WithBaseEndpoint` sempre. Anula a cadeia default (env vars, IRSA, IAM role); `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` de `.env.example:19-20` e `docker-compose.yml:72-73` são **mortos**. Qualquer deploy real assinaria com `test:test` e falharia.
- **Fix:** usar só quando `endpoint` aponta para emulador local; caso contrário cadeia default. Ou: confiar na cadeia default e remover o provider estático (LocalStack aceita qualquer credencial).

## Prioridade média — robustez e observabilidade

### M1. Heartbeat SQS sem timeout (goroutine leak / shutdown travado)
- `consumer.go:514,524` — `ChangeMessageVisibility(context.Background(), ...)` sem deadline; broker pendurado retém por minutos (retryer do SDK) → `defer { hbCancel(); <-hbDone }` (`:209-212`) trava e o `Run` não encerra; shutdown só escapa via `fx.StopTimeout`.
- **Fix:** `context.WithTimeout(..., min(intervalo, 5s))` em toda chamada, logar timeout.

### M2. Roles engolem erro de setup e o app sobe "sadio" com o papel morto
- `bootstrap.go:139-143` (`sqs.NewSender`), `:202-208` (`sqs.NewClient` do consumidor) — erro do papel só loga e o `fx.Invoke` retorna nil; `/health/ready` continua verde.
- **Fix:** propagar o erro do invoke (abortar boot) ou mover construção do cliente p/ `fx.Provide` que falha rápido; readiness do papel `outbox-publisher` deve checar a fila de eventos.

### M3. Reversão concorrente → 500 em vez de 422
- `processwager.go:383` — `GetReversalForReference` lê sem lock; duas reversões concorrentes passam, segunda `UpdateTerminal` viola o índice único parcial `wager_resolved_reference_uniq` → `ErrDuplicate` não mapeado → 500 (`httpapi/errors.go:64-66`).
- **Fix:** `SELECT ... FOR UPDATE` no alvo antes do movimento, ou mapear `ErrDuplicate`/`23505` → `FailureAlreadyReversed`.

### M4. Idempotência burlável por falta de normalização de strings
- `handlers.go:287,390-401` — `Idempotency-Key`, providerId, externalTransactionId, playerId, walletId usados **brutos** (só `TrimSpace` na checagem de vazio). `" key-1 "` ≠ `"key-1"` → linha nova → movimentação duplicada; mesmo no hash do payload.
- **Fix:** `TrimSpace` + limite de tamanho em todos os campos livres antes do hash/persistência (HTTP e SQS `consumer.go:356,385`).

### M5. `isRetryable` manda corrida legítima para a DLQ
- `consumer.go:413-426` — `postgres.ErrOptimisticLock` (de `UpdateTerminal`/`UpdatePendingReference`, RowsAffected==0) e `ErrDuplicate` não são retryable.
- **Fix:** incluir ambos em `isRetryable` (são transitórios por natureza).

### M6. Sem `statement_timeout`/`lock_timeout` no Postgres
- `postgres/pool.go:29-39` (DSN puro), `httpapi.go:19-26` (sem deadline no handler), `referenceworker/worker.go:98-120` (batch sem lock_timeout).
- Cliente que conecta e pausa segura `wallets FOR UPDATE` + conexão do pool indefinidamente → trava o worker e, com pool compartilhado, o consumer.
- **Fix:** `options=-c lock_timeout=5s -c statement_timeout=30s -c idle_in_transaction_session_timeout=30s` no DSN/config + deadline por request no handler.

### M7. Lease da outbox < pior caso do batch
- `outboxpublisher/publisher.go:52-63` (`Lease=30s`, `SendTimeout=10s`, `BatchSize=10`) — claim seta `locked_until = now()+lease` p/ o batch inteiro; envio sequencial de 10 com timeout de 10s pode terminar ~100s depois → segunda instância re-claima no meio e double-publish (mascarado hoje pelo dedup FIFO + guarda do `MarkPublished`). `next_attempt_at` também é postergado pro batch todo → crash deixa pendentes retidos por 30s.
- **Fix:** validar `Lease ≥ BatchSize×SendTimeout`, ou renovar o lease por evento antes do envio (com guarda de ownership), ou claim por evento.

### M8. HTTP server sem timeouts completos
- `httpapi.go:21-25`, `bootstrap.go:345-349` — só `ReadHeaderTimeout`. Sem `ReadTimeout`/`WriteTimeout`/`IdleTimeout` (slowloris, keep-alive sem teto, SQL bloqueado segura goroutine).
- **Fix:** `ReadTimeout=15s`, `WriteTimeout=30s`, `IdleTimeout=60s` + `MaxHeaderBytes`.

### M9. JWKS: refresh por `kid` desconhecido (DoS) e sem `singleflight`
- `jwks.go:171-186,153` — `kid` fora do cache dispara GET síncrono ao Keycloak por request (5s de bloqueio por goroutine). Sem admission control.
- **Fix:** só refresh quando `!fresh()`; kid desconhecido com cache fresco → `ErrUnauthenticated` sem rede; `singleflight.Group` + serve-stale-while-revalidate.

### M10. Readiness vaza detalhe interno e não indica draining
- `httpapi.go:33-34` — 503 `NOT_READY` com `err.Error()` (host/mensagens de rede). Sem flag de draining: no `OnStop` o pod segue verde no `/ready` até fechar o listener.
- **Fix:** logar o detalhe no servidor, responder código genérico; atomic `draining` no início do `OnStop` → `/ready` 503.

### M11. Outbox: índice da claim não serve o sort
- `postgres/outbox.go:44-56` + `migrations/000005 ... .up.sql:17` — filtro em `locked_until`/`next_attempt_at` + `ORDER BY occurred_at FOR UPDATE SKIP LOCKED`; índice parcial cobre só `next_attempt_at`. Heap-fetch + sort do backlog a cada poll.
- **Fix:** `CREATE INDEX ... (occurred_at, next_attempt_at) WHERE published_at IS NULL`.

### M12. Log de errors/resultados ausente em caminhos quentes
- `consumer.go:288` `_ = res` (descarta o estado PROCESSED/REJECTED); `dead()` incrementa antes do envio à DLQ e falha só loga (`:454-488`); claim da outbox sem log (`publisher.go:99-157`); log storm por ciclo com broker/DB fora (sem backoff entre ciclos).
- **Fix:** log de resultado por mensagem; métrica `sqs_dlq_send_failures_total`; gauge `outbox_backlog`; backoff no loop de erro.

## Prioridade baixa — manutenção e DX

### L1. Backoff implementado duas vezes
`consumer.go:537-558` vs `platform/backoff/backoff.go:19-40` (já divergem: jitter floor 1s vs 30ms). Unificar; consumidor deve chamar o pacote compartilhado.

### L2. Duplicação de helpers SQL e config de workers
Helpers `nullable`/`zeroable`/`nilString`/`timeOrNil` repetidos em `outbox.go:126-138` e `wager_transactions.go:262-290` → consolidar em `scan.go`. Config triplicada com defaults que já divergem (`config.go:101` 1s vs `publisher.go:56` 500ms; `:114` vs `worker.go:52`).

### L3. Índice `NULLS FIRST` vs índice ASC
`wager_transactions.go:236-244` ordena `next_attempt_at NULLS FIRST`, índice `(state, next_attempt_at)` ASC/NULLS LAST → re-sort por ciclo. Index para casar.

### L4. Config engole erro de parse
`config.go:179-201` `envOrInt` retorna default silenciosamente em `APP_OUTBOX_BATCH_SIZE=abc`; `validate()` não exige presença de `APP_DATABASE_URL`/queues por papel. Tornar `Load()` falho-explícito + exigências por papel.

### L5. `.env.example` defasado e secrets em texto claro
`.env.example` omite `APP_SQS_DLQ_URL`, `APP_SQS_MAX_RECEIVE_COUNT`, `APP_SQS_VISIBILITY_TIMEOUT`, `APP_SQS_CONSUMER_*`, `APP_OUTBOX_*`, `APP_REFERENCE_WORKER_*`; inclui `APP_KEYCLOAK_CLIENT_SECRET` que **não é lido em lugar nenhum**. `POSTGRES_PASSWORD: app`, `KEYCLOAK_ADMIN_PASSWORD: admin` hardcoded no compose. Regenerar `.env.example` do struct de config.

### L6. Sem CI, sem lint, `make test` usa cache
Sem `.github/workflows`, sem alvo `lint` (staticcheck), sem `coverage`, sem `-count=1` em e2e/integration.

### L7. Testes — lacunas de cobertura e higiene
- Zero unit de `internal/infra/sqsconsumer/` e `internal/infra/postgres/` (tudo depende de LocalStack/Postgres real).
- Sem teste do JWKS com Keycloak **fora do ar** (degraded mode sem definição).
- `bootstrap_test.go:62,72` usa `os.Setenv` sem restore (usar `t.Setenv`); ports fixos `:18080/:19090` colidem em execuções paralelas no host.
- Zero `t.Parallel()` no unit (fix: fixar lock de `recordingAPI`/contadores de metrics e paralelizar).
- `_ = context.Background()` morto em `metrics_test.go:52`.
- Prioridade de novos testes: outbox send-failure unit, consumer visibility/retry unit, JWKS fetch-failure.

## Ordem de ataque (alta prioridade)

1. **A1** — JWKS `sync.Once` envenenado.
2. **A2** — backoff p/ 0 → visibility 0.
3. **A3** — `MarkFailed` sem guarda de estado.
4. **A4** — credenciais SQS hardcoded.