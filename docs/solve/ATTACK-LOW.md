# Ataque à prioridade baixa (L1–L7) — `docs/solve/IMPROVEMENTS.md`

Implementação e validação dos itens de **prioridade baixa** da análise
(`docs/solve/IMPROVEMENTS.md`), na branch `fix/low-priority-improvements`
(criada a partir de `main`; o PR de média — `fix/medium-priority-improvements`
— permanece independente). Cada item mudou só manutenção/DX: nenhuma
alteração de comportamento de produção além de defaults já­alinhados e
config fail-explícito.

## L1 — Backoff implementado duas vezes

- Unificado em `internal/platform/backoff/backoff.go`: `Delay(attempts, base,
  max, jitter, floor)` é a implementação única; `Next` passou a ser um wrapper
  (`floor=30ms`, mesmo comportamento de antes).
- O consumidor SQS (`internal/infra/sqsconsumer/consumer.go`) agora chama
  `backoff.Delay(count-1, base, max, jitter, time.Second)` — o piso de 1s do
  A2/M-médio vive no pacote compartilhado, não mais duplicado.
- **Paridade preservada:** `backoff(count)` antigo == `Delay(count-1, ...)`;
  `TestBackoffNeverBelowOneSecond`/`TestBackoffRespectsCeiling` seguem verdes
  sem alteração.

## L2 — Duplicação de helpers SQL e config de workers

- Novo `internal/infra/postgres/scan.go` consolida `nullable`, `deref` e
  `timeOrNil`; as cópias de `outbox.go` e `wager_transactions.go` foram
  removidas (`zeroableMinor` permanece local, e `marshalPayload`/helpers de
  envelope da outbox ficam onde fazem sentido).
- Os defaults de `PollInterval` do `outboxpublisher.New` e do
  `referenceworker.New` foram alinhados ao default da `config`
  (500ms → `time.Second`): agora `New(...)` com `Config` zero e o caminho
  `Load()` produzem o mesmo comportamento.

## L3 — Índice `NULLS FIRST`

- Migração `000010_create_wager_pending_due_index`:
  `CREATE INDEX wager_pending_due_idx ON wager_transactions
   (next_attempt_at NULLS FIRST) WHERE state IN ('PENDING','PENDING_REFERENCE')`
  — casa exatamente o `ORDER BY next_attempt_at NULLS FIRST` do
  `FindPendingDue` do worker de referências. Down: `DROP INDEX`.
- **Decisão:** o índice antigo `wager_transactions_state_idx` não foi removido
  (evita regressão para queries que filtram por `state` sem sort); a migração
  é aditiva. `TestSchemaMigrationsAtLatest` foi para `version 10`.

## L4 — Config fail-explícito

- `Load()` agora **retorna erro** quando o parse falha (`APP_OUTBOX_BATCH_SIZE=abc`,
  `APP_OUTBOX_SEND_TIMEOUT=tres-segundos`, ...) em vez de engolir o default —
  erros são acumulados e devolvidos via `errors.Join`.
- `validate()` exige por papel: `APP_DATABASE_URL` sempre; `sqs-consumer`
  exige `APP_SQS_QUEUE_URL` + `APP_SQS_DLQ_URL`; `outbox-publisher` exige
  `APP_SQS_EVENTS_QUEUE_URL`. Tests novos:
  `TestLoadRejectsBadNumber`, `TestLoadRejectsBadDuration`,
  `TestLoadRejectsMissingDatabase`, `TestLoadRejectsMissingQueueByRole`;
  os testes de roles existentes foram ajustados para setar o mínimo exigido.
- O harness e2e já fornecia todas as variáveis por instância
  (base em `test/e2e/e2e_test.go:260-273` + overrides), então nada quebrou.

## L5 — `.env.example` regenerado

- `.env.example` reconstruído a partir do struct de config, com os blocos por
  papel (SQS consumer inclui as duas filas + `MAX_RECEIVE_COUNT` +
  `VISIBILITY_TIMEOUT` + `CONCURRENCY` + backoff; outbox com batch/lease/timeout
  e o comentário do invariante `Lease ≥ BatchSize×SendTimeout` do M-médio,
  default `100s`).
- `APP_KEYCLOAK_CLIENT_SECRET` removido — confirmado via `rg` que não há
  leitura do valor em lugar nenhum do código.

## L6 — CI e lint

- `.github/workflows/ci.yml`: checkout + setup-go (versão do `go.mod`) +
  gofmt + `go vet` + `go build` + unit `-race -count=1`, em push para `main`
  e pull requests.
- `Makefile`: alvo `lint` (staticcheck) e `coverage`; `-count=1` nos alvos
  `test-race`, `test-integration` e `test-e2e` (sem cache em CI/local).

## L7 — Higiene e cobertura de testes

- `metrics_test.go`: removido o `_ = context.Background()` morto; cobertura
  dos contadores `redrive`/`dlq_failed` (M-médio) em `TestCollectorsEmit`.
- `internal/infra/sqsconsumer/consumer_test.go`: **units novos de
  visibilidade/heartbeat** com fake da `messageAPI` —
  `TestHeartbeatReleasesOnShutdown` (release com visibility 0 no cancelamento
  do worker), `TestHeartbeatStopsWhenProcessingDone` (sem release no fim
  normal) e `TestHeartbeatExtendsVisibility` (extensão periódica sem release).
- Integração `test/integration/bootstrap_test.go`: `os.Setenv` →
  `t.Setenv` (restaura em cleanup) e as portas fixas `:18080/:19090`
  viraram portas efêmeras via `freePort(t)` (sem colisão em hosts com
  execuções paralelas).

## Validação

- `gofmt -l .` limpo, `go build ./...` e `go vet ./...` ok.
- Unit `go test -race -count=1 ./...` verde (inclui os novos testes de
  consumer/config/metrics).
- Integração `make test-integration` verde contra o Compose local com
  `make migrate-up` em nova base (migração `000010` aplicada; suíte
  completa + `TestSchemaMigrationsAtLatest = 10`).
- Branch baseada em `main`; a migração da média (`000009_create_outbox_claim_index`)
  vive na outra branch — numeração não colide (`009` vs `010`) e a ordem
  pós-merge fica correta. Possíveis conflitos triviais de merge em arquivos
  tocados pelas duas frentes (`outbox.go`, `consumer.go`) serão resolvidos na
  integração.