# CONTEXT — Desafio Backend: Processamento Distribuído de Apostas

> Contexto do projeto, convenções e **plano de execução com tarefas atômicas**. Atualize o **Status atual** a cada tarefa e mantenha o `README.md` com toda configuração e deploy.

## 1. Mapa dos documentos

| Documento | Responde a |
|---|---|
| `PRD.md` | **O quê**: requisitos (RF), garantias (G), eliminatórios, avaliação, entregáveis |
| `ARCHITECTURE.md` | **Como**: decisões técnicas e interpretações (entregável do desafio) |
| `docs/API.md` | Contrato HTTP, matriz de autorização, códigos de erro/rejeição |
| `docs/MESSAGING.md` | Filas SQS, envelope, retry/DLQ, eventos de saída |
| `docs/TESTING.md` | Estratégia de testes, harness, mapa requisito → teste |
| `CONTEXT.md` (este) | **Quando**: ordem de execução, convenções, progresso |

## 2. Resumo do projeto

Serviço Go (Uber Fx) que processa `BET/WIN/LOSS/REFUND/ROLLBACK` de provedores de jogos por HTTP e SQS, com PostgreSQL, ledger append-only, idempotência persistente, inbox/outbox e Keycloak, correto com múltiplas instâncias e falhas. Stack: Go, `pgx/v5`, Uber Fx, PostgreSQL, SQS (LocalStack/MiniStack), Keycloak, Docker Compose, `testing` + testcontainers.

## 3. Convenções

- **Commits:** `ADD` (funcionalidade nova), `TEST` (testes), `FIX` (correção). Ex.: `ADD: Money value object com parsing estrito`.
- Uma tarefa = um commit atômico, com `gofmt`, `go vet` e `go test ./...` verdes.
- `README.md`, este arquivo e os docs afetados são atualizados no mesmo commit que muda comportamento, configuração ou deploy.
- Nenhum `float32`/`float64` em código que toque dinheiro.
- Domínio sem imports de Fx, HTTP, SQS ou pgx.

## 4. Plano de execução

Legenda: `[ ]` pendente · `[~]` em andamento · `[x]` concluída. Entre parênteses, os requisitos cobertos.

### M0 — Fundação
- [x] 0.1 `ADD` `go mod init`; fixar versão do Go em `go.mod` e Dockerfile multi-stage
- [x] 0.2 `ADD` Docker Compose: postgres, keycloak, localstack/ministack, app; healthchecks
- [x] 0.3 `ADD` Makefile (`up`, `test`, `test-race`, `test-integration`, `test-e2e`, `vet`, `fmt`, `migrate-up/down`)
- [x] 0.4 `ADD` `.env.example`; esqueleto do `README.md`
- [x] 0.5 `ADD` `cmd/app` mínimo com Fx (config + logger + `/health/live`)

### M1 — Domínio puro (G1, G5, RF-04)
- [x] 1.1 `ADD` `Money` (parse, zero, Add/Sub/Neg, Compare, JSON) com overflow
- [x] 1.2 `TEST` `Money` (escala, limites, inválidos, moedas incompatíveis)
- [x] 1.3 `ADD` `Wallet` (New/Rehydrate, Credit/Debit, versão, invariantes)
- [x] 1.4 `ADD` `WalletLedgerEntry` imutável com validação `after = before ± money`
- [x] 1.5 `ADD` `WagerTransaction` (externa/`OPENING`, Rehydrate, transições, failure codes)
- [x] 1.6 `ADD` regras por tipo + política de zero + política de reversão
- [x] 1.7 `ADD` eventos tipados + envelope
- [x] 1.8 `ADD` hash canônico do payload (compartilhado HTTP/SQS)
- [x] 1.9 `TEST` invariantes, estados, hash (ordem de chaves/equivalência), abertura interna

### M2 — Persistência (G3, G5, G8)
- [x] 2.1 `ADD` migrations: wallets, wager_transactions, ledger (+triggers/REVOKE), inbox, outbox
- [x] 2.2 `ADD` roles de migração vs. aplicação; documentar up/down
- [x] 2.3 `ADD` `UnitOfWork` + repositórios pgx
- [x] 2.4 `ADD` constraint trigger deferida saldo × ledger; trigger de terminais imutáveis
- [x] 2.5 `TEST` integração: constraints, imutabilidade, `OPENING` único, negatividade, migrations up/down

### M3 — Casos de uso (G2, G7, RF-01, RF-03..05)
- [x] 3.1 `ADD` `OpenWallet`
- [x] 3.2 `ADD` `ProcessWagerTransaction` síncrono (`BET/WIN/LOSS`) com idempotência e rejeição persistida
- [x] 3.3 `ADD` `REFUND`/`ROLLBACK` com resolução de referência (`ARCHITECTURE` §6)
- [x] 3.4 `ADD` `PENDING_REFERENCE` (persistência + evento)
- [x] 3.5 `TEST` replay, conflito de hash, outra chave para mesma `(provider, extId)`, saldo original no replay
- [x] 3.6 `TEST` corrida 100.00 × 2×80.00 e 50 duplicatas (in-process, `-race`)

### M4 — HTTP + Auth (RF-02, RF-08..10)
- [x] 4.1 `ADD` realm Keycloak importável, clients/identidades de teste, script de token
- [x] 4.2 `ADD` middleware JWT (JWKS, iss/aud/exp) + matriz de autorização
- [x] 4.3 `ADD` handlers de carteira, ledger paginado, envio de operação, consultas de transação
- [x] 4.4 `ADD` mapeamento de erros → `docs/API.md` (inclui 503)
- [x] 4.5 `ADD` reconciliação (snapshot `REPEATABLE READ`)
- [x] 4.6 `ADD` health live/ready
- [ ] 4.7 `TEST` auth real e isolamento entre provedores (consulta e replay), sem efeito colateral em 401/403

### M5 — Outbox (G4, RF-07)
- [x] 5.1 `ADD` publisher com lease, `SKIP LOCKED`, backoff
- [x] 5.2 `ADD` fila `wager-events.fifo`; contratos em `docs/MESSAGING.md`
- [x] 5.3 `TEST` dois publishers; crash commit→publicar e publicar→marcar; `eventId` preservado

### M6 — Worker de referências (RF-05)
- [x] 6.1 `ADD` worker `SKIP LOCKED` com backoff exponencial e `max_attempts`/TTL
- [x] 6.2 `ADD` resolução tardia e rejeição por expiração
- [x] 6.3 `ADD` varredor de `PENDING` antigos
- [x] 6.4 `TEST` referência tardia, reinício no meio, expiração

### M7 — SQS (RF-06)
- [x] 7.1 `ADD` provisionamento das filas, redrive e políticas
- [x] 7.2 `ADD` consumidor (long polling, concorrência limitada, heartbeat)
- [x] 7.3 `ADD` inbox na mesma transação; inválidas e hash divergente
- [x] 7.4 `ADD` retry com backoff e DLQ
- [x] 7.5 `TEST` reentrega, DLQ, inválidas, HTTP×SQS na mesma operação

### M8 — Fx, shutdown e observabilidade (RF-11)
- [x] 8.1 `ADD` módulos Fx finais + `APP_ROLES` + lifecycle
- [x] 8.2 `ADD` shutdown ordenado (`SIGTERM`, drenagem/liberação)
- [x] 8.3 `ADD` métricas Prometheus + logs JSON com correlação
- [x] 8.4 `TEST` composição Fx (`ValidateApp`, start/stop, recursos liberados)

### M9 — Multi-instância e falhas
- [x] 9.1 `ADD` build tag `faultinject` e pontos de crash
- [x] 9.2 `ADD` harness com 3 processos independentes
- [x] 9.3 `TEST` cenários 1–8 do desafio (`docs/TESTING.md` §4)
- [x] 9.4 `TEST` conferência final saldo × ledger e reconciliação
- [ ] 9.5 `TEST` PostgreSQL/SQS indisponíveis temporariamente

### M10 — Documentação e entrega
- [x] 10.1 Finalizar `ARCHITECTURE.md` (incl. limitações e trabalho não concluído)
- [x] 10.2 `README.md` reproduzível (pré-requisitos, env, filas, migrations up/down, `curl` autenticados, testes)
- [x] 10.3 Validar em clone limpo: `docker compose up --build`, `go test ./...`, `-race`, `go vet`, `gofmt -l`
- [ ] 10.4 (opcional) partidas dobradas, tracing OTel, teste de carga reproduzível

## 5. Decisões tomadas

- **Versão do Go:** `go 1.27` (`go.mod` e `golang:1.27-alpine` no Dockerfile).
- **Roteador HTTP:** `net/http` stdlib (padrão método+eixo do Go 1.22+), sem dependência de roteador.
- **Migrations:** `golang-migrate`.
- **Emulador SQS:** LocalStack (compatível com SDK v2).
- **SDK SQS:** `aws-sdk-go-v2`.
- **Roles de banco:** `app` (migração, dona do schema) vs `wager_app` (aplicação, sem escrita no ledger e sem DDL), criados na migration `000006` (`wager_app` por padrão no Compose/.env).
- **Interpretações `ARCHITECTURE.md` §14: todas as 7 confirmadas** — sem normalização monetária; 1 reversão por alvo; `WIN` com referência segue reversões; provedor opera qualquer carteira (consulta a transações isolada); identidade SQS depende do broker; carteira inexistente = `404`; `LOSS` `0.00` sem ledger/versão.
- **Integridade no banco (migration `000008`):** ledger append-only (trigger `BEFORE UPDATE/DELETE`, vale até para o role de migração; `wager_app` não tem privilégio), terminais `PROCESSED`/`REJECTED`/`FAILED` imutáveis (campos de retry continuam mutáveis) e `constraint trigger` deferida no COMMIT valida `balance_minor` = último `balance_after` e `version` = nº de lançamentos + 1. `RAISE` (SQLSTATE `P0001`) é mapeado para `postgres.ErrCheck`; limpeza de teste usa `TRUNCATE` (não dispara triggers).
- **Camada de aplicação (M3):** casos de uso em `internal/app/<caso>` (portas e adaptadores). Portas em `internal/app/storage` (repositórios + `UnitOfWork` + `Database`); adaptador pgx em `postgres.Database` (satisaz `storage.Database`). Ids de wallet/transação/ledger/evento gerados na aplicação (UUID v4 hex, sem dependência externa). Abertura (RF-01): saldo `>0` persiste no mesmo commit carteira (versão 2), `OPENING` `PROCESSED`, crédito no ledger e outbox de `WagerTransactionProcessed` + `WalletBalanceChanged`; saldo `0.00` grava só a carteira (versão 1); `(playerId, currency)` duplicado → conflito.
- **Processo síncrono (3.2+3.3+3.4, RF-03/RF-04, G2):** hash canônico calculado no caso de uso (compartilhado HTTP/SQS, `wager.HashPayload`). Ordem dos locks: (1) claim do `idempotency_key` no INSERT, (2) lock da carteira. `BET` débito com rejeição persistida `INSUFFICIENT_FUNDS` (REJECTED, sem movimento), `WIN` crédito, `LOSS` `0.00` só registra `WagerTransactionProcessed` (sem ledger/versão). Toda mudança de saldo persiste via `UpdateBalance` (`WHERE version = nova−1`, repo M2.3) + `WalletBalanceChanged` na outbox. Replay: `ON CONFLICT (idempotency_key) DO NOTHING` → mesma chave com hash igual devolve o resultado original (saldo = `result_balance`, `idempotentReplay:true`); estado não-terminal (PENDING/PENDING_REFERENCE) devolve o estado atual como replay; hash divergente → `ErrIdempotencyConflict`; o `23505` do índice parcial `(provider, externalId)` só ocorre com chave DIFFERENTE (com chave igual o `ON CONFLICT` não insere), e o 23505 aborta a transação no Postgres → a confirmação é feita em transação nova (linha já commitada) → `ErrExternalConflict`. Referências (3.3+3.4, `ARCHITECTURE` §6): após claim + lock da carteira, `resolveReference` classifica o alvo por `GetByReferenceExternal` — alvo ausente ou não-terminal (PENDING/PENDING_REFERENCE) → `pendReference` persiste `PENDING_REFERENCE` via `UpdatePendingReference` + evento `WagerTransactionPendingReference` na outbox (202, sem movimento de saldo/ledger; resolução posterior fica para o worker de referências, M6); alvo `REJECTED`/`FAILED` → rejeição imediata `REFERENCE_NOT_PROCESSED`; `GetReversalForReference` PROCESSED → `ALREADY_REVERSED` (índice parcial `wager_resolved_reference_uniq`); `wager.ResolveReversal` mapeia mismatch → `REFERENCE_MISMATCH`, kind incompatível → `INVALID_REFERENCE_KIND`. `WIN` com referência segue a mesma tabela (`ValidateWinReference` → `REFERENCE_MISMATCH`/`INVALID_REFERENCE_KIND`; sucesso credita e resolve a referência para a `BET` da mesma rodada). Rejeições definidas passam por `reject` (Reject + `UpdateTerminal` + evento `WagerTransactionRejected`), que também centraliza as injeções de rejeição de todas as branches.
- **Worker de referências (M6, RF-05):** `internal/infra/referenceworker` roda um laço `Run(ctx)` (papel `reference-worker`) que reclama com `FOR UPDATE SKIP LOCKED` um lote de pendências vencidas — `PENDING_REFERENCE`, ou `PENDING` com `reference_external_id` (órfãs do slot de crash do claim, M6.3) — e as resolve na MESMA transação do claim: só o commit encerra a posse (o lock da linha é o lease; sem tabela de lease separada). Resolução usa o caso de uso (struct `processwager.Service.ResolvePending`) reutilizando `applyResolved`/`reject`, então movimenta saldo+ledger+outbox atomicamente com lock da carteira; alvo ainda indisponível → `NextAttempt` com backoff exponencial compartilhado (`internal/platform/backoff`, base 1 s, teto 15 min, jitter 30%) que vence por `next_attempt_at`; TTL desde `created_at` (24 h) ou `attempt >= max_attempts` (30) → rejeição final `REFERENCE_NOT_FOUND` via `RejectPending`. `reference_resolutions_total{status}` (resolved/rejected/retry/expired/failed) alimenta o Prometheus. Operações SEM referência (BET/LOSS comuns) nunca ficam sob a alçada do worker — o produtor as resolve no caminho idempotente; o worker só assume o que o fluxo de referências possui. Retry de deadlock (`40P01`) no `Process` do serviço: o Postgres aborta a transação perdedora e a operação é reexecutada (M3.6 — na corrida de duas BETs o retry rejeita com `INSUFFICIENT_FUNDS`, desfecho esperado).
- **Harness M9 (RF-06/G6/G2):** suíte em `test/e2e` (`//go:build integration && faultinject`). O `TestMain` mata órfãos de execuções abortadas (`pgrep -f 'e2e-bin[^ ]*/app'` + TERM/KILL), apaga+recria fila de entrada, DLQ e a fila dedicada do crash (o `PurgeQueue` não remove mensagens em voo), `TRUNCATE` das tabelas e compila um único binário com `faultinject`. Harness após 3 instâncias com `APP_ROLES`/portas próprias, env base + overrides (helper `overrideEnv` **remove a chave base antes de appending**: no runtime a primeira ocorrência vence — append simples não sobreporia `APP_SQS_QUEUE_URL` nem `FAULT_*`), `waitReady` por `/health/ready` e `SIGTERM` graceful ou `SIGKILL` (o `stop()` não reagenda espera quando o processo já saiu — `cmd.ProcessState != nil`). Para o **crash do consumidor** observamos que o LocalStack não requeueia mensagens de consumidor morto sem delete (invisível para sempre); o teste assume o commit durável (saldo 75.00, 1 wager, inbox completa) e simula a reentrega com retry do produtor (mesmo envelope/`messageId`, dedup novo) que percorre o caminho de replay e é apagado sem reaplicar. Como instâncias de cenários anteriores em drenagem podiam consumir a mensagem do crash antes da instância-fault, o cenário usa uma **fila FIFO dedicada** (`wager-crash-test.fifo`, `APP_SQS_QUEUE_URL` só nas instâncias do crash) — suíte verde e determinística com `-race` (4 execuções `-count=1`).
- **Canal SQS (M7, RF-06/G4):** consumidor em `internal/infra/sqsconsumer` com long polling (20 s), visibilidade 30 s + heartbeat (extensão a cada 15 s; no shutdown `SIGTERM` libera com visibilidade 0), concorrência configurável (`SQS_CONSUMER_CONCURRENCY`, default 4) e reentrega com backoff exponencial + jitter por `ApproximateReceiveCount` (base 1 s, teto 15 min, `maxReceiveCount` 5 → redrive automático para a DLQ). Falhas permanentes (envelope malformado, tipo desconhecido, `OPENING`, hash divergente por `messageId`, e erros de negócio terminais) são publicadas na DLQ com atributos `failureReason`/`failureDetail` e apagadas. Rejeições de negócio commitadas (`REJECTED`), `PENDING_REFERENCE` persistida e replay idempotente são APAGADAS da fila (sucesso durável). `processwager` e o canal compartilham o mesmo caso de uso: `ProcessOn` roda sobre um `UnitOfWork` já aberto (a transação do consumidor faz `inbox insert ON CONFLICT` → caso de uso → `inbox complete` → commit; a outbox já é gravada pelo caso de uso no mesmo tx), enquanto `Process` abre/comita a própria transação para o HTTP e repete `ErrStaleClaim` até 3×. A inbox usa identidade `(consumer_name="process-wager", message_id)`, verifica o hash em reentregas, e mensagens concluídas viraram delete simples (at-least-once idempotente).
- **Composição Fx (M8, RF-11):** o grafo completo vive em `internal/app/bootstrap` (`Module()`), usado pelo `cmd/app` e pelos testes (M8.4). Papéis por `APP_ROLES` decidem os invokes; conexões (pool pgx) são fechadas no OnStop. `fx.ValidateApp` valida a composição sem rede; o pri prove de prontidão pings o PostgreSQL e, no papel de consumidor, confirma a fila de entrada. O scop de shutdown ordenado é `APP_SHUTDOWN_TIMEOUT` (default 30 s); os hooks param em ordem reversa do start: workers SQS/outbox → HTTP → pool. Verifier JWT é construído lazily (`NewLazyJWKSVerifier`): a montagem do grafo e o boot não dependem de o Keycloak estar no ar na inicialização.
- **Observabilidade (M8.3):** todos os logs em JSON estruturado (slog) no stderr, com correlação: HTTP propaga/gera `X-Correlation-Id` (e o anota nos registros da requisição) e o consumidor SQS usa `messageId` como correlação. Métricas Prometheus em `/metrics` na porta própria (`APP_METRICS_ADDR`, default :9090, exposta também no Compose): `http_requests_total`/`http_request_duration_seconds` (rota combinada por `r.Pattern`), `sqs_messages_total` (processed/dead/retry/replay) e `outbox_events_total` (published/failed).

## 6. Status atual

- **Suíte de integração endurecida (diagnóstico de `docs/solve/TEST-INTEGRATION.md`):** corrigidas as três falhas apontadas — (1) `noEvent` fazia `sleep` fixo + `ReceiveMessage` de long-poll que podia amostrar **após** a expiração do lease (2 s) e acusar "durante o lease" uma publicação legítima do novo dono; agora faz polling estrito com long-poll zero e deadline que nunca ultrapassa o fim do lease; (2) pools de teste sem teto explícito saturavam o `max_connections=100` do Compose (com 50 chamadas concorrentes + app residente); agora todo pool da suíte usa `MaxConns=8` via `newTestPool`; (3) consumidor SQS tinha `stop()` sem `defer` — uma `t.Fatalf` no meio fechava o pool com o goroutine vivo (erro "closed pool" vazando entre testes); agora `runConsumer` registra `t.Cleanup(stop)` idempotente imediatamente após o start. Suíte `-tags=integration -race -count=1` **verde** (54s; teste do lease 5/5), unit e vet limpos.
- Durante a validação de clone, corrigida uma última corrida real de readiness: `serveHTTP` no `OnStart` usava `go ListenAndServe()` e retornava antes de o listener existir → `TestBootstrapStartStop` pegava `connection refused` aleatório. Agora o listener é ligado **sincronamente** via `net.Listen` dentro do `OnStart` (`FIX: http: ligar o listener sincronamente no OnStart — readiness real (RF-11)`, `a69822d`); métricas seguem o mesmo padrão.
- **Bloco anterior da suíte de integração esclarecido:** as falhas de `TestBootstrapStartStop` (0.03s/connection refused) e outbox (`53300`) vistas no meio da validação vinham de um **worktree desatualizado** (clone de estado intermediário, antes do `FIX` `a69822d`), e não do estado commitado — no HEAD atual tudo verde.
- Pendências **fora do corte** (não bloqueiam entrega): 4.7 (auth/keycloak integração real de isolamento) e 9.5 (indisponibilidade temporária).
- Última tarefa concluída: 10.3 (validação em clone limpo + corrida de readiness corrigida)
- Próxima tarefa: —
- Bloqueios: —
- **Decisões do harness M9 (registrar em `ARCHITECTURE.md` §9):** o LocalStack **não requeueia** (redrive/reentrega automática) mensagens cujo consumidor morreu sem delete — a reentrega do crash é simulada com um retry do produtor (mesmo envelope e `messageId`, dedup novo) que percorre exatamente o caminho de replay; o cenário de crash roda em **fila FIFO dedicada** `wager-crash-test.fifo` (recriada no `TestMain`) para nenhuma instância em drenagem de cenário anterior disputar a mensagem; overrides de env por instância removem a chave base primeiro (no runtime a primeira ocorrência vence); `TestMain` mata órfãos de execuções abortadas por timeout (`pgrep -f 'e2e-bin[^ ]*/app'` + TERM/KILL) e recria as filas do zero (o `PurgeQueue` não remove mensagens em voo).
- Última tarefa concluída: 9.4 (`TEST` conferência final saldo × ledger e reconciliação)
- Próxima tarefa: 10.1 (finalizar `ARCHITECTURE.md` §15 e revisar) — pendências opcionais para a entrega: 4.7 (auth/keycloak integração) e 9.5 (indisponibilidade temporária)
- Bloqueios: —
