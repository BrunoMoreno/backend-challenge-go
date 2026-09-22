# ARCHITECTURE

> **Este documento define o como.** Entregável do desafio (marco M10), mantido em dia com a implementação. Requisitos em `PRD.md`; contratos externos em `docs/API.md` e `docs/MESSAGING.md`; estratégia de testes em `docs/TESTING.md`; guia de autenticação em `docs/AUTHENTICATION.md`.

## 1. Dinheiro (`Money`) — atende G1

- `Money{minor int64, currency Currency}` imutável; escala fixa 2; moeda ISO 4217 (3 letras maiúsculas).
- Parsing por regex estrita `^(0|[1-9][0-9]*)\.[0-9]{2}$`. Rejeita vazio, `NaN`, `Infinity`, notação científica, sinal, escala diferente de 2. **Sem normalização** (`"25"` e `"25.0"` são inválidos), o que simplifica o hash de idempotência.
- Overflow tratado em parsing, `Add`, `Sub`, `Neg`. Aritmética e comparação exigem moedas iguais.
- Negativos existem só em cálculos internos; o saldo da carteira nunca é negativo.
- Persistência: `BIGINT` em unidades mínimas + `CHAR(3)`. Limite: ±9.223.372.036.854.775.807 centavos.

## 2. Acesso ao banco e transação

- `pgx/v5` (`pgxpool`) com SQL explícito.
- Porta `UnitOfWork.Do(ctx, func(ctx) error)`: a transação viaja no contexto; repositórios usam a tx ativa. O domínio não importa pgx, Fx, HTTP nem SQS.
- Toda I/O recebe `context.Context` com prazo. Erros de domínio via tipos/`errors.Is`/`errors.As`; sem `panic` para regra de negócio.

## 3. Concorrência — atende G3, G6, G7

- **Locking pessimista por carteira:** `SELECT … FROM wallets WHERE id=$1 FOR UPDATE` na transação. Carteiras diferentes não se bloqueiam.
- Defesa no banco: `CHECK (balance_minor >= 0)`, `version` incrementada só quando o saldo muda, `UPDATE … WHERE id=$1 AND version=$2`.
- Ordem de locks fixa: (1) insert da chave de idempotência; (2) lock da carteira; (3) lock da transação referenciada (mesma carteira, já serializada).
- **Constraint trigger deferida** no commit: `wallets.balance_minor` = `balance_after` do último lançamento e `version` coerente.
- Deadlock/serialization failure → rollback e retry limitado; contado na métrica de conflitos.

## 4. Idempotência — atende G2

- `wager_transactions`: `UNIQUE (idempotency_key)` e `UNIQUE (provider_id, external_transaction_id) WHERE origin='EXTERNAL'`.
- Fluxo: `INSERT … ON CONFLICT DO NOTHING RETURNING`. Se não inseriu, lê o existente (o conflito espera commit/rollback do concorrente) e compara `payload_hash`:
  - igual → replay do resultado persistido (`idempotentReplay:true`), com o **saldo do processamento original** (`result_balance_minor`);
  - diferente → conflito de chave;
  - mesmo `(provider, externalId)` com outra chave → conflito de transação externa.
- **Hash:** SHA-256 do JSON canônico (chaves ordenadas, sem espaços) de `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId`. Exclui a chave de idempotência, `messageId`, `occurredAt` e metadados de transporte. Um único código serve HTTP e SQS.
- O servidor nunca substitui a chave recebida por uma calculada.

## 5. Estados da transação

`PENDING → PROCESSED | REJECTED | PENDING_REFERENCE | FAILED`; `PENDING_REFERENCE → PROCESSED | REJECTED | FAILED`. Terminais imutáveis (domínio **e** trigger no banco).

- Operações sem dependência concluem em **uma transação**: insert `PENDING` → aplicar → `PROCESSED`/`REJECTED` → ledger → outbox → commit. Não há commit intermediário de aceite.
- Rejeição de negócio é **commitada** (registro + evento) e reproduzida em replays.
- Falha **transitória** (timeout, conexão, deadlock): rollback, nada persistido, `503`/retry. `FAILED` é só falha permanente de infraestrutura/dados, com `failure_code`, para auditoria.
- Varredor de segurança: `PENDING` órfão do slot de crash do claim (com `reference_external_id`) é retomado por qualquer instância (`FOR UPDATE SKIP LOCKED`), cobrindo qualquer aceite assíncrono futuro. Operações sem referência nunca ficam paradas em `PENDING`: são resolvidas pelo produtor no caminho idempotente.

## 6. Reversões e referências

Resolução por `(providerId, referenceExternalTransactionId)`.

| Situação da referência | Resultado |
|---|---|
| Não encontrada | `PENDING_REFERENCE`, retry exponencial com jitter; esgotou tentativas/TTL → `REJECTED REFERENCE_NOT_FOUND` |
| Existe e `PENDING`/`PENDING_REFERENCE` | permanece `PENDING_REFERENCE` (mesma política de TTL) |
| Existe e `REJECTED`/`FAILED` | `REJECTED REFERENCE_NOT_PROCESSED` imediato |
| Diverge em provedor/jogador/carteira/moeda/rodada/valor | `REJECTED REFERENCE_MISMATCH` |
| Tipo inválido (`REFUND` só sobre `BET`; `ROLLBACK` sobre `BET/WIN/REFUND`) | `REJECTED INVALID_REFERENCE_KIND` |
| Já revertida | `REJECTED ALREADY_REVERSED` |
| Reversão exigiria débito > saldo | `REJECTED REVERSAL_INSUFFICIENT_FUNDS` (≠ `INSUFFICIENT_FUNDS` de `BET`) |

**Resolução tardia (M6, RF-05):** o `reference-worker` (`internal/infra/referenceworker`, papel `APP_ROLES=reference-worker`) reclama em cada ciclo um lote (`APP_REFERENCE_WORKER_BATCH_SIZE`, 10) de pendências vencidas — `PENDING_REFERENCE`, ou `PENDING` com `reference_external_id` — com `FOR UPDATE SKIP LOCKED` no `next_attempt_at` (`ORDER BY next_attempt_at NULLS FIRST`). Todo o ciclo roda em **uma transação**: o lock da linha é o lease (commit encerra a posse), sem tabela de lease separada; clones concorrentes não reclamam a mesma linha. Para cada pendência: expirada (TTL desde `created_at`, 24 h) ou `attempt >= max_attempts` (30) → `REJECTED REFERENCE_NOT_FOUND` (evento na outbox); senão `ResolvePending` re-execução do caso de uso (movimentação + ledger + outbox no mesmo commit), alvo ainda ausente → `PEND_FOR_REFERENCE`/`NextAttempt` com backoff (base `APP_REFERENCE_WORKER_BACKOFF_BASE` 1 s, teto `_MAX` 15 min, jitter 30%) persistido como próximo `next_attempt_at`. Falha em qualquer linha aborta o lote (rollback); a próxima instância assume.

**Regra REFUND × ROLLBACK:** índice único parcial `UNIQUE (resolved_reference_id) WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')`. Cada transação-alvo recebe no máximo uma reversão bem-sucedida, de qualquer tipo, impedindo devolução duplicada do mesmo débito. Consequência: `BET` reembolsada não aceita `ROLLBACK` e vice-versa; `ROLLBACK` de um `REFUND` é permitido (alvo distinto).

`WIN` com referência informada segue a mesma tabela (deve apontar `BET` da mesma rodada); sem referência, processa direto.

## 7. Schema (pontos principais) — atende G3, G5, G8

- `wallets(id, player_id, currency, balance_minor, version, created_at, updated_at)`; `UNIQUE(player_id, currency)`; `CHECK(balance_minor >= 0)`.
- `wager_transactions`: coluna `origin (INTERNAL|EXTERNAL)` com `CHECK`s — externo exige provider, externalId, chave, hash, rodada, jogo e `kind <> 'OPENING'`; interno exige `kind='OPENING'` e esses campos nulos. `UNIQUE(wallet_id) WHERE kind='OPENING'`.
- `wallet_ledger_entries(seq identity, id, wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after, created_at)`: `UNIQUE(wallet_id, transaction_id)`, `CHECK(amount_minor > 0)`, `CHECK` da fórmula por direção, trigger `BEFORE UPDATE/DELETE/TRUNCATE` que aborta e `REVOKE UPDATE, DELETE` do role da aplicação (role de migração separado).
- `inbox(consumer_name, message_id, payload_hash, received_at, completed_at)`, `UNIQUE(consumer_name, message_id)`.
- `outbox_events(event_id PK, aggregate_id, event_type, version, correlation_id, causation_id, payload jsonb, occurred_at, attempts, next_attempt_at, locked_by, locked_until, published_at)`.
- IDs UUIDv7; timestamps UTC. Cursor do ledger = `seq` codificado em base64 (a ordem de `seq` coincide com a de commit por carteira, pois o lock da carteira serializa).

## 8. Autenticação e autorização

- **Keycloak** no Compose com realm importado; `client_credentials` entre serviços. Justificativa: IdP real e padrão de mercado, sem código próprio de credenciais.
- Validação de JWT: JWKS com cache/rotação, `iss`, `aud`, `exp`, `nbf`, algoritmo permitido.
- `providerId` autorizado vem da claim `provider_id` (protocol mapper fixo por client). Roles: `wagering:provider`, `wallet:internal`.
- Provedor não vê dados de outro: consulta alheia devolve **404** (sem vazar existência), inclusive em replays.
- Matriz completa em `docs/API.md` §2.
- Mensageria: credenciais e políticas distintas para produtor/consumidor. Limitação: o envelope SQS não carrega JWT, então a identidade do produtor depende do controle de acesso do broker; o consumidor mantém todas as validações de domínio. O LocalStack não impõe IAM por padrão (documentar).

## 9. Mensageria de entrada e inbox

Detalhes e contratos em `docs/MESSAGING.md`. Regra central: `inbox insert` + caso de uso + `inbox complete` + outbox na **mesma transação**; `DeleteMessage` só depois do commit.

O cenário de crash do consumidor (M9) roda em uma **fila FIFO dedicada** (`wager-crash-test.fifo`, recriada no `TestMain` do e2e) para que nenhuma instância em drenagem de cenários anteriores dispute a mensagem com a instância injetada com `FAULT_AFTER_COMMIT_BEFORE_DELETE`. Observamos que o LocalStack **não requeueia** mensagens cujo consumidor morreu sem delete (at-least-once é garantia do SQS real, não do emulador); a reentrega é então simulada com um retry do produtor — mesmo envelope e `messageId`, `MessageDeduplicationId` novo — que percorre exatamente o caminho de replay (hash idêntico + inbox completa → delete sem reaplicar).

## 10. Outbox

- Publisher com *claim* por lease: `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) SET locked_until = now()+lease RETURNING`. Lease expirada = trabalho abandonado, assumido por outra instância.
- Publica com `eventId` estável (`MessageDeduplicationId = eventId`), depois marca `published_at`. Falha → `attempts++` e backoff exponencial. Consumidores deduplicam por `eventId` (at-least-once).
- Payload é snapshot imutável; tipo e versão definidos pelo construtor do evento.

## 11. Fx e ciclo de vida

- Um binário, papéis por `APP_ROLES=http,sqs-consumer,outbox-publisher,reference-worker`, para escalar instâncias com a mesma imagem.
- O grafo completo — `config`, `logging`, `database`, `auth`, `casos de uso`, `http`, `outbox`, `sqsmsg`, `metrics` — vive em `internal/app/bootstrap.Module()` (usado pelo `cmd/app` e pelos testes de composição, M8.4); construtores via `fx.Provide`, ativação por papel via `fx.Invoke`.
- `OnStart`: pool pgx (ping), verifier JWT lazily (JWKS só na primeira verificação), sobe workers com `context` cancelável.
- `OnStop` (ordem inversa, prazo `APP_SHUTDOWN_TIMEOUT`): parar polling SQS (drenagem) → outbox → `http.Server.Shutdown` + `/metrics` → **por último** fechar o pool (registrado primeiro, logo para por último).

## 12. Observabilidade

- `log/slog` JSON (stderr); correlação carregada no contexto e anotada em cada registro: `X-Correlation-Id` no HTTP (aceito ou gerado) e `messageId` nas mensagens SQS. Sem credenciais nem payload financeiro completo.
- Prometheus em `/metrics` na porta própria (`APP_METRICS_ADDR`, default `:9090`, independente dos papéis): `http_requests_total` + `http_request_duration_seconds` (rota combinada por `r.Pattern`), `sqs_messages_total` (processed/dead/retry/replay), `outbox_events_total` (published/failed) e `reference_resolutions_total` (resolved/rejected/retry/expired/failed).

## 13. Estrutura de pastas

```
cmd/app/main.go
internal/domain/{money,wallet,ledger,wager,events}
internal/app/{openwallet,processwager,query,bootstrap}
internal/infra/{postgres,sqs,sqsconsumer,outboxpublisher,referenceworker} internal/platform/{config,logging,backoff,metrics}
migrations/   deploy/{keycloak,localstack}/   test/integration/
```

## 14. Interpretações adotadas (registrar/confirmar)

1. Entradas `"25"`/`"25.0"` rejeitadas, sem normalização.
2. Uma única reversão bem-sucedida por alvo (`REFUND` **ou** `ROLLBACK`).
3. `WIN` com referência espera a referência como as reversões.
4. Qualquer provedor autenticado pode operar sobre qualquer carteira; o desafio só restringe o acesso às **transações** do provedor. Acesso direto à carteira é só do serviço interno.
5. Identidade do produtor SQS depende do broker (§8).
6. Carteira inexistente é erro de entrada (`404`, nada persistido), pois a transação exige FK para a carteira.
7. `LOSS` exige `0.00` e moeda da carteira; não gera ledger nem muda a versão.

## 15. Limitações e trabalho não concluído

- **LocalStack ≠ SQS real:** não impõe IAM por padrão e **não requeueia** mensagens cujo consumidor morreu sem `DeleteMessage` (fica invisível para sempre). O e2e de crash contorna simulando a reentrega com retry do produtor (§9); em produção, o SQS redrive real entrega novamente após a visibilidade expirar — o fluxo de replay idempotente é o mesmo que validamos.
- **Identidade do produtor SQS** depende do controle de acesso do broker (§8); o consumidor mantém todas as validações de domínio.
- **Consistência saldo × ledger** é imposta no banco: `constraint trigger` deferida valida `wallets.balance_minor` = último `balance_after` do ledger com `version` coerente no COMMIT, e `UNIQUE(wallet_id, transaction_id)` impede lançamento duplicado (incl. após reentrega de uma mensagem já processada).
- **Pendências de teste** (`docs/CONTEXT.md`): 4.7 isolamento de auth/keycloak via integração real e 9.5 indisponibilidade temporária de PostgreSQL/SQS em e2e — previstos, não implementados.
- **Sem tracing distribuído (OTel)** e sem roteamento por partições personalizadas do SQS FIFO; ambos são extensões possíveis.
