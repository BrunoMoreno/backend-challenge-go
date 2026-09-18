# Mensageria (SQS) e eventos

> Contratos de entrada (`WagerTransactionRequested`) e saída (eventos da outbox). Decisões gerais em `ARCHITECTURE.md`.

## 1. Filas

| Fila | Tipo | Uso |
|---|---|---|
| `wager-transactions.fifo` | FIFO | entrada de operações |
| `wager-transactions-dlq.fifo` | FIFO | mensagens mortas (redrive `maxReceiveCount=5`) |
| `wager-events.fifo` | FIFO | eventos de saída (destino da outbox) |

Provisionamento automático no init do LocalStack/MiniStack (`deploy/localstack/`), incluindo redrive e políticas.

## 2. Entrada: `WagerTransactionRequested`

Envelope:
```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a", "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "…", "walletId": "…", "roundId": "round-987", "gameId": "fortune-chimp",
    "kind": "BET", "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```
- Identidade durável para inbox: `messageId` (com `consumerName`). O hash da mensagem é verificado em reentregas.
- Idempotência financeira: `data.idempotencyKey`, com o mesmo hash de negócio do HTTP (exclui `messageId`, `occurredAt`, `idempotencyKey`).
- `MessageGroupId = walletId` (ordem por carteira; carteiras distintas em paralelo).
- `MessageDeduplicationId = messageId`. A dedup do FIFO **não** é garantia financeira; valem inbox + constraints.

## 3. Processamento por mensagem

1. Valida envelope e `type`; malformada → §4.
2. Uma transação SQL: `inbox insert (ON CONFLICT)` → caso de uso compartilhado com HTTP → `inbox complete` → outbox.
3. Após o commit: `DeleteMessage`.

| Resultado | Ação na fila |
|---|---|
| Sucesso | apaga |
| Inbox já concluída | apaga |
| Rejeição de negócio commitada (terminal) | apaga |
| `PENDING_REFERENCE` persistida | apaga (o worker de referências assume) |
| Falha transitória (banco indisponível, timeout, deadlock) | não apaga; `ChangeMessageVisibility` com backoff por `ApproximateReceiveCount` |
| Mesmo `messageId` com hash diferente | permanente → DLQ |
| Mensagem malformada / `OPENING` / tipo desconhecido | permanente → publica na DLQ com atributo do motivo e apaga |
| Tentativas esgotadas | redrive automático para a DLQ |

## 4. Parâmetros

| Parâmetro | Valor |
|---|---|
| Visibility timeout | 30 s, com heartbeat (extensão) enquanto processa |
| Long polling | 20 s |
| `maxReceiveCount` | 5 |
| Backoff de retry | exponencial com jitter, teto 15 min (`ChangeMessageVisibility`) |
| Concorrência do consumidor | configurável (`SQS_CONSUMER_CONCURRENCY`) |

## 5. Shutdown (`SIGTERM`)

Para de buscar novas mensagens → aguarda as em voo até o prazo → o que não terminar tem visibilidade liberada (`ChangeMessageVisibility 0`) para reentrega segura. Reentrega é segura porque a inbox e as constraints tornam o reprocessamento idempotente.

## 6. Saída: eventos da outbox

Envelope:
```json
{
  "eventId": "…", "eventType": "WalletBalanceChanged", "aggregateId": "<walletId|transactionId>",
  "correlationId": "…", "causationId": "…", "occurredAt": "2026-09-08T12:00:00Z",
  "version": 1, "data": { }
}
```

| Evento | Gatilho | `aggregateId` |
|---|---|---|
| `WagerTransactionProcessed` | conclusão com sucesso (inclui `LOSS` e `OPENING`) | `transactionId` |
| `WagerTransactionRejected` | rejeição definitiva por regra de negócio (inclui expiração de referência) | `transactionId` |
| `WalletBalanceChanged` | alteração efetiva do saldo (nunca em `LOSS`) | `walletId` |
| `WagerTransactionPendingReference` | registro de espera pela referência | `transactionId` |

`WalletBalanceChanged.data`: `walletId, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion`. Eventos de origem interna (`OPENING`) não carregam metadados externos inaplicáveis.

Publicação: `MessageGroupId = aggregateId`; `MessageDeduplicationId = eventId`; atributo de mensagem `eventType` para roteamento. Republicação preserva o `eventId`. Consumidores devem deduplicar por `eventId` (entrega at-least-once).
