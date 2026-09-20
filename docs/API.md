# API HTTP

> Contrato externo da API. Decisões internas em `ARCHITECTURE.md`; mensageria em `docs/MESSAGING.md`.

## 1. Convenções

- JSON UTF-8; `Authorization: Bearer <JWT>` em todas as rotas de negócio.
- Dinheiro: `{"amount":"25.00","currency":"BRL"}` (string decimal, escala 2, ISO 4217).
- Timestamps UTC RFC 3339; IDs UUIDv7 (exceto IDs externos, que são strings do provedor).
- Corpo de erro: `{"error":{"code":"...","message":"...","details":[...]}}`.
- `X-Correlation-Id` aceito na entrada (gerado se ausente) e devolvido na resposta.

## 2. Autorização

| Rota | Permissão |
|---|---|
| `POST /wagering/transactions` | role `wagering:provider`; `body.providerId` deve ser igual à claim `provider_id` (senão 403) |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | role `wagering:provider` e `:providerId` = claim (senão 403); interno pode ler qualquer |
| `GET /wagering/transactions/:transactionId` | provedor dono (senão **404**) ou interno |
| `POST /wallets`, `GET /wallets/:id`, `GET /wallets/:id/ledger`, `POST /wallets/:id/reconciliation` | somente `wallet:internal` |
| `GET /health/live`, `GET /health/ready` | públicas |

Acessos negados (401/403/404) não produzem efeito financeiro nem expõem dados.

## 3. Endpoints

### `POST /wallets`
Corpo: `{"playerId":"…","initialBalance":{"amount":"1000.00","currency":"BRL"}}`.
- `201` `{"id","playerId","balance","version":1}`.
- Saldo positivo: cria `OPENING` `PROCESSED`, crédito no ledger e outbox de `WagerTransactionProcessed` + `WalletBalanceChanged` no mesmo commit. Saldo `0.00`: nada disso.
- `409 WALLET_ALREADY_EXISTS` para o mesmo `(playerId, currency)`. Sem idempotência de replay.

### `GET /wallets/:walletId` → `200` `{"id","playerId","balance","version"}`; `404 WALLET_NOT_FOUND`.

### `GET /wallets/:walletId/ledger?cursor=&limit=50`
`limit` 1–200. Resposta: `{"items":[{"id","transactionId","direction","money","balanceBefore","balanceAfter","createdAt"}],"nextCursor":"…|null"}`. Cursor opaco, ordenação estável. `cursor` inválido → `400 INVALID_CURSOR`.

### `POST /wagering/transactions`
Headers: `Idempotency-Key` (obrigatório). Corpo: campos do enunciado; reversões (e `WIN` opcionalmente) incluem `referenceExternalTransactionId`.

| Situação | HTTP | Corpo |
|---|---|---|
| Processada (1ª vez) | 201 | `{"transactionId","status":"PROCESSED","balance","idempotentReplay":false}` |
| Replay de processada | 200 | idem, `idempotentReplay:true`, `balance` do processamento original |
| Rejeição de negócio (inclui replay) | 422 | `{"transactionId","status":"REJECTED","failureCode","idempotentReplay"}` |
| Aguardando referência | 202 | `{"transactionId","status":"PENDING_REFERENCE","idempotentReplay"}` |
| Entrada inválida (corrigível) | 400 | `error.code` da §5 |
| Chave ausente | 400 | `MISSING_IDEMPOTENCY_KEY` |
| Chave reutilizada com conteúdo diferente | 409 | `IDEMPOTENCY_KEY_CONFLICT` |
| Mesmo `(provider, externalId)` com outra chave | 409 | `EXTERNAL_TRANSACTION_CONFLICT` |
| Carteira inexistente | 404 | `WALLET_NOT_FOUND` |
| Sem/inválido/expirado token | 401 | `UNAUTHENTICATED` |
| Sem permissão / `providerId` ≠ identidade | 403 | `FORBIDDEN` |
| PostgreSQL indisponível / timeout | 503 + `Retry-After` | `UNAVAILABLE` |
| Concorrência transitória (stale claim em outro processador) | 503 + `Retry-After: 1` | `UNAVAILABLE` (cliente deve repetir) |

Replay de `PENDING_REFERENCE` devolve 202 com o estado atual; se já mudou para terminal, devolve o resultado terminal correspondente.

### `GET /wagering/transactions/:id` e `GET /providers/:pid/wagering/transactions/:extId`
`200` com `{"transactionId","providerId","externalTransactionId","kind","status","failureCode","money","referenceExternalTransactionId","resolvedReferenceTransactionId","attempts","nextAttemptAt","balance","createdAt","updatedAt"}` (campos aplicáveis ao estado). Permite acompanhar pendências e consultar códigos de rejeição/falha. `404 TRANSACTION_NOT_FOUND` quando não existe ou, no caso por ID, pertence a outro provedor.

### `POST /wallets/:walletId/reconciliation`
`200` `{"walletId","storedBalance","calculatedBalance","difference","consistent","checkedEntries"}`. Lê saldo e ledger em um único snapshot (`REPEATABLE READ`, somente leitura); `difference = stored − calculated`; nunca altera saldo. Divergência gera log de erro e incrementa métrica.

### Health
`GET /health/live` → `200` se o processo responde. `GET /health/ready` → `200` se PostgreSQL responde (probe do SQS entra com o consumidor, M7); senão `503` com o componente falho (`NOT_READY`).

## 4. Códigos de rejeição (`failureCode`, persistidos, definitivos)

| Código | Quando |
|---|---|
| `INSUFFICIENT_FUNDS` | `BET` sem saldo suficiente |
| `REVERSAL_INSUFFICIENT_FUNDS` | reversão exigiria debitar além do saldo |
| `REFERENCE_NOT_FOUND` | referência não apareceu até esgotar tentativas/TTL |
| `REFERENCE_NOT_PROCESSED` | referência terminou `REJECTED`/`FAILED` |
| `REFERENCE_MISMATCH` | provedor, jogador, carteira, moeda, rodada ou valor divergem |
| `INVALID_REFERENCE_KIND` | tipo da referência incompatível com a operação |
| `ALREADY_REVERSED` | alvo já tem reversão bem-sucedida |
| `PLAYER_MISMATCH` | `playerId` ≠ dono da carteira |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da carteira |

`FAILED` usa `failureCode` de infraestrutura (ex.: `INTERNAL_INVARIANT_VIOLATION`), visível só na consulta da transação.

## 5. Erros de entrada (não persistidos, corrigíveis)

`INVALID_JSON`, `INVALID_FIELD`, `INVALID_MONEY` (formato/escala/negativo/overflow), `INVALID_CURRENCY`, `UNKNOWN_KIND`, `OPENING_NOT_ALLOWED`, `INVALID_AMOUNT_FOR_KIND` (zero em `BET/WIN/REFUND/ROLLBACK`, diferente de `0.00` em `LOSS`), `MISSING_REFERENCE` (`REFUND`/`ROLLBACK` sem referência), `MISSING_IDEMPOTENCY_KEY`, `INVALID_CURSOR` (página do ledger).

## 6. Exemplos (`curl`)

_(preencher no M10 com token via `client_credentials` do Keycloak)_
