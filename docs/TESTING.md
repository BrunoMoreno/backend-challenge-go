# Estratégia de testes

> Como cada requisito e garantia do `PRD.md` é provado. Marque os testes no `CONTEXT.md` ao concluí-los.

## 1. Níveis

| Nível | Escopo | Infra |
|---|---|---|
| Unitário | `Money`, `Wallet`, ledger, máquina de estados, regras por tipo, hash, eventos | nenhuma |
| Integração | migrations, constraints, atomicidade, inbox, outbox, retry, DLQ, auth | PostgreSQL, Keycloak e LocalStack/MiniStack **reais** em containers (testcontainers-go) |
| Multi-instância / falhas | cenários de concorrência e recuperação | 3 processos independentes do binário, cada um com pool e memória próprios |
| Composição Fx | `fx.ValidateApp`, start/stop, liberação de recursos | `fxtest` + banco real |

Nada de mocks substituindo toda a infraestrutura.

## 2. Harness

- `TestMain` compila o binário e sobe os containers uma vez por pacote.
- Helper inicia N processos (`exec.Command`) com portas e `APP_ROLES` distintos, aguarda `/health/ready`, encerra com `SIGTERM` (graceful) ou `SIGKILL` (crash).
- Tokens reais obtidos do Keycloak via `client_credentials`.
- **Pontos de falha** com a build tag `faultinject` (fora do binário de produção), acionados por env:
  - `FAULT_AFTER_COMMIT_BEFORE_DELETE` (consumidor SQS)
  - `FAULT_AFTER_PUBLISH_BEFORE_MARK` (publisher)
  - `FAULT_AFTER_COMMIT_PENDING` (aceite `PENDING`, se usado)
- O cenário de crash usa uma **fila FIFO dedicada** (`wager-crash-test.fifo`, recriada no `TestMain`),
  isolando-o da entrada compartilhada — instâncias em drenagem de cenários anteriores não "roubam" a
  mensagem do crash. O harness sobrepõe env por instância com **remoção prévia da chave** (a primeira
  ocorrência vence no runtime, um append simples não sobreporia `APP_SQS_QUEUE_URL`/`FAULT_*`).
- Duplicidade provada por contadores/métricas de recebimentos repetidos, não só pelo resultado final.

## 3. Comandos

```sh
go test ./...                                   # unitários (rápidos)
go test -race ./...                             # com detector de corridas
go test -tags=integration -race ./test/integration/...
go test -tags='integration faultinject' -race ./test/e2e/...   # multi-instância e falhas
go vet ./... && gofmt -l .
```
Preparação das dependências (containers, imagens, realm) documentada no `README.md`.

## 4. Mapa requisito → teste

| Requisito | Teste | Nível |
|---|---|---|
| G1 / RF-04 `Money` | parsing, escala, limites, overflow, inválidos, moedas incompatíveis | unit |
| G3, G8 | constraints: `balance>=0`, `UNIQUE(wallet_id, transaction_id)`, `OPENING` único, checks interno/externo | integração |
| G5, G8 | `UPDATE`/`DELETE`/`TRUNCATE` no ledger falham (trigger + `REVOKE`) | integração |
| Estados | transições válidas/ inválidas; terminais imutáveis (domínio + banco) | unit + integração |
| RF-01 | abertura com/sem saldo, eventos, conflito por `(player, currency)` | integração |
| RF-03 / G2 | replay, conflito de hash, mesma `(provider, extId)` com outra chave, saldo original no replay | integração |
| G7 / RF-04 | **50 envios idênticos em paralelo** → 1 débito | e2e (3 instâncias) |
| Concorrência | **100.00 com 2×80.00** → 1 `PROCESSED`, 1 `INSUFFICIENT_FUNDS`, saldo 20.00, 1 débito; reenvios não alteram | e2e (3 instâncias) |
| G6 | carteiras distintas simultâneas avançam em paralelo | e2e |
| RF-05 | `REFUND`/`ROLLBACK` antes da referência → resolve depois; expira → `REFERENCE_NOT_FOUND`; reinício no meio | integração + e2e |
| Reversões | `REFUND`×`ROLLBACK` sobre a mesma `BET`; `ROLLBACK` sem saldo → `REVERSAL_INSUFFICIENT_FUNDS` | integração |
| RF-06 | reentrega, hash divergente, mensagem inválida, DLQ, HTTP×SQS na mesma operação | integração + e2e |
| Crash consumidor | `FAULT_AFTER_COMMIT_BEFORE_DELETE`: commit → `exit(1)` antes do delete; retry do produtor (mesmo envelope/`messageId`) → replay sem duplicar | e2e (`faultinject`, fila dedicada) |
| G4 / RF-07 | dois publishers disputando; crash commit→publicar e publicar→marcar; `eventId` preservado; lease abandonada assumida | e2e (`faultinject`) |
| G2 | reinício de todos os processos preserva idempotência, pendências e consistência | e2e |
| RF-08 | reconciliação consistente; divergência forçada só reportada, saldo intacto | integração |
| RF-09 | ready falha com PostgreSQL/SQS pausados | integração |
| Indisponibilidade | pausar PostgreSQL/SQS → `503`/retry, sem perda nem duplicidade | e2e |
| RF-10 | sem token, inválido, expirado, `aud`/`iss` errados; isolamento entre provedores (consulta e replay); rotas internas negadas a provedor; sem efeito financeiro em 401/403 | integração |
| Fx | `ValidateApp`; start/stop; workers terminam; recursos liberados (sem goroutine vazando) | integração |
| Fechamento | ao fim de cada e2e: saldo = Σ créditos − Σ débitos do ledger | e2e |

## 5. Critérios de aceite

- Todos os testes verdes com `-race`.
- Cada cenário multi-instância roda com ≥ 3 instâncias.
- Nenhum teste financeiro depende de ordem de execução ou de `sleep` fixo (usar polling com prazo).
