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
- [ ] 2.3 `ADD` `UnitOfWork` + repositórios pgx
- [ ] 2.4 `ADD` constraint trigger deferida saldo × ledger; trigger de terminais imutáveis
- [ ] 2.5 `TEST` integração: constraints, imutabilidade, `OPENING` único, negatividade, migrations up/down

### M3 — Casos de uso (G2, G7, RF-01, RF-03..05)
- [ ] 3.1 `ADD` `OpenWallet`
- [ ] 3.2 `ADD` `ProcessWagerTransaction` síncrono (`BET/WIN/LOSS`) com idempotência e rejeição persistida
- [ ] 3.3 `ADD` `REFUND`/`ROLLBACK` com resolução de referência (`ARCHITECTURE` §6)
- [ ] 3.4 `ADD` `PENDING_REFERENCE` (persistência + evento)
- [ ] 3.5 `TEST` replay, conflito de hash, outra chave para mesma `(provider, extId)`, saldo original no replay
- [ ] 3.6 `TEST` corrida 100.00 × 2×80.00 e 50 duplicatas (in-process, `-race`)

### M4 — HTTP + Auth (RF-02, RF-08..10)
- [ ] 4.1 `ADD` realm Keycloak importável, clients/identidades de teste, script de token
- [ ] 4.2 `ADD` middleware JWT (JWKS, iss/aud/exp) + matriz de autorização
- [ ] 4.3 `ADD` handlers de carteira, ledger paginado, envio de operação, consultas de transação
- [ ] 4.4 `ADD` mapeamento de erros → `docs/API.md` (inclui 503)
- [ ] 4.5 `ADD` reconciliação (snapshot `REPEATABLE READ`)
- [ ] 4.6 `ADD` health live/ready
- [ ] 4.7 `TEST` auth real e isolamento entre provedores (consulta e replay), sem efeito colateral em 401/403

### M5 — Outbox (G4, RF-07)
- [ ] 5.1 `ADD` publisher com lease, `SKIP LOCKED`, backoff
- [ ] 5.2 `ADD` fila `wager-events.fifo`; contratos em `docs/MESSAGING.md`
- [ ] 5.3 `TEST` dois publishers; crash commit→publicar e publicar→marcar; `eventId` preservado

### M6 — Worker de referências (RF-05)
- [ ] 6.1 `ADD` worker `SKIP LOCKED` com backoff exponencial e `max_attempts`/TTL
- [ ] 6.2 `ADD` resolução tardia e rejeição por expiração
- [ ] 6.3 `ADD` varredor de `PENDING` antigos
- [ ] 6.4 `TEST` referência tardia, reinício no meio, expiração

### M7 — SQS (RF-06)
- [ ] 7.1 `ADD` provisionamento das filas, redrive e políticas
- [ ] 7.2 `ADD` consumidor (long polling, concorrência limitada, heartbeat)
- [ ] 7.3 `ADD` inbox na mesma transação; inválidas e hash divergente
- [ ] 7.4 `ADD` retry com backoff e DLQ
- [ ] 7.5 `TEST` reentrega, DLQ, inválidas, HTTP×SQS na mesma operação

### M8 — Fx, shutdown e observabilidade (RF-11)
- [ ] 8.1 `ADD` módulos Fx finais + `APP_ROLES` + lifecycle
- [ ] 8.2 `ADD` shutdown ordenado (`SIGTERM`, drenagem/liberação)
- [ ] 8.3 `ADD` métricas Prometheus + logs JSON com correlação
- [ ] 8.4 `TEST` composição Fx (`ValidateApp`, start/stop, recursos liberados)

### M9 — Multi-instância e falhas
- [ ] 9.1 `ADD` build tag `faultinject` e pontos de crash
- [ ] 9.2 `ADD` harness com 3 processos independentes
- [ ] 9.3 `TEST` cenários 1–8 do desafio (`docs/TESTING.md` §4)
- [ ] 9.4 `TEST` conferência final saldo × ledger e reconciliação
- [ ] 9.5 `TEST` PostgreSQL/SQS indisponíveis temporariamente

### M10 — Documentação e entrega
- [ ] 10.1 Finalizar `ARCHITECTURE.md` (incl. limitações e trabalho não concluído)
- [ ] 10.2 `README.md` reproduzível (pré-requisitos, env, filas, migrations up/down, `curl` autenticados, testes)
- [ ] 10.3 Validar em clone limpo: `docker compose up --build`, `go test ./...`, `-race`, `go vet`, `gofmt -l`
- [ ] 10.4 (opcional) partidas dobradas, tracing OTel, teste de carga reproduzível

## 5. Decisões tomadas

- **Versão do Go:** `go 1.27` (`go.mod` e `golang:1.27-alpine` no Dockerfile).
- **Roteador HTTP:** `net/http` stdlib (padrão método+eixo do Go 1.22+), sem dependência de roteador.
- **Migrations:** `golang-migrate`.
- **Emulador SQS:** LocalStack (compatível com SDK v2).
- **SDK SQS:** `aws-sdk-go-v2`.
- **Roles de banco:** `app` (migração, dona do schema) vs `wager_app` (aplicação, sem escrita no ledger e sem DDL), criados na migration `000006` (`wager_app` por padrão no Compose/.env).
- **Interpretações `ARCHITECTURE.md` §14: todas as 7 confirmadas** — sem normalização monetária; 1 reversão por alvo; `WIN` com referência segue reversões; provedor opera qualquer carteira (consulta a transações isolada); identidade SQS depende do broker; carteira inexistente = `404`; `LOSS` `0.00` sem ledger/versão.

## 6. Status atual

- Fase: **M2 em andamento** — schema migrado e roles separados (migração × aplicação) validados no banco
- Última tarefa concluída: 2.2
- Próxima tarefa: 2.3 (`UnitOfWork` + repositórios pgx)
- Bloqueios: —
