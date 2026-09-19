# PRD — Processamento Distribuído de Apostas

> **Este documento define o quê.** Como implementar está em `ARCHITECTURE.md`; contratos em `docs/API.md` e `docs/MESSAGING.md`; verificação em `docs/TESTING.md`; execução e progresso em `CONTEXT.md`.

## 1. Objetivo

Serviço em **Go + Uber Fx** que movimenta carteiras de jogadores a partir de operações financeiras de provedores de jogos, por **API HTTP** e **consumidor SQS**, com as mesmas garantias, corretas com **várias instâncias** e com **falhas entre etapas**.

## 2. Escopo

**Dentro:** carteiras, ledger, operações `BET/WIN/LOSS/REFUND/ROLLBACK`, idempotência persistente, referências pendentes, inbox/outbox, reconciliação, autenticação OIDC, observabilidade, ambiente Docker Compose reproduzível.

**Fora:** cadastro de senhas e emissão própria de tokens; reversões parciais; multimoeda nos cenários principais (o tipo `Money` carrega moeda e há teste de incompatibilidade).

**Diferenciais opcionais:** partidas dobradas, tracing OpenTelemetry, teste de carga reproduzível.

## 3. Atores

| Ator | Identidade | Pode |
|---|---|---|
| Provedor de jogos | client OIDC com claim `provider_id` | enviar operações; consultar **apenas** suas transações |
| Serviço interno | client OIDC com role `wallet:internal` | abrir/consultar carteiras, ledger, reconciliar, consultar transações |
| Produtor SQS | credenciais/política do broker | publicar em `wager-transactions.fifo` |
| Consumidor de eventos | credenciais/política do broker | ler `wager-events.fifo` |

## 4. Requisitos funcionais

| ID | Requisito |
|---|---|
| RF-01 | Abrir carteira (`POST /wallets`): saldo positivo cria `OPENING` `PROCESSED`, crédito no ledger e 2 eventos no mesmo commit; saldo zero não cria nada disso; `(playerId, currency)` duplicado → conflito |
| RF-02 | Consultar carteira, ledger (cursor opaco, ordenação estável) e transações (por id interno e por `(providerId, externalTransactionId)`) |
| RF-03 | Enviar operação (`POST /wagering/transactions`) com `Idempotency-Key` obrigatório, replay com `idempotentReplay:true`, conflito de conteúdo/chave |
| RF-04 | Regras por tipo: `BET` débito; `WIN` crédito; `LOSS` sem movimentação (`0.00`); `REFUND` crédito integral de `BET`; `ROLLBACK` movimento contrário integral de `BET/WIN/REFUND`; `OPENING` rejeitado por HTTP/SQS |
| RF-05 | Referência indisponível → `PENDING_REFERENCE` com retry/backoff durável, TTL/limite de tentativas e rejeição final `REFERENCE_NOT_FOUND` |
| RF-06 | Consumidor SQS FIFO com inbox, DLQ, redrive, retry com backoff e shutdown seguro |
| RF-07 | Outbox transacional com múltiplos publishers e 4 eventos tipados |
| RF-08 | Reconciliação (`POST /wallets/:id/reconciliation`) sem alterar saldo; diverge → resposta + log + métrica |
| RF-09 | `GET /health/live` e `/health/ready` (PostgreSQL + SQS), públicos |
| RF-10 | AuthN/AuthZ por IdP externo (Keycloak) em todos os endpoints de negócio |
| RF-11 | Logs JSON com correlação e métricas exigidas |

## 5. Garantias obrigatórias

| ID | Garantia |
|---|---|
| G1 | Dinheiro nunca passa por `float32`/`float64` (parsing, cálculo, serialização, persistência) |
| G2 | Idempotência persistente, sobrevive ao reinício de todos os processos |
| G3 | Invariantes financeiras garantidas **no banco**, independentes de locks locais e da dedup do SQS FIFO |
| G4 | Eventos externos só publicados após o commit que os originou |
| G5 | Ledger append-only; correção = novo lançamento |
| G6 | Carteiras independentes avançam em paralelo; sem lock global |
| G7 | Sem lost updates em saldo |
| G8 | Unicidade, não negatividade e imutabilidade impostas por schema, constraints e proteções do banco |

Cenários de falha cobertos: duplicatas (HTTP e SQS), reversão antes da referência, concorrência na mesma carteira, kill antes/depois do commit, republicação de evento, indisponibilidade temporária de PostgreSQL/SQS. Nenhum pode gerar movimentação duplicada, saldo negativo ou perda de evento com registro confirmado.

## 6. Critérios eliminatórios

- Sem autenticação efetiva nos endpoints de negócio
- Acesso não autorizado a operações/transações
- Cálculo monetário em ponto flutuante
- Saldo negativo por concorrência
- Movimentação duplicada
- Idempotência só em memória
- Dependência de uma única instância
- Publicação antes do commit
- Sem ledger auditável
- PostgreSQL, SQS e IdP totalmente substituídos por mocks nos testes

## 7. Avaliação

| Critério | Pontos |
|---|---:|
| Integridade financeira | 20 |
| Concorrência | 20 |
| Idempotência | 15 |
| Mensageria e recuperação | 15 |
| Modelagem e arquitetura | 10 |
| Testes | 10 |
| Observabilidade | 5 |
| Documentação | 5 |
| **Total** | **100** |

## 8. Entregáveis

- Código formatado (`gofmt`), `go.mod`/`go.sum`, migrations (aplicação e reversão documentadas), Docker Compose
- `README.md` (pré-requisitos, env, filas, migrations, execução, exemplos, testes), `.env.example`, `ARCHITECTURE.md`
- Provisionamento automático do IdP, identidades de teste e instruções dos fluxos autenticados
- Comandos: `docker compose up --build`, `go test ./...`, `go test -race ./...`, `go vet ./...`

## 9. Definição de pronto

- [ ] Todos os RF e G rastreados a pelo menos um teste (`docs/TESTING.md` §4)
- [ ] Nenhum eliminatório presente
- [ ] Checkout limpo reproduz: compose, migrations, filas, IdP, testes e `-race`
- [ ] `ARCHITECTURE.md` registra decisões, interpretações, limitações e trabalho não concluído
