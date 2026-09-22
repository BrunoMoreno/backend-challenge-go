# Autenticação e autorização

> Guia ponta a ponta de **quem** pode chamar **o quê** na API, **como os tokens são
> emitidos** e **como o serviço os valida**. A matriz resumida está em `docs/API.md` §2;
> este documento explica o modelo, o passo a passo no código e os casos de falha.

## 1. O modelo em 30 segundos

- A API é um **resource server** OIDC: não emite token, não guarda senha de usuário
  e **não se autentica no Keycloak** (não usa client credentials com a app).
- Toda rota de negócio exige `Authorization: Bearer <access_token>`.
- O token é um **JWT RS256 assinado pelo Keycloak**. O serviço baixa as chaves
  públicas do **JWKS** e valida: algoritmo, assinatura, `iss`, `aud`, `exp` e `nbf`.
- Depois de validar, extrai **`provider_id`** e **`realm_access.roles`** e aplica a
  matriz de autorização (§4).
- **401/403/404 nunca produzem efeito financeiro** — a autorização roda antes de
  qualquer caso de uso (RF-10).

Fluxo resumido:

```
Cliente (Keycloak client)
   │  client_credentials → token JWT
   ▼
POST/GET /...  Authorization: Bearer <jwt>
   ▼
Authenticate            → extrai Bearer; ausente/malformado = 401      auth.go:68
   ▼
JWKSVerifier.Verify     → RS256, kid→JWKS, assinatura, iss, aud, exp, nbf  jwks.go:97
   ▼
RequireRoles            → role ausente = 403 (sem identidade = 401)    auth.go:86
   ▼
Regras de provedor      → 3 mecanismos (§5)
   ▼
Handler / caso de uso
```

## 2. Componentes

### 2.1 Keycloak — realm `wagering` (`deploy/keycloak/wagering-realm.json`)

O realm é importado no boot do container (`make up`). Contém 4 clients:

| Client | Emite token? | Credenciais | Roles (via `realm_access.roles`) | Claims adicionais |
|---|---|---|---|---|
| `provider-a` | sim (`service_accounts`) | `provider-a-secret` | `wagering:provider` | `provider_id=provider-a`, `aud` inclui `wager-api` |
| `provider-b` | sim (`service_accounts`) | `provider-b-secret` | `wagering:provider` | `provider_id=provider-b`, `aud` inclui `wager-api` |
| `wagering-internal` | sim (`service_accounts`) | `wagering-internal-secret` | `wagering:internal`, `wallet:internal` | `aud` inclui `wager-api` |
| `wager-api` | **não** (nada habilitado) | `wager-api-secret` (**nunca usado**) | — | — (só serve de valor de `aud`) |

- Os três clients que emitem token têm `serviceAccountsEnabled: true` e usam o
  fluxo `client_credentials` (máquina a máquina, sem usuário).
- As roles são **roles de realm** atribuídas aos *service accounts* (usuários
  `service-account-*`); elas aparecem no token na claim `realm_access.roles`.
- `wager-api` é apenas um **marcador de audiência**: não emite token, não tem flow
  habilitado, e seu `secret` declarado no JSON de importação **nunca é consumido**
  por nada. Ele existe para que os mappers de audience injetem `aud=wager-api` nos
  tokens dos outros clients.
- O `provider_id` é injetado por um **`oidc-hardcoded-claim-mapper` por client**:
  todo token de `provider-a` carrega `provider_id=provider-a` etc. Isso significa
  que, **no modelo atual, um provedor real precisa de um client Keycloak próprio
  por `provider_id`** (ou de um mapper dinâmico). O serviço confia na claim vinda
  do Keycloak — em produção o Keycloak é a autoridade que decide quem representa
  qual provedor.

### 2.2 Configuração do serviço (env)

| Variável | Config (`config.go`) | O que é | Regra de ouro |
|---|---|---|---|
| `APP_KEYCLOAK_ISSUER` | `KeycloakIssuer` | Valor **exato** da claim `iss` esperada | Deve ser a URL **frontal** do Keycloak — a mesma que emitiu o token. Em dev: `http://localhost:8081/realms/wagering` |
| `APP_KEYCLOAK_JWKS_URL` | `KeycloakJWKSURL` | URL de onde baixar as chaves públicas | Buscada pelo servidor; pode ser a URL interna (`http://keycloak:8080/...` no Compose) |
| `APP_KEYCLOAK_CLIENT_ID` | `KeycloakAudience` | Valor esperado da claim `aud` | **É a audiência, não um client id desta app.** O serviço não é um client OIDC; guarde o clientId do resource server (`wager-api`) |

> **⚠️ Armadilha do Compose:** `APP_KEYCLOAK_ISSUER` recebia `http://keycloak:8080/...`
> (DNS interno) — mas os tokens emitidos via `make kc-token`/`localhost:8081` carregam
> `iss=http://localhost:8081/realms/wagering` (o Keycloak em `start-dev` deriva o issuer
> da URL frontal da requisição). Como a validação de `iss` é por **igualdade exata**
> (`jwks.go:134`), a app sob o Compose rejeitava **toda** chamada de negócio com 401.
> Corrigido em `docker-compose.yml` (`APP_KEYCLOAK_ISSUER: http://localhost:8081/realms/wagering`).
> Se você rodar a app no host, use os valores de `.env.example` (ambos `localhost:8081`).

## 3. Claims que a API entende

Decodificação em `tokenClaims` (`jwks.go:153`):

| Claim | Uso |
|---|---|
| `alg` (header) | fixado em `RS256` (`jwks.go:114`) — nunca aceita `none`/`HS256` |
| `kid` (header) | seleciona a chave pública no JWKS |
| `iss` | deve ser **exatamente** `APP_KEYCLOAK_ISSUER` |
| `aud` | string ou lista; deve **conter** `APP_KEYCLOAK_CLIENT_ID` (`wager-api`) |
| `exp` | obrigatório; `now` além de `exp + 30s` de leeway = 401 |
| `nbf` | opcional; token antes de `nbf − 30s` = 401 |
| `provider_id` | identidade do provedor (vazia em tokens internos) |
| `realm_access.roles` | roles de realm: `wagering:provider`, `wagering:internal`, `wallet:internal` |

Resultado → `Identity{Subject, ProviderID, Roles}` (`auth.go:23`).

## 4. Matriz de autorização (rota → cadeia de middlewares)

Montagem em `buildHandler` (`router.go:33`), cadeia em `authChain` (`router.go:93`):
`Authenticate → RequireRoles(roles...) → [regras extras, perto do handler]`.

| Rota | Roles exigidas | Regra extra | Comportamento fora da regra |
|---|---|---|---|
| `POST /wagering/transactions` | `wagering:provider` | `body.providerId == provider_id` (`CanSubmitAsProvider`, `handlers.go:371`) | 403 |
| `GET /providers/{providerId}/wagering/transactions/{extId}` | `wagering:provider` **ou** `wagering:internal` | `:providerId == provider_id` (`RequireProviderPath`, `router.go:71`); interno liberado | 403 |
| `GET /wagering/transactions/{id}` | `wagering:provider` **ou** `wagering:internal` | `t.ProviderID() == provider_id`, checado no handler (`handlers.go:331`) | **404** (não vaza existência) |
| `POST /wallets` | `wallet:internal` | — | 403 |
| `GET /wallets/{id}` | `wallet:internal` | — | 403 |
| `GET /wallets/{id}/ledger` | `wallet:internal` | — | 403 |
| `POST /wallets/{id}/reconciliation` | `wallet:internal` | — | 403 |
| `GET /health/live`, `/health/ready`, `/openapi.yaml`, `/swagger`, `/metrics` | **públicas** | — | — |

Sem token → 401 `UNAUTHENTICATED` em qualquer rota protegida.

## 5. Isolamento entre provedores — 3 mecanismos

O sistema aplica **três mecanismos distintos** para a mesma regra ("um provedor só
toca o que é dele") — atenção ao ler o código:

| Mecanismo | Onde | Regra | Código de erro |
|---|---|---|---|
| `CanSubmitAsProvider` | handler de `POST /wagering/transactions` | exige `RoleProvider` **e** `provider_id == body.providerId` | 403 |
| `RequireProviderPath` | rota `GET /providers/{providerId}/...` | `RoleWageringInternal` passa direto; senão `RoleProvider` + `provider_id == :providerId` | 403 |
| checagem no handler | rota `GET /wagering/transactions/{id}` | se usada por não-interno, `t.ProviderID()` precisa ser `== provider_id` | **404** |

- As duas primeiras são **anticipação** (query no path/body antes de tocar o banco);
  a terceira é **pós-consulta** e devolve 404 de propósito (anti-protocolo de
  enumeração: o client não descobre nem que a transação existe).
- `GET /wallets/*` e `POST /wallets` são **exclusivos** da role `wallet:internal` —
  provedor não consulta carteira nem ledger.
- `POST /wagering/transactions` não admite a role interna: a interna envia operações
  apenas pela fila SQS (o consumidor valida o envelope, não o HTTP).

## 6. Passo a passo prático (dev)

Pré-requisitos: `docker compose up` (Keycloak em `localhost:8081`), migrations
aplicadas e o serviço no ar (Compose sobe o `app`; para rodar no host, veja o README).

```sh
# 1. Obter o token de um provedor
make kc-token CLIENT=provider-a            # imprime só o access_token
TOKEN=$(make kc-token CLIENT=provider-a)

# 2. Token do serviço interno (carteiras, ledger, reconciliação)
INT_TOKEN=$(make kc-token CLIENT=wagering-internal)

# 3. Abrir uma carteira (wallet:internal)
curl -s http://localhost:8080/wallets -H "Authorization: Bearer $INT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"p-100","initialBalance":{"amount":"100.00","currency":"BRL"}}'
# 201 {"id":"...","playerId":"p-100","balance":{"amount":"100.00","currency":"BRL"},"version":2}
```

O mesmo token pode:

```sh
# 4. Enviar um BET (wagering:provider + providerId == provider_id)
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: bet-001' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-001","playerId":"p-100",
       "walletId":"<ID da carteira do passo 3>","roundId":"r-1","gameId":"g-1",
       "kind":"BET","amount":{"amount":"25.00","currency":"BRL"}}'
# 201 {"transactionId":"...","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}

# 5. Mesma chave de novo → replay idempotente, sem novo débito
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: bet-001' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-001","playerId":"p-100",
       "walletId":"<ID>","roundId":"r-1","gameId":"g-1",
       "kind":"BET","amount":{"amount":"25.00","currency":"BRL"}}'
# 200 {... "idempotentReplay":true}   → saldo permanece 75.00

# 6. provider-a tentando operar como provider-b → 403
OTHER_TOKEN=$(make kc-token CLIENT=provider-b)
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $OTHER_TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: bet-999' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-999","playerId":"p-100",
       "walletId":"<ID>","roundId":"r-9","gameId":"g-9",
       "kind":"BET","amount":{"amount":"5.00","currency":"BRL"}}'
# 403 {"error":{"code":"FORBIDDEN",...}}

# 7. Sem token → 401
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/wallets/p-100
# 401 UNAUTHENTICATED
```

Mais exemplos (ledger, consultas, reconciliação, rejeições) em `docs/API.md` §6.

## 7. Troubleshooting

| Sintoma | Causa provável | Onde olhar |
|---|---|---|
| 401 em **toda** rota de negócio no Compose | `iss` do token ≠ `APP_KEYCLOAK_ISSUER` (URL frontal vs interna) | §2.2; `docker compose config` e compare com o `iss` do token |
| 401 com Keycloak fora do ar | JWKS não pode ser baixado; verifier lazy serve a falha e retenta com backoff (`jwks.go:186`) | logs do `app`; `curl` no `APP_KEYCLOAK_JWKS_URL` |
| 401 token "válido" gerado por outro realm | `iss` e `aud` são de outro realm/client | confira `well-known` do realm |
| 403 no `POST /wagering/transactions` | `body.providerId` ≠ claim `provider_id` (ou token sem `wagering:provider`) | §5; decode o token e confira `provider_id`/`realm_access.roles` |
| 404 em `GET /wagering/transactions/{id}` de outro provedor | comportamento intencional (anti-enumeração) | §5 |
| 401 logo após regenerar o realm | `aud` ausente: o client precisa do mapper "audience wager-api" | §2.1 |

Dica: decodifique o token localmente para inspecionar claims:
`echo "$TOKEN" | cut -d. -f2 | base64 -d 2>/dev/null` (adicione padding se preciso).

## 8. Perguntas frequentes

- **Por que o serviço tem `APP_KEYCLOAK_CLIENT_ID` se não é client?** Ele é lido como
  **`KeycloakAudience`** (`config.go:119`) — o valor esperado de `aud`. O nome é
  legado/confuso; semanticamente é "o clientId do resource server cuja aud os tokens
  devem conter". O app nunca troca secrets com o Keycloak.
- **O client `wager-api` precisa de secret fixo?** Não. O secret no realm de importação
  é decorativo; nenhum código o consome.
- **Posso configurar um client Keycloak existente, sem payload custom?** O `provider_id`
  vem de hardcoded claim mapper — sem ele, um client `wagering:provider` vira 403 no
  submit e 404 nas consultas de provedor (`RequireProviderPath`/handler exigem a claim).
- **Como criar um provedor novo em dev?** Novo client com `serviceAccountsEnabled`,
  role `wagering:provider` no service account, mappers `provider_id` (hardcoded com o
  id do provedor) e `audience wager-api`. Reinicie o container com `--import-realm`.