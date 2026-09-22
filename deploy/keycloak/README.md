# Keycloak — realm de teste `wagering`

> Repositório estrutural do realm importado no boot do container. O guia **ponta a
> ponta** de autenticação (modelo OIDC, claims, validação no serviço, matriz e
> isolamento entre provedores) está em [`docs/AUTHENTICATION.md`](../../docs/AUTHENTICATION.md).

O realm de desenvolvimento é importado automaticamente pelo Docker Compose no
boot do Keycloak (`command: start-dev --import-realm`, volume
`./deploy/keycloak:/opt/keycloak/data/import`). Recarregue com:

```sh
docker compose up -d --force-recreate keycloak
```

## Identidades de teste (somente dev)

Clients confidenciais (grant `client_credentials`), secret fixo de teste:

| Client | `secret` | Roles | Claims |
|---|---|---|---|
| `provider-a` | `provider-a-secret` | `wagering:provider` | `provider_id=provider-a` |
| `provider-b` | `provider-b-secret` | `wagering:provider` | `provider_id=provider-b` |
| `wagering-internal` | `wagering-internal-secret` | `wagering:internal`, `wallet:internal` | — |
| `wager-api` | `wager-api-secret` | — (resource server, auditado via `aud`) | — |

Todos os tokens de negócio têm `aud` contendo `wager-api` (aplicação que valida).

> `wager-api` **não emite token** (nenhum flow habilitado) e seu secret declarado aqui
> **nunca é consumido**: ele existe apenas como marcador de audiência. `provider_id` é
> injetado por hardcoded claim mapper — cada client de provedor carrega sempre o mesmo
> `provider_id`; em produção, o Keycloak é quem autoriza qual entidade representa qual
> provedor. Detalhes em `docs/AUTHENTICATION.md`.

> Secrets acima são apenas para desenvolvimento/testes. Em produção, secrets são
> emitidos/rotacionados pelo Keycloak (o realm importável entrega somente a
> configuração estrutural).

## Obter um token

```sh
make kc-token CLIENT=provider-a          # ou:
./deploy/keycloak/token.sh provider-b
KEYCLOAK_URL=https://... ./deploy/keycloak/token.sh wagering-internal
```

O script imprime apenas o `access_token` no stdout (via `jq`).