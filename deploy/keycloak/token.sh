#!/usr/bin/env bash
# Obtém um access token OIDC (client_credentials) de um cliente de teste do
# realm wagering. Uso: ./deploy/keycloak/token.sh <provider-a|provider-b|wagering-internal>
set -euo pipefail

base="${KEYCLOAK_URL:-http://localhost:8081}"
realm="${KEYCLOAK_REALM:-wagering}"
client="${1:?uso: token.sh <provider-a|provider-b|wagering-internal>}"

secret_for() {
  case "$1" in
    provider-a) echo "provider-a-secret" ;;
    provider-b) echo "provider-b-secret" ;;
    wagering-internal) echo "wagering-internal-secret" ;;
    *) echo "client desconhecido: $1" >&2; exit 64 ;;
  esac
}

secret="$(secret_for "$client")"

curl -fsS -X POST \
  -d "grant_type=client_credentials" \
  -d "client_id=$client" \
  -d "client_secret=$secret" \
  "$base/realms/$realm/protocol/openid-connect/token" |
  jq -er '.access_token'