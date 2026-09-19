package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// Roles de realm usadas na matriz de autorização (`docs/API.md` §2).
const (
	RoleProvider         = "wagering:provider"
	RoleWageringInternal = "wagering:internal"
	RoleWalletInternal   = "wallet:internal"
)

// ErrUnauthenticated é devolvido quando o token está ausente, malformado,
// expirado ou não passa na validação de assinatura/issuer/audiência.
var ErrUnauthenticated = errors.New("httpapi: não autenticado")

// Identity é o resultado da verificação de um access token.
type Identity struct {
	// Subject é a claim `sub`.
	Subject string
	// ProviderID é a claim `provider_id` (vazia para identidades internas).
	ProviderID string
	// Roles são as roles de realm (claim `realm_access.roles`).
	Roles []string
}

// HasRole informa se a identidade possui a role informada.
func (id Identity) HasRole(role string) bool {
	for _, r := range id.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// CanSubmitAsProvider informa se a identidade pode enviar operações em nome de
// providerID: exige a role de provedor e `provider_id` igual ao alvo. Usado no
// handler de `POST /wagering/transactions` (403 quando falso).
func (id Identity) CanSubmitAsProvider(providerID string) bool {
	return id.HasRole(RoleProvider) && id.ProviderID != "" && id.ProviderID == providerID
}

// Verifier valida um access token bruto e devolve a identidade.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (Identity, error)
}

type identityContextKey struct{}

func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, id)
}

// IdentityFromContext devolve a identidade injetada por Authenticate.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityContextKey{}).(Identity)
	return id, ok
}

// Authenticate exige `Authorization: Bearer <jwt>` válido e injeta a identidade
// no contexto. Falhas resultam em 401 UNAUTHENTICATED (sem efeito no negócio).
func Authenticate(v Verifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "token de acesso ausente")
			return
		}
		id, err := v.Verify(r.Context(), raw)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "token inválido ou expirado")
			return
		}
		next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), id)))
	})
}

// RequireRoles permite apenas identidades que possuam ao menos uma das roles.
// Ausência de identidade é 401; identidade sem permissão é 403 FORBIDDEN.
func RequireRoles(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, ok := IdentityFromContext(r.Context())
			if !ok {
				writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "requer autenticação")
				return
			}
			for _, role := range roles {
				if id.HasRole(role) {
					next.ServeHTTP(w, r)
					return
				}
			}
			writeError(w, http.StatusForbidden, "FORBIDDEN", "permissão insuficiente")
		})
	}
}

// RequireProviderPath garante que um provedor só opere sobre o próprio
// `provider_id`: o valor do path param precisa coincidir com a claim. A role
// interna (`wagering:internal`) acessa qualquer provedor. Divergência é 403.
func RequireProviderPath(param string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, ok := IdentityFromContext(r.Context())
			if !ok {
				writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "requer autenticação")
				return
			}
			if id.HasRole(RoleWageringInternal) {
				next.ServeHTTP(w, r)
				return
			}
			target := r.PathValue(param)
			if id.HasRole(RoleProvider) && id.ProviderID != "" && id.ProviderID == target {
				next.ServeHTTP(w, r)
				return
			}
			writeError(w, http.StatusForbidden, "FORBIDDEN", "provedor não corresponde à identidade")
		})
	}
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}

// writeError escreve o envelope de erro descrito em `docs/API.md` §1.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message},
	})
}
