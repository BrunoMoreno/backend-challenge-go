package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubVerifier struct {
	id  Identity
	err error
}

func (s stubVerifier) Verify(context.Context, string) (Identity, error) { return s.id, s.err }

func providerIdentity() Identity {
	return Identity{Subject: "sa", ProviderID: "provider-a", Roles: []string{RoleProvider}}
}

func internalIdentity() Identity {
	return Identity{Subject: "sa-internal", Roles: []string{RoleWageringInternal, RoleWalletInternal}}
}

func TestAuthenticateRejectsMissingToken(t *testing.T) {
	h := Authenticate(stubVerifier{}, okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	assertErrorCode(t, rec, "UNAUTHENTICATED")
}

func TestAuthenticateRejectsMalformedHeader(t *testing.T) {
	h := Authenticate(stubVerifier{}, okHandler())
	for _, header := range []string{"Token abc", "Bearer ", "abc", "Basic abc"} {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.Header.Set("Authorization", header)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization=%q status = %d, want 401", header, rec.Code)
		}
	}
}

func TestAuthenticateRejectsInvalidToken(t *testing.T) {
	h := Authenticate(stubVerifier{err: ErrUnauthenticated}, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer qualquer")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	assertErrorCode(t, rec, "UNAUTHENTICATED")
}

func TestAuthenticateInjectsIdentity(t *testing.T) {
	var got Identity
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = IdentityFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := Authenticate(stubVerifier{id: providerIdentity()}, next)
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer valido")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got.ProviderID != "provider-a" || !got.HasRole(RoleProvider) {
		t.Fatalf("identity no contexto = %+v, want provider-a/wagering:provider", got)
	}
}

func TestRequireRoles(t *testing.T) {
	h := RequireRoles(RoleProvider)(okHandler())

	t.Run("allowed", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, requestWithIdentity(providerIdentity()))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("forbidden", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, requestWithIdentity(internalIdentity()))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rec.Code)
		}
		assertErrorCode(t, rec, "FORBIDDEN")
	})

	t.Run("unauthenticated", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	})
}

func TestRequireRolesAnyOf(t *testing.T) {
	h := RequireRoles(RoleWageringInternal, RoleWalletInternal)(okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, requestWithIdentity(internalIdentity()))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestRequireProviderPath(t *testing.T) {
	h := RequireProviderPath("providerId")(okHandler())

	cases := []struct {
		name     string
		identity Identity
		provider string
		want     int
	}{
		{"provedor corresponde", providerIdentity(), "provider-a", http.StatusOK},
		{"provedor diverge", providerIdentity(), "provider-b", http.StatusForbidden},
		{"interno acessa qualquer", internalIdentity(), "provider-b", http.StatusOK},
		{"provedor sem claim", Identity{Roles: []string{RoleProvider}}, "provider-a", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := requestWithIdentity(tc.identity)
			req.SetPathValue("providerId", tc.provider)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestCanSubmitAsProvider(t *testing.T) {
	if !providerIdentity().CanSubmitAsProvider("provider-a") {
		t.Error("provider-a deveria poder enviar como provider-a")
	}
	if providerIdentity().CanSubmitAsProvider("provider-b") {
		t.Error("provider-a não deveria poder enviar como provider-b")
	}
	if internalIdentity().CanSubmitAsProvider("provider-a") {
		t.Error("identidade interna sem role de provedor não deveria enviar operação")
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func requestWithIdentity(id Identity) *http.Request {
	return httptest.NewRequest(http.MethodGet, "/x", nil).WithContext(withIdentity(context.Background(), id))
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("resposta de erro não é JSON: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != want {
		t.Fatalf("error.code = %q, want %q", body.Error.Code, want)
	}
}
