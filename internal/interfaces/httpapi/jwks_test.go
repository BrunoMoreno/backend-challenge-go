package httpapi

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	testIssuer   = "http://keycloak:8080/realms/wagering"
	testAudience = "wager-api"
	testKid      = "test-key-1"
)

func TestJWKSVerifierAcceptsValidToken(t *testing.T) {
	key := mustRSAKey(t)
	url := jwksServer(t, testKid, &key.PublicKey)
	v := mustVerifier(t, url)

	token := signRS256(t, key, testKid, validClaims(testAudience))
	id, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if id.ProviderID != "provider-a" {
		t.Errorf("ProviderID = %q, want provider-a", id.ProviderID)
	}
	if !id.HasRole(RoleProvider) {
		t.Errorf("Roles = %v, want %s", id.Roles, RoleProvider)
	}
}

func TestJWKSVerifierAcceptsStringAudience(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	claims := validClaims(testAudience)
	claims["aud"] = testAudience // aud como string única
	if _, err := v.Verify(context.Background(), signRS256(t, key, testKid, claims)); err != nil {
		t.Fatalf("Verify() com aud string error = %v", err)
	}
}

func TestJWKSVerifierRejectsExpired(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	claims := validClaims(testAudience)
	claims["exp"] = time.Now().Add(-2 * time.Hour).Unix()
	if _, err := v.Verify(context.Background(), signRS256(t, key, testKid, claims)); err != ErrUnauthenticated {
		t.Fatalf("Verify() expirado error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsMissingExp(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	claims := validClaims(testAudience)
	delete(claims, "exp")
	if _, err := v.Verify(context.Background(), signRS256(t, key, testKid, claims)); err != ErrUnauthenticated {
		t.Fatalf("Verify() sem exp error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsWrongIssuer(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	claims := validClaims(testAudience)
	claims["iss"] = "http://outro-idp/realms/x"
	if _, err := v.Verify(context.Background(), signRS256(t, key, testKid, claims)); err != ErrUnauthenticated {
		t.Fatalf("Verify() issuer errado error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsWrongAudience(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	claims := validClaims("outro-client")
	if _, err := v.Verify(context.Background(), signRS256(t, key, testKid, claims)); err != ErrUnauthenticated {
		t.Fatalf("Verify() audience errada error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsTamperedSignature(t *testing.T) {
	key := mustRSAKey(t)
	other := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	// Assinado com outra chave, mas com o mesmo `kid` da chave publicada.
	if _, err := v.Verify(context.Background(), signRS256(t, other, testKid, validClaims(testAudience))); err != ErrUnauthenticated {
		t.Fatalf("Verify() assinatura inválida error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsUnknownKid(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	if _, err := v.Verify(context.Background(), signRS256(t, key, "kid-inexistente", validClaims(testAudience))); err != ErrUnauthenticated {
		t.Fatalf("Verify() kid desconhecido error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsAlgNone(t *testing.T) {
	key := mustRSAKey(t)
	v := mustVerifier(t, jwksServer(t, testKid, &key.PublicKey))

	none := signWithHeader(t, key, map[string]any{"alg": "none", "typ": "JWT", "kid": testKid}, validClaims(testAudience))
	if _, err := v.Verify(context.Background(), none); err != ErrUnauthenticated {
		t.Fatalf("Verify() alg none error = %v, want ErrUnauthenticated", err)
	}
}

func TestJWKSVerifierRejectsMalformed(t *testing.T) {
	v := mustVerifier(t, jwksServer(t, testKid, &mustRSAKey(t).PublicKey))
	for _, raw := range []string{"", "a.b", "a.b.c.d", "não-e-jwt"} {
		if _, err := v.Verify(context.Background(), raw); err != ErrUnauthenticated {
			t.Errorf("Verify(%q) error = %v, want ErrUnauthenticated", raw, err)
		}
	}
}

func TestJWKSVerifierRefreshesRotatedKey(t *testing.T) {
	first := mustRSAKey(t)
	second := mustRSAKey(t)
	server := rotatingJWKS(t, testKid, &first.PublicKey)
	v := mustVerifier(t, server.URL)

	if _, err := v.Verify(context.Background(), signRS256(t, first, testKid, validClaims(testAudience))); err != nil {
		t.Fatalf("Verify() chave inicial error = %v", err)
	}

	server.rotate(testKid, &second.PublicKey)
	if _, err := v.Verify(context.Background(), signRS256(t, second, testKid, validClaims(testAudience))); err != nil {
		t.Fatalf("Verify() após rotação error = %v (esperava refresh do JWKS)", err)
	}
}

func mustVerifier(t *testing.T, jwksURL string) *JWKSVerifier {
	t.Helper()
	v, err := NewJWKSVerifier(context.Background(), testIssuer, jwksURL, testAudience)
	if err != nil {
		t.Fatalf("NewJWKSVerifier() error = %v", err)
	}
	return v
}

func mustRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return key
}

func validClaims(audience string) map[string]any {
	now := time.Now()
	return map[string]any{
		"iss":         testIssuer,
		"sub":         "service-account-provider-a",
		"aud":         []string{audience, "account"},
		"exp":         now.Add(time.Hour).Unix(),
		"iat":         now.Unix(),
		"provider_id": "provider-a",
		"realm_access": map[string]any{
			"roles": []string{RoleProvider},
		},
	}
}

func signRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	return signWithHeader(t, key, map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}, claims)
}

func signWithHeader(t *testing.T, key *rsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	hb, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	signing := b64(hb) + "." + b64(pb)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("SignPKCS1v15: %v", err)
	}
	return signing + "." + b64(sig)
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func jwksServer(t *testing.T, kid string, pub *rsa.PublicKey) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJWKS(w, kid, pub)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

type rotatingJWKSServer struct {
	*httptest.Server
	mu  sync.Mutex
	kid string
	pub *rsa.PublicKey
}

func (s *rotatingJWKSServer) rotate(kid string, pub *rsa.PublicKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kid, s.pub = kid, pub
}

func rotatingJWKS(t *testing.T, kid string, pub *rsa.PublicKey) *rotatingJWKSServer {
	t.Helper()
	rs := &rotatingJWKSServer{kid: kid, pub: pub}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rs.mu.Lock()
		defer rs.mu.Unlock()
		writeJWKS(w, rs.kid, rs.pub)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func writeJWKS(w http.ResponseWriter, kid string, pub *rsa.PublicKey) {
	e := big.NewInt(int64(pub.E)).Bytes()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
			"n": b64(pub.N.Bytes()), "e": b64(e),
		}},
	})
}
