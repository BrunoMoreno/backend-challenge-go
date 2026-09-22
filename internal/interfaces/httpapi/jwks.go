package httpapi

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// leeway tolera pequenas diferenças de relógio ao validar `exp`/`nbf`.
const leeway = 30 * time.Second

// jwksTTL é o tempo em que as chaves públicas em cache são consideradas frescas.
const jwksTTL = 10 * time.Minute

// initBackoff é o intervalo mínimo entre tentativas do JWKS inicial após a
// primeira falha: o Keycloak fora do ar não pode virar um GET por request.
const initBackoff = 5 * time.Second

// missingKeyRefreshGrace rate-limita o refresh disparado por um kid
// desconhecido com o cache fresco: sem isso, cada token de um kid inventado
// geraria um GET no Keycloak (DoS, M9).
const missingKeyRefreshGrace = 5 * time.Second

// JWKSVerifier valida JWTs RS256 (Keycloak) contra um JWKS, checando assinatura,
// `iss`, `aud` e `exp`, e extrai `provider_id` e as roles de realm.
type JWKSVerifier struct {
	issuer   string
	audience string
	jwksURL  string
	client   *http.Client
	ttl      time.Duration
	lazy     bool

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time

	// initMu serializa a primeira carga (lazy); a falha NÃO é memoizada — a
	// próxima requisição retenta, respeitando initRetryAfter.
	initMu          sync.Mutex
	initialized     bool
	nextInitAttempt time.Time
	initRetryAfter  time.Duration

	// unknownMu protege o throttle de refresh por kid desconhecido.
	unknownMu          sync.Mutex
	lastUnknownRefresh time.Time
}

// NewJWKSVerifier busca o JWKS inicial; falha se a fonte estiver inacessível.
func NewJWKSVerifier(ctx context.Context, issuer, jwksURL, audience string) (*JWKSVerifier, error) {
	if jwksURL == "" {
		return nil, errors.New("httpapi: APP_KEYCLOAK_JWKS_URL vazia")
	}
	v := newJWKSVerifier(issuer, jwksURL, audience)
	if err := v.refresh(ctx); err != nil {
		return nil, fmt.Errorf("httpapi: carregar JWKS: %w", err)
	}
	return v, nil
}

// NewLazyJWKSVerifier monta o verifier sem contato de rede na construção; a
// primeira verificação faz a carga do JWKS. O Fx usa esta variante para a
// composição validar sem depender de o Keycloak estar no ar.
func NewLazyJWKSVerifier(issuer, jwksURL, audience string) (*JWKSVerifier, error) {
	if jwksURL == "" {
		return nil, errors.New("httpapi: APP_KEYCLOAK_JWKS_URL vazia")
	}
	v := newJWKSVerifier(issuer, jwksURL, audience)
	v.lazy = true
	return v, nil
}

func newJWKSVerifier(issuer, jwksURL, audience string) *JWKSVerifier {
	return &JWKSVerifier{
		issuer:         issuer,
		audience:       audience,
		jwksURL:        jwksURL,
		client:         &http.Client{Timeout: 5 * time.Second},
		ttl:            jwksTTL,
		initRetryAfter: initBackoff,
	}
}

// Verify valida o token bruto e devolve a identidade. Qualquer falha resulta em
// ErrUnauthenticated (sem vazar detalhes ao cliente).
func (v *JWKSVerifier) Verify(ctx context.Context, raw string) (Identity, error) {
	if v.lazy {
		if err := v.ensureKeys(ctx); err != nil {
			return Identity{}, ErrUnauthenticated
		}
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return Identity{}, ErrUnauthenticated
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := decodeSegment(parts[0], &header); err != nil {
		return Identity{}, ErrUnauthenticated
	}
	if header.Alg != "RS256" { // fixa o algoritmo; nunca aceitar `none`/HS256
		return Identity{}, ErrUnauthenticated
	}
	key, err := v.publicKey(ctx, header.Kid)
	if err != nil {
		return Identity{}, err
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}
	if err := v.verifySignature(ctx, key, header.Kid, parts[0]+"."+parts[1], sig); err != nil {
		return Identity{}, err
	}

	var claims tokenClaims
	if err := decodeSegment(parts[1], &claims); err != nil {
		return Identity{}, ErrUnauthenticated
	}
	now := time.Now()
	if v.issuer != "" && claims.Iss != v.issuer {
		return Identity{}, ErrUnauthenticated
	}
	if v.audience != "" && !audienceContains(claims.Aud, v.audience) {
		return Identity{}, ErrUnauthenticated
	}
	if claims.Exp == nil || now.After(time.Unix(int64(*claims.Exp), 0).Add(leeway)) {
		return Identity{}, ErrUnauthenticated
	}
	if claims.Nbf != nil && now.Add(leeway).Before(time.Unix(int64(*claims.Nbf), 0)) {
		return Identity{}, ErrUnauthenticated
	}
	return Identity{
		Subject:    claims.Sub,
		ProviderID: claims.ProviderID,
		Roles:      claims.RealmAccess.Roles,
	}, nil
}

type tokenClaims struct {
	Iss         string      `json:"iss"`
	Sub         string      `json:"sub"`
	Exp         *float64    `json:"exp"`
	Nbf         *float64    `json:"nbf"`
	Aud         interface{} `json:"aud"`
	ProviderID  string      `json:"provider_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// verifySignature valida a assinatura; em falha, força uma atualização do JWKS
// e tenta uma única vez (rotação de chave mantendo o mesmo `kid`).
func (v *JWKSVerifier) verifySignature(ctx context.Context, key *rsa.PublicKey, kid, signingInput string, sig []byte) error {
	digest := sha256.Sum256([]byte(signingInput))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig) == nil {
		return nil
	}
	if err := v.refresh(ctx); err != nil {
		return ErrUnauthenticated
	}
	rotated, ok := v.lookup(kid)
	if !ok || rsa.VerifyPKCS1v15(rotated, crypto.SHA256, digest[:], sig) != nil {
		return ErrUnauthenticated
	}
	return nil
}

// ensureKeys carrega o JWKS na conjugação lazily; falha aberta →
// ErrUnauthenticated. A falha inicial NÃO é memoizada: a próxima requisição
// retenta (respeitando o backoff para não martelar um Keycloak fora do ar), de
// modo que a recuperação da fonte não exige restart.
func (v *JWKSVerifier) ensureKeys(ctx context.Context) error {
	v.initMu.Lock()
	defer v.initMu.Unlock()
	if v.initialized {
		return nil
	}
	if time.Now().Before(v.nextInitAttempt) {
		return ErrUnauthenticated
	}
	if err := v.refresh(ctx); err != nil {
		v.nextInitAttempt = time.Now().Add(v.initRetryAfter)
		return err
	}
	v.initialized = true
	return nil
}

// publicKey devolve a chave do `kid`, atualizando o cache quando necessário.
func (v *JWKSVerifier) publicKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	key, cached := v.lookup(kid)
	if cached && v.fresh() {
		return key, nil
	}
	// M9: kid desconhecido com cache fresco é normalmente rejeição (ou rotação
	// há instantes); sem o throttle cada token forjado bateria no Keycloak.
	// Cache vencido continua fazendo refresh sempre (rotação + recuperação).
	if !cached && v.fresh() && !v.unknownRefreshAllowed() {
		return nil, ErrUnauthenticated
	}
	if err := v.refresh(ctx); err != nil {
		if cached { // JWKS indisponível: serve a chave em cache
			return key, nil
		}
		return nil, ErrUnauthenticated
	}
	if key, ok := v.lookup(kid); ok {
		return key, nil
	}
	return nil, ErrUnauthenticated
}

// unknownRefreshAllowed concede o refresh por kid desconhecido de tempo em
// tempo (missingKeyRefreshGrace) — rotação adiciona o kid NOVO ao JWKS e o
// primeiro token com ele precisa de um refresh para ser aceito.
func (v *JWKSVerifier) unknownRefreshAllowed() bool {
	v.unknownMu.Lock()
	defer v.unknownMu.Unlock()
	if time.Since(v.lastUnknownRefresh) < missingKeyRefreshGrace {
		return false
	}
	v.lastUnknownRefresh = time.Now()
	return true
}

// lookup consulta as chaves em cache; JWKS sem `kid` aceita a única chave.
func (v *JWKSVerifier) lookup(kid string) (*rsa.PublicKey, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if key, ok := v.keys[kid]; ok {
		return key, true
	}
	if kid == "" && len(v.keys) == 1 {
		for _, only := range v.keys {
			return only, true
		}
	}
	return nil, false
}

func (v *JWKSVerifier) fresh() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return time.Since(v.fetchedAt) < v.ttl
}

func (v *JWKSVerifier) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS HTTP %d", resp.StatusCode)
	}
	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return err
	}
	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pub, err := k.rsaPublicKey()
		if err != nil {
			return err
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return errors.New("JWKS sem chaves RSA")
	}
	v.mu.Lock()
	v.keys = keys
	v.fetchedAt = time.Now()
	v.mu.Unlock()
	return nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (k jwk) rsaPublicKey() (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("JWKS n inválido: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("JWKS e inválido: %w", err)
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}
	if len(nBytes) == 0 || e == 0 {
		return nil, errors.New("JWKS chave RSA incompleta")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}

func decodeSegment(segment string, dst any) error {
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// audienceContains aceita `aud` como string ou lista de strings.
func audienceContains(aud any, want string) bool {
	switch v := aud.(type) {
	case string:
		return v == want
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}
