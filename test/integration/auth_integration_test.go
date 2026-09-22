//go:build integration

// 4.7 — Auth real e isolamento entre provedores contra Keycloak real (RF-10).
//
// Sobe a app via bootstrap.Module() + Fx em portas efêmeras (sem process
// externo), obtém tokens JWT reais via client_credentials e verifica:
//
//   - 401 sem token, token inválido e token com string "expirado" (header malformado)
//   - 403 com role errada (interno tenta POST /wagering; provedor tenta POST /wallets)
//   - 403 CanSubmitAsProvider (provider-a envia como provider-b)
//   - 403 RequireProviderPath (provider-b acessa path de provider-a)
//   - 404 isolamento por ID (provider-b consulta tx de provider-a)
//   - replay com token real não duplica saldo (idempotência com JWT real)
//   - 401/403 não produzem efeito financeiro (saldo inalterado após rejeição)
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/bootstrap"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"go.uber.org/fx"
)

// ---- constantes Keycloak ----

const (
	kcTokenURL = "http://localhost:8081/realms/wagering/protocol/openid-connect/token"
	kcIssuer   = "http://localhost:8081/realms/wagering"
	kcJWKSURL  = "http://localhost:8081/realms/wagering/protocol/openid-connect/certs"
	kcAudience = "wager-api"
)

// ---- helper: token real ----

// kcToken obtém um access token JWT real do Keycloak via client_credentials.
// Aguarda até 30 s caso o Keycloak ainda esteja subindo.
func kcToken(t *testing.T, clientID, clientSecret string) string {
	t.Helper()
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.PostForm(kcTokenURL, form)
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var tok struct {
					AccessToken string `json:"access_token"`
				}
				if json.Unmarshal(raw, &tok) == nil && tok.AccessToken != "" {
					return tok.AccessToken
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("keycloak indisponível em %s (rode `make up`)", kcTokenURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Tokens para os três clientes do realm wagering.
func kcProviderA(t *testing.T) string { return kcToken(t, "provider-a", "provider-a-secret") }
func kcProviderB(t *testing.T) string { return kcToken(t, "provider-b", "provider-b-secret") }
func kcInternal(t *testing.T) string {
	return kcToken(t, "wagering-internal", "wagering-internal-secret")
}

// ---- helper: app em processo (bootstrap.Module) ----

// authTestApp sobe a app Fx em portas efêmeras e devolve a baseURL e a função
// de teardown. Usa somente o papel "http" — SQS/outbox não são necessários
// para os testes de autenticação.
func authTestApp(t *testing.T) (baseURL string, stop func()) {
	t.Helper()
	httpAddr, httpURL := freePort(t)
	metricsAddr, _ := freePort(t)

	cfg := config.Config{
		AppRoles:               []string{"http"},
		HTTPAddr:               httpAddr,
		DatabaseURL:            testURL(),
		SQSEndpoint:            sqsEndpoint,
		SQSRegion:              "us-east-1",
		KeycloakIssuer:         kcIssuer,
		KeycloakJWKSURL:        kcJWKSURL,
		KeycloakAudience:       kcAudience,
		LogLevel:               "error",
		OutboxEventsQueueURL:   eventsQueueURL("auth"),
		OutboxBatchSize:        10,
		OutboxPollInterval:     time.Second,
		OutboxLease:            30 * time.Second,
		OutboxSendTimeout:      5 * time.Second,
		OutboxBackoffBase:      100 * time.Millisecond,
		OutboxBackoffMax:       10 * time.Second,
		SQSConsumerQueueURL:    inputQueueURL("auth"),
		SQSConsumerDLQURL:      dlqURL("auth"),
		SQSMaxReceiveCount:     5,
		SQSVisibilityTimeout:   5 * time.Second,
		SQSConsumerConcurrency: 2,
		SQSConsumerBackoffBase: time.Second,
		SQSConsumerBackoffMax:  time.Minute,
		MetricsAddr:            metricsAddr,
		ShutdownTimeout:        10 * time.Second,
	}

	app := fx.New(bootstrap.Module(), fx.Replace(cfg), fx.NopLogger)
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	// Aguarda o servidor HTTP estar pronto.
	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(httpURL + "/health/live")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop = func() {
		if err := app.Stop(ctx); err != nil {
			t.Logf("stop app: %v", err)
		}
	}
	t.Cleanup(stop)
	return httpURL, stop
}

// ---- helper: requisições HTTP ----

type authHTTPResult struct {
	StatusCode int
	Body       []byte
}

// authDo executa uma requisição HTTP com (ou sem) token Bearer.
func authDo(t *testing.T, method, target, token, idemKey string, body any) authHTTPResult {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, target, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return authHTTPResult{StatusCode: resp.StatusCode, Body: raw}
}

// assertErrorCode lê error.code de uma resposta JSON e compara.
func assertAuthErrorCode(t *testing.T, r authHTTPResult, want string) {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		t.Fatalf("decode error body: %v (%s)", err, r.Body)
	}
	if body.Error.Code != want {
		t.Fatalf("error.code = %q, want %q (body: %s)", body.Error.Code, want, r.Body)
	}
}

// openWalletAuth abre uma carteira usando o token interno e devolve o walletID.
func openWalletAuth(t *testing.T, base, token, player, amount string) string {
	t.Helper()
	r := authDo(t, "POST", base+"/wallets", token, "", map[string]any{
		"playerId": player,
		"initialBalance": map[string]any{
			"amount":   amount,
			"currency": "BRL",
		},
	})
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("open wallet = %d %s", r.StatusCode, r.Body)
	}
	var w struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(r.Body, &w); err != nil {
		t.Fatalf("decode wallet: %v", err)
	}
	return w.ID
}

// getWalletBalance lê o saldo minor da carteira pelo ID.
func getWalletBalance(t *testing.T, base, token, walletID string) string {
	t.Helper()
	r := authDo(t, "GET", base+"/wallets/"+walletID, token, "", nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("get wallet = %d %s", r.StatusCode, r.Body)
	}
	var w struct {
		Balance struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	if err := json.Unmarshal(r.Body, &w); err != nil {
		t.Fatalf("decode wallet: %v", err)
	}
	return w.Balance.Amount
}

// submitBet envia POST /wagering/transactions e devolve o status + corpo.
func submitBet(t *testing.T, base, token, idemKey, provider, ext, player, wallet, kind, amount string) authHTTPResult {
	t.Helper()
	body := map[string]any{
		"providerId":            provider,
		"externalTransactionId": ext,
		"playerId":              player,
		"walletId":              wallet,
		"roundId":               fmt.Sprintf("r-%s", ext),
		"gameId":                fmt.Sprintf("g-%s", ext),
		"kind":                  kind,
		"amount": map[string]any{
			"amount":   amount,
			"currency": "BRL",
		},
	}
	return authDo(t, "POST", base+"/wagering/transactions", token, idemKey, body)
}

// ===== TESTES =====

// ---- Tarefa 3: sem token, token inválido, string falsa ----

// TestAuth_NoToken verifica 401 em rota protegida sem Authorization.
func TestAuth_NoToken(t *testing.T) {
	base, _ := authTestApp(t)

	routes := []struct {
		method string
		path   string
	}{
		{"POST", "/wagering/transactions"},
		{"GET", "/wagering/transactions/any-id"},
		{"GET", "/providers/provider-a/wagering/transactions/any-ext"},
		{"POST", "/wallets"},
		{"GET", "/wallets/any-id"},
		{"GET", "/wallets/any-id/ledger"},
		{"POST", "/wallets/any-id/reconciliation"},
	}
	for _, tc := range routes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := authDo(t, tc.method, base+tc.path, "" /* sem token */, "k1", nil)
			if r.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (body: %s)", r.StatusCode, r.Body)
			}
			assertAuthErrorCode(t, r, "UNAUTHENTICATED")
		})
	}
}

// TestAuth_InvalidToken verifica 401 com um token JWT sintaticamente inválido.
func TestAuth_InvalidToken(t *testing.T) {
	base, _ := authTestApp(t)

	r := authDo(t, "GET", base+"/wallets/any-id", "nao.e.um.jwt.valido", "", nil)
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "UNAUTHENTICATED")
}

// TestAuth_MalformedBearerHeader verifica 401 para headers Bearer mal formados.
func TestAuth_MalformedBearerHeader(t *testing.T) {
	base, _ := authTestApp(t)
	client := &http.Client{Timeout: 10 * time.Second}

	// Token vazio / bearer sem valor → o middleware rejeita antes de verificar.
	for _, header := range []string{
		"Bearer ",
		"Token abc",
		"Basic abc",
		"abc",
	} {
		req, _ := http.NewRequest("GET", base+"/wallets/any-id", nil)
		req.Header.Set("Authorization", header)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Authorization=%q: status = %d, want 401 (body: %s)", header, resp.StatusCode, raw)
		}
	}
}

// TestAuth_WrongIssuerToken verifica 401 com um token que apesar de JWT válido
// contém issuer errado (emitido por outro realm / string inventada). Usa um
// token de um client real mas substituímos 1 char do payload para corromper
// a assinatura — simula um token de outro ambiente.
func TestAuth_TamperedToken(t *testing.T) {
	base, _ := authTestApp(t)
	tok := kcProviderA(t)
	// Altera um byte do payload (parte central) para invalidar a assinatura.
	parts := splitJWT(tok)
	if len(parts) != 3 {
		t.Fatalf("token não tem 3 partes")
	}
	payload := []byte(parts[1])
	payload[0] ^= 0x01 // flip de 1 bit
	tampered := parts[0] + "." + string(payload) + "." + parts[2]

	r := authDo(t, "GET", base+"/wallets/any-id", tampered, "", nil)
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "UNAUTHENTICATED")
}

// splitJWT separa as 3 partes de um token JWT sem validar.
func splitJWT(tok string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(tok); i++ {
		if tok[i] == '.' {
			parts = append(parts, tok[start:i])
			start = i + 1
		}
	}
	parts = append(parts, tok[start:])
	return parts
}

// ---- Tarefa 4: role errada → 403 ----

// TestAuth_InternalTriesPostWagering verifica que o serviço interno (sem role
// wagering:provider) recebe 403 ao tentar POST /wagering/transactions.
func TestAuth_InternalTriesPostWagering(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)

	// Usa um body válido sintaticamente — a autorização deve barrar antes do caso de uso.
	r := submitBet(t, base, intToken, "idem-int-1", "wagering-internal",
		"ext-int-1", "player-int-1", "wallet-int-1", "BET", "10.00")
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")
}

// TestAuth_ProviderTriesPostWallets verifica que provedor (sem role wallet:internal)
// recebe 403 ao tentar POST /wallets.
func TestAuth_ProviderTriesPostWallets(t *testing.T) {
	base, _ := authTestApp(t)
	tok := kcProviderA(t)

	r := authDo(t, "POST", base+"/wallets", tok, "", map[string]any{
		"playerId":       "p-wallet-1",
		"initialBalance": map[string]any{"amount": "100.00", "currency": "BRL"},
	})
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")
}

// TestAuth_ProviderTriesGetWallet verifica 403 ao tentar GET /wallets/:id.
func TestAuth_ProviderTriesGetWallet(t *testing.T) {
	base, _ := authTestApp(t)
	tok := kcProviderA(t)

	r := authDo(t, "GET", base+"/wallets/some-wallet-id", tok, "", nil)
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")
}

// ---- Tarefa 5: CanSubmitAsProvider — provider-a envia como provider-b ----

// TestAuth_CanSubmitAsProvider_CrossProvider verifica 403 quando provider-a
// tenta enviar com providerId="provider-b" no corpo.
func TestAuth_CanSubmitAsProvider_CrossProvider(t *testing.T) {
	base, _ := authTestApp(t)
	tokA := kcProviderA(t) // token de provider-a

	// Envia como provider-b — CanSubmitAsProvider deve barrar.
	r := submitBet(t, base, tokA, "idem-cross-1", "provider-b",
		"ext-cross-1", "player-cross-1", "wallet-cross-1", "BET", "10.00")
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")
}

// ---- Tarefa 6 & 7: isolamento de consulta por ID e path ----

// TestAuth_IsolationByID verifica que provider-b recebe 404 (não 403) ao
// consultar a transação de provider-a via GET /wagering/transactions/:id.
// 404 é intencional — evita vazar a existência da transação (anti-enumeração).
func TestAuth_IsolationByID(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)
	tokA := kcProviderA(t)
	tokB := kcProviderB(t)

	// Prepara: abre carteira e envia BET de provider-a.
	playerA := unique("player-iso-id")
	walletID := openWalletAuth(t, base, intToken, playerA, "100.00")

	r := submitBet(t, base, tokA, unique("idem-iso-id"),
		"provider-a", unique("ext-iso-id"), playerA, walletID, "BET", "10.00")
	// Pode retornar 201 (PROCESSED) ou 503 transitório — aguarda o resultado.
	for r.StatusCode == http.StatusServiceUnavailable {
		time.Sleep(150 * time.Millisecond)
		r = submitBet(t, base, tokA, unique("idem-iso-id2"),
			"provider-a", unique("ext-iso-id2"), playerA, walletID, "BET", "10.00")
	}
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("bet de provider-a = %d %s", r.StatusCode, r.Body)
	}
	var betRes struct {
		TransactionID string `json:"transactionId"`
	}
	if err := json.Unmarshal(r.Body, &betRes); err != nil {
		t.Fatalf("decode bet: %v", err)
	}
	txID := betRes.TransactionID

	// provider-a pode consultar a própria transação.
	rA := authDo(t, "GET", base+"/wagering/transactions/"+txID, tokA, "", nil)
	if rA.StatusCode != http.StatusOK {
		t.Fatalf("provider-a consulta própria tx = %d %s", rA.StatusCode, rA.Body)
	}

	// provider-b recebe 404 (não 403) — anti-enumeração.
	rB := authDo(t, "GET", base+"/wagering/transactions/"+txID, tokB, "", nil)
	if rB.StatusCode != http.StatusNotFound {
		t.Fatalf("provider-b consulta tx de provider-a = %d, want 404 (body: %s)", rB.StatusCode, rB.Body)
	}
	assertAuthErrorCode(t, rB, "TRANSACTION_NOT_FOUND")
}

// TestAuth_IsolationByProviderPath verifica 403 quando provider-b tenta acessar
// GET /providers/provider-a/wagering/transactions/:extId.
func TestAuth_IsolationByProviderPath(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)
	tokA := kcProviderA(t)
	tokB := kcProviderB(t)

	// Prepara: abre carteira e envia BET de provider-a.
	playerA := unique("player-iso-path")
	walletID := openWalletAuth(t, base, intToken, playerA, "100.00")
	extID := unique("ext-iso-path")

	r := submitBet(t, base, tokA, unique("idem-iso-path"),
		"provider-a", extID, playerA, walletID, "BET", "10.00")
	for r.StatusCode == http.StatusServiceUnavailable {
		time.Sleep(150 * time.Millisecond)
		r = submitBet(t, base, tokA, unique("idem-iso-path2"),
			"provider-a", extID, playerA, walletID, "BET", "10.00")
	}
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("bet de provider-a = %d %s", r.StatusCode, r.Body)
	}

	// provider-a pode consultar pelo path próprio.
	rA := authDo(t, "GET",
		base+"/providers/provider-a/wagering/transactions/"+extID, tokA, "", nil)
	if rA.StatusCode != http.StatusOK {
		t.Fatalf("provider-a consulta próprio path = %d %s", rA.StatusCode, rA.Body)
	}

	// provider-b recebe 403 ao tentar o path de provider-a.
	rB := authDo(t, "GET",
		base+"/providers/provider-a/wagering/transactions/"+extID, tokB, "", nil)
	if rB.StatusCode != http.StatusForbidden {
		t.Fatalf("provider-b acessa path de provider-a = %d, want 403 (body: %s)", rB.StatusCode, rB.Body)
	}
	assertAuthErrorCode(t, rB, "FORBIDDEN")

	// Interno pode consultar qualquer path sem ser bloqueado por RequireProviderPath.
	rInt := authDo(t, "GET",
		base+"/providers/provider-a/wagering/transactions/"+extID, intToken, "", nil)
	if rInt.StatusCode != http.StatusOK {
		t.Fatalf("interno consulta qualquer path = %d %s", rInt.StatusCode, rInt.Body)
	}
}

// ---- Tarefa 8: replay com token real não duplica saldo ----

// TestAuth_ReplayIdempotent verifica que um segundo POST /wagering/transactions
// com a mesma Idempotency-Key e token real devolve idempotentReplay:true sem
// movimentar saldo.
func TestAuth_ReplayIdempotent(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)
	tokA := kcProviderA(t)

	player := unique("player-replay")
	walletID := openWalletAuth(t, base, intToken, player, "100.00")
	extID := unique("ext-replay")
	idemKey := unique("idem-replay")

	// Primeira submissão.
	r1 := submitBet(t, base, tokA, idemKey, "provider-a", extID, player, walletID, "BET", "20.00")
	for r1.StatusCode == http.StatusServiceUnavailable {
		time.Sleep(150 * time.Millisecond)
		r1 = submitBet(t, base, tokA, idemKey, "provider-a", extID, player, walletID, "BET", "20.00")
	}
	if r1.StatusCode != http.StatusCreated {
		t.Fatalf("1ª bet = %d %s", r1.StatusCode, r1.Body)
	}
	var res1 struct {
		IdempotentReplay bool                     `json:"idempotentReplay"`
		Balance          *struct{ Amount string } `json:"balance"`
	}
	if err := json.Unmarshal(r1.Body, &res1); err != nil {
		t.Fatalf("decode 1ª bet: %v", err)
	}
	if res1.IdempotentReplay {
		t.Fatal("1ª submissão não pode ser replay")
	}
	balanceAfterFirst := res1.Balance.Amount // deve ser "80.00"

	// Segunda submissão com a mesma chave de idempotência.
	r2 := submitBet(t, base, tokA, idemKey, "provider-a", extID, player, walletID, "BET", "20.00")
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("2ª bet (replay) = %d %s", r2.StatusCode, r2.Body)
	}
	var res2 struct {
		IdempotentReplay bool                     `json:"idempotentReplay"`
		Balance          *struct{ Amount string } `json:"balance"`
	}
	if err := json.Unmarshal(r2.Body, &res2); err != nil {
		t.Fatalf("decode 2ª bet: %v", err)
	}
	if !res2.IdempotentReplay {
		t.Fatal("2ª submissão deve ser idempotentReplay:true")
	}
	if res2.Balance == nil || res2.Balance.Amount != balanceAfterFirst {
		t.Fatalf("saldo do replay = %v, want %s", res2.Balance, balanceAfterFirst)
	}

	// Confirma que o saldo real na carteira não mudou na segunda chamada.
	currentBalance := getWalletBalance(t, base, intToken, walletID)
	if currentBalance != balanceAfterFirst {
		t.Fatalf("saldo atual = %s, want %s (saldo duplicado pelo replay?)", currentBalance, balanceAfterFirst)
	}
}

// ---- Tarefa 9: 401/403 não produzem efeito financeiro ----

// TestAuth_NoFinancialEffectOn401 verifica que requisições rejeitadas com 401
// não movimentam saldo nem inserem transações.
func TestAuth_NoFinancialEffectOn401(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)

	player := unique("player-noeff401")
	walletID := openWalletAuth(t, base, intToken, player, "50.00")
	balanceBefore := getWalletBalance(t, base, intToken, walletID)

	// Sem token: 401.
	r := submitBet(t, base, "" /* sem token */, unique("idem-401"),
		"provider-a", unique("ext-401"), player, walletID, "BET", "5.00")
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %s)", r.StatusCode, r.Body)
	}

	// Token inválido: 401.
	r2 := submitBet(t, base, "token.invalido.jwt", unique("idem-401b"),
		"provider-a", unique("ext-401b"), player, walletID, "BET", "5.00")
	if r2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %s)", r2.StatusCode, r2.Body)
	}

	// Saldo deve ser idêntico ao inicial.
	balanceAfter := getWalletBalance(t, base, intToken, walletID)
	if balanceAfter != balanceBefore {
		t.Fatalf("saldo alterado por 401: antes=%s depois=%s", balanceBefore, balanceAfter)
	}
}

// TestAuth_NoFinancialEffectOn403 verifica que requisições rejeitadas com 403
// não movimentam saldo.
func TestAuth_NoFinancialEffectOn403(t *testing.T) {
	base, _ := authTestApp(t)
	intToken := kcInternal(t)
	tokB := kcProviderB(t) // provider-b tentando enviar como provider-a

	player := unique("player-noeff403")
	walletID := openWalletAuth(t, base, intToken, player, "50.00")
	balanceBefore := getWalletBalance(t, base, intToken, walletID)

	// provider-b tenta enviar como provider-a: 403 CanSubmitAsProvider.
	r := submitBet(t, base, tokB, unique("idem-403"),
		"provider-a", unique("ext-403"), player, walletID, "BET", "5.00")
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")

	// Interno tentando POST /wagering: 403.
	r2 := submitBet(t, base, intToken, unique("idem-403b"),
		"wagering-internal", unique("ext-403b"), player, walletID, "BET", "5.00")
	if r2.StatusCode != http.StatusForbidden {
		t.Fatalf("status interno = %d, want 403 (body: %s)", r2.StatusCode, r2.Body)
	}

	// Saldo deve ser idêntico ao inicial.
	balanceAfter := getWalletBalance(t, base, intToken, walletID)
	if balanceAfter != balanceBefore {
		t.Fatalf("saldo alterado por 403: antes=%s depois=%s", balanceBefore, balanceAfter)
	}
}

// TestAuth_ProviderBCannotSubmitAsBForProviderAWallet verifica que provider-b
// usando o próprio providerId (provider-b) não consegue BET na carteira de um
// player de provider-a (a carteira não pertence ao provider-b, mas o token
// está correto para provider-b — o check aqui é só de carteira/provedor).
// Este teste valida que o 403 chega ANTES de qualquer acesso ao banco quando
// o corpo tem providerId diferente do token.
func TestAuth_TokenProviderMatchesBodyProviderId(t *testing.T) {
	base, _ := authTestApp(t)
	tokA := kcProviderA(t)

	// Token de provider-a, corpo com providerId=provider-b → 403 imediato.
	r := submitBet(t, base, tokA, unique("idem-mismatch"),
		"provider-b", unique("ext-mismatch"), "player-any", "wallet-any", "BET", "10.00")
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body: %s)", r.StatusCode, r.Body)
	}
	assertAuthErrorCode(t, r, "FORBIDDEN")
}
