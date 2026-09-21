package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
)

func providerBIdentity() Identity {
	return Identity{Subject: "sa-b", ProviderID: "provider-b", Roles: []string{RoleProvider}}
}

func newBet(t *testing.T, kind wager.Kind, state wager.State) wager.WagerTransaction {
	t.Helper()
	tx, err := wager.NewExternal("tx-1", "provider-a", "bet-ext-1", "key-1", "hash-1",
		"w-1", "p-1", "r-1", "g-1", kind, money.MoneyOf(8000, "BRL"), "")
	if err != nil {
		t.Fatalf("criar transação: %v", err)
	}
	switch state {
	case wager.StateProcessed:
		if err := tx.MarkProcessed(money.MoneyOf(2000, "BRL")); err != nil {
			t.Fatalf("processar: %v", err)
		}
	case wager.StateRejected:
		if err := tx.Reject(wager.FailureInsufficientFunds); err != nil {
			t.Fatalf("rejeitar: %v", err)
		}
	case wager.StatePendingReference:
		if err := tx.PendForReference(); err != nil {
			t.Fatalf("pendência: %v", err)
		}
	}
	return tx
}

const betBody = `{"providerId":"provider-a","externalTransactionId":"bet-ext-1",` +
	`"playerId":"p-1","walletId":"w-1","roundId":"r-1","gameId":"g-1",` +
	`"kind":"BET","amount":{"amount":"80.00","currency":"BRL"}}`

func submitRouter(f *fakeWagerService, id Identity) http.Handler {
	return testHandler(Deps{Verifier: stubVerifier{id: id}, Wagers: f, Queries: &fakeQueryService{}})
}

func TestSubmitFirstTime(t *testing.T) {
	bal := money.MoneyOf(2000, "BRL")
	f := fakeWagerService{res: processwager.Result{
		TransactionID: "tx-1",
		State:         wager.StateProcessed,
		Balance:       &bal,
	}}
	h := submitRouter(&f, providerIdentity())
	rec := doSubmit(h, betBody)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[processResultDTO](t, rec)
	if got.TransactionID != "tx-1" || got.Status != "PROCESSED" ||
		got.IdempotentReplay || got.Balance == nil || got.Balance.String() != "20.00" {
		t.Fatalf("resposta inválida: %+v", got)
	}
	if f.in.Kind != wager.KindBet || f.in.Amount.String() != "80.00" || f.in.WalletID != "w-1" {
		t.Fatalf("input = %+v", f.in)
	}
}

func TestSubmitReplay(t *testing.T) {
	bal := money.MoneyOf(2000, "BRL")
	f := fakeWagerService{res: processwager.Result{
		TransactionID:    "tx-1",
		State:            wager.StateProcessed,
		Balance:          &bal,
		IdempotentReplay: true,
	}}
	h := submitRouter(&f, providerIdentity())
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(betBody))
	req.Header.Set("Authorization", "Bearer token-de-teste")
	req.Header.Set("Idempotency-Key", "key-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[processResultDTO](t, rec)
	if !got.IdempotentReplay || got.Status != "PROCESSED" {
		t.Fatalf("resposta inválida: %+v", got)
	}
	if f.in.IdempotencyKey != "key-1" {
		t.Fatalf("idempotencyKey = %q, want key-1", f.in.IdempotencyKey)
	}
}

func TestSubmitRejected(t *testing.T) {
	f := fakeWagerService{res: processwager.Result{
		TransactionID: "tx-1",
		State:         wager.StateRejected,
		FailureCode:   wager.FailureInsufficientFunds,
	}}
	h := submitRouter(&f, providerIdentity())
	rec := doSubmit(h, betBody)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[processResultDTO](t, rec)
	if got.Status != "REJECTED" || got.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("resposta inválida: %+v", got)
	}
}

func TestSubmitPendingReference(t *testing.T) {
	f := fakeWagerService{res: processwager.Result{
		TransactionID: "tx-1",
		State:         wager.StatePendingReference,
	}}
	h := submitRouter(&f, providerIdentity())
	rec := doSubmit(h, betBody)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[processResultDTO](t, rec)
	if got.Status != "PENDING_REFERENCE" || got.IdempotentReplay {
		t.Fatalf("resposta inválida: %+v", got)
	}
}

func TestSubmitAuthMatrix(t *testing.T) {
	cases := []struct {
		name   string
		id     Identity
		body   string
		status int
		code   string
	}{
		{"provedor B enviando como A", providerBIdentity(), betBody, http.StatusForbidden, "FORBIDDEN"},
		{"sem role de provedor", internalIdentity(), betBody, http.StatusForbidden, "FORBIDDEN"},
		{"provedor no corpo divergente", providerIdentity(),
			`{"providerId":"provider-b","externalTransactionId":"e","playerId":"p","walletId":"w",` +
				`"kind":"BET","amount":{"amount":"1.00","currency":"BRL"}}`,
			http.StatusForbidden, "FORBIDDEN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeWagerService{}
			h := submitRouter(&f, tc.id)
			rec := doReq(h, http.MethodPost, "/wagering/transactions", tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.status, rec.Body.String())
			}
			assertErrorCode(t, rec, tc.code)
			// Demandamos que a chamada do caso de uso não ocorra.
			if f.in.ProviderID != "" {
				t.Fatalf("caso de uso chamado com providerID=%q após erro %d", f.in.ProviderID, rec.Code)
			}
		})
	}
}

func TestSubmitNoToken(t *testing.T) {
	f := fakeWagerService{}
	h := submitRouter(&f, providerIdentity())
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(betBody))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "UNAUTHENTICATED")
}

func TestSubmitValidation(t *testing.T) {
	f := fakeWagerService{}
	h := submitRouter(&f, providerIdentity())
	cases := []struct {
		name string
		body string
		code string
	}{
		{"sem Idempotency-Key", betBody, "MISSING_IDEMPOTENCY_KEY"},
		{"JSON inválido", `{`, "INVALID_JSON"},
		{"sem externalTransactionId",
			`{"providerId":"provider-a","playerId":"p-1","walletId":"w-1",` +
				`"kind":"BET","amount":{"amount":"80.00","currency":"BRL"}}`,
			"INVALID_FIELD"},
		{"kind OPENING",
			`{"providerId":"provider-a","externalTransactionId":"bet-ext-1","playerId":"p-1",` +
				`"walletId":"w-1","kind":"OPENING","amount":{"amount":"80.00","currency":"BRL"}}`,
			"OPENING_NOT_ALLOWED"},
		{"kind desconhecido",
			`{"providerId":"provider-a","externalTransactionId":"bet-ext-1","playerId":"p-1",` +
				`"walletId":"w-1","kind":"CASH_OUT","amount":{"amount":"80.00","currency":"BRL"}}`,
			"UNKNOWN_KIND"},
		{"dinheiro malformado",
			`{"providerId":"provider-a","externalTransactionId":"bet-ext-1","playerId":"p-1",` +
				`"walletId":"w-1","kind":"BET","amount":{"amount":"abc","currency":"BRL"}}`,
			"INVALID_MONEY"},
		{"moeda inválida",
			`{"providerId":"provider-a","externalTransactionId":"bet-ext-1","playerId":"p-1",` +
				`"walletId":"w-1","kind":"BET","amount":{"amount":"1.00","currency":"BRLX"}}`,
			"INVALID_CURRENCY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			if tc.name == "sem Idempotency-Key" {
				rec = doReq(h, http.MethodPost, "/wagering/transactions", tc.body)
			} else {
				rec = doSubmit(h, tc.body)
			}
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", rec.Code, rec.Body.String())
			}
			assertErrorCode(t, rec, tc.code)
		})
	}
}

func TestSubmitBusinessErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code string
	}{
		{"carteira inexistente", processwager.ErrWalletNotFound, "WALLET_NOT_FOUND"},
		{"conflito de idempotência", processwager.ErrIdempotencyConflict, "IDEMPOTENCY_KEY_CONFLICT"},
		{"conflito externo", processwager.ErrExternalConflict, "EXTERNAL_TRANSACTION_CONFLICT"},
		{"moeda divergente", processwager.ErrWalletCurrencyMismatch, "INVALID_CURRENCY"},
		{"conflito transitório", processwager.ErrStaleClaim, "UNAVAILABLE"},
		{"falha ao gerar id", processwager.ErrGenerateID, "UNAVAILABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeWagerService{err: tc.err}
			h := submitRouter(&f, providerIdentity())
			rec := doSubmit(h, betBody)
			switch tc.err {
			case processwager.ErrWalletNotFound:
				if rec.Code != http.StatusNotFound {
					t.Fatalf("status = %d, want 404 (%s)", rec.Code, rec.Body.String())
				}
			case processwager.ErrIdempotencyConflict, processwager.ErrExternalConflict:
				if rec.Code != http.StatusConflict {
					t.Fatalf("status = %d, want 409 (%s)", rec.Code, rec.Body.String())
				}
			case processwager.ErrWalletCurrencyMismatch:
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 (%s)", rec.Code, rec.Body.String())
				}
			case processwager.ErrStaleClaim, processwager.ErrGenerateID:
				if rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("status = %d, want 503 (%s)", rec.Code, rec.Body.String())
				}
			}
			assertErrorCode(t, rec, tc.code)
		})
	}
}

func TestSubmitRetryAfterOnUnavailable(t *testing.T) {
	f := fakeWagerService{err: processwager.ErrStaleClaim}
	h := submitRouter(&f, providerIdentity())
	rec := doSubmit(h, betBody)
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After ausente no 503")
	}
}

// --- GET /wagering/transactions/:transactionId ---

func TestGetTransactionOwnership(t *testing.T) {
	tx := newBet(t, wager.KindBet, wager.StateProcessed)
	cases := []struct {
		name   string
		id     Identity
		status int
	}{
		{"provedor dono", providerIdentity(), http.StatusOK},
		{"interno", internalIdentity(), http.StatusOK},
		{"outro provedor", providerBIdentity(), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeQueryService{tx: tx}
			h := testHandler(Deps{Verifier: stubVerifier{id: tc.id}, Queries: &f})
			rec := doReq(h, http.MethodGet, "/wagering/transactions/tx-1", "")
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.status, rec.Body.String())
			}
			if tc.status == http.StatusNotFound {
				assertErrorCode(t, rec, "TRANSACTION_NOT_FOUND")
			}
		})
	}
}

func TestGetTransactionShape(t *testing.T) {
	tx := newBet(t, wager.KindBet, wager.StateProcessed)
	f := fakeQueryService{tx: tx}
	h := testHandler(Deps{Verifier: stubVerifier{id: providerIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wagering/transactions/tx-1", "")

	got := decodeBody[transactionDTO](t, rec)
	if got.TransactionID != "tx-1" || got.ProviderID != "provider-a" ||
		got.ExternalTransactionID != "bet-ext-1" || got.Kind != "BET" ||
		got.Status != "PROCESSED" || got.Balance == nil || got.Balance.String() != "20.00" ||
		got.Money.String() != "80.00" {
		t.Fatalf("resposta inválida: %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatal("timestamps ausentes")
	}
}

func TestGetTransactionNotFound(t *testing.T) {
	f := fakeQueryService{txErr: query.ErrTransactionNotFound}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wagering/transactions/nao-existe", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	assertErrorCode(t, rec, "TRANSACTION_NOT_FOUND")
}

// --- GET /providers/:providerId/wagering/transactions/:externalTransactionId ---

func TestGetProviderTransactionByExternalID(t *testing.T) {
	tx := newBet(t, wager.KindBet, wager.StateProcessed)
	f := fakeQueryService{providerTx: tx}
	h := testHandler(Deps{
		Verifier: stubVerifier{id: providerIdentity()},
		Queries:  &f,
	})
	rec := doReq(h, http.MethodGet,
		"/providers/provider-a/wagering/transactions/bet-ext-1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[transactionDTO](t, rec)
	if got.TransactionID != "tx-1" || got.Status != "PROCESSED" {
		t.Fatalf("resposta inválida: %+v", got)
	}
}

func TestGetProviderTransactionCrossProvider(t *testing.T) {
	h := testHandler(Deps{
		Verifier: stubVerifier{id: providerBIdentity()},
		Queries:  &fakeQueryService{providerTx: newBet(t, wager.KindBet, wager.StateProcessed)},
	})
	rec := doReq(h, http.MethodGet,
		"/providers/provider-a/wagering/transactions/bet-ext-1", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "FORBIDDEN")
}

func TestGetProviderTransactionNotFound(t *testing.T) {
	f := fakeQueryService{providerErr: query.ErrTransactionNotFound}
	h := testHandler(Deps{
		Verifier: stubVerifier{id: internalIdentity()},
		Queries:  &f,
	})
	rec := doReq(h, http.MethodGet,
		"/providers/provider-a/wagering/transactions/nao-existe", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "TRANSACTION_NOT_FOUND")
}
