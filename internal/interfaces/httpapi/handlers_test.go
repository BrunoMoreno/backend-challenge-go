package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// --- fakes dos casos de uso ---

type fakeWalletService struct {
	in  openwallet.Input
	res openwallet.Result
	err error
}

func (f *fakeWalletService) Open(_ context.Context, in openwallet.Input) (openwallet.Result, error) {
	f.in = in
	return f.res, f.err
}

type fakeWagerService struct {
	in  processwager.Input
	res processwager.Result
	err error
}

func (f *fakeWagerService) Process(_ context.Context, in processwager.Input) (processwager.Result, error) {
	f.in = in
	return f.res, f.err
}

type fakeQueryService struct {
	wallet      wallet.Wallet
	walletErr   error
	page        query.LedgerPage
	pageErr     error
	pageLimit   int
	pageCursor  string
	tx          wager.WagerTransaction
	txErr       error
	providerTx  wager.WagerTransaction
	providerErr error
}

func (f *fakeQueryService) Wallet(context.Context, string) (wallet.Wallet, error) {
	return f.wallet, f.walletErr
}
func (f *fakeQueryService) Ledger(_ context.Context, _ string, cursor string, limit int) (query.LedgerPage, error) {
	f.pageCursor = cursor
	f.pageLimit = limit
	return f.page, f.pageErr
}
func (f *fakeQueryService) Transaction(context.Context, string) (wager.WagerTransaction, error) {
	return f.tx, f.txErr
}
func (f *fakeQueryService) ProviderTransaction(context.Context, string, string) (wager.WagerTransaction, error) {
	return f.providerTx, f.providerErr
}

// --- helpers ---

func testHandler(deps Deps) http.Handler {
	return buildHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
}

func TestHealthReady(t *testing.T) {
	ok := func(context.Context) error { return nil }
	t.Run("ok", func(t *testing.T) {
		rec := doReq(testHandler(Deps{Ready: ok}), http.MethodGet, "/health/ready", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
	})
	t.Run("componente indisponível", func(t *testing.T) {
		h := testHandler(Deps{Ready: func(context.Context) error {
			return errors.New("postgres indisponível")
		}})
		rec := doReq(h, http.MethodGet, "/health/ready", "")
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 (%s)", rec.Code, rec.Body.String())
		}
		assertErrorCode(t, rec, "NOT_READY")
	})
	t.Run("sem probe configurado", func(t *testing.T) {
		rec := doReq(testHandler(Deps{}), http.MethodGet, "/health/ready", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})
}

func newWallet(t *testing.T, id, player, balance string) wallet.Wallet {
	t.Helper()
	m, err := money.Parse(balance, "BRL")
	if err != nil {
		t.Fatalf("criar wallet: %v", err)
	}
	w, err := wallet.New(id, player, m)
	if err != nil {
		t.Fatalf("criar wallet: %v", err)
	}
	return w
}

func doReq(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-de-teste")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// doSubmit envia POST /wagering/transactions com headers de auth e idempotência.
func doSubmit(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-de-teste")
	req.Header.Set("Idempotency-Key", "key-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("resposta não é JSON: %v (%s)", err, rec.Body.String())
	}
	return v
}

// --- POST /wallets ---

func TestOpenWalletSuccess(t *testing.T) {
	w := newWallet(t, "w-1", "p-1", "1000.00")
	f := fakeWalletService{res: openwallet.Result{Wallet: w}}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Wallets: &f})

	rec := doReq(h, http.MethodPost, "/wallets",
		`{"playerId":"p-1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[walletDTO](t, rec)
	if got.ID != "w-1" || got.PlayerID != "p-1" || got.Balance.String() != "1000.00" || got.Version != 1 {
		t.Fatalf("resposta inválida: %+v", got)
	}
	if f.in.PlayerID != "p-1" || f.in.InitialBalance.String() != "1000.00" {
		t.Fatalf("input = %+v", f.in)
	}
	if got := rec.Header().Get("X-Correlation-Id"); got == "" {
		t.Fatal("X-Correlation-Id ausente na resposta")
	}
}

func TestOpenWalletAuthMatrix(t *testing.T) {
	// Provedor (sem wallet:internal) não pode abrir carteira.
	h := testHandler(Deps{
		Verifier: stubVerifier{id: providerIdentity()},
		Wallets:  &fakeWalletService{res: openwallet.Result{Wallet: newWallet(t, "w-1", "p-1", "0.00")}},
	})
	rec := doReq(h, http.MethodPost, "/wallets",
		`{"playerId":"p-1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "FORBIDDEN")

	// Sem token nenhum.
	h2 := testHandler(Deps{Verifier: stubVerifier{err: ErrUnauthenticated}, Wallets: &fakeWalletService{}})
	rec2 := doReq(h2, http.MethodPost, "/wallets", `{"playerId":"p-1"}`)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec2.Code)
	}
	assertErrorCode(t, rec2, "UNAUTHENTICATED")
}

func TestOpenWalletDuplicate(t *testing.T) {
	f := fakeWalletService{err: postgres.ErrDuplicate}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Wallets: &f})
	rec := doReq(h, http.MethodPost, "/wallets",
		`{"playerId":"p-1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "WALLET_ALREADY_EXISTS")
}

func TestOpenWalletValidation(t *testing.T) {
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Wallets: &fakeWalletService{}})
	cases := []struct {
		name string
		body string
		code string
	}{
		{"sem playerId", `{"initialBalance":{"amount":"10.00","currency":"BRL"}}`, "INVALID_FIELD"},
		{"JSON inválido", `{`, "INVALID_JSON"},
		{"sem initialBalance", `{"playerId":"p-1"}`, "INVALID_MONEY"},
		{"moeda ausente", `{"playerId":"p-1","initialBalance":{"amount":"10.00"}}`, "INVALID_CURRENCY"},
		{"formato de dinheiro", `{"playerId":"p-1","initialBalance":{"amount":"abc","currency":"BRL"}}`, "INVALID_MONEY"},
		{"moeda inválida", `{"playerId":"p-1","initialBalance":{"amount":"10.00","currency":"BR"}}`, "INVALID_CURRENCY"},
		{"saldo negativo", `{"playerId":"p-1","initialBalance":{"amount":"-10.00","currency":"BRL"}}`, "INVALID_MONEY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(h, http.MethodPost, "/wallets", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", rec.Code, rec.Body.String())
			}
			assertErrorCode(t, rec, tc.code)
		})
	}
}

// --- GET /wallets/:walletId ---

func TestGetWallet(t *testing.T) {
	w := newWallet(t, "w-1", "p-1", "500.00")
	f := fakeQueryService{wallet: w}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/w-1", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[walletDTO](t, rec)
	if got.ID != "w-1" || got.Balance.String() != "500.00" {
		t.Fatalf("resposta inválida: %+v", got)
	}
}

func TestGetWalletNotFound(t *testing.T) {
	f := fakeQueryService{walletErr: query.ErrWalletNotFound}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/nao-existe", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "WALLET_NOT_FOUND")
}

func TestGetWalletAuthMatrix(t *testing.T) {
	h := testHandler(Deps{Verifier: stubVerifier{id: providerIdentity()}, Queries: &fakeQueryService{}})
	rec := doReq(h, http.MethodGet, "/wallets/w-1", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	assertErrorCode(t, rec, "FORBIDDEN")
}

// --- GET /wallets/:walletId/ledger ---

func TestLedgerPagination(t *testing.T) {
	now := time.Now().UTC()
	entry, err := ledger.New("l-1", "w-1", "tx-1", ledger.DirectionCredit,
		money.MoneyOf(1000, "BRL"), money.MoneyOf(0, "BRL"), money.MoneyOf(1000, "BRL"), now)
	if err != nil {
		t.Fatalf("criar entry: %v", err)
	}
	f := fakeQueryService{page: query.LedgerPage{
		Entries:    []ledger.Entry{entry},
		NextCursor: "cur-opaco",
	}}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/w-1/ledger?limit=1", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	got := decodeBody[ledgerPageDTO](t, rec)
	if len(got.Items) != 1 || got.Items[0].TransactionID != "tx-1" ||
		got.Items[0].Direction != "CREDIT" || got.Items[0].Money.String() != "10.00" ||
		got.Items[0].BalanceAfter.String() != "10.00" {
		t.Fatalf("página inválida: %+v", got)
	}
	if got.NextCursor == nil || *got.NextCursor != "cur-opaco" {
		t.Fatalf("nextCursor = %v, want cur-opaco", got.NextCursor)
	}
}

func TestLedgerNoNextPage(t *testing.T) {
	f := fakeQueryService{page: query.LedgerPage{}}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/w-1/ledger", "")
	got := decodeBody[ledgerPageDTO](t, rec)
	if got.Items == nil || len(got.Items) != 0 {
		t.Fatalf("items = %v, want []", got.Items)
	}
	if got.NextCursor != nil {
		t.Fatalf("nextCursor = %v, want null", got.NextCursor)
	}
}

func TestLedgerInvalidLimit(t *testing.T) {
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &fakeQueryService{}})
	for _, limit := range []string{"0", "-1", "201", "abc"} {
		rec := doReq(h, http.MethodGet, "/wallets/w-1/ledger?limit="+limit, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("limit=%s status = %d, want 400 (%s)", limit, rec.Code, rec.Body.String())
		}
		assertErrorCode(t, rec, "INVALID_FIELD")
	}
}

func TestLedgerParameters(t *testing.T) {
	f := fakeQueryService{page: query.LedgerPage{}}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/w-1/ledger?limit=7&cursor=cur-1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	if f.pageLimit != 7 || f.pageCursor != "cur-1" {
		t.Fatalf("ledger(limit=%d, cursor=%q), want (7, cur-1)", f.pageLimit, f.pageCursor)
	}

	// Sem limit/cursor: default 50 e cursor vazio.
	f2 := fakeQueryService{}
	h2 := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f2})
	doReq(h2, http.MethodGet, "/wallets/w-1/ledger", "")
	if f2.pageLimit != 50 || f2.pageCursor != "" {
		t.Fatalf("ledger(limit=%d, cursor=%q), want (50, \"\")", f2.pageLimit, f2.pageCursor)
	}
}

func TestLedgerInvalidCursor(t *testing.T) {
	f := fakeQueryService{pageErr: query.ErrInvalidCursor}
	h := testHandler(Deps{Verifier: stubVerifier{id: internalIdentity()}, Queries: &f})
	rec := doReq(h, http.MethodGet, "/wallets/w-1/ledger?cursor=nao-opaco", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (%s)", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "INVALID_CURSOR")
}
