package openwallet

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// fakeDB é um storage em memória com semântica de transação por snapshot
// (Rollback descarta o que a transação fez; Commit consolida).
type fakeDB struct {
	mu     sync.Mutex
	state  fakeState
	beginN int
}

type fakeState struct {
	wallets []wallet.Wallet
	wagers  []wager.WagerTransaction
	ledger  []ledger.Entry
	outbox  []events.Envelope
}

func (s fakeState) clone() fakeState {
	s.wallets = append([]wallet.Wallet(nil), s.wallets...)
	s.wagers = append([]wager.WagerTransaction(nil), s.wagers...)
	s.ledger = append([]ledger.Entry(nil), s.ledger...)
	s.outbox = append([]events.Envelope(nil), s.outbox...)
	return s
}

func (d *fakeDB) Begin(ctx context.Context) (storage.UnitOfWork, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.beginN++
	return &fakeUoW{db: d, live: d.state.clone()}, nil
}

type fakeUoW struct {
	db   *fakeDB
	live fakeState // acumula as mudanças desta transação
}

func (u *fakeUoW) Wallets() storage.WalletRepository { return &fakeWalletRepo{u} }
func (u *fakeUoW) Wagers() storage.WagerRepository   { return &fakeWagerRepo{u} }
func (u *fakeUoW) Ledger() storage.LedgerRepository  { return &fakeLedgerRepo{u} }
func (u *fakeUoW) Outbox() storage.OutboxRepository  { return &fakeOutboxRepo{u} }

func (u *fakeUoW) Commit(ctx context.Context) error {
	u.db.mu.Lock()
	defer u.db.mu.Unlock()
	u.db.state = u.live
	return nil
}

func (u *fakeUoW) Rollback(ctx context.Context) error { return nil }

type fakeWalletRepo struct{ u *fakeUoW }

func (r *fakeWalletRepo) walletExists(walletID, player, currency string) bool {
	w := r.u.live.wallets
	if walletID != "" {
		for _, ex := range w {
			if ex.ID() == walletID {
				return true
			}
		}
	}
	for _, ex := range w {
		if ex.PlayerID() == player && string(ex.Currency()) == currency {
			return true
		}
	}
	return false
}

func (r *fakeWalletRepo) Insert(ctx context.Context, w wallet.Wallet) error {
	if r.walletExists(w.ID(), w.PlayerID(), string(w.Currency())) {
		return postgres.ErrDuplicate
	}
	r.u.live.wallets = append(r.u.live.wallets, w)
	return nil
}

func (r *fakeWalletRepo) Get(ctx context.Context, id string) (wallet.Wallet, error) {
	for _, w := range r.u.live.wallets {
		if w.ID() == id {
			return w, nil
		}
	}
	return wallet.Wallet{}, postgres.ErrNotFound
}

func (r *fakeWalletRepo) LockForUpdate(ctx context.Context, id string) (wallet.Wallet, error) {
	return r.Get(ctx, id)
}

func (r *fakeWalletRepo) UpdateBalance(ctx context.Context, w wallet.Wallet) error {
	for i, ex := range r.u.live.wallets {
		if ex.ID() == w.ID() {
			if ex.Version() != w.Version() {
				return postgres.ErrOptimisticLock
			}
			r.u.live.wallets[i] = w
			return nil
		}
	}
	return postgres.ErrNotFound
}

type fakeWagerRepo struct{ u *fakeUoW }

func (r *fakeWagerRepo) Insert(ctx context.Context, t wager.WagerTransaction) error {
	r.u.live.wagers = append(r.u.live.wagers, t)
	return nil
}

func (r *fakeWagerRepo) InsertIfAbsent(ctx context.Context, t wager.WagerTransaction) (bool, error) {
	for _, ex := range r.u.live.wagers {
		if ex.ID() == t.ID() {
			return false, nil
		}
	}
	r.u.live.wagers = append(r.u.live.wagers, t)
	return true, nil
}

func (r *fakeWagerRepo) UpdateTerminal(ctx context.Context, t wager.WagerTransaction) error {
	for i, ex := range r.u.live.wagers {
		if ex.ID() == t.ID() {
			r.u.live.wagers[i] = t
			return nil
		}
	}
	return postgres.ErrNotFound
}

func (r *fakeWagerRepo) UpdatePendingReference(ctx context.Context, t wager.WagerTransaction) error {
	for i, ex := range r.u.live.wagers {
		if ex.ID() == t.ID() {
			r.u.live.wagers[i] = t
			return nil
		}
	}
	return postgres.ErrNotFound
}

func (r *fakeWagerRepo) GetByIdempotencyKey(ctx context.Context, key string) (wager.WagerTransaction, error) {
	for _, ex := range r.u.live.wagers {
		if ex.IdempotencyKey() == key {
			return ex, nil
		}
	}
	return wager.WagerTransaction{}, postgres.ErrNotFound
}

func (r *fakeWagerRepo) GetByIdempotencyKeyForUpdate(ctx context.Context, key string) (wager.WagerTransaction, error) {
	return r.GetByIdempotencyKey(ctx, key)
}

func (r *fakeWagerRepo) GetByProviderExternal(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error) {
	for _, ex := range r.u.live.wagers {
		if ex.ProviderID() == providerID && ex.ExternalTransactionID() == externalID {
			return ex, nil
		}
	}
	return wager.WagerTransaction{}, postgres.ErrNotFound
}

func (r *fakeWagerRepo) GetByReferenceExternal(ctx context.Context, providerID, referenceExternalID string) (wager.WagerTransaction, error) {
	return r.GetByProviderExternal(ctx, providerID, referenceExternalID)
}

func (r *fakeWagerRepo) GetReversalForReference(ctx context.Context, targetID string) (wager.WagerTransaction, error) {
	for _, ex := range r.u.live.wagers {
		if ex.ResolvedReferenceTransactionID() == targetID {
			return ex, nil
		}
	}
	return wager.WagerTransaction{}, postgres.ErrNotFound
}

type fakeLedgerRepo struct{ u *fakeUoW }

func (r *fakeLedgerRepo) Insert(ctx context.Context, e ledger.Entry) error {
	r.u.live.ledger = append(r.u.live.ledger, e)
	return nil
}

type fakeOutboxRepo struct{ u *fakeUoW }

func (r *fakeOutboxRepo) Insert(ctx context.Context, env events.Envelope) error {
	r.u.live.outbox = append(r.u.live.outbox, env)
	return nil
}

// contador determinístico de ids.
func countIDs() func() string {
	var i int
	return func() string {
		i++
		return "id-" + countStr(i)
	}
}

func countStr(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('A' + i - 10))
}

func moneyBRL(t *testing.T, v string) money.Money {
	t.Helper()
	m, err := money.Parse(v, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOpenZeroBalanceDoesNothingElse(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{}
	s := NewServiceWithIDs(db, countIDs())

	res, err := s.Open(ctx, Input{PlayerID: "p1", InitialBalance: moneyBRL(t, "0.00")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if res.Opening != nil {
		t.Fatal("saldo zero não deveria criar OPENING")
	}
	if res.Wallet.Balance().Minor() != 0 || res.Wallet.Version() != 1 {
		t.Fatalf("wallet = saldo %d v%d, want 0/1", res.Wallet.Balance().Minor(), res.Wallet.Version())
	}
	if len(db.state.wallets) != 1 || len(db.state.wagers) != 0 || len(db.state.ledger) != 0 || len(db.state.outbox) != 0 {
		t.Fatalf("persistido: wallets=%d wagers=%d ledger=%d outbox=%d",
			len(db.state.wallets), len(db.state.wagers), len(db.state.ledger), len(db.state.outbox))
	}
}

func TestOpenPositivePersistsOpeningLedgerAndTwoEvents(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{}
	s := NewServiceWithIDs(db, countIDs())

	res, err := s.Open(ctx, Input{PlayerID: "p1", InitialBalance: moneyBRL(t, "5.00")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if res.Wallet.Version() != 2 || res.Wallet.Balance().Minor() != 500 {
		t.Fatalf("wallet = saldo %d v%d, want 500/2", res.Wallet.Balance().Minor(), res.Wallet.Version())
	}
	if res.Opening == nil {
		t.Fatal("saldo positivo deveria criar OPENING")
	}
	tx := *res.Opening
	if tx.Kind() != wager.KindOpening || !tx.IsTerminal() || tx.State() != wager.StateProcessed {
		t.Fatalf("opening = %s/%s, want OPENING PROCESSED", tx.Kind(), tx.State())
	}

	if len(db.state.wagers) != 1 || len(db.state.ledger) != 1 || len(db.state.outbox) != 2 {
		t.Fatalf("persistido: wagers=%d ledger=%d outbox=%d, want 1/1/2",
			len(db.state.wagers), len(db.state.ledger), len(db.state.outbox))
	}
	e := db.state.ledger[0]
	if e.Direction() != ledger.DirectionCredit || e.BalanceBefore().Minor() != 0 || e.BalanceAfter().Minor() != 500 {
		t.Fatalf("ledger = %s %d→%d, want CREDIT 0→500",
			e.Direction(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor())
	}

	types := map[events.Type]bool{}
	var balanceVersion int64
	for _, env := range db.state.outbox {
		types[env.EventType] = true
		if env.EventType == events.TypeWalletBalanceChanged {
			var d events.WalletBalanceChangedData
			if err := json.Unmarshal(env.Data, &d); err != nil {
				t.Fatal(err)
			}
			balanceVersion = d.WalletVersion
		}
	}
	if !types[events.TypeWagerTransactionProcessed] || !types[events.TypeWalletBalanceChanged] {
		t.Fatalf("eventos = %v, want Processed + BalanceChanged", types)
	}
	if balanceVersion != 2 {
		t.Fatalf("walletVersion do evento = %d, want 2", balanceVersion)
	}
}

func TestOpenDuplicatePlayerCurrencyRejected(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{}
	s := NewServiceWithIDs(db, countIDs())

	if _, err := s.Open(ctx, Input{PlayerID: "p1", InitialBalance: moneyBRL(t, "1.00")}); err != nil {
		t.Fatalf("primeira abertura: %v", err)
	}
	_, err := s.Open(ctx, Input{PlayerID: "p1", InitialBalance: moneyBRL(t, "1.00")})
	if !errors.Is(err, postgres.ErrDuplicate) {
		t.Fatalf("conflito = %v, want postgres.ErrDuplicate", err)
	}
	if len(db.state.wallets) != 1 {
		t.Fatalf("carteiras = %d, want 1 (segunda transação descartada)", len(db.state.wallets))
	}
}

func TestOpenValidation(t *testing.T) {
	ctx := context.Background()
	s := NewServiceWithIDs(&fakeDB{}, countIDs())

	cases := []struct {
		name string
		in   Input
		want error
	}{
		{"sem player", Input{InitialBalance: moneyBRL(t, "1.00")}, ErrMissingPlayer},
		{"sem moeda", Input{PlayerID: "p1", InitialBalance: money.MoneyOf(0, money.Currency(""))}, ErrMissingCurrency},
		{"saldo negativo", Input{PlayerID: "p1", InitialBalance: money.MoneyOf(-1, "BRL")}, ErrNegativeBalance},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Open(ctx, tc.in); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}
