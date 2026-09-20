package query

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// fakeDB mínimo para o serviço de consultas (somente leitura).
type fakeDB struct {
	wallets []wallet.Wallet
	wagers  []wager.WagerTransaction
	ledger  []ledger.Entry
}

func (d *fakeDB) Begin(context.Context) (storage.UnitOfWork, error) { return &fakeUoW{db: d}, nil }

type fakeUoW struct{ db *fakeDB }

func (u *fakeUoW) Wallets() storage.WalletRepository { return fakeWalletRepo{u.db} }
func (u *fakeUoW) Wagers() storage.WagerRepository   { return fakeWagerRepo{u.db} }
func (u *fakeUoW) Ledger() storage.LedgerRepository  { return fakeLedgerRepo{u.db} }
func (u *fakeUoW) Outbox() storage.OutboxRepository  { return fakeOutboxRepo{} }
func (u *fakeUoW) Commit(context.Context) error      { return nil }
func (u *fakeUoW) Rollback(context.Context) error    { return nil }

type fakeWalletRepo struct{ db *fakeDB }

func (r fakeWalletRepo) Insert(context.Context, wallet.Wallet) error { return nil }
func (r fakeWalletRepo) Get(_ context.Context, id string) (wallet.Wallet, error) {
	for _, w := range r.db.wallets {
		if w.ID() == id {
			return w, nil
		}
	}
	return wallet.Wallet{}, postgres.ErrNotFound
}
func (r fakeWalletRepo) LockForUpdate(ctx context.Context, id string) (wallet.Wallet, error) {
	return r.Get(ctx, id)
}
func (r fakeWalletRepo) UpdateBalance(context.Context, wallet.Wallet) error { return nil }

type fakeWagerRepo struct{ db *fakeDB }

func (r fakeWagerRepo) Insert(context.Context, wager.WagerTransaction) error { return nil }
func (r fakeWagerRepo) InsertIfAbsent(context.Context, wager.WagerTransaction) (bool, error) {
	return true, nil
}
func (r fakeWagerRepo) UpdateTerminal(context.Context, wager.WagerTransaction) error { return nil }
func (r fakeWagerRepo) UpdatePendingReference(context.Context, wager.WagerTransaction) error {
	return nil
}
func (r fakeWagerRepo) GetByIdempotencyKey(_ context.Context, key string) (wager.WagerTransaction, error) {
	return r.GetByProviderExternal(context.Background(), "", key)
}
func (r fakeWagerRepo) GetByIdempotencyKeyForUpdate(context.Context, string) (wager.WagerTransaction, error) {
	return wager.WagerTransaction{}, postgres.ErrNotFound
}
func (r fakeWagerRepo) GetByReferenceExternal(context.Context, string, string) (wager.WagerTransaction, error) {
	return wager.WagerTransaction{}, postgres.ErrNotFound
}
func (r fakeWagerRepo) GetReversalForReference(context.Context, string) (wager.WagerTransaction, error) {
	return wager.WagerTransaction{}, postgres.ErrNotFound
}
func (r fakeWagerRepo) GetByID(_ context.Context, id string) (wager.WagerTransaction, error) {
	for _, t := range r.db.wagers {
		if t.ID() == id {
			return t, nil
		}
	}
	return wager.WagerTransaction{}, postgres.ErrNotFound
}
func (r fakeWagerRepo) GetByProviderExternal(_ context.Context, pid, ext string) (wager.WagerTransaction, error) {
	for _, t := range r.db.wagers {
		if t.ProviderID() == pid && t.ExternalTransactionID() == ext {
			return t, nil
		}
	}
	return wager.WagerTransaction{}, postgres.ErrNotFound
}

type fakeLedgerRepo struct{ db *fakeDB }

func (r fakeLedgerRepo) Insert(context.Context, ledger.Entry) error { return nil }

func (r fakeLedgerRepo) ListByWallet(_ context.Context, walletID string, _ money.Currency,
	afterCreatedAt time.Time, afterID string, limit int) ([]ledger.Entry, error) {
	var out []ledger.Entry
	for _, e := range r.db.ledger {
		if e.WalletID() != walletID {
			continue
		}
		if !afterCreatedAt.IsZero() {
			if e.CreatedAt().After(afterCreatedAt) ||
				(e.CreatedAt().Equal(afterCreatedAt) && e.ID() >= afterID) {
				continue
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt().Equal(out[j].CreatedAt()) {
			return out[i].ID() > out[j].ID()
		}
		return out[i].CreatedAt().After(out[j].CreatedAt())
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type fakeOutboxRepo struct{}

func (r fakeOutboxRepo) Insert(context.Context, events.Envelope) error { return nil }

// --- helpers ---

func walletOf(t *testing.T, id string, balance int64) wallet.Wallet {
	t.Helper()
	w, err := wallet.New(id, "p-1", money.MoneyOf(balance, "BRL"))
	if err != nil {
		t.Fatalf("wallet.New: %v", err)
	}
	return w
}

func entry(t *testing.T, id, txID string, at time.Time, after int64) ledger.Entry {
	t.Helper()
	e, err := ledger.New(id, "w-1", txID, ledger.DirectionCredit,
		money.MoneyOf(1000, "BRL"), money.MoneyOf(after-1000, "BRL"),
		money.MoneyOf(after, "BRL"), at)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	return e
}

// --- cursor ---

func TestCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 20, 14, 30, 0, 123456789, time.UTC)
	enc := encodeCursor(at, "l-9")
	gotT, gotID, err := decodeCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !gotT.Equal(at) || gotID != "l-9" {
		t.Fatalf("cursor = (%v, %q), want (%v, l-9)", gotT, gotID, at)
	}
}

func TestCursorRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "nao-base64!!!", "eyJ0IjoieHh4In0"} {
		if _, _, err := decodeCursor(s); err == nil {
			t.Errorf("cursor %q deveria ser inválido", s)
		}
	}
}

func TestCursorRejectsEmptyFields(t *testing.T) {
	enc := encodeCursor(time.Time{}, "")
	if _, _, err := decodeCursor(enc); err == nil {
		t.Fatal("cursor vazio deveria ser inválido")
	}
}

// --- Service.Ledger ---

func TestLedgerPaginationFlow(t *testing.T) {
	base := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	db := &fakeDB{
		wallets: []wallet.Wallet{walletOf(t, "w-1", 5000)},
		ledger: []ledger.Entry{
			entry(t, "l-1", "tx-1", base, 1000),
			entry(t, "l-2", "tx-2", base, 2000),
			entry(t, "l-3", "tx-3", base.Add(1*time.Second), 3000),
			entry(t, "l-4", "tx-4", base.Add(2*time.Second), 4000),
			entry(t, "l-5", "tx-5", base.Add(3*time.Second), 5000),
		},
	}
	svc := NewService(db)

	page1, err := svc.Ledger(context.Background(), "w-1", "", 2)
	if err != nil {
		t.Fatalf("página 1: %v", err)
	}
	if len(page1.Entries) != 2 || page1.NextCursor == "" {
		t.Fatalf("página 1 = %d itens, cursor %q", len(page1.Entries), page1.NextCursor)
	}
	if page1.Entries[0].ID() != "l-5" || page1.Entries[1].ID() != "l-4" {
		t.Fatalf("ordem esperada l-5, l-4; got %s, %s", page1.Entries[0].ID(), page1.Entries[1].ID())
	}

	page2, err := svc.Ledger(context.Background(), "w-1", page1.NextCursor, 2)
	if err != nil {
		t.Fatalf("página 2: %v", err)
	}
	if len(page2.Entries) != 2 || page2.NextCursor == "" {
		t.Fatalf("página 2 = %d itens, cursor %q", len(page2.Entries), page2.NextCursor)
	}
	if page2.Entries[0].ID() != "l-3" || page2.Entries[1].ID() != "l-2" {
		t.Fatalf("ordem esperada l-3, l-2; got %s, %s", page2.Entries[0].ID(), page2.Entries[1].ID())
	}

	page3, err := svc.Ledger(context.Background(), "w-1", page2.NextCursor, 2)
	if err != nil {
		t.Fatalf("página final: %v", err)
	}
	if len(page3.Entries) != 1 || page3.NextCursor != "" {
		t.Fatalf("página final = %d itens, cursor %q; want 1 e vazio",
			len(page3.Entries), page3.NextCursor)
	}
	if page3.Entries[0].ID() != "l-1" {
		t.Fatalf("página final = %s, want l-1", page3.Entries[0].ID())
	}
}

func TestLedgerClampsLimit(t *testing.T) {
	db := &fakeDB{wallets: []wallet.Wallet{walletOf(t, "w-1", 1000)}}
	svc := NewService(db)

	pg, err := svc.Ledger(context.Background(), "w-1", "", 0)
	if err != nil {
		t.Fatalf("limit 0: %v", err)
	}
	if len(pg.Entries) != 0 {
		t.Fatalf("limit 0 deveria virar default, sem itens: %d", len(pg.Entries))
	}

	pg, err = svc.Ledger(context.Background(), "w-1", "", 9999)
	if err != nil {
		t.Fatalf("limit 9999: %v", err)
	}
	if len(pg.Entries) != 0 {
		t.Fatalf("limit acima do max deveria ser truncado: %d", len(pg.Entries))
	}
}

func TestLedgerCursorNotFound(t *testing.T) {
	db := &fakeDB{wallets: []wallet.Wallet{walletOf(t, "w-1", 1000)}}
	svc := NewService(db)
	if _, err := svc.Ledger(context.Background(), "w-1", "nao-opaco", 10); err != ErrInvalidCursor {
		t.Fatalf("err = %v, want ErrInvalidCursor", err)
	}
}

func TestLedgerWalletNotFound(t *testing.T) {
	db := &fakeDB{}
	svc := NewService(db)
	if _, err := svc.Ledger(context.Background(), "inexistente", "", 10); err != ErrWalletNotFound {
		t.Fatalf("err = %v, want ErrWalletNotFound", err)
	}
}

func TestWalletNotFound(t *testing.T) {
	svc := NewService(&fakeDB{})
	if _, err := svc.Wallet(context.Background(), "x"); err != ErrWalletNotFound {
		t.Fatalf("err = %v, want ErrWalletNotFound", err)
	}
}

func TestProviderTransactionNotFound(t *testing.T) {
	svc := NewService(&fakeDB{})
	if _, err := svc.ProviderTransaction(context.Background(), "provider-a", "e-1"); err != ErrTransactionNotFound {
		t.Fatalf("err = %v, want ErrTransactionNotFound", err)
	}
}
