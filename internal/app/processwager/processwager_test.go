package processwager_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// fakeState é o espelho persistido do banco (snapshot por transação).
type fakeState struct {
	wallets []wallet.Wallet
	wagers  []wager.WagerTransaction
	ledger  []ledger.Entry
	outbox  []events.Envelope
}

func (s fakeState) clone() fakeState {
	return fakeState{
		wallets: append([]wallet.Wallet(nil), s.wallets...),
		wagers:  append([]wager.WagerTransaction(nil), s.wagers...),
		ledger:  append([]ledger.Entry(nil), s.ledger...),
		outbox:  append([]events.Envelope(nil), s.outbox...),
	}
}

func (s *fakeState) wallet(id string) (wallet.Wallet, bool) {
	for _, w := range s.wallets {
		if w.ID() == id {
			return w, true
		}
	}
	return wallet.Wallet{}, false
}

func (s *fakeState) byKey(key string) (wager.WagerTransaction, bool) {
	for _, t := range s.wagers {
		if t.IdempotencyKey() == key {
			return t, true
		}
	}
	return wager.WagerTransaction{}, false
}

func (s *fakeState) byProviderExternal(provider, external string) (wager.WagerTransaction, bool) {
	for _, t := range s.wagers {
		if t.ProviderID() == provider && t.ExternalTransactionID() == external {
			return t, true
		}
	}
	return wager.WagerTransaction{}, false
}

func (s *fakeState) countEvent(typ events.Type) int {
	n := 0
	for _, e := range s.outbox {
		if e.EventType == typ {
			n++
		}
	}
	return n
}

type fakeDB struct {
	mu     sync.Mutex
	state  fakeState
	beginN int
}

func (d *fakeDB) Begin(ctx context.Context) (storage.UnitOfWork, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.beginN++
	return &fakeUoW{db: d, live: d.state.clone()}, nil
}

type fakeUoW struct {
	db   *fakeDB
	live fakeState
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

func (r *fakeWalletRepo) Insert(ctx context.Context, w wallet.Wallet) error {
	if _, ok := r.u.live.wallet(w.ID()); ok {
		return postgres.ErrDuplicate
	}
	r.u.live.wallets = append(r.u.live.wallets, w)
	return nil
}

func (r *fakeWalletRepo) Get(ctx context.Context, id string) (wallet.Wallet, error) {
	w, ok := r.u.live.wallet(id)
	if !ok {
		return wallet.Wallet{}, postgres.ErrNotFound
	}
	return w, nil
}

func (r *fakeWalletRepo) LockForUpdate(ctx context.Context, id string) (wallet.Wallet, error) {
	return r.Get(ctx, id)
}

func (r *fakeWalletRepo) UpdateBalance(ctx context.Context, w wallet.Wallet) error {
	for i, ex := range r.u.live.wallets {
		if ex.ID() == w.ID() {
			// Espelha o repo real: WHERE version = w.Version()-1 (versão lida
			// no LockForUpdate antes da mutação do agregado).
			if ex.Version() != w.Version()-1 {
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

// InsertIfAbsent replica as semânticas do Postgres: conflito na chave de
// idempotência → inserted=false (DO NOTHING); conflito no único parcial
// (provider, externalId) com outra chave → ErrDuplicate (23505).
func (r *fakeWagerRepo) InsertIfAbsent(ctx context.Context, t wager.WagerTransaction) (bool, error) {
	if _, ok := r.u.live.byKey(t.IdempotencyKey()); ok {
		return false, nil
	}
	if ex, ok := r.u.live.byProviderExternal(t.ProviderID(), t.ExternalTransactionID()); ok &&
		ex.IdempotencyKey() != t.IdempotencyKey() {
		return false, postgres.ErrDuplicate
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

func (r *fakeWagerRepo) GetByIdempotencyKey(ctx context.Context, key string) (wager.WagerTransaction, error) {
	t, ok := r.u.live.byKey(key)
	if !ok {
		return wager.WagerTransaction{}, postgres.ErrNotFound
	}
	return t, nil
}

func (r *fakeWagerRepo) GetByIdempotencyKeyForUpdate(ctx context.Context, key string) (wager.WagerTransaction, error) {
	return r.GetByIdempotencyKey(ctx, key)
}

func (r *fakeWagerRepo) GetByProviderExternal(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error) {
	t, ok := r.u.live.byProviderExternal(providerID, externalID)
	if !ok {
		return wager.WagerTransaction{}, postgres.ErrNotFound
	}
	return t, nil
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

// helpers de teste.

const playerID = "player-1"
const walletID = "wallet-1"

func seedWallet(t *testing.T, db *fakeDB, balance string) {
	t.Helper()
	w, err := wallet.New(walletID, playerID, moneyBRL(t, balance))
	if err != nil {
		t.Fatal(err)
	}
	db.state.wallets = append(db.state.wallets, w)
}

func makeInput(kind wager.Kind, amount string, ref, key string) processwager.Input {
	return processwager.Input{
		ProviderID:                     "provider-1",
		ExternalTransactionID:          "ext-1",
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           kind,
		Amount:                         moneyBRLUnsafe(amount),
		ReferenceExternalTransactionID: ref,
		IdempotencyKey:                 key,
	}
}

func expectedHash(in processwager.Input) string {
	h, err := wager.HashPayload(wager.PayloadFields{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		PlayerID:                       in.PlayerID,
		WalletID:                       in.WalletID,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           in.Kind,
		Amount:                         in.Amount,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	})
	if err != nil {
		panic(err)
	}
	return h
}

func moneyBRL(t *testing.T, v string) money.Money {
	t.Helper()
	m, err := money.Parse(v, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func moneyBRLUnsafe(v string) money.Money {
	m, err := money.Parse(v, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}

func newIDSeq() func() string {
	var i int
	return func() string {
		i++
		return "id-" + string(rune('A'+i-1))
	}
}

func run(t *testing.T, db *fakeDB, in processwager.Input) (processwager.Result, error) {
	t.Helper()
	s := processwager.NewServiceWithIDs(db, newIDSeq())
	return s.Process(context.Background(), in)
}

func TestBetDebitsWalletPersistsLedgerAndEvents(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	in := makeInput(wager.KindBet, "30.00", "", "key-1")

	res, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.IdempotentReplay {
		t.Fatalf("primeira execução não pode ser replay: %+v", res)
	}
	if res.State != wager.StateProcessed {
		t.Fatalf("state = %s, quero PROCESSED", res.State)
	}
	if res.Balance == nil || res.Balance.String() != "70.00" {
		t.Fatalf("balance = %v, quero 70.00", res.Balance)
	}

	tx, ok := db.state.byKey("key-1")
	if !ok {
		t.Fatal("transação não persistida pelo idempotency key")
	}
	if tx.State() != wager.StateProcessed || tx.ResultBalance().String() != "70.00" {
		t.Fatalf("transação inválida: %s / %s", tx.State(), tx.ResultBalance())
	}
	if tx.PayloadHash() != expectedHash(in) {
		t.Fatalf("payloadHash divergente persistido")
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "70.00" || w.Version() != 2 {
		t.Fatalf("carteira = %s v%d, quero 70.00 v2", w.Balance(), w.Version())
	}
	if len(db.state.ledger) != 1 {
		t.Fatalf("ledger = %d lançamentos, quero 1", len(db.state.ledger))
	}
	e := db.state.ledger[0]
	if e.Direction() != ledger.DirectionDebit || e.Amount().String() != "30.00" ||
		e.BalanceBefore().String() != "100.00" || e.BalanceAfter().String() != "70.00" {
		t.Fatalf("lançamento inválido: %+v", e)
	}
	if db.state.countEvent(events.TypeWagerTransactionProcessed) != 1 ||
		db.state.countEvent(events.TypeWalletBalanceChanged) != 1 {
		t.Fatalf("outbox deve ter 1 Processed + 1 BalanceChanged, tem %d+%d",
			db.state.countEvent(events.TypeWagerTransactionProcessed),
			db.state.countEvent(events.TypeWalletBalanceChanged))
	}
	for _, env := range db.state.outbox {
		if env.EventType == events.TypeWalletBalanceChanged {
			var d events.WalletBalanceChangedData
			if err := json.Unmarshal(env.Data, &d); err != nil {
				t.Fatal(err)
			}
			if d.Direction != "DEBIT" || d.BalanceBefore.String() != "100.00" ||
				d.BalanceAfter.String() != "70.00" || d.WalletVersion != 2 {
				t.Fatalf("balanceChanged inválido: %+v", d)
			}
		}
	}
}

func TestWinCreditsWallet(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "0.00")
	in := makeInput(wager.KindWin, "50.00", "", "key-win")

	res, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "50.00" {
		t.Fatalf("result inválido: %+v", res)
	}
	if len(db.state.ledger) != 1 || db.state.ledger[0].Direction() != ledger.DirectionCredit {
		t.Fatalf("ledger inválido: %+v", db.state.ledger)
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "50.00" || w.Version() != 2 {
		t.Fatalf("carteira = %s v%d", w.Balance(), w.Version())
	}
}

func TestLossDoesNotTouchWalletOrLedger(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	in := makeInput(wager.KindLoss, "0.00", "", "key-loss")

	res, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "100.00" {
		t.Fatalf("result inválido: %+v", res)
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "100.00" || w.Version() != 1 {
		t.Fatalf("carteira mudou: %s v%d", w.Balance(), w.Version())
	}
	if len(db.state.ledger) != 0 {
		t.Fatalf("LOSS não pode gerar lançamento: %+v", db.state.ledger)
	}
	if db.state.countEvent(events.TypeWalletBalanceChanged) != 0 {
		t.Fatalf("LOSS não pode gerar balanceChanged")
	}
	tx, _ := db.state.byKey("key-loss")
	if tx.State() != wager.StateProcessed || tx.ResultBalance().String() != "100.00" {
		t.Fatalf("transação inválida: %+v", tx)
	}
}

func TestBetInsufficientFundsRejectedPersistedAndReplays(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "10.00")
	in := makeInput(wager.KindBet, "50.00", "", "key-rej")

	res, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateRejected || res.FailureCode != wager.FailureInsufficientFunds {
		t.Fatalf("result inválido: %+v", res)
	}
	if len(db.state.ledger) != 0 {
		t.Fatalf("rejeição não gera lançamento")
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "10.00" || w.Version() != 1 {
		t.Fatalf("carteira mudou sem saldo: %s v%d", w.Balance(), w.Version())
	}
	tx, _ := db.state.byKey("key-rej")
	if tx.State() != wager.StateRejected || tx.FailureCode() != wager.FailureInsufficientFunds {
		t.Fatalf("rejeição não persistida: %+v", tx)
	}
	if db.state.countEvent(events.TypeWagerTransactionRejected) != 1 {
		t.Fatalf("deve haver exatamente 1 evento de rejeição")
	}

	res2, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if !res2.IdempotentReplay || res2.State != wager.StateRejected ||
		res2.FailureCode != wager.FailureInsufficientFunds || res2.TransactionID != tx.ID() {
		t.Fatalf("replay de rejeição inválido: %+v", res2)
	}
}

func TestReplaySameKeyReturnsOriginalResultWithoutDoubleDebit(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	in := makeInput(wager.KindBet, "30.00", "", "key-replay")

	first, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := run(t, db, in)
	if err != nil {
		t.Fatal(err)
	}
	if !second.IdempotentReplay {
		t.Fatalf("segunda execução deve ser replay")
	}
	if second.TransactionID != first.TransactionID || second.Balance.String() != "70.00" {
		t.Fatalf("replay divergente: %+v vs %+v", first, second)
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "70.00" || w.Version() != 2 {
		t.Fatalf("replay debitou de novo: %s v%d", w.Balance(), w.Version())
	}
	if len(db.state.wagers) != 1 {
		t.Fatalf("replay criou transação nova: %d", len(db.state.wagers))
	}
}

func TestIdempotencyKeyReusedWithDifferentPayload(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	if _, err := run(t, db, makeInput(wager.KindBet, "30.00", "", "key-same")); err != nil {
		t.Fatal(err)
	}
	_, err := run(t, db, makeInput(wager.KindBet, "40.00", "", "key-same"))
	if !errors.Is(err, processwager.ErrIdempotencyConflict) {
		t.Fatalf("quero ErrIdempotencyConflict, veio %v", err)
	}
}

func TestExternalTransactionConflict(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	if _, err := run(t, db, makeInput(wager.KindBet, "30.00", "", "key-1")); err != nil {
		t.Fatal(err)
	}
	other := makeInput(wager.KindWin, "30.00", "", "key-2")
	other.ExternalTransactionID = "ext-1"
	_, err := run(t, db, other)
	if !errors.Is(err, processwager.ErrExternalConflict) {
		t.Fatalf("quero ErrExternalConflict, veio %v", err)
	}
}

func TestWalletNotFoundRollsBackClaim(t *testing.T) {
	db := &fakeDB{}
	in := makeInput(wager.KindBet, "30.00", "", "key-nf")
	_, err := run(t, db, in)
	if !errors.Is(err, processwager.ErrWalletNotFound) {
		t.Fatalf("quero ErrWalletNotFound, veio %v", err)
	}
	if len(db.state.wagers) != 0 {
		t.Fatalf("claim deve ser descartado com rollback: %d transações", len(db.state.wagers))
	}
	if len(db.state.outbox) != 0 {
		t.Fatalf("rollback não pode publicar eventos")
	}
}

func TestWalletCurrencyMismatch(t *testing.T) {
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	in := makeInput(wager.KindBet, "30.00", "", "key-cur")
	in.Amount = money.MoneyOf(3000, "USD")
	_, err := run(t, db, in)
	if !errors.Is(err, processwager.ErrWalletCurrencyMismatch) {
		t.Fatalf("quero ErrWalletCurrencyMismatch, veio %v", err)
	}
	w, _ := db.state.wallet(walletID)
	if w.Balance().String() != "100.00" {
		t.Fatalf("moeda divergente não pode movimentar: %s", w.Balance())
	}
}

func TestValidationRejectsBeforeTouchingDB(t *testing.T) {
	cases := []struct {
		name string
		in   processwager.Input
		want error
	}{
		{"sem chave de idempotência", makeInput(wager.KindBet, "10.00", "", ""), processwager.ErrMissingIdempotencyKey},
		{"REFUND fora de escopo", makeInput(wager.KindRefund, "10.00", "ext-0", "k"), processwager.ErrUnsupportedKind},
		{"ROLLBACK fora de escopo", makeInput(wager.KindRollback, "10.00", "ext-0", "k"), processwager.ErrUnsupportedKind},
		{"WIN com referência", makeInput(wager.KindWin, "10.00", "ext-0", "k"), processwager.ErrWinReferencePending},
		{"BET com valor zero", makeInput(wager.KindBet, "0.00", "", "k"), processwager.ErrInvalidAmountForKind},
		{"LOSS com valor não nulo", makeInput(wager.KindLoss, "10.00", "", "k"), processwager.ErrInvalidAmountForKind},
	}
	db := &fakeDB{}
	seedWallet(t, db, "100.00")
	for _, c := range cases {
		_, err := run(t, db, c.in)
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: quero %v, veio %v", c.name, c.want, err)
		}
	}
	if len(db.state.wagers) != 0 {
		t.Fatalf("nenhuma validação pode criar claim: %d", len(db.state.wagers))
	}
}
