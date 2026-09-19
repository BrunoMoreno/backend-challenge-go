//go:build integration

// RF-01: abertura de carteira (openwallet) contra o PostgreSQL real —
// criando OPENING PROCESSED + crédito no ledger + 2 eventos na outbox no mesmo
// commit quando o saldo inicial é positivo; apenas a carteira quando é 0.00.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

func newOpenWalletService(t *testing.T) *openwallet.Service {
	t.Helper()
	return openwallet.NewService(postgres.NewDatabase(newTestUoW(t)))
}

func countRows(t *testing.T, q string, args ...any) int64 {
	t.Helper()
	ctx := context.Background()
	f := newTestUoW(t)
	uow := beginOK(t, f)
	var n int64
	if err := uow.Tx().QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	_ = uow.Rollback(ctx)
	return n
}

func TestOpenWalletPositiveCreatesOpeningLedgerAndTwoEvents(t *testing.T) {
	ctx := context.Background()
	s := newOpenWalletService(t)

	player := unique("player-open-pos")
	res, err := s.Open(ctx, openwallet.Input{PlayerID: player, InitialBalance: mm(t, "5.00", "BRL")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Carteira: saldo 5.00, versão 2 (criação + crédito), moeda BRL.
	if res.Wallet.Balance().Minor() != 500 || res.Wallet.Version() != 2 {
		t.Fatalf("wallet = saldo %d v%d, want 500/2",
			res.Wallet.Balance().Minor(), res.Wallet.Version())
	}
	if res.Opening == nil || res.Opening.WalletID() != res.Wallet.ID() {
		t.Fatal("OPENING interno esperado para a carteira criada")
	}

	// No banco: 1 transação OPENING PROCESSED, 1 lançamento de crédito, 2 eventos da outbox.
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, res.Wallet.ID()); n != 1 {
		t.Fatalf("wager_transactions = %d, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, res.Wallet.ID()); n != 1 {
		t.Fatalf("ledger = %d, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events
		 WHERE aggregate_id = $1 OR payload->>'transactionId' = $2`,
		res.Wallet.ID(), res.Opening.ID()); n != 2 {
		t.Fatalf("outbox = %d, want 2", n)
	}

	// OPENING persistido como PROCESSED com resultado = saldo após abertura.
	var state, kind, origin string
	var result int64
	f := newTestUoW(t)
	uow := beginOK(t, f)
	if err := uow.Tx().QueryRow(ctx,
		`SELECT state, kind, origin, result_balance_minor
		   FROM wager_transactions WHERE id = $1`, res.Opening.ID()).
		Scan(&state, &kind, &origin, &result); err != nil {
		t.Fatalf("read opening: %v", err)
	}
	_ = uow.Rollback(ctx)
	if state != "PROCESSED" || kind != "OPENING" || origin != "INTERNAL" || result != 500 {
		t.Fatalf("opening = %s/%s/%s result=%d, want PROCESSED/OPENING/INTERNAL/500",
			state, kind, origin, result)
	}
}

func TestOpenWalletZeroBalanceCreatesNothingElse(t *testing.T) {
	ctx := context.Background()
	s := newOpenWalletService(t)

	res, err := s.Open(ctx, openwallet.Input{
		PlayerID: unique("player-open-zero"), InitialBalance: mm(t, "0.00", "BRL")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if res.Wallet.Version() != 1 || res.Wallet.Balance().Minor() != 0 {
		t.Fatalf("wallet = saldo %d v%d, want 0/1", res.Wallet.Balance().Minor(), res.Wallet.Version())
	}
	if res.Opening != nil {
		t.Fatal("saldo zero não deveria criar OPENING")
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, res.Wallet.ID()); n != 0 {
		t.Fatalf("wager_transactions = %d, want 0", n)
	}
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, res.Wallet.ID()); n != 0 {
		t.Fatalf("ledger = %d, want 0", n)
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, res.Wallet.ID()); n != 0 {
		t.Fatalf("outbox = %d, want 0", n)
	}
}

func TestOpenWalletDuplicatePlayerCurrencyConflict(t *testing.T) {
	ctx := context.Background()
	s := newOpenWalletService(t)

	player := unique("player-open-dup")
	if _, err := s.Open(ctx, openwallet.Input{PlayerID: player, InitialBalance: mm(t, "1.00", "BRL")}); err != nil {
		t.Fatalf("primeira abertura: %v", err)
	}
	// Moeda diferente é permitido; mesma (player, currency) conflita.
	if _, err := s.Open(ctx, openwallet.Input{PlayerID: player, InitialBalance: mm(t, "1.00", "BRL")}); !errors.Is(err, postgres.ErrDuplicate) {
		t.Fatalf("conflito = %v, want postgres.ErrDuplicate", err)
	}
	if n := countRows(t, `SELECT count(*) FROM wallets WHERE player_id = $1`, player); n != 1 {
		t.Fatalf("carteiras de %s = %d, want 1", player, n)
	}
}

func TestOpenWalletOutboxClaimable(t *testing.T) {
	ctx := context.Background()
	s := newOpenWalletService(t)

	res, err := s.Open(ctx, openwallet.Input{
		PlayerID: unique("player-open-outbox"), InitialBalance: mm(t, "2.00", "BRL")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if res.Opening == nil {
		t.Fatal("queria OPENING para linkar evento")
	}

	// Os dois eventos da abertura precisam ser reclamáveis pelo publisher.
	f := newTestUoW(t)
	uow := beginOK(t, f)
	claimed, err := uow.OutboxRepository.ClaimPending(ctx, 100, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ours := 0
	for _, row := range claimed {
		env, err := uow.OutboxRepository.GetEvent(ctx, row.EventID)
		if err != nil {
			t.Fatalf("get event %s: %v", row.EventID, err)
		}
		switch env.EventType {
		case "WagerTransactionProcessed":
			var d events.WagerTransactionProcessedData
			if err := json.Unmarshal(env.Data, &d); err == nil && d.TransactionID == res.Opening.ID() {
				ours++
			}
		case "WalletBalanceChanged":
			var d events.WalletBalanceChangedData
			if err := json.Unmarshal(env.Data, &d); err == nil && d.WalletID == res.Wallet.ID() {
				ours++
			}
		}
		if err := uow.OutboxRepository.MarkPublished(ctx, row.EventID); err != nil {
			t.Fatalf("mark published %s: %v", row.EventID, err)
		}
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if ours != 2 {
		t.Fatalf("eventos da abertura reclamáveis = %d, want 2", ours)
	}
}
