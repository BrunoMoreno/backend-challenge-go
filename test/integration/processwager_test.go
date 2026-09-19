//go:build integration

// RF-03/G2: ProcessWagerTransaction síncrono (BET/WIN/LOSS) contra o
// PostgreSQL real — movimentação do saldo + ledger + eventos da outbox no mesmo
// commit, com rejeição persistida e replay idempotente (RF-04).
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

func newProcessWagerService(t *testing.T) *processwager.Service {
	t.Helper()
	return processwager.NewService(postgres.NewDatabase(newTestUoW(t)))
}

func pwInput(t *testing.T, provider, ext, player, walletID, key string, kind wager.Kind, amount string) processwager.Input {
	t.Helper()
	return processwager.Input{
		ProviderID:            unique(provider),
		ExternalTransactionID: unique(ext),
		PlayerID:              unique(player),
		WalletID:              unique(walletID),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  kind,
		Amount:                mm(t, amount, "BRL"),
		IdempotencyKey:        unique(key),
	}
}

func pwRefInput(t *testing.T, provider, ext, player, walletID, key string, kind wager.Kind, amount, ref string) processwager.Input {
	t.Helper()
	in := pwInput(t, provider, ext, player, walletID, key, kind, amount)
	in.ReferenceExternalTransactionID = ref
	return in
}

func walletBalance(t *testing.T, walletID string) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	f := newTestUoW(t)
	uow := beginOK(t, f)
	var balance, version int64
	if err := uow.Tx().QueryRow(ctx,
		`SELECT balance_minor, version FROM wallets WHERE id = $1`, unique(walletID)).
		Scan(&balance, &version); err != nil {
		t.Fatalf("read wallet: %v", err)
	}
	_ = uow.Rollback(ctx)
	return balance, version
}

func countWagersForWallet(t *testing.T, walletID string) int64 {
	return countRows(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, unique(walletID))
}

func countLedgerForWallet(t *testing.T, walletID string) int64 {
	return countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, unique(walletID))
}

func countEventsForTx(t *testing.T, txID string) int64 {
	return countRows(t, `SELECT count(*) FROM outbox_events WHERE payload->>'transactionId' = $1`, txID)
}

func TestProcessBetSynchronous(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-bet", "player-bet", "100.00", 10000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwInput(t, "p-bet", "ext-bet", "player-bet", "wal-bet", "key-bet", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.IdempotentReplay {
		t.Fatal("primeira execução não pode ser replay")
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "70.00" {
		t.Fatalf("result = %+v, want PROCESSED 70.00", res)
	}

	// Saldo 100-30=70; versão 2 (seed) + 1 mudança = 3; 1 lançamento novo de débito.
	if b, v := walletBalance(t, "wal-bet"); b != 7000 || v != 3 {
		t.Fatalf("wallet = %d v%d, want 7000/3", b, v)
	}
	if n := countWagersForWallet(t, "wal-bet"); n != 2 { // OPENING seed + BET
		t.Fatalf("wager_transactions = %d, want 2", n)
	}
	if n := countLedgerForWallet(t, "wal-bet"); n != 2 { // OPENING credit + BET debit
		t.Fatalf("ledger = %d, want 2", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 2 {
		t.Fatalf("eventos da outbox = %d, want 2 (Processed + BalanceChanged)", n)
	}

	// BET persistido como PROCESSED com resultado.
	var state string
	var result int64
	uow := beginOK(t, f)
	if err := uow.Tx().QueryRow(ctx,
		`SELECT state, result_balance_minor FROM wager_transactions WHERE id = $1`,
		res.TransactionID).Scan(&state, &result); err != nil {
		t.Fatal(err)
	}
	_ = uow.Rollback(ctx)
	if state != "PROCESSED" || result != 7000 {
		t.Fatalf("bet = %s result=%d, want PROCESSED/7000", state, result)
	}
}

func TestProcessWinCredits(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-win", "player-win", "10.00", 1000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwInput(t, "p-win", "ext-win", "player-win", "wal-win", "key-win", wager.KindWin, "50.00"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "60.00" {
		t.Fatalf("result = %+v, want PROCESSED 60.00", res)
	}
	if b, v := walletBalance(t, "wal-win"); b != 6000 || v != 3 {
		t.Fatalf("wallet = %d v%d, want 6000/3", b, v)
	}
	if n := countLedgerForWallet(t, "wal-win"); n != 2 {
		t.Fatalf("ledger = %d, want 2 (open credit + win credit)", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 2 {
		t.Fatalf("eventos = %d, want 2", n)
	}
}

func TestProcessLossNoMovement(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-loss", "player-loss", "100.00", 10000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwInput(t, "p-loss", "ext-loss", "player-loss", "wal-loss", "key-loss", wager.KindLoss, "0.00"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "100.00" {
		t.Fatalf("result = %+v, want PROCESSED 100.00", res)
	}
	if b, v := walletBalance(t, "wal-loss"); b != 10000 || v != 2 {
		t.Fatalf("wallet mudou: %d v%d, want 10000/2", b, v)
	}
	if n := countLedgerForWallet(t, "wal-loss"); n != 1 {
		t.Fatalf("LOSS não pode gerar lançamento: ledger=%d, want 1", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 1 {
		t.Fatalf("eventos = %d, want 1 (só Processed)", n)
	}
}

func TestProcessBetInsufficientFundsRejectedAndReplays(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rej", "player-rej", "10.00", 1000)
	s := newProcessWagerService(t)
	in := pwInput(t, "p-rej", "ext-rej", "player-rej", "wal-rej", "key-rej", wager.KindBet, "50.00")

	res, err := s.Process(ctx, in)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.State != wager.StateRejected || res.FailureCode != wager.FailureInsufficientFunds {
		t.Fatalf("result = %+v, want REJECTED/INSUFFICIENT_FUNDS", res)
	}
	if b, v := walletBalance(t, "wal-rej"); b != 1000 || v != 2 {
		t.Fatalf("carteira mudou na rejeição: %d v%d", b, v)
	}
	if n := countLedgerForWallet(t, "wal-rej"); n != 1 {
		t.Fatalf("rejeição não gera lançamento: %d", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 1 {
		t.Fatalf("eventos = %d, want 1 (Rejected)", n)
	}

	// Replay devolve o mesmo terminal sem re-processar (sem criu moeda nova).
	res2, err := s.Process(ctx, in)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !res2.IdempotentReplay || res2.State != wager.StateRejected ||
		res2.FailureCode != wager.FailureInsufficientFunds || res2.TransactionID != res.TransactionID {
		t.Fatalf("replay = %+v, want replay do mesmo terminal", res2)
	}
}

func TestProcessIdempotentReplayKeepsOriginalResult(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rep", "player-rep", "100.00", 10000)
	s := newProcessWagerService(t)
	in := pwInput(t, "p-rep", "ext-rep", "player-rep", "wal-rep", "key-rep", wager.KindBet, "30.00")

	first, err := s.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if !second.IdempotentReplay || second.TransactionID != first.TransactionID ||
		second.Balance == nil || second.Balance.String() != "70.00" {
		t.Fatalf("replay = %+v, want idem %s (70.00)", second, first.TransactionID)
	}
	if b, v := walletBalance(t, "wal-rep"); b != 7000 || v != 3 {
		t.Fatalf("replay debitou duas vezes: %d v%d, want 7000/3", b, v)
	}
	if n := countWagersForWallet(t, "wal-rep"); n != 2 {
		t.Fatalf("replay criou transação nova: %d, want 2", n)
	}
}

func TestProcessIdempotencyConflict(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-conf", "player-conf", "100.00", 10000)
	s := newProcessWagerService(t)

	if _, err := s.Process(ctx, pwInput(t, "p-conf", "ext-a", "player-conf", "wal-conf", "key-x", wager.KindBet, "30.00")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Process(ctx, pwInput(t, "p-conf", "ext-b", "player-conf", "wal-conf", "key-x", wager.KindBet, "40.00"))
	if !errors.Is(err, processwager.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
	if b, _ := walletBalance(t, "wal-conf"); b != 7000 {
		t.Fatalf("conflito deve ser atômico: saldo=%d, want 7000", b)
	}
}

func TestProcessExternalTransactionConflict(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-ext", "player-ext", "100.00", 10000)
	s := newProcessWagerService(t)

	if _, err := s.Process(ctx, pwInput(t, "p-ext", "ext-x", "player-ext", "wal-ext", "key-1", wager.KindBet, "30.00")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Process(ctx, pwInput(t, "p-ext", "ext-x", "player-ext", "wal-ext", "key-2", wager.KindBet, "30.00"))
	if !errors.Is(err, processwager.ErrExternalConflict) {
		t.Fatalf("err = %v, want ErrExternalConflict", err)
	}
	if b, _ := walletBalance(t, "wal-ext"); b != 7000 {
		t.Fatalf("conflito deve ser atômico: saldo=%d, want 7000", b)
	}
}

func TestProcessWalletNotFound(t *testing.T) {
	ctx := context.Background()
	s := newProcessWagerService(t)

	_, err := s.Process(ctx, processwager.Input{
		ProviderID:            "p-x",
		ExternalTransactionID: "ext-x",
		PlayerID:              "player-x",
		WalletID:              "nao-existe",
		RoundID:               "r",
		GameID:                "g",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "10.00", "BRL"),
		IdempotencyKey:        "key-x",
	})
	if !errors.Is(err, processwager.ErrWalletNotFound) {
		t.Fatalf("err = %v, want ErrWalletNotFound", err)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE idempotency_key = $1`, "key-x"); n != 0 {
		t.Fatalf("claim deve ser descartado: %d", n)
	}
}

func TestProcessOutboxClaimable(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-claim", "player-claim", "100.00", 10000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwInput(t, "p-claim", "ext-claim", "player-claim", "wal-claim", "key-claim", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatal(err)
	}

	uow := beginOK(t, f)
	claimed, err := uow.OutboxRepository.ClaimPending(ctx, 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ours := 0
	for _, row := range claimed {
		env, err := uow.OutboxRepository.GetEvent(ctx, row.EventID)
		if err != nil {
			t.Fatalf("get event: %v", err)
		}
		var probe struct {
			TransactionID string `json:"transactionId"`
		}
		if json.Unmarshal(env.Data, &probe) == nil && probe.TransactionID == res.TransactionID {
			ours++
		}
		if err := uow.OutboxRepository.MarkPublished(ctx, row.EventID); err != nil {
			t.Fatalf("mark published: %v", err)
		}
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if ours != 2 {
		t.Fatalf("eventos reclamáveis da transação = %d, want 2", ours)
	}
}

func resolvedReference(t *testing.T, txID string) string {
	t.Helper()
	ctx := context.Background()
	f := newTestUoW(t)
	uow := beginOK(t, f)
	var ref *string
	if err := uow.Tx().QueryRow(ctx,
		`SELECT resolved_reference_id FROM wager_transactions WHERE id = $1`, txID).
		Scan(&ref); err != nil {
		t.Fatalf("read resolved ref: %v", err)
	}
	_ = uow.Rollback(ctx)
	if ref == nil {
		return ""
	}
	return *ref
}

func TestProcessRefundResolvesBet(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-ref", "player-ref", "100.00", 10000)
	s := newProcessWagerService(t)

	bet, err := s.Process(ctx, pwInput(t, "p-ref", "ext-bet", "player-ref", "wal-ref", "rfr-bet", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatalf("bet: %v", err)
	}

	res, err := s.Process(ctx, pwRefInput(t, "p-ref", "ext-ref", "player-ref", "wal-ref", "rfr-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "100.00" {
		t.Fatalf("refund = %+v, want PROCESSED 100.00", res)
	}
	if b, v := walletBalance(t, "wal-ref"); b != 10000 || v != 4 {
		t.Fatalf("wallet = %d v%d, want 10000/4", b, v)
	}
	if ref := resolvedReference(t, res.TransactionID); ref != bet.TransactionID {
		t.Fatalf("resolved_reference = %q, want %q", ref, bet.TransactionID)
	}
	if n := countLedgerForWallet(t, "wal-ref"); n != 3 { // open + bet + refund
		t.Fatalf("ledger = %d, want 3", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 2 {
		t.Fatalf("eventos = %d, want 2", n)
	}
}

func TestProcessRollbackDebitsWin(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rb", "player-rb", "100.00", 10000)
	s := newProcessWagerService(t)

	if _, err := s.Process(ctx, pwInput(t, "p-rb", "ext-bet", "player-rb", "wal-rb", "rbw-bet", wager.KindBet, "40.00")); err != nil {
		t.Fatal(err)
	}
	win, err := s.Process(ctx, pwInput(t, "p-rb", "ext-win", "player-rb", "wal-rb", "rbw-win", wager.KindWin, "60.00"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Process(ctx, pwRefInput(t, "p-rb", "ext-rb", "player-rb", "wal-rb", "rbw-rb", wager.KindRollback, "60.00", unique("ext-win")))
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "60.00" {
		t.Fatalf("rollback = %+v, want PROCESSED 60.00", res)
	}
	if b, v := walletBalance(t, "wal-rb"); b != 6000 || v != 5 {
		t.Fatalf("wallet = %d v%d, want 6000/5", b, v)
	}
	if ref := resolvedReference(t, res.TransactionID); ref != win.TransactionID {
		t.Fatalf("resolved_reference = %q, want %q", ref, win.TransactionID)
	}
	if n := countLedgerForWallet(t, "wal-rb"); n != 4 { // open + bet + win + rollback
		t.Fatalf("ledger = %d, want 4", n)
	}
}

func TestProcessReversalAlreadyReversedRejected(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-dup", "player-dup", "100.00", 10000)
	s := newProcessWagerService(t)

	bet, err := s.Process(ctx, pwInput(t, "p-dup", "ext-bet", "player-dup", "wal-dup", "dup-bet", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Process(ctx, pwRefInput(t, "p-dup", "ext-ref1", "player-dup", "wal-dup", "dup-ref1", wager.KindRefund, "30.00", unique("ext-bet"))); err != nil {
		t.Fatal(err)
	}
	res, err := s.Process(ctx, pwRefInput(t, "p-dup", "ext-ref2", "player-dup", "wal-dup", "dup-ref2", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatalf("segundo refund: %v", err)
	}
	if res.State != wager.StateRejected || res.FailureCode != wager.FailureAlreadyReversed {
		t.Fatalf("result = %+v, want REJECTED/ALREADY_REVERSED", res)
	}
	if b, v := walletBalance(t, "wal-dup"); b != 10000 || v != 4 { // rerun não pode movimentar
		t.Fatalf("wallet = %d v%d, want 10000/4", b, v)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions
		WHERE resolved_reference_id = $1 AND state = 'PROCESSED'
		  AND kind IN ('REFUND','ROLLBACK')`, bet.TransactionID); n != 1 {
		t.Fatalf("só uma reversão processada deve existir, veio %d", n)
	}
}

func TestProcessReversalMatchismatchRejected(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-mis", "player-mis", "100.00", 10000)
	s := newProcessWagerService(t)

	if _, err := s.Process(ctx, pwInput(t, "p-mis", "ext-bet", "player-mis", "wal-mis", "mis-bet", wager.KindBet, "30.00")); err != nil {
		t.Fatal(err)
	}
	res, err := s.Process(ctx, pwRefInput(t, "p-mis", "ext-ref", "player-mis", "wal-mis", "mis-ref", wager.KindRefund, "20.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateRejected || res.FailureCode != wager.FailureReferenceMismatch {
		t.Fatalf("result = %+v, want REJECTED/REFERENCE_MISMATCH", res)
	}
	if b, v := walletBalance(t, "wal-mis"); b != 7000 || v != 3 {
		t.Fatalf("wallet = %d v%d, want 7000/3", b, v)
	}
}

func TestProcessReversalReferencePendingPersisted(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-unr", "player-unr", "100.00", 10000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwRefInput(t, "p-unr", "ext-ref", "player-unr", "wal-unr", "unr-ref", wager.KindRefund, "30.00", unique("ext-missing")))
	if err != nil {
		t.Fatalf("referência ausente deve ser PENDING_REFERENCE, não erro: %v", err)
	}
	if res.State != wager.StatePendingReference || res.Balance != nil || res.IdempotentReplay {
		t.Fatalf("result = %+v, want PENDING_REFERENCE inédito", res)
	}
	if n := countRows(t, `SELECT count(*) FROM wager_transactions
		WHERE idempotency_key = $1 AND state = 'PENDING_REFERENCE'`,
		unique("unr-ref")); n != 1 {
		t.Fatalf("claim PENDING_REFERENCE deve persistir, veio %d", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 1 {
		t.Fatalf("evento de pendência esperado na outbox, veio %d", n)
	}
	if b, v := walletBalance(t, "wal-unr"); b != 10000 || v != 2 {
		t.Fatalf("sem movimento: wallet = %d v%d, want 10000/2", b, v)
	}
	if n := countLedgerForWallet(t, "wal-unr"); n != 1 { // só o open
		t.Fatalf("pendência não pode gerar lançamentos, veio %d", n)
	}
}

func TestProcessReversalPendingReplayIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-unr2", "player-unr2", "100.00", 10000)
	s := newProcessWagerService(t)

	in := pwRefInput(t, "p-unr", "ext-ref2", "player-unr", "wal-unr2", "unr-ref2", wager.KindRefund, "30.00", unique("ext-missing"))
	first, err := s.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if second.TransactionID != first.TransactionID || second.State != wager.StatePendingReference ||
		!second.IdempotentReplay {
		t.Fatalf("replay = %+v, want PENDING_REFERENCE com IdempotentReplay", second)
	}
	if countEventsForTx(t, first.TransactionID) != 1 {
		t.Fatalf("replay não pode duplicar eventos")
	}
}

func TestProcessWinReferenceResolvesBet(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-wref", "player-wref", "100.00", 10000)
	s := newProcessWagerService(t)

	bet, err := s.Process(ctx, pwInput(t, "p-wref", "ext-bet", "player-wref", "wal-wref", "wref-bet", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Process(ctx, pwRefInput(t, "p-wref", "ext-win", "player-wref", "wal-wref", "wref-win", wager.KindWin, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateProcessed || res.Balance == nil || res.Balance.String() != "100.00" {
		t.Fatalf("win = %+v, want PROCESSED 100.00", res)
	}
	if ref := resolvedReference(t, res.TransactionID); ref != bet.TransactionID {
		t.Fatalf("resolved_reference = %q, want %q", ref, bet.TransactionID)
	}
	if b, v := walletBalance(t, "wal-wref"); b != 10000 || v != 4 {
		t.Fatalf("wallet = %d v%d, want 10000/4", b, v)
	}
	if n := countLedgerForWallet(t, "wal-wref"); n != 3 { // open + bet + win
		t.Fatalf("ledger = %d, want 3", n)
	}
	if n := countEventsForTx(t, res.TransactionID); n != 2 {
		t.Fatalf("eventos do win = %d, want 2", n)
	}
}

func TestProcessWinReferencePendingPersisted(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-wref2", "player-wref2", "100.00", 10000)
	s := newProcessWagerService(t)

	res, err := s.Process(ctx, pwRefInput(t, "p-wref2", "ext-win", "player-wref2", "wal-wref2", "wref-win2", wager.KindWin, "30.00", unique("ext-missing")))
	if err != nil {
		t.Fatalf("WIN referenciando ausente deve pendurar: %v", err)
	}
	if res.State != wager.StatePendingReference {
		t.Fatalf("result = %+v, want PENDING_REFERENCE", res)
	}
	if b, v := walletBalance(t, "wal-wref2"); b != 10000 || v != 2 {
		t.Fatalf("sem movimento: wallet = %d v%d, want 10000/2", b, v)
	}
}

func TestProcessReversalOfRejectedTargetRejected(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-notok", "player-notok", "5.00", 500)
	s := newProcessWagerService(t)

	bad, err := s.Process(ctx, pwInput(t, "p-notok", "ext-bet-bad", "player-notok", "wal-notok", "ntk-bet", wager.KindBet, "50.00"))
	if err != nil {
		t.Fatal(err)
	}
	if bad.State != wager.StateRejected {
		t.Fatalf("bet = %+v, want REJECTED", bad)
	}
	res, err := s.Process(ctx, pwRefInput(t, "p-notok", "ext-ref", "player-notok", "wal-notok", "ntk-ref", wager.KindRefund, "50.00", unique("ext-bet-bad")))
	if err != nil {
		t.Fatal(err)
	}
	if res.State != wager.StateRejected || res.FailureCode != wager.FailureReferenceNotProcessed {
		t.Fatalf("result = %+v, want REJECTED/REFERENCE_NOT_PROCESSED", res)
	}
	if b, _ := walletBalance(t, "wal-notok"); b != 500 {
		t.Fatalf("wallet = %d, want 500", b)
	}
}
