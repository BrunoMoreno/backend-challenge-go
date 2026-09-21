//go:build integration

// M6 — worker de resolução tardia de referências (RF-05) contra PostgreSQL
// real:
//  1. Referência que chega depois → resolver no worker (late resolution).
//  2. Retry com backoff durável até o limite de tentativas → rejeição final
//     REFERENCE_NOT_FOUND; TTL vencido → mesma rejeição.
//  3. Varredor de PENDING órfãos (M6.3) → pendura e agenda retry.
//  4. Dois workers disputam a mesma pendência → resolve exatamente uma vez.
//
// IMPORTANTE: rode este pacote com o `app` do Compose parado
// (`docker compose stop app`), senão o papel reference-worker residente
// disputa as pendências dos testes (veja README §Testes).
package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/referenceworker"
)

func newRefWorker(t *testing.T, f *postgres.UnitOfWorkFactory, cfg referenceworker.Config) *referenceworker.Worker {
	t.Helper()
	svc := processwager.NewService(postgres.NewDatabase(f))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return referenceworker.New(f, svc, logger, cfg)
}

// refCfg devolve uma configuração mínima e saudável para os ciclos dos testes.
// BatchSize alto para o teste não depender da ordem de reclamação (o seedWallet
// deixa uma OPENING em PENDING do harness, que é reclamada antes das pendências).
func refCfg() referenceworker.Config {
	return referenceworker.Config{
		BatchSize: 10, PollInterval: time.Second, MaxAttempts: 30,
		TTL: 24 * time.Hour, BackoffBase: time.Second, BackoffMax: time.Minute,
	}
}

// runBatch executa um único ciclo do worker e avalia o erro no teste.
func runBatch(t *testing.T, w *referenceworker.Worker) {
	t.Helper()
	if err := w.ResolveBatch(context.Background()); err != nil {
		t.Fatalf("ResolveBatch: %v", err)
	}
}

func TestReferenceWorkerLateResolution(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rw6", "player-rw6", "100.00", 10000)
	s := newProcessWagerService(t)

	// REFUND referenciando uma aposta que ainda não existe → pendência.
	pending, err := s.Process(ctx, pwRefInput(t, "p-m6", "ext-ref", "player-rw6",
		"wal-rw6", "m6-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}
	if pending.State != wager.StatePendingReference {
		t.Fatalf("refund sem alvo = %v, want PENDING_REFERENCE", pending.State)
	}

	// A aposta referenciada chega LATER (caminho síncrono).
	bet, err := s.Process(ctx, pwInput(t, "p-m6", "ext-bet", "player-rw6",
		"wal-rw6", "m6-bet", wager.KindBet, "30.00"))
	if err != nil {
		t.Fatalf("bet: %v", err)
	}

	// O worker re-resolve a pendência.
	w := newRefWorker(t, f, refCfg())
	runBatch(t, w)

	got := fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StateProcessed {
		t.Fatalf("pendência = %v, want PROCESSED após resolução", got.State())
	}
	if got.ResolvedReferenceTransactionID() != bet.TransactionID {
		t.Fatalf("resolved_reference = %q, want %q",
			got.ResolvedReferenceTransactionID(), bet.TransactionID)
	}
	if b, v := walletBalance(t, "wal-rw6"); b != 10000 || v != 4 {
		t.Fatalf("wallet = %d v%d, want 10000/4", b, v)
	}
	if n := countLedgerForWallet(t, "wal-rw6"); n != 3 { // open + bet + refund
		t.Fatalf("ledger = %d, want 3", n)
	}
	if n := countEventsForTx(t, pending.TransactionID); n < 2 {
		t.Fatalf("eventos = %d, want >= 2 (pendência + resolução)", n)
	}
}

func TestReferenceWorkerRetriesThenRejectsAtAttemptLimit(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rw62", "player-rw62", "100.00", 10000)
	s := newProcessWagerService(t)

	pending, err := s.Process(ctx, pwRefInput(t, "p-m62", "ext-ref", "player-rw62",
		"wal-rw62", "m62-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}

	// Limite de 1 tentativa: o primeiro ciclo só agenda o retry.
	w := newRefWorker(t, f, refCfg()) // MaxAttempts padrão (30)
	runBatch(t, w)
	after := fetchTx(t, f, pending.TransactionID)
	if after.State() != wager.StatePendingReference || after.Attempts() != 1 {
		t.Fatalf("após retry = %v attempts=%d, want PENDING_REFERENCE/1",
			after.State(), after.Attempts())
	}
	if after.NextAttemptAt().IsZero() {
		t.Fatal("next_attempt_at não agendado")
	}

	// Vence o retry e agora força o limite de tentativas em uma nova instância.
	backdateNextAttempt(t, f, pending.TransactionID)
	wrej := newRefWorker(t, f, referenceworker.Config{MaxAttempts: 1}) // defaults para o resto
	runBatch(t, wrej)

	got := fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StateRejected || got.FailureCode() != wager.FailureReferenceNotFound {
		t.Fatalf("rejeição = %v/%s, want REJECTED/REFERENCE_NOT_FOUND",
			got.State(), got.FailureCode())
	}
	if b, v := walletBalance(t, "wal-rw62"); b != 10000 || v != 2 {
		t.Fatalf("rejeição não pode movimentar: wallet = %d v%d, want 10000/2", b, v)
	}
}

func TestReferenceWorkerRejectsExpiredAfterTTL(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rw63", "player-rw63", "100.00", 10000)
	s := newProcessWagerService(t)

	pending, err := s.Process(ctx, pwRefInput(t, "p-m63", "ext-ref", "player-rw63",
		"wal-rw63", "m63-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}

	// Envelhece a pendência além do TTL (2h) e roda o worker.
	tOneHourBack := time.Now().Add(-3 * time.Hour)
	uow := beginOK(t, f)
	_, err = uow.Tx().Exec(ctx,
		`UPDATE wager_transactions SET created_at = $2 WHERE id = $1`, pending.TransactionID, tOneHourBack)
	if err != nil {
		t.Fatal(err)
	}
	_ = uow.Commit(ctx)

	w := newRefWorker(t, f, referenceworker.Config{TTL: 2 * time.Hour})
	runBatch(t, w)

	got := fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StateRejected || got.FailureCode() != wager.FailureReferenceNotFound {
		t.Fatalf("expirada = %v/%s, want REJECTED/REFERENCE_NOT_FOUND",
			got.State(), got.FailureCode())
	}
}

// TestReferenceWorkerSweepsStrandedPending (M6.3): uma linha PENDING órfã (crash
// entre o insert do claim e a pendura) é varrida, pendurada, e resolvida quando
// a referência chega.
func TestReferenceWorkerSweepsStrandedPending(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rw64", "player-rw64", "100.00", 10000)
	s := newProcessWagerService(t)

	pending, err := s.Process(ctx, pwRefInput(t, "p-m64", "ext-ref", "player-rw64",
		"wal-rw64", "m64-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}

	// Simula a linha órfã deixada por um claim quebrado: back a PENDING inicial.
	uow := beginOK(t, f)
	if _, err := uow.Tx().Exec(ctx,
		`UPDATE wager_transactions SET state = 'PENDING', attempt = 0, next_attempt_at = NULL WHERE id = $1`,
		pending.TransactionID); err != nil {
		t.Fatal(err)
	}
	_ = uow.Commit(ctx)

	// Varredura: referência ainda ausente → pendura e agenda retry.
	w := newRefWorker(t, f, refCfg())
	runBatch(t, w)
	got := fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StatePendingReference || got.Attempts() != 1 {
		t.Fatalf("varrida = %v attempts=%d, want PENDING_REFERENCE/1", got.State(), got.Attempts())
	}

	// A referência chega e o retry seguinte resolve.
	if _, err := s.Process(ctx, pwInput(t, "p-m64", "ext-bet", "player-rw64",
		"wal-rw64", "m64-bet", wager.KindBet, "30.00")); err != nil {
		t.Fatal(err)
	}
	backdateNextAttempt(t, f, pending.TransactionID)
	runBatch(t, w)

	got = fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StateProcessed {
		t.Fatalf("após segunda rodada = %v, want PROCESSED", got.State())
	}
	if b, _ := walletBalance(t, "wal-rw64"); b != 10000 {
		t.Fatalf("wallet = %d, want 10000", b)
	}
}

// TestReferenceWorkerTwoInstancesResolveOnce (M6.4): dois workers reclamam a
// mesma pendência; o SKIP LOCKED garante que apenas um resolve e o outro
// oferece o ciclo seguinte (0 linhas) sem error. A movimentação acontece 1x.
func TestReferenceWorkerTwoInstancesResolveOnce(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-rw65", "player-rw65", "100.00", 10000)
	s := newProcessWagerService(t)

	pending, err := s.Process(ctx, pwRefInput(t, "p-m65", "ext-ref", "player-rw65",
		"wal-rw65", "m65-ref", wager.KindRefund, "30.00", unique("ext-bet")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Process(ctx, pwInput(t, "p-m65", "ext-bet", "player-rw65",
		"wal-rw65", "m65-bet", wager.KindBet, "30.00")); err != nil {
		t.Fatal(err)
	}

	cfg := refCfg()
	cfg.BatchSize = 10 // ambos reclamam do mesmo lote
	wa, wb := newRefWorker(t, f, cfg), newRefWorker(t, f, cfg)

	var mu sync.Mutex
	errs := make([]error, 2)
	var wg sync.WaitGroup
	workers := []*referenceworker.Worker{wa, wb}
	for i, w := range workers {
		wg.Add(1)
		go func(i int, w *referenceworker.Worker) {
			defer wg.Done()
			e := w.ResolveBatch(ctx)
			mu.Lock()
			errs[i] = e
			mu.Unlock()
		}(i, w)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("worker %d: %v", i, e)
		}
	}

	got := fetchTx(t, f, pending.TransactionID)
	if got.State() != wager.StateProcessed {
		t.Fatalf("pendência = %v, want PROCESSED", got.State())
	}
	// Exatamente uma resolução financeira.
	if n := countRows(t, `SELECT count(*) FROM wager_transactions WHERE id = $1 AND state = 'PROCESSED'`,
		pending.TransactionID); n != 1 {
		t.Fatalf("linhas PROCESSED = %d, want 1", n)
	}
	if n := countLedgerForWallet(t, "wal-rw65"); n != 3 {
		t.Fatalf("ledger = %d, want 3 (open + bet + refund)", n)
	}
	if b, v := walletBalance(t, "wal-rw65"); b != 10000 || v != 4 {
		t.Fatalf("wallet = %d v%d, want 10000/4", b, v)
	}
}

// fetchTx lê uma transação persistida pelo id.
func fetchTx(t *testing.T, f *postgres.UnitOfWorkFactory, id string) wager.WagerTransaction {
	t.Helper()
	uow := beginOK(t, f)
	defer uow.Rollback(context.Background())
	tx, err := uow.Wagers().GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("get tx %s: %v", id, err)
	}
	return tx
}

// backdateNextAttempt faz o retry vencer imediatamente no próximo ciclo.
func backdateNextAttempt(t *testing.T, f *postgres.UnitOfWorkFactory, id string) {
	t.Helper()
	ctx := context.Background()
	uow := beginOK(t, f)
	if _, err := uow.Tx().Exec(ctx,
		`UPDATE wager_transactions SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	_ = uow.Commit(ctx)
}
