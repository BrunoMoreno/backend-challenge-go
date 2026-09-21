//go:build integration

// RF-09: reconciliação de carteira contra o PostgreSQL real — saldo
// armazenado × saldo reconstruído do ledger em um snapshot consistente.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

func TestReconcileConsistentWithLedger(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	// Carteira 100.00 + crédito inicial (1 lançamento no ledger).
	seedWallet(t, f, "wal-rec-1", "player-rec-1", "100.00", 10000)

	// Aposta de 25.00: débito, saldo 75.00 e lançamento correspondente.
	uow := beginOK(t, f)
	w, err := uow.WalletRepository.LockForUpdate(ctx, unique("wal-rec-1"))
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, err := w.Debit(mm(t, "25.00", "BRL")); err != nil {
		t.Fatalf("debit 25: %v", err)
	}
	if err := uow.WalletRepository.UpdateBalance(ctx, w); err != nil {
		t.Fatalf("update balance: %v", err)
	}
	bet := newBET(t, "tx-rec-1", "wal-rec-1", "player-rec-1", "ext-rec-1", "key-rec-1", "hash-rec-1")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatalf("insert bet: %v", err)
	}
	entry := mustValue(ledger.New(unique("led-rec-1"), unique("wal-rec-1"), unique("tx-rec-1"),
		ledger.DirectionDebit, mm(t, "25.00", "BRL"), mm(t, "100.00", "BRL"),
		mm(t, "75.00", "BRL"), time.Now()))
	if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
		t.Fatalf("insert debit ledger: %v", err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	svc := query.NewService(postgres.NewDatabase(f))
	res, err := svc.Reconcile(ctx, unique("wal-rec-1"))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if res.StoredBalance.Minor() != 7500 || res.CalculatedBalance.Minor() != 7500 {
		t.Fatalf("stored=%d calculated=%d, want 7500/7500",
			res.StoredBalance.Minor(), res.CalculatedBalance.Minor())
	}
	if !res.Consistent || !res.Difference.IsZero() || res.CheckedEntries != 2 {
		t.Fatalf("reconciliação = %+v, want consistente/0/2 lançamentos", res)
	}
}

func TestReconcileRejectsUnknownWallet(t *testing.T) {
	svc := query.NewService(postgres.NewDatabase(newTestUoW(t)))
	_, err := svc.Reconcile(context.Background(), unique("wal-rec-nao-existe"))
	if err != query.ErrWalletNotFound {
		t.Fatalf("err = %v, want query.ErrWalletNotFound", err)
	}
}
