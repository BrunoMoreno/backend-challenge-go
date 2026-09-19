//go:build integration

// Testes de integridade impostos pelo banco (migration 000008) e pelas
// constraints do schema: ledger append-only, terminais imutáveis, consistência
// deferida saldo x ledger, CHECKs de reconciliação e o estado final das
// migrations. Requer PostgreSQL do Compose migrado (make migrate-up).
package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func assertPgErr(t *testing.T, wantCode string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("esperava erro com SQLSTATE %s, got nil", wantCode)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("esperava *pgconn.PgError %s, got %T %v", wantCode, err, err)
	}
	if pgErr.Code != wantCode {
		t.Fatalf("SQLSTATE = %s (%s), want %s", pgErr.Code, pgErr.Message, wantCode)
	}
}

func rehydratedWallet(t *testing.T, id, player string, minor, version int64) wallet.Wallet {
	t.Helper()
	return mustValue(wallet.Rehydrate(unique(id), unique(player), "BRL",
		money.MoneyOf(minor, "BRL"), version, time.Now().UTC(), time.Now().UTC()))
}

// TestDeferredWalletLedgerConsistency cobre a constraint trigger deferida:
// o COMMIT valida saldo e versão contra o ledger, rejeitando divergências e
// descartando a transação inteira.
func TestDeferredWalletLedgerConsistency(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	t.Run("saldo sem lançamento", func(t *testing.T) {
		uow := beginOK(t, f)
		if err := uow.WalletRepository.Insert(ctx, rehydratedWallet(t, "wal-m1", "player-m1", 500, 1)); err != nil {
			t.Fatal(err)
		}
		if err := uow.Commit(ctx); !errors.Is(err, postgres.ErrCheck) {
			t.Fatalf("commit incoerente: err = %v, want postgres.ErrCheck", err)
		}

		uow = beginOK(t, f)
		if _, err := uow.WalletRepository.Get(ctx, unique("wal-m1")); !errors.Is(err, postgres.ErrNotFound) {
			t.Fatalf("commit rejeitado deveria descartar tudo, got %v", err)
		}
		_ = uow.Rollback(ctx)
	})

	t.Run("versao diverge do numero de lancamentos", func(t *testing.T) {
		uow := beginOK(t, f)
		w := rehydratedWallet(t, "wal-m2", "player-m2", 1000, 4)
		if err := uow.WalletRepository.Insert(ctx, w); err != nil {
			t.Fatal(err)
		}
		txID := unique("wal-m2-op")
		opening := mustValue(wager.NewOpening(txID, unique("wal-m2"), unique("player-m2"), mm(t, "10.00", "BRL")))
		if err := uow.WagerRepository.Insert(ctx, opening); err != nil {
			t.Fatal(err)
		}
		entry := mustValue(ledger.New(unique("wal-m2-op-led"), unique("wal-m2"), txID,
			ledger.DirectionCredit, mm(t, "10.00", "BRL"), mm(t, "0.00", "BRL"),
			mm(t, "10.00", "BRL"), time.Now()))
		if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
			t.Fatal(err)
		}
		// versão 4 ≠ count(1)+1 = 2 apesar de saldo (10) == último after (10).
		if err := uow.Commit(ctx); !errors.Is(err, postgres.ErrCheck) {
			t.Fatalf("commit de versão divergente: err = %v, want postgres.ErrCheck", err)
		}
	})

	t.Run("carteira coerente persiste", func(t *testing.T) {
		seedWallet(t, f, "wal-m3", "player-m3", "50.00", 5000)
		uow := beginOK(t, f)
		got, err := uow.WalletRepository.Get(ctx, unique("wal-m3"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Balance().Minor() != 5000 || got.Version() != 2 {
			t.Fatalf("saldo=%d versão=%d, want 5000/2", got.Balance().Minor(), got.Version())
		}
		_ = uow.Rollback(ctx)
	})

	t.Run("mudanca sem lancamento nao comita", func(t *testing.T) {
		seedWallet(t, f, "wal-m4", "player-m4", "50.00", 5000)
		uow := beginOK(t, f)
		locked, err := uow.WalletRepository.LockForUpdate(ctx, unique("wal-m4"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := locked.Debit(mm(t, "10.00", "BRL")); err != nil {
			t.Fatal(err)
		}
		if err := uow.WalletRepository.UpdateBalance(ctx, locked); err != nil {
			t.Fatal(err)
		}
		// Nenhum lançamento de ledger para o débito → rejeitado no COMMIT.
		if err := uow.Commit(ctx); !errors.Is(err, postgres.ErrCheck) {
			t.Fatalf("commit sem lançamento: err = %v, want postgres.ErrCheck", err)
		}

		uow = beginOK(t, f)
		after, err := uow.WalletRepository.Get(ctx, unique("wal-m4"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if after.Balance().Minor() != 5000 || after.Version() != 2 {
			t.Fatalf("saldo=%d versão=%d, want 5000/2 (transação preservada)",
				after.Balance().Minor(), after.Version())
		}
		_ = uow.Rollback(ctx)
	})

	t.Run("lancamento e saldo coerentes comitam", func(t *testing.T) {
		seedWallet(t, f, "wal-m5", "player-m5", "50.00", 5000)
		uow := beginOK(t, f)
		locked, err := uow.WalletRepository.LockForUpdate(ctx, unique("wal-m5"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := locked.Credit(mm(t, "10.00", "BRL")); err != nil {
			t.Fatal(err)
		}
		if err := uow.WalletRepository.UpdateBalance(ctx, locked); err != nil {
			t.Fatal(err)
		}
		bet := newBET(t, "tx-m5", "wal-m5", "player-m5", "ext-m5", "key-m5", "hash-m5")
		if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
			t.Fatal(err)
		}
		entry := mustValue(ledger.New(unique("led-m5"), unique("wal-m5"), unique("tx-m5"),
			ledger.DirectionCredit, mm(t, "10.00", "BRL"), mm(t, "50.00", "BRL"),
			mm(t, "60.00", "BRL"), time.Now()))
		if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
			t.Fatal(err)
		}
		if err := uow.Commit(ctx); err != nil {
			t.Fatalf("commit coerente: %v", err)
		}

		uow = beginOK(t, f)
		after, err := uow.WalletRepository.Get(ctx, unique("wal-m5"))
		if err != nil {
			t.Fatal(err)
		}
		if after.Balance().Minor() != 6000 || after.Version() != 3 {
			t.Fatalf("saldo=%d versão=%d, want 6000/3", after.Balance().Minor(), after.Version())
		}
		_ = uow.Rollback(ctx)
	})
}

// TestLedgerTriggerBlockUpdateDelete prova que o ledger é append-only também
// para quem tem privilégio de escrita (role de migração/app): o trigger
// BEFORE UPDATE/DELETE (000008) bloqueia qualquer alteração.
func TestLedgerTriggerBlockUpdateDelete(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-m6", "player-m6", "25.00", 2500)

	pool, err := pgxpool.New(ctx, migrateURL())
	if err != nil {
		t.Fatalf("pool app: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, stmt := range []string{
		`UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = $1`,
		`DELETE FROM wallet_ledger_entries WHERE id = $1`,
	} {
		_, err := pool.Exec(ctx, stmt, unique("wal-m6-op-led"))
		assertPgErr(t, "P0001", err)
	}

	// A linha continua inalterada.
	uow := beginOK(t, f)
	var amount int64
	if err := uow.Tx().QueryRow(ctx,
		`SELECT amount_minor FROM wallet_ledger_entries WHERE id = $1`,
		unique("wal-m6-op-led")).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	if amount != 2500 {
		t.Fatalf("ledger adulterado: amount=%d", amount)
	}
	_ = uow.Rollback(ctx)
}

// TestTerminalImmutableTrigger: transações PROCESSED/REJECTED/FAILED não podem
// mudar estado, código de falha, resultado ou referência resolvida; campos de
// tentativa e linhas não terminais continuam mutáveis.
func TestTerminalImmutableTrigger(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-t1", "player-t1", "4.00", 400)

	uow := beginOK(t, f)
	bet := newBET(t, "tx-t1", "wal-t1", "player-t1", "ext-t1", "key-t1", "hash-t1")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatal(err)
	}
	if err := bet.Reject(wager.FailureInsufficientFunds); err != nil {
		t.Fatal(err)
	}
	if err := uow.WagerRepository.UpdateTerminal(ctx, bet); err != nil {
		t.Fatal(err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	pending := newBET(t, "tx-t2", "wal-t1", "player-t1", "ext-t2", "key-t2", "hash-t2")
	uow = beginOK(t, f)
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	for _, stmt := range map[string]string{
		"flip de estado":       `UPDATE wager_transactions SET state = 'PROCESSED' WHERE id = $1`,
		"mudanca de falha":     `UPDATE wager_transactions SET failure_code = 'PLAYER_MISMATCH' WHERE id = $1`,
		"mudanca de resultado": `UPDATE wager_transactions SET result_balance_minor = 42 WHERE id = $1`,
		"referencia resolvida": `UPDATE wager_transactions SET resolved_reference_id = 'x' WHERE id = $1`,
	} {
		uow = beginOK(t, f)
		_, err := uow.Tx().Exec(ctx, stmt, unique("tx-t1"))
		assertPgErr(t, "P0001", err)
		_ = uow.Rollback(ctx)
	}

	// Controles: campos de retry seguem mutáveis mesmo em terminal; na linha
	// PENDING a transição de tentativa também é permitida.
	uow = beginOK(t, f)
	if _, err := uow.Tx().Exec(ctx,
		`UPDATE wager_transactions SET attempt = attempt + 1 WHERE id = $1`, unique("tx-t1")); err != nil {
		t.Fatalf("attempt em terminal deveria ser permitido: %v", err)
	}
	_ = uow.Rollback(ctx)

	uow = beginOK(t, f)
	if _, err := uow.Tx().Exec(ctx,
		`UPDATE wager_transactions SET next_attempt_at = $2 WHERE id = $1`,
		unique("tx-t2"), time.Now().UTC()); err != nil {
		t.Fatalf("next_attempt_at em PENDING deveria ser permitido: %v", err)
	}
	_ = uow.Rollback(ctx)
}

// TestLedgerReconciliationCheck: os CHECKs do schema (reconciliação direção e
// saldo final não-negativo) rejeitam lançamentos inválidos mesmo via SQL bruto,
// independentemente das guardas do domínio.
func TestLedgerReconciliationCheck(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-m7", "player-m7", "0.00", 0)

	uow := beginOK(t, f)
	bet := newBET(t, "tx-m7", "wal-m7", "player-m7", "ext-m7", "key-m7", "hash-m7")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatal(err)
	}

	// CREDIT: after deve ser before + amount (aqui 0 + 10 ≠ 5).
	uow = beginOK(t, f)
	_, err := uow.Tx().Exec(ctx, `
		INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, 'CREDIT', 1000, 0, 500, now())`,
		unique("led-m7-bad"), unique("wal-m7"), unique("tx-m7"))
	assertPgErr(t, "23514", err)
	_ = uow.Rollback(ctx)

	// DEBIT com saldo final negativo.
	uow = beginOK(t, f)
	_, err = uow.Tx().Exec(ctx, `
		INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, 'DEBIT', 1000, 0, -900, now())`,
		unique("led-m7-neg"), unique("wal-m7"), unique("tx-m7"))
	assertPgErr(t, "23514", err)
	_ = uow.Rollback(ctx)
}

// TestSchemaMigrationsAtLatest garante que o banco está no schema esperado
// (versão mais recente aplicada) e que os objetos de integridade existem.
func TestSchemaMigrationsAtLatest(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migrateURL())
	if err != nil {
		t.Fatalf("pool app: %v", err)
	}
	t.Cleanup(pool.Close)

	var version int64
	if err := pool.QueryRow(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version); err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	if version != 8 {
		t.Fatalf("migrations no banco = %d, want 8 (make migrate-up)", version)
	}

	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_trigger
		 WHERE tgname IN ('ledger_entries_immutable', 'wager_terminal_immutable', 'wallet_ledger_consistency')
		   AND NOT tgisinternal`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("triggers encontrados = %d, want 3", n)
	}
}
