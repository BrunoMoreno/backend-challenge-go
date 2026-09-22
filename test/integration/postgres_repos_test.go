//go:build integration

// Integração mínima dos repositórios pgx. Requer o PostgreSQL do Compose
// migrado (`make up` + `make migrate-up`). Conecta como wager_app; a limpeza
// e o TestMain usam o role de migração (app), que pode excluir do ledger.
package integration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testURL() string {
	if u := os.Getenv("APP_DATABASE_URL"); u != "" {
		return u
	}
	return "postgres://wager_app:wager_app@localhost:5432/wagering?sslmode=disable"
}

func migrateURL() string {
	if u := os.Getenv("DATABASE_URL"); u != "" {
		return u
	}
	return "postgres://app:app@localhost:5432/wagering?sslmode=disable"
}

// TestMain limpa resíduos de execuções anteriores (role de migração).
// TRUNCATE não dispara triggers de linha, então consegue limpar o ledger
// imutável; a aplicação wager_app não tem esse privilégio. As filas SQS são
// purgadas para os testes não verem mensagens de execuções passadas.
func TestMain(m *testing.M) {
	testSQSClient().PurgeQueue(context.Background(), &awssqs.PurgeQueueInput{
		QueueUrl: aws.String(sqsEndpoint + "/000000000000/wager-transactions.fifo"),
	})
	testSQSClient().PurgeQueue(context.Background(), &awssqs.PurgeQueueInput{
		QueueUrl: aws.String(sqsEndpoint + "/000000000000/wager-transactions-dlq.fifo"),
	})
	testSQSClient().PurgeQueue(context.Background(), &awssqs.PurgeQueueInput{
		QueueUrl: aws.String(sqsEndpoint + "/000000000000/wager-events.fifo"),
	})

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migrateURL())
	if err == nil {
		_, _ = pool.Exec(ctx, `TRUNCATE outbox_events, inbox, wallet_ledger_entries,
			wager_transactions, wallets RESTART IDENTITY CASCADE`)
		pool.Close()
	}
	os.Exit(m.Run())
}

var runID = "itest-" + os.Getenv("GO_TEST_SERIAL")

func unique(id string) string { return runID + "-" + id }

// testPoolMaxConns limita as conexões de cada pool de teste: a suíte abre
// muitos pools simultâneos e o PostgreSQL do Compose opera com
// max_connections=100. Um pool sem teto explícito contribui com até NumCPU
// conexões e, somado ao app residente/TablePlus/execuções paralelas, satura o
// banco — novos BEGIN passam a ser recusados (docs/solve/TEST-INTEGRATION.md).
const testPoolMaxConns = 8

// newTestPool abre um pool pgx com MaxConns fixado e o fecha ao fim do teste.
func newTestPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	cfg.MaxConns = testPoolMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newTestUoW(t *testing.T) *postgres.UnitOfWorkFactory {
	t.Helper()
	return postgres.NewUnitOfWorkFactory(newTestPool(t, testURL()))
}

// beginOK abre uma transação e garante rollback no fim (mesmo em falha),
// para o pool nunca reter conexão em transação aberta.
func beginOK(t *testing.T, f *postgres.UnitOfWorkFactory) *postgres.UnitOfWork {
	t.Helper()
	uow, err := f.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = uow.Rollback(context.Background()) })
	return uow
}

func mm(t *testing.T, v, c string) money.Money {
	t.Helper()
	m, err := money.Parse(v, c)
	if err != nil {
		t.Fatalf("money.Parse: %v", err)
	}
	return m
}

func freshWallet(t *testing.T, id, player, seed string) wallet.Wallet {
	t.Helper()
	return mustValue(wallet.New(unique(id), unique(player), mm(t, seed, "BRL")))
}

// seedWallet cria carteira + OPENING + lançamento de ledger consistente, como
// o caso de uso fará — a constraint trigger deferida exige balance == último
// lançamento e version == count+1 no COMMIT. O crédito inicial sobe a versão
// para 2 (como wallet.New + Credit), alinhado ao domínio.
func seedWallet(t *testing.T, f *postgres.UnitOfWorkFactory, id, player, seed string, balance int64) {
	t.Helper()
	ctx := context.Background()
	amount := mm(t, seed, "BRL")
	w := freshWallet(t, id, player, "0.00")
	if balance != 0 {
		// O crédito inicial: wallet.New(0) + Credit(initial) → saldo=initial e
		// versão 2, aplicado ANTES do INSERT para a constraint trigger deferida
		// ver saldo/versão finais.
		if _, err := w.Credit(amount); err != nil {
			t.Fatalf("credit: %v", err)
		}
	}
	uow := beginOK(t, f)
	if err := uow.WalletRepository.Insert(ctx, w); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}
	if balance != 0 {
		txID := unique(id + "-op")
		opening := mustValue(wager.NewOpening(txID, unique(id), unique(player), amount))
		if err := uow.WagerRepository.Insert(ctx, opening); err != nil {
			t.Fatalf("insert opening: %v", err)
		}
		entry := mustValue(ledger.New(unique(id+"-op-led"), unique(id), txID,
			ledger.DirectionCredit, amount, mm(t, "0.00", "BRL"), amount, time.Now()))
		if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
			t.Fatalf("insert opening ledger: %v", err)
		}
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func newBET(t *testing.T, id, walletID, playerID, ext, key, hash string) wager.WagerTransaction {
	t.Helper()
	return mustValue(wager.NewExternal(unique(id), "provider-a", unique(ext), unique(key),
		unique(hash), unique(walletID), unique(playerID), "r1", "g1",
		wager.KindBet, mm(t, "4.00", "BRL"), ""))
}

func mustValue[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestWalletRepoOptimisticBalance(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	seedWallet(t, f, "wal-w1", "player-1", "100.00", 10000)

	uow := beginOK(t, f)
	locked, err := uow.WalletRepository.LockForUpdate(ctx, unique("wal-w1"))
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, err := locked.Debit(mm(t, "80.00", "BRL")); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if err := uow.WalletRepository.UpdateBalance(ctx, locked); err != nil {
		t.Fatalf("update: %v", err)
	}
	bet := newBET(t, "tx-debit-1", "wal-w1", "player-1", "ext-d1", "key-d1", "hash-d1")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatalf("insert bet: %v", err)
	}
	entry := mustValue(ledger.New(unique("led-d1"), unique("wal-w1"), unique("tx-debit-1"),
		ledger.DirectionDebit, mm(t, "80.00", "BRL"), mm(t, "100.00", "BRL"),
		mm(t, "20.00", "BRL"), time.Now()))
	if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
		t.Fatalf("insert debit ledger: %v", err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	uow = beginOK(t, f)
	after, err := uow.WalletRepository.Get(ctx, unique("wal-w1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// 100 - 80 = 20; versão: 1 (criação) + 2 mudanças = 3.
	if after.Balance().Minor() != 2000 || after.Version() != 3 {
		t.Fatalf("saldo=%d versão=%d, want minor=2000 versão=3",
			after.Balance().Minor(), after.Version())
	}
	_ = uow.Rollback(ctx)
}

func TestOptimisticLockStaleVersion(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	seedWallet(t, f, "wal-w2", "player-2", "10.00", 1000)
	w := freshWallet(t, "wal-w2", "player-2", "10.00")

	// Cópia obsoleta (versão antiga) não sobrescreve o saldo.
	uow := beginOK(t, f)
	stale := overrideBalance(w, 0)
	if err := uow.WalletRepository.UpdateBalance(ctx, stale); err == nil {
		t.Fatal("esperava ErrOptimisticLock")
	} else if !errors.Is(err, postgres.ErrOptimisticLock) {
		t.Fatalf("err = %v, want ErrOptimisticLock", err)
	}
	_ = uow.Rollback(ctx)
}

func overrideBalance(w wallet.Wallet, minor int64) wallet.Wallet {
	cur := w.Balance().Currency()
	return mustValue(wallet.Rehydrate(w.ID(), w.PlayerID(), cur,
		money.MoneyOf(minor, cur), 1, w.CreatedAt(), w.UpdatedAt()))
}

func TestOpeningUniquePerWallet(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	uow := beginOK(t, f)
	if err := uow.WalletRepository.Insert(ctx, freshWallet(t, "wal-w3", "player-3", "0.00")); err != nil {
		t.Fatal(err)
	}
	_ = uow.Commit(ctx)

	// Primeiro OPENING: ok e persistido.
	uow = beginOK(t, f)
	opening := mustValue(wager.NewOpening(unique("tx-open-1"), unique("wal-w3"), unique("player-3"), mm(t, "0.00", "BRL")))
	if err := uow.WagerRepository.Insert(ctx, opening); err != nil {
		t.Fatalf("insert opening: %v", err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Segundo OPENING da mesma carteira: rejeitado pelo índice parcial.
	uow = beginOK(t, f)
	dup := mustValue(wager.NewOpening(unique("tx-open-2"), unique("wal-w3"), unique("player-3"), mm(t, "0.00", "BRL")))
	if err := uow.WagerRepository.Insert(ctx, dup); err == nil {
		t.Fatal("segundo OPENING da mesma carteira deveria falhar (índice parcial)")
	} else if !errors.Is(err, postgres.ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
	_ = uow.Rollback(ctx)

	uow = beginOK(t, f)
	var n int
	if err := uow.Tx().QueryRow(ctx,
		`SELECT count(*) FROM wager_transactions WHERE kind = 'OPENING' AND wallet_id = $1`,
		unique("wal-w3")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("OPENING count = %d, want 1", n)
	}
	_ = uow.Rollback(ctx)
}

func TestIdempotencyInsert(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	seedWallet(t, f, "wal-w4", "player-4", "50.00", 5000)

	bet := newBET(t, "tx-bet-1", "wal-w4", "player-4", "ext-4", "key-4", "hash-4")

	uow := beginOK(t, f)
	inserted, err := uow.WagerRepository.InsertIfAbsent(ctx, bet)
	if err != nil {
		t.Fatalf("primeiro insert: %v", err)
	}
	if !inserted {
		t.Fatal("primeira inserção deveria criar a linha")
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Reenvio (idempotência): mesma chave, não insere, replay lê o existente.
	uow = beginOK(t, f)
	again, err := uow.WagerRepository.InsertIfAbsent(ctx, bet)
	if err != nil {
		t.Fatalf("segundo insert: %v", err)
	}
	if again {
		t.Fatal("segundo envio com a mesma chave não deveria inserir")
	}
	existing, err := uow.WagerRepository.GetByIdempotencyKey(ctx, unique("key-4"))
	if err != nil {
		t.Fatalf("get by key: %v", err)
	}
	if existing.ExternalTransactionID() != unique("ext-4") {
		t.Fatalf("replay incorreto: %s", existing.ExternalTransactionID())
	}

	// Mesma (provider, extId) com outra chave → conflito de transação externa.
	clash := newBET(t, "tx-bet-2", "wal-w4", "player-4", "ext-4", "outra-chave", "hash-x")
	_, err = uow.WagerRepository.InsertIfAbsent(ctx, clash)
	if err == nil {
		t.Fatal("mesmo (provider, extId) com outra chave deveria violar o único parcial")
	} else if !errors.Is(err, postgres.ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
	_ = uow.Rollback(ctx)
}

func TestLedgerAppendOnlyAndImmutable(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	uow := beginOK(t, f)
	if err := uow.WalletRepository.Insert(ctx, freshWallet(t, "wal-w5", "player-5", "0.00")); err != nil {
		t.Fatal(err)
	}
	_ = uow.Commit(ctx)

	uow = beginOK(t, f)
	bet := newBET(t, "tx-led-bet", "wal-w5", "player-5", "ext-led-5", "key-led-5", "hash-led-5")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatalf("insert bet: %v", err)
	}
	entry := mustValue(ledger.New(unique("led-1"), unique("wal-w5"), unique("tx-led-bet"),
		ledger.DirectionCredit, mm(t, "10.00", "BRL"), mm(t, "0.00", "BRL"),
		mm(t, "10.00", "BRL"), time.Now()))
	if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
		t.Fatalf("insert ledger: %v", err)
	}
	// Mesma (wallet, transaction): ON CONFLICT DO NOTHING — sem duplicar.
	if err := uow.LedgerRepository.Insert(ctx, entry); err != nil {
		t.Fatalf("reinsert ledger: %v", err)
	}
	_ = uow.Commit(ctx)

	// wager_app não pode UPDATE/DELETE no ledger (REVOKE da migration 000006).
	for _, stmt := range []string{
		`UPDATE wallet_ledger_entries SET amount_minor = 1`,
		`DELETE FROM wallet_ledger_entries`,
	} {
		uow = beginOK(t, f)
		if _, err := uow.Tx().Exec(ctx, stmt); err == nil {
			t.Fatalf("stmt deveria falhar com REVOKE: %s", stmt)
		} else {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("err = %v, want SQLSTATE 42501", err)
			}
		}
		_ = uow.Rollback(ctx)
	}
}

func TestInboxInsertComplete(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	uow := beginOK(t, f)
	consumer, msg := unique("consumer-sqs"), unique("msg-1")
	ok, err := uow.InboxRepository.TryInsert(ctx, consumer, msg, "abc123")
	if err != nil || !ok {
		t.Fatalf("try insert: ok=%v err=%v", ok, err)
	}
	ok, err = uow.InboxRepository.TryInsert(ctx, consumer, msg, "abc123")
	if err != nil || ok {
		t.Fatalf("dup deveria ser false: ok=%v err=%v", ok, err)
	}
	if err := uow.InboxRepository.MarkCompleted(ctx, consumer, msg); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	done, err := uow.InboxRepository.IsCompleted(ctx, consumer, msg)
	if err != nil || !done {
		t.Fatalf("completed: %v %v", done, err)
	}
	_ = uow.Rollback(ctx)
}

func TestOutboxClaimAndPublish(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	uow := beginOK(t, f)
	env := mustValue(events.NewWalletBalanceChanged(unique("evt-1"), unique("corr-1"), "",
		events.WalletBalanceChangedData{
			WalletID: unique("wal-w5"), TransactionID: unique("tx-1"), Direction: "CREDIT",
			Money: mm(t, "1.00", "BRL"), BalanceBefore: mm(t, "0.00", "BRL"),
			BalanceAfter: mm(t, "1.00", "BRL"), WalletVersion: 1,
		}))
	if err := uow.OutboxRepository.Insert(ctx, env); err != nil {
		t.Fatalf("insert outbox: %v", err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	uow = beginOK(t, f)
	claimed, err := uow.OutboxRepository.ClaimPending(ctx, 100, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	found := false
	for _, row := range claimed {
		if row.EventID == unique("evt-1") {
			found = true
		}
		// Publica todos os pendentes para não vazar entre testes.
		if err := uow.OutboxRepository.MarkPublished(ctx, row.EventID); err != nil {
			t.Fatalf("mark published %s: %v", row.EventID, err)
		}
	}
	if !found {
		t.Fatalf("evt-1 não foi reclamado: %+v", claimed)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	uow = beginOK(t, f)
	still, err := uow.OutboxRepository.ClaimPending(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 0 {
		t.Fatalf("evento publicado não deveria ser reclamado: %+v", still)
	}
	_ = uow.Rollback(ctx)
}

func TestUoWAllOrNothing(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)

	// Transação que insere wallet + BET e depois descarta: nada é persistido.
	uow := beginOK(t, f)
	if err := uow.WalletRepository.Insert(ctx, freshWallet(t, "wal-w6", "player-6", "1.00")); err != nil {
		t.Fatal(err)
	}
	bet := newBET(t, "tx-bet-9", "wal-w6", "player-6", "ext-9", "key-9", "hash-9")
	if _, err := uow.WagerRepository.InsertIfAbsent(ctx, bet); err != nil {
		t.Fatal(err)
	}
	if err := uow.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	uow = beginOK(t, f)
	if _, err := uow.WalletRepository.Get(ctx, unique("wal-w6")); err == nil {
		t.Fatal("rollback deveria descartar a carteira")
	} else if !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	_ = uow.Rollback(ctx)
}
