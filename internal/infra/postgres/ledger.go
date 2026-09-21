package postgres

import (
	"context"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/jackc/pgx/v5"
)

// LedgerRepository persiste lançamentos (append-only, sem UPDATE/DELETE).
type LedgerRepository struct {
	tx pgx.Tx
}

// Insert adiciona um lançamento imutável. O par (wallet, transaction) é único:
// a reexecução da mesma transação não duplica o lançamento.
func (r *LedgerRepository) Insert(ctx context.Context, e ledger.Entry) error {
	_, err := r.tx.Exec(ctx,
		`INSERT INTO wallet_ledger_entries
		   (id, wallet_id, transaction_id, direction, amount_minor,
		    balance_before, balance_after, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (wallet_id, transaction_id) DO NOTHING`,
		e.ID(), e.WalletID(), e.TransactionID(), e.Direction(),
		e.Amount().Minor(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(),
		e.CreatedAt())
	return mapError(err)
}

// ListByWallet lista os lançamentos da carteira em ordem decrescente
// (created_at, id). Paginação por keyset: afterCreatedAt/afterID marcam o
// lançamento imediatamente anterior à página solicitada; zero indica o início.
// A currency da carteira (argent) é necessária porque a tabela guarda apenas
// minor units e a reconstrução exige money.Money completo.
func (r *LedgerRepository) ListByWallet(ctx context.Context, walletID string, currency money.Currency,
	afterCreatedAt time.Time, afterID string, limit int) ([]ledger.Entry, error) {

	rows, err := r.tx.Query(ctx,
		`SELECT id, wallet_id, transaction_id, direction, amount_minor,
		        balance_before, balance_after, created_at
		   FROM wallet_ledger_entries
		  WHERE wallet_id = $1
		    AND (
		      $4 = TIMESTAMPTZ 'epoch'
		      OR created_at < $4
		      OR (created_at = $4 AND id < $5)
		    )
		  ORDER BY created_at DESC, id DESC
		  LIMIT $3`,
		walletID, walletID, limit, afterCreatedAt, afterID)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	var entries []ledger.Entry
	for rows.Next() {
		var (
			id            string
			walletID      string
			transactionID string
			direction     ledger.Direction
			amountMinor   int64
			beforeMinor   int64
			afterMinor    int64
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &walletID, &transactionID, &direction,
			&amountMinor, &beforeMinor, &afterMinor, &createdAt); err != nil {
			return nil, mapError(err)
		}
		entry, err := ledger.New(
			id, walletID, transactionID, direction,
			money.MoneyOf(amountMinor, currency),
			money.MoneyOf(beforeMinor, currency),
			money.MoneyOf(afterMinor, currency),
			createdAt,
		)
		if err != nil {
			return nil, mapError(err)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// AggregateByWallet devolve a soma de créditos, de débitos e o total de
// lançamentos da carteira para a reconciliação. Chamado na mesma transação
// de snapshot do saldo (BeginReadOnly), garante a visão consistente.
func (r *LedgerRepository) AggregateByWallet(ctx context.Context, walletID string) (storage.LedgerAggregate, error) {
	var a storage.LedgerAggregate
	err := r.tx.QueryRow(ctx,
		`SELECT
		   COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::bigint,
		   COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::bigint,
		   COUNT(*)
		 FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).
		Scan(&a.CreditsMinor, &a.DebitsMinor, &a.Count)
	if err != nil {
		return storage.LedgerAggregate{}, mapError(err)
	}
	return a, nil
}
