package postgres

import (
	"context"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
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
