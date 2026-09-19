package postgres

import (
	"context"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/jackc/pgx/v5"
)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (wallet.Wallet, error) {
	var (
		id, playerID, currency string
		balanceMinor           int64
		version                int64
		createdAt, updatedAt   time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balanceMinor, &version, &createdAt, &updatedAt); err != nil {
		return wallet.Wallet{}, mapError(err)
	}
	return wallet.Rehydrate(id, playerID, money.Currency(currency),
		money.MoneyOf(balanceMinor, money.Currency(currency)),
		version, createdAt, updatedAt)
}

// WalletRepository persiste carteiras.
type WalletRepository struct {
	tx pgx.Tx
}

// Insert cria a carteira; versão começa em 1.
func (r *WalletRepository) Insert(ctx context.Context, w wallet.Wallet) error {
	_, err := r.tx.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().Minor(), w.Version(),
		w.CreatedAt(), w.UpdatedAt())
	return mapError(err)
}

// LockForUpdate lê a carteira travando-a até o commit (pessimista, G3).
func (r *WalletRepository) LockForUpdate(ctx context.Context, id string) (wallet.Wallet, error) {
	row := r.tx.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
	return scanWallet(row)
}

// Get lê a carteira sem travamento (reconciliação, consultas).
func (r *WalletRepository) Get(ctx context.Context, id string) (wallet.Wallet, error) {
	row := r.tx.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
	return scanWallet(row)
}

// GetByPlayerCurrency lê a carteira do jogador na moeda.
func (r *WalletRepository) GetByPlayerCurrency(ctx context.Context, playerID, currency string) (wallet.Wallet, error) {
	row := r.tx.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE player_id = $1 AND currency = $2`,
		playerID, currency)
	return scanWallet(row)
}

// UpdateBalance persiste saldo e versão com controle otimista (WHERE version).
// Se 0 linhas, a carteira foi modificada por outra transação.
func (r *WalletRepository) UpdateBalance(ctx context.Context, w wallet.Wallet) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE wallets
		   SET balance_minor = $2, version = $3, updated_at = $4
		 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), w.Version()-1)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrOptimisticLock
	}
	return nil
}
