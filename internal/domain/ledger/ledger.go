// Package ledger define o lançamento imutável do livro-razão da carteira.
package ledger

import (
	"errors"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// Direction indica o sentido da movimentação no ledger.
type Direction string

const (
	DirectionCredit Direction = "CREDIT"
	DirectionDebit  Direction = "DEBIT"
)

// Erros de domínio do lançamento de ledger.
var (
	ErrInvalidDirection  = errors.New("ledger: direção inválida")
	ErrMissingID         = errors.New("ledger: identificador ausente")
	ErrAmountNotPositive = errors.New("ledger: valor deve ser positivo")
	ErrBadBalance        = errors.New("ledger: saldo não casa com a fórmula da direção")
	ErrNegativeBalance   = errors.New("ledger: saldo posterior negativo")
)

// Entry é um lançamento imutável: idempotente por (walletID, transactionID).
type Entry struct {
	id            string
	walletID      string
	transactionID string
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// New valida e constrói um lançamento. A regra é `after = before ± amount`
// conforme a direção. O valor deve ser positivo.
func New(id, walletID, transactionID string, direction Direction,
	amount, before, after money.Money, createdAt time.Time) (Entry, error) {
	if id == "" || walletID == "" || transactionID == "" {
		return Entry{}, ErrMissingID
	}
	if direction != DirectionCredit && direction != DirectionDebit {
		return Entry{}, ErrInvalidDirection
	}
	if amount.IsNegative() || amount.IsZero() {
		return Entry{}, ErrAmountNotPositive
	}
	if amount.Currency() != before.Currency() || before.Currency() != after.Currency() {
		return Entry{}, money.ErrCurrencyMismatch
	}
	var expected money.Money
	var err error
	switch direction {
	case DirectionCredit:
		expected, err = before.Add(amount)
	case DirectionDebit:
		expected, err = before.Sub(amount)
	}
	if err != nil {
		return Entry{}, err
	}
	if expected.Minor() != after.Minor() {
		return Entry{}, ErrBadBalance
	}
	if after.IsNegative() {
		return Entry{}, ErrNegativeBalance
	}
	return Entry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     createdAt,
	}, nil
}

func (e Entry) ID() string                 { return e.id }
func (e Entry) WalletID() string           { return e.walletID }
func (e Entry) TransactionID() string      { return e.transactionID }
func (e Entry) Direction() Direction       { return e.direction }
func (e Entry) Amount() money.Money        { return e.amount }
func (e Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e Entry) CreatedAt() time.Time       { return e.createdAt }
