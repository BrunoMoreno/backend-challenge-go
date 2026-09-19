// Package wallet define o agregado raiz financeiro: a carteira do jogador.
package wallet

import (
	"errors"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// Erros de domínio da carteira.
var (
	ErrMissingID         = errors.New("wallet: identificador ausente")
	ErrMissingPlayerID   = errors.New("wallet: jogador ausente")
	ErrNegativeInitial   = errors.New("wallet: saldo inicial negativo")
	ErrNegativeAmount    = errors.New("wallet: valor negativo não permitido")
	ErrInsufficientFunds = errors.New("wallet: saldo insuficiente")
)

// Wallet é a raiz do agregado financeiro. O saldo nunca é negativo e o
// controle do saldo e da versão são responsabilidade exclusivos do agregado.
type Wallet struct {
	id        string
	playerID  string
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// New cria uma carteira com saldo inicial (>= 0) e versão 1.
func New(id, playerID string, initial money.Money) (Wallet, error) {
	if id == "" {
		return Wallet{}, ErrMissingID
	}
	if playerID == "" {
		return Wallet{}, ErrMissingPlayerID
	}
	if initial.IsNegative() {
		return Wallet{}, ErrNegativeInitial
	}
	now := time.Now().UTC()
	return Wallet{
		id:        id,
		playerID:  playerID,
		currency:  initial.Currency(),
		balance:   initial,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// Rehydrate reconstrói uma carteira persistida sem reaplicar movimentações.
// O estado informado é confiado ao chamador (leitura do banco).
func Rehydrate(id, playerID string, currency money.Currency, balance money.Money,
	version int64, createdAt, updatedAt time.Time) (Wallet, error) {
	if id == "" {
		return Wallet{}, ErrMissingID
	}
	if playerID == "" {
		return Wallet{}, ErrMissingPlayerID
	}
	if currency != balance.Currency() {
		return Wallet{}, money.ErrCurrencyMismatch
	}
	return Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}, nil
}

func (w Wallet) ID() string               { return w.id }
func (w Wallet) PlayerID() string         { return w.playerID }
func (w Wallet) Currency() money.Currency { return w.currency }
func (w Wallet) Balance() money.Money     { return w.balance }
func (w Wallet) Version() int64           { return w.version }
func (w Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// Credit credita o valor na carteira. Valor nulo não altera saldo nem versão.
func (w *Wallet) Credit(amount money.Money) (money.Money, error) {
	if amount.Currency() != w.currency {
		return money.Money{}, money.ErrCurrencyMismatch
	}
	if amount.IsNegative() {
		return money.Money{}, ErrNegativeAmount
	}
	if amount.IsZero() {
		return w.balance, nil
	}
	next, err := w.balance.Add(amount)
	if err != nil {
		return money.Money{}, err
	}
	return w.apply(next), nil
}

// Debit debita o valor da carteira, preservando o saldo >= 0.
// Valor nulo não altera saldo nem versão.
func (w *Wallet) Debit(amount money.Money) (money.Money, error) {
	if amount.Currency() != w.currency {
		return money.Money{}, money.ErrCurrencyMismatch
	}
	if amount.IsNegative() {
		return money.Money{}, ErrNegativeAmount
	}
	if amount.IsZero() {
		return w.balance, nil
	}
	if cmp, _ := w.balance.Compare(amount); cmp < 0 {
		return money.Money{}, ErrInsufficientFunds
	}
	next, err := w.balance.Sub(amount)
	if err != nil {
		return money.Money{}, err
	}
	return w.apply(next), nil
}

func (w *Wallet) apply(next money.Money) money.Money {
	w.balance = next
	w.version++
	w.updatedAt = time.Now().UTC()
	return w.balance
}
