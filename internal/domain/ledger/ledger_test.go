package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

func mm(minor int64, c string) money.Money { return money.MoneyOf(minor, money.Currency(c)) }

func ts() time.Time { return time.Unix(0, 0).UTC() }

func TestNewCredit(t *testing.T) {
	e, err := New("e1", "w1", "t1", DirectionCredit, mm(2500, "BRL"), mm(10000, "BRL"), mm(12500, "BRL"), ts())
	if err != nil {
		t.Fatalf("New(credit) error = %v", err)
	}
	if e.Direction() != DirectionCredit || e.Amount().Minor() != 2500 {
		t.Errorf("entry = %v %d", e.Direction(), e.Amount().Minor())
	}
	if e.BalanceBefore().Minor() != 10000 || e.BalanceAfter().Minor() != 12500 {
		t.Errorf("saldo antes/depois = %d/%d", e.BalanceBefore().Minor(), e.BalanceAfter().Minor())
	}
}

func TestNewDebit(t *testing.T) {
	if _, err := New("e1", "w1", "t1", DirectionDebit, mm(3000, "BRL"), mm(10000, "BRL"), mm(7000, "BRL"), ts()); err != nil {
		t.Fatalf("New(debit) error = %v", err)
	}
}

func TestNewFormulaMismatch(t *testing.T) {
	if _, err := New("e1", "w1", "t1", DirectionCredit, mm(2500, "BRL"), mm(10000, "BRL"), mm(12000, "BRL"), ts()); !errors.Is(err, ErrBadBalance) {
		t.Errorf("credit errado = %v, want ErrBadBalance", err)
	}
	if _, err := New("e1", "w1", "t1", DirectionDebit, mm(3000, "BRL"), mm(10000, "BRL"), mm(7500, "BRL"), ts()); !errors.Is(err, ErrBadBalance) {
		t.Errorf("debit errado = %v, want ErrBadBalance", err)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New("", "w1", "t1", DirectionCredit, mm(1, "BRL"), mm(0, "BRL"), mm(1, "BRL"), ts()); !errors.Is(err, ErrMissingID) {
		t.Errorf("id vazio = %v, want ErrMissingID", err)
	}
	if _, err := New("e1", "w1", "t1", "LATERAL", mm(1, "BRL"), mm(0, "BRL"), mm(1, "BRL"), ts()); !errors.Is(err, ErrInvalidDirection) {
		t.Errorf("direção inválida = %v, want ErrInvalidDirection", err)
	}
	if _, err := New("e1", "w1", "t1", DirectionCredit, mm(0, "BRL"), mm(0, "BRL"), mm(0, "BRL"), ts()); !errors.Is(err, ErrAmountNotPositive) {
		t.Errorf("valor zero = %v, want ErrAmountNotPositive", err)
	}
	if _, err := New("e1", "w1", "t1", DirectionCredit, mm(-1, "BRL"), mm(0, "BRL"), mm(-1, "BRL"), ts()); !errors.Is(err, ErrAmountNotPositive) {
		t.Errorf("valor negativo = %v, want ErrAmountNotPositive", err)
	}
}

func TestNewCurrencyMismatch(t *testing.T) {
	if _, err := New("e1", "w1", "t1", DirectionCredit, mm(1, "BRL"), mm(1, "USD"), mm(2, "BRL"), ts()); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("moedas divergentes = %v, want ErrCurrencyMismatch", err)
	}
}

func TestNewNegativeBalanceAfter(t *testing.T) {
	if _, err := New("e1", "w1", "t1", DirectionDebit, mm(5000, "BRL"), mm(1000, "BRL"), mm(-4000, "BRL"), ts()); !errors.Is(err, ErrNegativeBalance) {
		t.Errorf("saldo após negativo = %v, want ErrNegativeBalance", err)
	}
}

func TestImmutability(t *testing.T) {
	e, err := New("e1", "w1", "t1", DirectionCredit, mm(2500, "BRL"), mm(10000, "BRL"), mm(12500, "BRL"), ts())
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	if e.ID() != "e1" || e.WalletID() != "w1" || e.TransactionID() != "t1" {
		t.Errorf("identidade da entry incompleta")
	}
}
