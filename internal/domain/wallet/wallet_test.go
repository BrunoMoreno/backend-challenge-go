package wallet

import (
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

func zeroTime() time.Time { return time.Time{} }

func mustMoney(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("money.Parse(%q, %q) error = %v", amount, currency, err)
	}
	return m
}

func TestNewWallet(t *testing.T) {
	w, err := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	if w.ID() != "w1" || w.PlayerID() != "p1" {
		t.Errorf("identidade = (%q, %q)", w.ID(), w.PlayerID())
	}
	if w.Version() != 1 {
		t.Errorf("Version = %d, want 1", w.Version())
	}
	if got := w.Balance().String(); got != "100.00" {
		t.Errorf("Balance = %s, want 100.00", got)
	}
	if w.Currency().String() != "BRL" {
		t.Errorf("Currency = %q, want BRL", w.Currency())
	}
}

func TestNewWalletZeroBalance(t *testing.T) {
	w, err := New("w1", "p1", money.Zero("BRL"))
	if err != nil {
		t.Fatalf("New(zero) error = %v", err)
	}
	if !w.Balance().IsZero() || w.Version() != 1 {
		t.Errorf("carteira zero = %v v%d, want zero v1", w.Balance(), w.Version())
	}
}

func TestNewWalletInvariants(t *testing.T) {
	if _, err := New("", "p1", mustMoney(t, "1.00", "BRL")); !errors.Is(err, ErrMissingID) {
		t.Errorf("id vazio = %v, want ErrMissingID", err)
	}
	if _, err := New("w1", "", mustMoney(t, "1.00", "BRL")); !errors.Is(err, ErrMissingPlayerID) {
		t.Errorf("player vazio = %v, want ErrMissingPlayerID", err)
	}
	if _, err := New("w1", "p1", MoneyOf(-100, "BRL")); !errors.Is(err, ErrNegativeInitial) {
		t.Errorf("saldo inicial negativo = %v, want ErrNegativeInitial", err)
	}
}

func MoneyOf(minor int64, c string) money.Money { return money.MoneyOf(minor, money.Currency(c)) }

func TestCredit(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	got, err := w.Credit(mustMoney(t, "50.00", "BRL"))
	if err != nil {
		t.Fatalf("Credit error = %v", err)
	}
	if got.String() != "150.00" {
		t.Errorf("saldo = %s, want 150.00", got)
	}
	if w.Version() != 2 {
		t.Errorf("Version = %d, want 2", w.Version())
	}
	if w.Balance().String() != "150.00" {
		t.Errorf("Balance = %s, want 150.00", w.Balance())
	}
}

func TestCreditZeroNoOp(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	got, err := w.Credit(money.Zero("BRL"))
	if err != nil {
		t.Fatalf("Credit(zero) error = %v", err)
	}
	if got.String() != "100.00" || w.Version() != 1 {
		t.Errorf("credit zero: saldo=%s v%d, want 100.00 v1", got, w.Version())
	}
}

func TestCreditErrors(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	if _, err := w.Credit(MoneyOf(-1, "BRL")); !errors.Is(err, ErrNegativeAmount) {
		t.Errorf("credit negativo = %v, want ErrNegativeAmount", err)
	}
	if _, err := w.Credit(MoneyOf(1, "USD")); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("credit moeda distinta = %v, want ErrCurrencyMismatch", err)
	}
}

func TestDebit(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	got, err := w.Debit(mustMoney(t, "30.00", "BRL"))
	if err != nil {
		t.Fatalf("Debit error = %v", err)
	}
	if got.String() != "70.00" {
		t.Errorf("saldo = %s, want 70.00", got)
	}
	if w.Version() != 2 {
		t.Errorf("Version = %d, want 2", w.Version())
	}
}

func TestDebitInsufficientFunds(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "10.00", "BRL"))
	if _, err := w.Debit(mustMoney(t, "10.01", "BRL")); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("debit acima do saldo = %v, want ErrInsufficientFunds", err)
	}
	if w.Balance().String() != "10.00" || w.Version() != 1 {
		t.Errorf("saldo/versão alterados após falha: %s v%d", w.Balance(), w.Version())
	}
}

func TestDebitExactBalance(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "10.00", "BRL"))
	got, err := w.Debit(mustMoney(t, "10.00", "BRL"))
	if err != nil {
		t.Fatalf("Debit(exato) error = %v", err)
	}
	if !got.IsZero() {
		t.Errorf("saldo = %s, want 0.00", got)
	}
	if w.Version() != 2 {
		t.Errorf("Version = %d, want 2", w.Version())
	}
}

func TestDebitZeroNoOp(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	if _, err := w.Debit(money.Zero("BRL")); err != nil {
		t.Fatalf("Debit(zero) error = %v", err)
	}
	if w.Version() != 1 {
		t.Errorf("debit zero alterou versão para %d, want 1", w.Version())
	}
}

func TestDebitErrors(t *testing.T) {
	w, _ := New("w1", "p1", mustMoney(t, "100.00", "BRL"))
	if _, err := w.Debit(MoneyOf(-1, "BRL")); !errors.Is(err, ErrNegativeAmount) {
		t.Errorf("debit negativo = %v, want ErrNegativeAmount", err)
	}
	if _, err := w.Debit(MoneyOf(1, "USD")); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("debit moeda distinta = %v, want ErrCurrencyMismatch", err)
	}
}

func TestRehydrate(t *testing.T) {
	w, err := Rehydrate("w1", "p1", "BRL", mustMoney(t, "75.00", "BRL"), 3, zeroTime(), zeroTime())
	if err != nil {
		t.Fatalf("Rehydrate error = %v", err)
	}
	if want := mustMoney(t, "75.00", "BRL"); w.Balance().Minor() != want.Minor() {
		t.Errorf("saldo reidratado = %s, want %s", w.Balance(), want)
	}
	if w.Version() != 3 {
		t.Errorf("Version = %d, want 3", w.Version())
	}
	if _, err := Rehydrate("w1", "p1", "BRL", MoneyOf(1, "USD"), 1, zeroTime(), zeroTime()); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("reidratação moeda divergente = %v, want ErrCurrencyMismatch", err)
	}
}
