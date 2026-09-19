package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func helpParse(t *testing.T, amount, currency string) Money {
	t.Helper()
	m, err := Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q, %q) error = %v", amount, currency, err)
	}
	return m
}

func TestParseValid(t *testing.T) {
	cases := []struct {
		amount   string
		currency string
		minor    int64
	}{
		{"0.00", "BRL", 0},
		{"0.01", "BRL", 1},
		{"25.00", "BRL", 2500},
		{"1234.56", "USD", 123456},
		{"9999999999999999.99", "BRL", 999999999999999999},
	}
	for _, c := range cases {
		m, err := Parse(c.amount, c.currency)
		if err != nil {
			t.Errorf("Parse(%q, %q) error = %v, want nil", c.amount, c.currency, err)
			continue
		}
		if m.Minor() != c.minor {
			t.Errorf("Parse(%q).Minor() = %d, want %d", c.amount, m.Minor(), c.minor)
		}
		if m.Currency().String() != c.currency {
			t.Errorf("Parse(%q).Currency() = %q, want %q", c.amount, m.Currency(), c.currency)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	cases := []struct{ amount, currency string }{
		{"", "BRL"},
		{"25.00", ""},
		{"25.00", "brl"},
		{"25.00", "BRLX"},
		{"25", "BRL"},
		{"25.0", "BRL"},
		{"25.000", "BRL"},
		{"-25.00", "BRL"},
		{"+25.00", "BRL"},
		{"00.00", "BRL"},
		{"01.00", "BRL"},
		{".00", "BRL"},
		{"25.", "BRL"},
		{"25,00", "BRL"},
		{" 25.00", "BRL"},
		{"25.00 ", "BRL"},
		{"25.00\n", "BRL"},
		{"NaN", "BRL"},
		{"Infinity", "BRL"},
		{"1e2.00", "BRL"},
		{"1.5E2", "BRL"},
		{"abc", "BRL"},
		{"92233720368547758.08", "BRL"},
	}
	for _, c := range cases {
		if _, err := Parse(c.amount, c.currency); err == nil {
			t.Errorf("Parse(%q, %q) = nil error, want error", c.amount, c.currency)
		}
	}
}

func TestParseMaxInt64(t *testing.T) {
	max := "92233720368547758.07"
	m, err := Parse(max, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", max, err)
	}
	if m.Minor() != math.MaxInt64 {
		t.Fatalf("Minor() = %d, want MaxInt64", m.Minor())
	}
}

func TestParseOverflow(t *testing.T) {
	if _, err := Parse("92233720368547758.08", "BRL"); !errors.Is(err, ErrOverflow) {
		t.Errorf("overflow = %v, want ErrOverflow", err)
	}
}

func TestAdd(t *testing.T) {
	a := helpParse(t, "10.00", "BRL")
	b := helpParse(t, "0.05", "BRL")
	got, err := a.Add(b)
	if err != nil {
		t.Fatalf("Add error = %v", err)
	}
	want := helpParse(t, "10.05", "BRL")
	if c, _ := got.Compare(want); c != 0 {
		t.Errorf("Add = %s, want %s", got, want)
	}
}

func TestAddOverflow(t *testing.T) {
	max := MoneyOf(math.MaxInt64, "BRL")
	if _, err := max.Add(MoneyOf(1, "BRL")); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add overflow = %v, want ErrOverflow", err)
	}
	min := MoneyOf(math.MinInt64, "BRL")
	if _, err := min.Add(MoneyOf(-1, "BRL")); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add underflow = %v, want ErrOverflow", err)
	}
}

func TestAddCurrencyMismatch(t *testing.T) {
	a := MoneyOf(100, "BRL")
	b := MoneyOf(100, "USD")
	if _, err := a.Add(b); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add = %v, want ErrCurrencyMismatch", err)
	}
}

func TestSub(t *testing.T) {
	a := helpParse(t, "10.00", "BRL")
	b := helpParse(t, "0.25", "BRL")
	got, err := a.Sub(b)
	if err != nil {
		t.Fatalf("Sub error = %v", err)
	}
	want := helpParse(t, "9.75", "BRL")
	if c, _ := got.Compare(want); c != 0 {
		t.Errorf("Sub = %s, want %s", got, want)
	}
	if _, err := a.Sub(a); err != nil {
		t.Errorf("Sub(self) = %v, want nil (resultado pode ser zero)", err)
	}
}

func TestSubMinInt64(t *testing.T) {
	m := MoneyOf(math.MinInt64, "BRL")
	if _, err := m.Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("Neg(MinInt64) = %v, want ErrOverflow", err)
	}
	if _, err := m.Sub(MoneyOf(1, "BRL")); !errors.Is(err, ErrOverflow) {
		t.Errorf("Sub(MinInt64, +1) = %v, want ErrOverflow", err)
	}
	if _, err := m.Sub(MoneyOf(-1, "BRL")); err != nil {
		t.Errorf("Sub(MinInt64, -1) = %v, want nil (resultado é MinInt64+1)", err)
	}
}

func TestCompare(t *testing.T) {
	a := MoneyOf(100, "BRL")
	b := MoneyOf(200, "BRL")
	if c, _ := a.Compare(b); c != -1 {
		t.Errorf("Compare(100, 200) = %d, want -1", c)
	}
	if c, _ := b.Compare(a); c != 1 {
		t.Errorf("Compare(200, 100) = %d, want 1", c)
	}
	if c, _ := a.Compare(MoneyOf(100, "BRL")); c != 0 {
		t.Errorf("Compare(100, 100) = %d, want 0", c)
	}
	if _, err := a.Compare(MoneyOf(100, "USD")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Compare moedas distintas = %v, want ErrCurrencyMismatch", err)
	}
}

func TestZero(t *testing.T) {
	z := Zero("BRL")
	if !z.IsZero() {
		t.Error("Zero().IsZero() = false")
	}
	if got, err := z.Add(MoneyOf(1, "BRL")); err != nil || got.Minor() != 1 {
		t.Errorf("Zero+1 = %d (err %v), want minor 1", got.Minor(), err)
	}
}

func TestNeg(t *testing.T) {
	m := MoneyOf(2500, "BRL")
	n, err := m.Neg()
	if err != nil {
		t.Fatalf("Neg error = %v", err)
	}
	if n.Minor() != -2500 || !n.IsNegative() {
		t.Errorf("Neg = %d (neg=%v), want -2500 (true)", n.Minor(), n.IsNegative())
	}
	if back, err := n.Neg(); err != nil || back.Minor() != 2500 {
		t.Errorf("Neg(-2500) = %d (err %v), want 2500", back.Minor(), err)
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		minor int64
		want  string
	}{
		{0, "0.00"},
		{5, "0.05"},
		{7, "0.07"},
		{2500, "25.00"},
		{-5, "-0.05"},
		{-2500, "-25.00"},
		{123456, "1234.56"},
		{math.MaxInt64, "92233720368547758.07"},
	}
	for _, c := range cases {
		if got := MoneyOf(c.minor, "BRL").String(); got != c.want {
			t.Errorf("String(%d) = %q, want %q", c.minor, got, c.want)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m := helpParse(t, "25.00", "BRL")
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}
	if string(raw) != `{"amount":"25.00","currency":"BRL"}` {
		t.Errorf("Marshal = %s", raw)
	}
	var got Money
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal error = %v", err)
	}
	if c, _ := got.Compare(m); c != 0 {
		t.Errorf("Unmarshal roundtrip = %s, want %s", got, m)
	}
}

func TestJSONInvalid(t *testing.T) {
	cases := []string{
		`{"amount":"25.00"}`,
		`{"currency":"BRL"}`,
		`{"amount":"25.0","currency":"BRL"}`,
		`{"amount":"25.00","currency":"brl"}`,
		`"25.00"`,
		`25`,
	}
	for _, c := range cases {
		var m Money
		if err := json.Unmarshal([]byte(c), &m); err == nil {
			t.Errorf("Unmarshal(%s) = nil error, want error", c)
		}
	}
}
