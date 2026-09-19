// Package money define o value object Money, imutável, com escala fixa de 2.
package money

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Erros de domínio do Money.
var (
	ErrInvalidFormat    = errors.New("money: formato inválido")
	ErrInvalidCurrency  = errors.New("money: moeda inválida")
	ErrCurrencyMismatch = errors.New("money: moedas incompatíveis")
	ErrOverflow         = errors.New("money: overflow")
)

// Currency é o código de moeda ISO 4217 (3 letras maiúsculas).
type Currency string

// ParseCurrency valida e normaliza o código de moeda.
func ParseCurrency(s string) (Currency, error) {
	if !currencyPattern.MatchString(s) {
		return "", ErrInvalidCurrency
	}
	return Currency(s), nil
}

func (c Currency) String() string { return string(c) }

var (
	amountPattern   = regexp.MustCompile(`^(0|[1-9][0-9]*)\.([0-9]{2})$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Money é um valor monetário imutável em unidades mínimas (centavos).
type Money struct {
	minor    int64
	currency Currency
}

// Parse interpreta amount como decimal estrito de escala 2 e currency como ISO 4217.
// Rejeita vazio, NaN, Infinity, notação científica, sinal, zeros à esquerda e escala
// diferente de 2. Não normaliza: "25" e "25.0" são inválidos.
func Parse(amount, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	m := amountPattern.FindStringSubmatch(amount)
	if m == nil {
		return Money{}, ErrInvalidFormat
	}
	minor, err := strconv.ParseInt(m[1]+m[2], 10, 64)
	if err != nil {
		return Money{}, ErrOverflow
	}
	return Money{minor: minor, currency: cur}, nil
}

// MoneyOf constrói Money a partir de unidades mínimas. Aceita negativos para
// cálculos internos; o saldo da carteira nunca deve ser negativo.
func MoneyOf(minor int64, currency Currency) Money {
	return Money{minor: minor, currency: currency}
}

// Zero devolve o zero da moeda.
func Zero(currency Currency) Money {
	return Money{minor: 0, currency: currency}
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }

// Add soma dois valores da mesma moeda, com tratamento de overflow.
func (m Money) Add(o Money) (Money, error) {
	if m.currency != o.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub subtrai o, exigindo a mesma moeda.
func (m Money) Sub(o Money) (Money, error) {
	if m.currency != o.currency {
		return Money{}, ErrCurrencyMismatch
	}
	n, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(n)
}

// Neg inverte o sinal, tratando MinInt64 como overflow.
func (m Money) Neg() (Money, error) {
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Compare compara dois valores da mesma moeda: -1, 0 ou 1.
func (m Money) Compare(o Money) (int, error) {
	if m.currency != o.currency {
		return 0, ErrCurrencyMismatch
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// String serializa como decimal de escala 2 ("25.00").
func (m Money) String() string {
	s := strconv.FormatInt(m.minor, 10)
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	if len(s) < 3 {
		s = strings.Repeat("0", 3-len(s)) + s
	}
	return sign + s[:len(s)-2] + "." + s[len(s)-2:]
}

type jsonMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonMoney{Amount: m.String(), Currency: string(m.currency)})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var jm jsonMoney
	if err := json.Unmarshal(data, &jm); err != nil {
		return err
	}
	parsed, err := Parse(jm.Amount, jm.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
