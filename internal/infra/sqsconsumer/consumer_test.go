package sqsconsumer

import (
	"log/slog"
	"math/rand"
	"testing"
	"time"
)

// TestBackoffNeverBelowOneSecond garante o invariante do A2: o backoff de
// reentrega nunca retorna menos de 1s, mesmo com `jitter` variando no máximo.
// Uma reentrega com delay≤1s viraria visibilidade 0 no SQS, redeliverando a
// mensagem imediatamente e inflando ApproximateReceiveCount — uma falha
// transitória iria para a DLQ antes da hora.
func TestBackoffNeverBelowOneSecond(t *testing.T) {
	c := &Consumer{
		cfg: Config{
			BackoffBase: time.Second,
			BackoffMax:  15 * time.Minute,
		},
		jitter: func(max time.Duration) time.Duration { return max }, // pior caso
		logger: slog.Default(),
	}
	for count := 1; count <= 12; count++ {
		if d := c.backoff(count); d < time.Second {
			t.Errorf("backoff(%d) = %v, want >= 1s", count, d)
		}
	}
}

// TestBackoffRespectsCeiling verifica que o delay nunca ultrapassa o teto e
// nunca excede o valor exponencial determinístico (Base*2^(count-1)) — o jitter
// só reduz o atraso, nunca o aumenta.
func TestBackoffRespectsCeiling(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	c := &Consumer{
		cfg: Config{
			BackoffBase: time.Second,
			BackoffMax:  10 * time.Second,
		},
		jitter: func(max time.Duration) time.Duration { return time.Duration(rng.Int63n(int64(max) + 1)) },
		logger: slog.Default(),
	}
	for count := 1; count <= 12; count++ {
		d := c.backoff(count)
		deterministic := time.Second << (count - 1)
		if deterministic > 10*time.Second {
			deterministic = 10 * time.Second
		}
		if d > deterministic {
			t.Fatalf("backoff(%d) = %v, excede o valor determinístico %v", count, d, deterministic)
		}
		if d > 10*time.Second {
			t.Fatalf("backoff(%d) = %v, ultrapassou BackoffMax", count, d)
		}
	}
}
