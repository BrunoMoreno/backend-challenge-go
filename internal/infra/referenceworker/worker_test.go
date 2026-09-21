package referenceworker

import (
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
)

func TestNextAttemptBackoff(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	w := New(nil, nil, nil, Config{
		BatchSize: 1, MaxAttempts: 30, TTL: 24 * time.Hour,
		BackoffBase: time.Second, BackoffMax: time.Minute,
	})
	w.jitter = func(time.Duration) time.Duration { return 0 } // sem jitter: determinístico

	prev := now
	var last time.Time
	var wantCap int64 = 60
	for i := 0; i < 8; i++ {
		last = w.nextAttempt(now, i)
		// base * 2^i, sem jitter e com teto de 1 minuto.
		want := int64(1 << uint(i))
		if want > wantCap {
			want = wantCap
		}
		if last.Sub(now) != time.Duration(want)*time.Second {
			t.Fatalf("attempts=%d delay=%v, want %vs", i, last.Sub(now), want)
		}
		if last.Before(prev) {
			t.Fatalf("backoff não é monotônico: %v depois de %v", last, prev)
		}
		prev = last
	}
	if last.Sub(now) > time.Minute {
		t.Fatalf("backoff ultrapassou o teto: %v", last.Sub(now))
	}
}

func TestNextAttemptJitterWithinRange(t *testing.T) {
	now := time.Now()
	w := New(nil, nil, nil, Config{
		BackoffBase: time.Second, BackoffMax: time.Minute,
	})
	target := w.nextAttempt(now, 2)
	expected := 4 * time.Second
	if target.Before(now) {
		t.Fatalf("nextAttempt no passado: %v", target)
	}
	// Jitter reduz o delay; nunca chega perto do dobro.
	if target.After(now.Add(expected + 2*time.Second)) {
		t.Fatalf("jitter ampliou demais o delay: %v (base 4s)", target.Sub(now))
	}
}

func TestExpiredTTL(t *testing.T) {
	w := New(nil, nil, nil, Config{TTL: 24 * time.Hour})
	now := time.Now()
	created := now.Add(-25 * time.Hour) // criada há 25h > TTL
	if !w.expired(mustTx(created), now) {
		t.Fatal("linha mais antiga que o TTL deveria estar expirada")
	}
	if w.expired(mustTx(now.Add(-time.Hour)), now) {
		t.Fatal("linha recente não deveria estar expirada")
	}
}

// mustTx constrói uma transação válida com o CreatedAt desejado.
func mustTx(created time.Time) wager.WagerTransaction {
	tx, err := wager.Rehydrate(wager.RehydrateInput{
		ID:        "id-x",
		Origin:    wager.OriginExternal,
		Kind:      wager.KindBet,
		State:     wager.StatePendingReference,
		WalletID:  "wal-x",
		PlayerID:  "player-x",
		Amount:    money.MoneyOf(1000, "BRL"),
		CreatedAt: created,
	})
	if err != nil {
		panic(err)
	}
	return tx
}
