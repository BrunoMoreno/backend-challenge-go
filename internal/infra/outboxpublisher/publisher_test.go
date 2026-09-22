package outboxpublisher

import (
	"testing"
	"time"
)

func TestNextAttemptBackoff(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	p := New(nil, nil, nil, Config{
		BatchSize: 1, Lease: time.Minute, SendTimeout: time.Second,
		BackoffBase: time.Second, BackoffMax: time.Minute,
	})
	p.now = func() time.Time { return now }
	p.jitter = func(time.Duration) time.Duration { return 0 } // sem jitter: determinístico

	prev := now
	var last time.Time
	var wantCap int64 = 60
	for i := 0; i < 8; i++ {
		last = p.nextAttempt(now, i)
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
	p := New(nil, nil, nil, Config{
		BackoffBase: time.Second, BackoffMax: time.Minute,
	})
	target := p.nextAttempt(now, 2)
	expected := 4 * time.Second
	if target.Before(now) {
		t.Fatalf("nextAttempt no passado: %v", target)
	}
	// Jitter reduz o delay; nunca chega perto do dobro.
	if target.After(now.Add(expected + 2*time.Second)) {
		t.Fatalf("jitter ampliou demais o delay: %v (base 4s)", target.Sub(now))
	}
}

// TestNewClampsLeaseToBatchCoverage garante o M7: o lease nunca fica menor que
// o pior caso de envio do lote (BatchSize×SendTimeout) — um lease menor
// estouraria no meio do lote e outra instância republicaria eventos em voo,
// quebrando a ordem FIFO da fila.
func TestNewClampsLeaseToBatchCoverage(t *testing.T) {
	cfg := Config{
		BatchSize:   10,
		SendTimeout: 10 * time.Second,
		Lease:       5 * time.Second, // menor que 100s
	}
	p := New(nil, nil, nil, cfg)
	if want := 100 * time.Second; p.cfg.Lease != want {
		t.Fatalf("Lease = %v, want %v (BatchSize×SendTimeout)", p.cfg.Lease, want)
	}
}
