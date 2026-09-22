// Package backoff calcula o próximo atraso com backoff exponencial e jitter,
// compartilhado entre o consumidor SQS, os workers de outbox e de referências
// (docs/solve/IMPROVEMENTS.md L1).
package backoff

import (
	"math/rand"
	"time"
)

// Random é o jitter padrão (uniforme até max), usado quando o chamador não
// injeta um determinístico nos testes.
func Random(max time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(max) + 1))
}

// Delay devolve o atraso de uma tentativa: base * 2^attempts com teto `max`,
// descontado de até 30% de jitter (o gerador é injetado para testes
// determinísticos). `floor` garante um atraso mínimo: o consumidor SQS exige
// 1s para a reentrega nunca virar visibilidade 0 (docs/solve/IMPROVEMENTS.md
// A2) e os workers usam um piso pequeno para nunca agendar retry imediato.
func Delay(attempts int, base, max time.Duration, jitter func(time.Duration) time.Duration, floor time.Duration) time.Duration {
	delay := base
	for i := 0; i < attempts; i++ {
		if delay >= max {
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	maxJitter := delay * 30 / 100
	if maxJitter < floor {
		maxJitter = floor
	}
	delay -= jitter(maxJitter)
	if delay < floor {
		return floor
	}
	return delay
}

// Next devolve o próximo instante de tentativa (workers de outbox e de
// referências): now + Delay com o piso padrão dos workers.
func Next(now time.Time, attempts int, base, max time.Duration, jitter func(time.Duration) time.Duration) time.Time {
	return now.Add(Delay(attempts, base, max, jitter, 30*time.Millisecond))
}
