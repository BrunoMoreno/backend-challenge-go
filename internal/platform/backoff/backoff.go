// Package backoff calcula o próximo agendamento com backoff exponencial e
// jitter, compartilhado entre os workers de outbox e de referências (M6.1).
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

// Next devolve o próximo instante de tentativa: base * 2^attempts (com teto
// max), descontado de até 30% de jitter para prevenir ondas de reprocessamento.
// O jitter é injetado para permitir testes determinísticos.
func Next(now time.Time, attempts int, base, max time.Duration, jitter func(time.Duration) time.Duration) time.Time {
	delay := base
	for i := 0; i < attempts; i++ {
		if delay >= max {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	ratio := 30 * time.Millisecond
	maxJitter := delay * 30 / 100
	if maxJitter < ratio {
		maxJitter = ratio
	}
	delay -= jitter(maxJitter)
	if delay < 0 {
		delay = 0
	}
	return now.Add(delay)
}
