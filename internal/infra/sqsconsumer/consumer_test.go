package sqsconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// fakeAPI registra chamadas de ChangeMessageVisibility (heartbeat).
type fakeAPI struct {
	visibilityCalls atomic.Int32
	zeroCalls       atomic.Int32
	extensions      atomic.Int32
}

func (f *fakeAPI) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput,
	...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return nil, nil
}
func (f *fakeAPI) DeleteMessage(context.Context, *sqs.DeleteMessageInput,
	...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return nil, nil
}
func (f *fakeAPI) SendMessage(context.Context, *sqs.SendMessageInput,
	...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return nil, nil
}
func (f *fakeAPI) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput,
	_ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.visibilityCalls.Add(1)
	if in.VisibilityTimeout == 0 {
		f.zeroCalls.Add(1)
	} else {
		f.extensions.Add(1)
	}
	return nil, nil
}

// TestHeartbeatReleasesOnShutdown cobre o L7 (visibilidade): ao cancelar o
// contexto do worker, o goroutine de heartbeat libera a mensagem em voo com
// VisibilityTimeout=0 para reentrega segura (MESSAGING §5).
func TestHeartbeatReleasesOnShutdown(t *testing.T) {
	api := &fakeAPI{}
	c := &Consumer{
		api:    api,
		cfg:    Config{QueueURL: "http://q", VisibilityTimeout: 100 * time.Millisecond},
		logger: slog.Default(),
	}

	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	proc, cancelProc := context.WithCancel(parent)
	defer cancelProc()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.heartbeat(proc, parent, "receipt")
	}()
	time.Sleep(50 * time.Millisecond) // deixa o heartbeat iniciar
	cancelParent()                    // shutdown
	<-done

	if api.zeroCalls.Load() != 1 {
		t.Errorf("liberação de visibilidade = %d, want 1", api.zeroCalls.Load())
	}
}

// TestHeartbeatStopsWhenProcessingDone garante que o fim normal do
// processamento (ctx do handle cancelado) NÃO dispara release: retry/delete já
// decidiram o destino, e liberar aqui reentregaria em duplicidade.
func TestHeartbeatStopsWhenProcessingDone(t *testing.T) {
	api := &fakeAPI{}
	c := &Consumer{
		api:    api,
		cfg:    Config{QueueURL: "http://q", VisibilityTimeout: 100 * time.Millisecond},
		logger: slog.Default(),
	}

	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	proc, cancelProc := context.WithCancel(parent)
	cancelProc() // processamento já concluído
	c.heartbeat(proc, parent, "receipt")

	if api.zeroCalls.Load() != 0 {
		t.Errorf("release no fim normal = %d, want 0", api.zeroCalls.Load())
	}
}

// TestHeartbeatExtendsVisibility valida a extensão periódica: com a mensagem
// ainda em processamento, o timeout de visibilidade é renovado em
// VisibilityTimeout/2 (mínimo 1s).
func TestHeartbeatExtendsVisibility(t *testing.T) {
	api := &fakeAPI{}
	c := &Consumer{
		api:    api,
		cfg:    Config{QueueURL: "http://q", VisibilityTimeout: 3 * time.Second},
		logger: slog.Default(),
	}

	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	proc, cancelProc := context.WithCancel(parent)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.heartbeat(proc, parent, "receipt")
	}()
	time.Sleep(2 * time.Second) // ticker de 1.5s dispara ao menos uma vez
	cancelProc()
	<-done

	if api.extensions.Load() < 1 {
		t.Errorf("extensões = %d, want >= 1", api.extensions.Load())
	}
	if api.zeroCalls.Load() != 0 {
		t.Errorf("release inesperado = %d, want 0", api.zeroCalls.Load())
	}
}

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

// TestIsRetryableClassifiesTransientFailures cobre o M5: conflito de versão
// otimista e duplicata de índice em corrida são transitórios (o commit do
// concorrente torna a reentrega um replay) e NÃO podem ir direto à DLQ.
func TestIsRetryableClassifiesTransientFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"lock otimista", postgres.ErrOptimisticLock, true},
		{"duplicata", postgres.ErrDuplicate, true},
		{"duplicata com constraint", fmt.Errorf("%w: wallets_unique", postgres.ErrDuplicate), true},
		{"serialização", postgres.ErrSerialization, true},
		{"deadlock", postgres.ErrDeadlock, true},
		{"inesperado", postgres.ErrUnexpected, true},
		{"não encontrado", postgres.ErrNotFound, true},
		{"erro de negócio", errors.New("PERMANENT_FAILURE: outra coisa"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Errorf("isRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
