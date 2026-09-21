// Package outboxpublisher implementa o worker de transacional outbox: reclama
// eventos pendentes sob lease (SKIP LOCKED no banco), publica na fila de
// eventos e confirma a publicação atômica na outbox. Múltiplas instâncias
// podem disputar os mesmos registros com segurança; trabalho abandonado
// (crash entre commit e confirmação) é assumido por outra instância após a
// expiração do lease, republicando com o mesmo eventId.
package outboxpublisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/backoff"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
)

// Sender é a porta de publicação do evento (SQS, fake em testes).
type Sender interface {
	Send(ctx context.Context, env events.Envelope) error
}

// Config parametriza um ciclo de publicação.
type Config struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	SendTimeout  time.Duration
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	OnSendError  func(eventID string, err error) // observabilidade, opcional
	Metrics      *metrics.Metrics                // contadores de desfecho; nil desativa
}

// Publisher publica os registros pendentes da outbox.
type Publisher struct {
	factory *postgres.UnitOfWorkFactory
	sender  Sender
	logger  *slog.Logger
	cfg     Config
	now     func() time.Time
	jitter  func(max time.Duration) time.Duration
}

// New cria o publisher da outbox.
func New(factory *postgres.UnitOfWorkFactory, sender Sender, logger *slog.Logger, cfg Config) *Publisher {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = 10 * time.Second
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 15 * time.Minute
	}
	return &Publisher{
		factory: factory,
		sender:  sender,
		logger:  logger,
		cfg:     cfg,
		now:     time.Now,
		jitter:  func(max time.Duration) time.Duration { return time.Duration(rand.Int63n(int64(max) + 1)) },
	}
}

// Run executa o ciclo até o cancelamento do contexto.
func (p *Publisher) Run(ctx context.Context) error {
	for {
		if err := p.PublishBatch(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.logger.Warn("outbox: ciclo com erro", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(p.cfg.PollInterval):
		}
	}
}

// PublishBatch reclama até BatchSize eventos pendentes, publica cada um e
// confirma/registra falha em transações próprias.
func (p *Publisher) PublishBatch(ctx context.Context) error {
	uow, err := p.factory.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox: begin: %w", err)
	}
	defer uow.Rollback(ctx)

	rows, err := uow.OutboxRepository.ClaimPending(ctx, p.cfg.BatchSize, p.cfg.Lease)
	if err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	if len(rows) == 0 {
		return uow.Commit(ctx)
	}

	envs := make([]events.Envelope, 0, len(rows))
	nexts := make([]time.Time, 0, len(rows))
	for _, row := range rows {
		env, err := uow.OutboxRepository.GetEvent(ctx, row.EventID)
		if err != nil {
			return fmt.Errorf("outbox: get %s: %w", row.EventID, err)
		}
		envs = append(envs, env)
		nexts = append(nexts, p.nextAttempt(p.now(), row.Attempts))
	}
	// Commit do claim: o lease fica persistido e o envio ocorre fora da
	// transação (publicar antes do commit é o que a outbox proíbe).
	if err := uow.Commit(ctx); err != nil {
		return fmt.Errorf("outbox: commit claim: %w", err)
	}

	for i := range envs {
		if err := p.publishOne(ctx, envs[i], nexts[i]); err != nil {
			p.logger.Error("outbox: falha ao finalizar evento", "eventId", envs[i].EventID, "error", err)
			if p.cfg.OnSendError != nil {
				p.cfg.OnSendError(envs[i].EventID, err)
			}
		}
	}
	return nil
}

func (p *Publisher) publishOne(ctx context.Context, env events.Envelope, next time.Time) error {
	sendCtx, cancel := context.WithTimeout(ctx, p.cfg.SendTimeout)
	defer cancel()
	if err := p.sender.Send(sendCtx, env); err != nil {
		p.cfg.Metrics.OutboxEvents("failed")
		p.logger.Warn("outbox: envio falhou", "eventId", env.EventID,
			"attempts", "retry", "error", err)
		return p.mark(ctx, func(uow *postgres.UnitOfWork) error {
			return uow.OutboxRepository.MarkFailed(ctx, env.EventID, next)
		})
	}
	p.cfg.Metrics.OutboxEvents("published")
	crashAfterPublishBeforeMark(ctx, p.logger)
	return p.mark(ctx, func(uow *postgres.UnitOfWork) error {
		return uow.OutboxRepository.MarkPublished(ctx, env.EventID)
	})
}

// mark confirma (publicado/falhou) em uma transação própria. ErrNotFound
// significa que outra instância já finalizou o evento — trata como sucesso.
func (p *Publisher) mark(ctx context.Context, fn func(*postgres.UnitOfWork) error) error {
	uow, err := p.factory.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox: begin mark: %w", err)
	}
	defer uow.Rollback(ctx)

	if err := fn(uow); err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return nil
		}
		return err
	}
	return uow.Commit(ctx)
}

// nextAttempt calcula o próximo envio com backoff exponencial (base * 2^tent)
// com jitter, limitado ao teto (compartilhado com o worker de referências).
func (p *Publisher) nextAttempt(now time.Time, attempts int) time.Time {
	return backoff.Next(now, attempts, p.cfg.BackoffBase, p.cfg.BackoffMax, p.jitter)
}
