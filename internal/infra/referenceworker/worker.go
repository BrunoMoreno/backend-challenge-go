// Package referenceworker implementa o worker de resolução tardia de
// referências (RF-05, M6.1): reclama linhas PENDING/PENDING_REFERENCE com
// SKIP LOCKED, re-resolve o alvo referenciado com o caso de uso compartilhado
// e agenda retry com backoff exponencial até o TTL/limite de tentativas
// (rejeição final REFERENCE_NOT_FOUND). Todo o trabalho é transacional: o
// lock da linha é o lease; commit encerra a posse.
package referenceworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/backoff"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
)

// Config parametriza um ciclo do worker.
type Config struct {
	BatchSize    int
	PollInterval time.Duration
	MaxAttempts  int
	TTL          time.Duration // desde created_at; expirado → REFERENCE_NOT_FOUND
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	Metrics      *metrics.Metrics // contadores de desfecho; nil desativa
}

// Worker resolve as referências pendentes no banco real.
type Worker struct {
	factory *postgres.UnitOfWorkFactory
	service *processwager.Service
	logger  *slog.Logger
	cfg     Config
	now     func() time.Time
	jitter  func(max time.Duration) time.Duration
}

// New cria o worker de referências.
func New(factory *postgres.UnitOfWorkFactory, service *processwager.Service, logger *slog.Logger, cfg Config) *Worker {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second // default alinhado à config (L2)
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 30
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 24 * time.Hour
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 15 * time.Minute
	}
	return &Worker{
		factory: factory,
		service: service,
		logger:  logger,
		cfg:     cfg,
		now:     time.Now,
		jitter:  func(max time.Duration) time.Duration { return time.Duration(rand.Int63n(int64(max) + 1)) },
	}
}

// Run executa o ciclo até o cancelamento do contexto.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := w.ResolveBatch(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Warn("reference: ciclo com erro", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// ResolveBatch reclama até BatchSize linhas pendentes e, em uma única
// transação, resolve/agenda/rejeita cada uma. O commit encerra a posse (o
// lock da linha é o lease); erro em qualquer linha aborta o lote e rola o
// resto — a próxima instância assume o que ficou.
func (w *Worker) ResolveBatch(ctx context.Context) error {
	uow, err := w.factory.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reference: begin: %w", err)
	}
	defer uow.Rollback(ctx)

	pending, err := uow.Wagers().FindPendingDue(ctx, w.now(), w.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("reference: claim: %w", err)
	}
	if len(pending) == 0 {
		return uow.Commit(ctx)
	}

	for _, p := range pending {
		if p.Kind() == wager.KindOpening {
			continue // aberta processa no caminho síncrono; não é alvo do worker
		}
		if err := w.resolveOne(ctx, uow, p); err != nil {
			return err
		}
	}
	return uow.Commit(ctx)
}

func (w *Worker) resolveOne(ctx context.Context, uow storage.UnitOfWork, p wager.WagerTransaction) error {
	now := w.now()

	// TTL/limite de tentativas → rejeição final REFERENCE_NOT_FOUND (RF-05).
	if w.expired(p, now) || p.Attempts() >= w.cfg.MaxAttempts {
		if _, err := w.service.RejectPending(ctx, uow, p, wager.FailureReferenceNotFound); err != nil {
			return fmt.Errorf("reference: rejeitar %s: %w", p.ID(), err)
		}
		w.cfg.Metrics.ReferenceResolutions("expired")
		w.logger.Info("reference: expirada",
			"tx", p.ID(), "attempts", p.Attempts(), "created", p.CreatedAt().UTC())
		return nil
	}

	res, err := w.service.ResolvePending(ctx, uow, p)
	switch {
	case err == nil && res.State == wager.StateRejected:
		w.cfg.Metrics.ReferenceResolutions("rejected")
		w.logger.Info("reference: rejeitada ao resolver",
			"tx", p.ID(), "failureCode", res.FailureCode)
		return nil
	case err == nil:
		w.cfg.Metrics.ReferenceResolutions("resolved")
		w.logger.Info("reference: resolvida", "tx", p.ID(), "state", res.State)
		return nil
	case errors.Is(err, processwager.ErrPendingReference):
		return w.reschedule(ctx, uow, p, now)
	default:
		w.cfg.Metrics.ReferenceResolutions("failed")
		return fmt.Errorf("reference: resolver %s: %w", p.ID(), err)
	}
}

// reschedule move uma linha PENDING retomada pelo varredor (M6.3) para
// PENDING_REFERENCE e agenda o próximo retry com backoff exponencial.
func (w *Worker) reschedule(ctx context.Context, uow storage.UnitOfWork, p wager.WagerTransaction, now time.Time) error {
	if p.State() == wager.StatePending {
		if err := p.PendForReference(); err != nil {
			return fmt.Errorf("reference: pendurar %s: %w", p.ID(), err)
		}
	}
	next := w.nextAttempt(now, p.Attempts())
	if err := p.NextAttempt(next); err != nil {
		return fmt.Errorf("reference: agendar %s: %w", p.ID(), err)
	}
	if err := uow.Wagers().UpdatePendingReference(ctx, p); err != nil {
		return fmt.Errorf("reference: update %s: %w", p.ID(), err)
	}
	w.cfg.Metrics.ReferenceResolutions("retry")
	w.logger.Info("reference: referência ainda pendente",
		"tx", p.ID(), "attempt", p.Attempts(), "next", p.NextAttemptAt().UTC())
	return nil
}

// expired informa se a linha excedeu o TTL configurado (desde a criação).
func (w *Worker) expired(p wager.WagerTransaction, now time.Time) bool {
	return now.After(p.CreatedAt().Add(w.cfg.TTL))
}

// nextAttempt calcula o próximo agendamento com backoff exponencial (base *
// 2^tentativas) com jitter, limitado ao teto (compartilhado com a outbox).
func (w *Worker) nextAttempt(now time.Time, attempts int) time.Time {
	return backoff.Next(now, attempts, w.cfg.BackoffBase, w.cfg.BackoffMax, w.jitter)
}
