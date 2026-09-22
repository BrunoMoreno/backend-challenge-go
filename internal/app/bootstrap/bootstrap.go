// Package bootstrap monta o grafo Fx da aplicação (M8.1): configuração,
// conexões, serviços e os invokes por papel. O mesmo Module() é usado pelo
// cmd/app e pelos testes de composição (M8.4).
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/fx"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/outboxpublisher"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/referenceworker"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/sqs"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/sqsconsumer"
	"github.com/BrunoMoreno/backend-challenge-go/internal/interfaces/httpapi"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/logging"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Module é o grafo completo da aplicação. O prazo de shutdown (M8.2) é
// aplicado pelo cmd (fx.StopTimeout a partir da configuração) e vale para
// todos os OnStop (parada em ordem reversa do start: workers SQS/outbox →
// HTTP → conexões).
func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			config.Load,
			provideLogger,
			providePool,
			postgres.NewUnitOfWorkFactory,
			postgres.NewDatabase,
			// As interfaces das portas precisam ser expostas em tipos
			// concretos para o Fx injetar nos construtores dos casos de uso.
			func(d *postgres.Database) storage.Database { return d },
			openwallet.NewService,
			processwager.NewService,
			query.NewService,
			provideVerifier,
			provideReady,
			provideDraining,
			provideHTTPDeps,
			httpapi.NewServerWithDeps,
			metrics.New,
		),
		fx.Invoke(serveHTTP, runMetricsServer, runOutboxPublisher, runSQSConsumer, runReferenceWorker),
	)
}

// provideLogger cria o logger JSON com o nível configurado.
func provideLogger(cfg config.Config) *slog.Logger {
	return logging.New(logging.ParseLevel(cfg.LogLevel))
}

// providePool abre o pool e o fecha no shutdown ordenado (M8.2).
func providePool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { pool.Close(); return nil },
	})
	return pool, nil
}

// provideVerifier valida as claims dos tokens contra o Keycloak configurado.
// Usa a construção lazily (primeira verificação carrega o JWKS) para o gráfo
// montar sem depender de o Keycloak estar no ar na inicialização.
func provideVerifier(cfg config.Config) (httpapi.Verifier, error) {
	return httpapi.NewLazyJWKSVerifier(cfg.KeycloakIssuer, cfg.KeycloakJWKSURL, cfg.KeycloakAudience)
}

// provideReady é o probe de prontidão: ping no PostgreSQL e verificação das
// filas de cada papel SQS ativo (inbox p/ sqs-consumer, eventos p/
// outbox-publisher). Um papel montado sem a fila correspondente (ou com o
// broker fora) responde /ready 503 — antes o outbox-publisher podia ficar
// "verde" com o envio morto (M2).
func provideReady(cfg config.Config, pool *pgxpool.Pool) func(context.Context) error {
	base := func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		if cfg.RolesSQSConsumer() {
			if err := sqsQueueReachable(ctx, cfg.SQSEndpoint, cfg.SQSRegion, cfg.SQSConsumerQueueURL); err != nil {
				return err
			}
		}
		if cfg.RolesOutboxPublisher() {
			if err := sqsQueueReachable(ctx, cfg.SQSEndpoint, cfg.SQSRegion, cfg.OutboxEventsQueueURL); err != nil {
				return err
			}
		}
		return nil
	}
	return base
}

// sqsQueueReachable confirma que a fila existe no broker configurado.
func sqsQueueReachable(ctx context.Context, endpoint, region, queueURL string) error {
	client, err := sqs.NewClient(ctx, endpoint, region)
	if err != nil {
		return err
	}
	_, err = client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(queueNameFromURL(queueURL))})
	return err
}

// provideDraining é o flag de desligamento ordenado: setado no início do OnStop
// do servidor HTTP e lido por /health/ready (503) para o LB parar de rotear
// antes do listener fechar (M10).
func provideDraining() *atomic.Bool {
	return new(atomic.Bool)
}

// queueNameFromURL extrai o nome da fila da URL do LocalStack/AWS.
func queueNameFromURL(url string) string {
	if i := strings.LastIndex(url, "/"); i >= 0 && i+1 < len(url) {
		return url[i+1:]
	}
	return url
}

// provideHTTPDeps constrói as dependências das rotas de negócio.
func provideHTTPDeps(v httpapi.Verifier, w *openwallet.Service, pw *processwager.Service,
	q *query.Service, ready func(context.Context) error, draining *atomic.Bool,
	m *metrics.Metrics) httpapi.Deps {
	return httpapi.Deps{Verifier: v, Wallets: w, Wagers: pw, Queries: q,
		Ready: ready, Draining: draining, Metrics: m}
}

// runOutboxPublisher inicia o worker de publicação da outbox quando o papel
// outbox-publisher está ativo. O envio usa a fila wager-events.fifo; a
// disputa entre instâncias é resolvida pelo lease + SKIP LOCKED no banco.
// Um erro ao montar o sender ABORTA o boot (fx) — antes o papel começava
// silenciosamente morto com /ready verde (M2).
func runOutboxPublisher(
	lc fx.Lifecycle,
	cfg config.Config,
	factory *postgres.UnitOfWorkFactory,
	logger *slog.Logger,
	metricsBox *metrics.Metrics,
) error {
	if !cfg.RolesOutboxPublisher() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sender, err := sqs.NewSender(ctx, cfg.SQSEndpoint, cfg.SQSRegion, cfg.OutboxEventsQueueURL)
	if err != nil {
		return fmt.Errorf("outbox: criar sender SQS: %w", err)
	}

	pub := outboxpublisher.New(factory, sender, logger, outboxpublisher.Config{
		BatchSize:    cfg.OutboxBatchSize,
		Lease:        cfg.OutboxLease,
		SendTimeout:  cfg.OutboxSendTimeout,
		BackoffBase:  cfg.OutboxBackoffBase,
		BackoffMax:   cfg.OutboxBackoffMax,
		PollInterval: cfg.OutboxPollInterval,
		Metrics:      metricsBox,
		OnSendError: func(eventID string, err error) {
			logger.Error("outbox: evento não publicado", "eventId", eventID, "error", err)
		},
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				if err := pub.Run(runCtx); err != nil && runCtx.Err() == nil {
					logger.Error("outbox: publisher encerrou com erro", "error", err)
				}
			}()
			logger.Info("outbox: publisher iniciado",
				"batch", cfg.OutboxBatchSize, "lease", cfg.OutboxLease.String(),
				"queue", cfg.OutboxEventsQueueURL)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("outbox: desligando publisher")
			cancelRun()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return nil
}

// runSQSConsumer inicia o consumidor da fila de entrada quando o papel
// sqs-consumer está ativo. Long polling, concorrência limitada e heartbeat; a
// reentrega transiente usa backoff por ApproximateReceiveCount e falhas
// permanentes vão para a DLQ. No shutdown, processos em voo recebem
// visibilidade 0 (MESSAGING §5).
func runSQSConsumer(
	lc fx.Lifecycle,
	cfg config.Config,
	service *processwager.Service,
	factory *postgres.UnitOfWorkFactory,
	logger *slog.Logger,
	metricsBox *metrics.Metrics,
) error {
	if !cfg.RolesSQSConsumer() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := sqs.NewClient(ctx, cfg.SQSEndpoint, cfg.SQSRegion)
	if err != nil {
		return fmt.Errorf("sqs: criar cliente: %w", err)
	}

	consumer := sqsconsumer.New(client, service, factory, logger, sqsconsumer.Config{
		QueueURL:          cfg.SQSConsumerQueueURL,
		DLQURL:            cfg.SQSConsumerDLQURL,
		VisibilityTimeout: cfg.SQSVisibilityTimeout,
		WaitTime:          20 * time.Second,
		Concurrency:       cfg.SQSConsumerConcurrency,
		MaxReceiveCount:   cfg.SQSMaxReceiveCount,
		BackoffBase:       cfg.SQSConsumerBackoffBase,
		BackoffMax:        cfg.SQSConsumerBackoffMax,
		Metrics:           metricsBox,
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				if err := consumer.Run(runCtx); err != nil && runCtx.Err() == nil {
					logger.Error("sqs: consumidor encerrou com erro", "error", err)
				}
			}()
			logger.Info("sqs: consumidor iniciado",
				"queue", cfg.SQSConsumerQueueURL, "dlq", cfg.SQSConsumerDLQURL,
				"concurrency", cfg.SQSConsumerConcurrency, "maxReceiveCount", cfg.SQSMaxReceiveCount)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("sqs: desligando consumidor (drenagem)")
			cancelRun()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return nil
}

// runReferenceWorker inicia o worker de resolução tardia de referências quando
// o papel reference-worker está ativo (RF-05, M6): reclama pendências com SKIP
// LOCKED no banco, re-resolve o alvo e agenda retry com backoff até TTL/limite
// de tentativas. Todo o trabalho é transacional; a parada (SIGTERM) aguarda o
// ciclo corrente terminar.
func runReferenceWorker(
	lc fx.Lifecycle,
	cfg config.Config,
	factory *postgres.UnitOfWorkFactory,
	service *processwager.Service,
	logger *slog.Logger,
	metricsBox *metrics.Metrics,
) {
	if !cfg.RolesReferenceWorker() {
		return
	}
	worker := referenceworker.New(factory, service, logger, referenceworker.Config{
		BatchSize:    cfg.ReferenceWorkerBatchSize,
		PollInterval: cfg.ReferenceWorkerPollInterval,
		MaxAttempts:  cfg.ReferenceWorkerMaxAttempts,
		TTL:          cfg.ReferenceWorkerTTL,
		BackoffBase:  cfg.ReferenceWorkerBackoffBase,
		BackoffMax:   cfg.ReferenceWorkerBackoffMax,
		Metrics:      metricsBox,
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				if err := worker.Run(runCtx); err != nil && runCtx.Err() == nil {
					logger.Error("reference: worker encerrou com erro", "error", err)
				}
			}()
			logger.Info("reference: worker iniciado",
				"batch", cfg.ReferenceWorkerBatchSize, "maxAttempts", cfg.ReferenceWorkerMaxAttempts,
				"ttl", cfg.ReferenceWorkerTTL.String(), "poll", cfg.ReferenceWorkerPollInterval.String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("reference: desligando worker")
			cancelRun()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
}

// serveHTTP inicia o servidor de negócio (papel http) com shutdown ordenado.
func serveHTTP(
	lc fx.Lifecycle,
	cfg config.Config,
	logger *slog.Logger,
	srv *http.Server,
	draining *atomic.Bool,
) {
	if !cfg.RolesHTTP() {
		return
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Liga o listener ANTES de retornar: "servidor iniciado" e o fim do
			// OnStart significam armadilhado de verdade (readiness real, RF-11).
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return fmt.Errorf("http: escutar %s: %w", srv.Addr, err)
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("http: servidor encerrou com erro", "addr", cfg.HTTPAddr, "error", err)
				}
			}()
			logger.Info("http: servidor iniciado", "addr", cfg.HTTPAddr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// M10: marca o draining ANTES de fechar o listener — o LB para de
			// rotear (ready 503) enquanto o drenagem encerra os processamentos.
			logger.Info("http: desligando servidor (draining)")
			draining.Store(true)
			err := srv.Shutdown(ctx)
			draining.Store(false)
			return err
		},
	})
}

// runMetricsServer expõe o /metrics Prometheus em porta própria, para qualquer
// papel da instância ser observável (M8.3).
func runMetricsServer(
	lc fx.Lifecycle,
	cfg config.Config,
	logger *slog.Logger,
	m *metrics.Metrics,
) {
	srv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("metrics: servidor encerrou com erro", "addr", cfg.MetricsAddr, "error", err)
				}
			}()
			logger.Info("metrics: servidor iniciado", "addr", cfg.MetricsAddr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("metrics: desligando servidor")
			return srv.Shutdown(ctx)
		},
	})
}
