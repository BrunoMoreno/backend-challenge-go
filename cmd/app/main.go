// Command app é o ponto de entrada do serviço de processamento de apostas,
// composto com Uber Fx.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.uber.org/fx"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/interfaces/httpapi"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	app := fx.New(
		fx.Provide(
			config.Load,
			func(cfg config.Config) *slog.Logger {
				return logging.New(logging.ParseLevel(cfg.LogLevel))
			},
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
			provideHTTPDeps,
			httpapi.NewServerWithDeps,
		),
		fx.Invoke(serveHTTP),
	)

	app.Run()
}

// providePool abre o pool e o fecha no shutdown ordenado (M8).
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
func provideVerifier(cfg config.Config) (httpapi.Verifier, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpapi.NewJWKSVerifier(ctx, cfg.KeycloakIssuer, cfg.KeycloakJWKSURL, cfg.KeycloakAudience)
}

func provideHTTPDeps(v httpapi.Verifier, w *openwallet.Service, pw *processwager.Service, q *query.Service) httpapi.Deps {
	return httpapi.Deps{Verifier: v, Wallets: w, Wagers: pw, Queries: q}
}

func serveHTTP(
	lc fx.Lifecycle,
	cfg config.Config,
	logger *slog.Logger,
	srv *http.Server,
) {
	if !cfg.RolesHTTP() {
		return
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("http: servidor encerrou com erro", "addr", cfg.HTTPAddr, "error", err)
				}
			}()
			logger.Info("http: servidor iniciado", "addr", cfg.HTTPAddr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("http: desligando servidor")
			return srv.Shutdown(ctx)
		},
	})
}
