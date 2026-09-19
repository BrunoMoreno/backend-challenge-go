// Command app é o ponto de entrada do serviço de processamento de apostas,
// composto com Uber Fx.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"go.uber.org/fx"

	"github.com/BrunoMoreno/backend-challenge-go/internal/interfaces/httpapi"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/logging"
)

func main() {
	app := fx.New(
		fx.Provide(
			config.Load,
			func(cfg config.Config) *slog.Logger {
				return logging.New(logging.ParseLevel(cfg.LogLevel))
			},
			httpapi.NewServer,
		),
		fx.Invoke(serveHTTP),
	)

	app.Run()
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
