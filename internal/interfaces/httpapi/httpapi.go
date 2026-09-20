// Package httpapi expõe o servidor HTTP e as rotas públicas.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

// NewServer constrói o http.Server sem rotas de negócio (apenas health).
func NewServer(cfg config.Config, logger *slog.Logger) *http.Server {
	return NewServerWithDeps(cfg, logger, Deps{})
}

func handleLive(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			logger.Warn("health/live: falha ao escrever resposta", "error", err)
		}
	}
}
