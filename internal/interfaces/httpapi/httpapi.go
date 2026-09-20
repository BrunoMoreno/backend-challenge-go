// Package httpapi expõe o servidor HTTP e as rotas públicas.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

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

// handleReady responde 200 quando o processo está pronto para tráfego e 503
// nomeando o componente que falhou no probe.
func handleReady(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := deps.Ready(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "NOT_READY", err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}
