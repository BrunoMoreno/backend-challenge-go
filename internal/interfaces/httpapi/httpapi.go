// Package httpapi expõe o servidor HTTP e as rotas públicas.
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

// NewServer constrói o http.Server com o mux de rotas.
func NewServer(cfg config.Config, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", handleLive(logger))
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func handleLive(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			logger.Warn("health/live: falha ao escrever resposta", "error", err)
		}
	}
}
