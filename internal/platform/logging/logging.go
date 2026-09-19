// Package logging configura o logger slog em formato JSON.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// ParseLevel converte texto em slog.Level, com padrão info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New cria um logger slog estruturado em JSON no stderr.
func New(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
