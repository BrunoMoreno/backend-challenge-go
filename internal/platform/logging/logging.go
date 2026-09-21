// Package logging configura o logger slog em formato JSON e anexa
// correlação por requisição/mensagem nos registros.
package logging

import (
	"context"
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

// WithCorrelationHandler embrulha o handler para anexar o atributo
// correlationId (extraído do contexto) a cada registro. Chamadas com o
// contexto carregado — via WithCorrelation — propagam o id sem depender de o
// chamador anotar cada log.
func WithCorrelationHandler(next slog.Handler) slog.Handler {
	return &correlationHandler{next: next}
}

type correlationHandler struct {
	next slog.Handler
}

func (h *correlationHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *correlationHandler) Handle(ctx context.Context, r slog.Record) error {
	if id, ok := correlationFrom(ctx); ok {
		if !recordHasAttr(r, "correlationId") {
			r.AddAttrs(slog.String("correlationId", id))
		}
	}
	return h.next.Handle(ctx, r)
}

func (h *correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &correlationHandler{next: h.next.WithAttrs(attrs)}
}

func (h *correlationHandler) WithGroup(name string) slog.Handler {
	return &correlationHandler{next: h.next.WithGroup(name)}
}

func recordHasAttr(r slog.Record, want string) bool {
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == want {
			found = true
			return false
		}
		return true
	})
	return found
}

type correlationKey struct{}

// WithCorrelation carrega um identificador de correlação no contexto.
func WithCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func correlationFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationKey{}).(string)
	return id, ok
}
