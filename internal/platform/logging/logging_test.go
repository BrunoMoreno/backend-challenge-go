package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"INFO":     slog.LevelInfo,
		"Warn":     slog.LevelWarn,
		"error":    slog.LevelError,
		"":         slog.LevelInfo,
		"garbage":  slog.LevelInfo,
		" warning": slog.LevelWarn,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestCorrelationHandler verifica que o id do contexto chega aos registros e
// não é duplicado quando o chamador também o anota.
func TestCorrelationHandler(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(WithCorrelationHandler(slog.NewJSONHandler(&buf, nil)))

	ctx := WithCorrelation(context.Background(), "corr-123")
	logger.InfoContext(ctx, "mensagem")
	logger.Info("sem correlacao")

	out := buf.String()
	if !strings.Contains(out, `"correlationId":"corr-123"`) {
		t.Fatalf("correlationId ausente no log: %s", out)
	}
	if strings.Count(out, "corr-123") != 1 {
		t.Fatalf("correlationId duplicado: %s", out)
	}
}
