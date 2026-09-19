package logging

import (
	"log/slog"
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
