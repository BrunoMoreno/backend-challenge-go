package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

func TestHealthLive(t *testing.T) {
	cfg := config.Config{HTTPAddr: ":0"}
	srv := NewServer(cfg, discardLogger())
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if body, _ := io.ReadAll(rec.Result().Body); string(body) != "OK" {
		t.Errorf("body = %q, want OK", body)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
