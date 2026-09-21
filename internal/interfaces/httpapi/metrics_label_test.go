package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
)

// TestMetricsRouteLabel verifica que o label de rota usa o padrão combinado
// pelo ServeMux (r.Pattern) e não "unmatched".
func TestMetricsRouteLabel(t *testing.T) {
	m := metrics.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServerWithDeps(config.Config{HTTPAddr: ":0"}, logger, Deps{Ready: func(context.Context) error { return nil }, Metrics: m})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health/live = %d", rec.Code)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `route="GET /health/live"`) {
		t.Errorf("métrica com rota combinada ausente:\n%s", body)
	}
}

func scrapeMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}
