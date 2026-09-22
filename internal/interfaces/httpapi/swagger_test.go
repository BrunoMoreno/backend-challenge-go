package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

func TestOpenAPIAndSwaggerUIArePublic(t *testing.T) {
	srv := NewServer(config.Config{HTTPAddr: ":0"}, discardLogger())

	t.Run("specification", func(t *testing.T) {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/yaml") {
			t.Fatalf("Content-Type = %q, want application/yaml", got)
		}
		if !strings.Contains(rec.Body.String(), "openapi: 3.1.0") {
			t.Fatal("especificação OpenAPI ausente ou inválida")
		}
	})

	t.Run("ui", func(t *testing.T) {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/swagger/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "SwaggerUIBundle") || !strings.Contains(rec.Body.String(), "/openapi.yaml") {
			t.Fatal("página Swagger UI não aponta para a especificação")
		}
		const nonceMarker = "'nonce-"
		csp := rec.Header().Get("Content-Security-Policy")
		nonceStart := strings.Index(csp, nonceMarker)
		if nonceStart < 0 {
			t.Fatal("CSP não contém nonce para o script de inicialização")
		}
		nonceStart += len(nonceMarker)
		nonceEnd := strings.Index(csp[nonceStart:], "'")
		if nonceEnd < 0 || !strings.Contains(rec.Body.String(), `nonce="`+csp[nonceStart:nonceStart+nonceEnd]+`"`) {
			t.Fatal("nonce da CSP não está aplicado ao script de inicialização")
		}
	})

	t.Run("canonical slash", func(t *testing.T) {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/swagger", nil))
		if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/swagger/" {
			t.Fatalf("redirect = %d %q, want 301 /swagger/", rec.Code, rec.Header().Get("Location"))
		}
	})
}
