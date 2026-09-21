package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCollectorsEmit(t *testing.T) {
	m := New()
	m.HTTPObserve("GET", "/wallets/{walletId}", 200, 10*time.Millisecond)
	m.HTTPObserve("POST", "/wagering/transactions", 429, 5*time.Millisecond)
	m.SQSMessages("processed")
	m.SQSMessages("dead")
	m.OutboxEvents("published")
	m.ReferenceResolutions("resolved")
	m.ReferenceResolutions("expired")

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		`http_requests_total{method="GET",route="/wallets/{walletId}",status="200"} 1`,
		`http_requests_total{method="POST",route="/wagering/transactions",status="429"} 1`,
		`sqs_messages_total{status="dead"} 1`,
		`outbox_events_total{status="published"} 1`,
		`reference_resolutions_total{status="resolved"} 1`,
		`reference_resolutions_total{status="expired"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("métrica ausente: %s\n---\n%s", want, body)
		}
	}
}

func TestNilSafety(t *testing.T) {
	var m *Metrics
	m.HTTPObserve("GET", "/", 200, time.Millisecond) // não deve entrar em panico
	m.SQSMessages("processed")
	m.OutboxEvents("failed")
	if h := m.Handler(); h == nil {
		t.Fatal("Handler nil")
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 404 {
		t.Fatalf("Handler nil = %d, want 404", rec.Code)
	}
	_ = context.Background
}
