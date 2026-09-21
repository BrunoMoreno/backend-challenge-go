//go:build integration

// M8.4 — composição Fx da aplicação completa: validação do grafo e ciclo
// start/stop com recursos liberados (portas HTTP e métricas devolvidas,
// conexões e workers encerrados).
package integration

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/bootstrap"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"go.uber.org/fx"
)

const (
	bootstrapHTTPPort   = ":18080"
	bootstrapMetrics    = ":19090"
	bootstrapHTTPURL    = "http://localhost:18080"
	bootstrapMetricsURL = "http://localhost:19090"
)

// bootstrapTestConfig monta a configuração para o módulo Fx apontando para a
// infraestrutura local do Compose. As portas de teste não são usadas por nada.
func bootstrapTestConfig() config.Config {
	return config.Config{
		AppRoles:               []string{"http", "sqs-consumer", "outbox-publisher"},
		HTTPAddr:               bootstrapHTTPPort,
		DatabaseURL:            testURL(),
		SQSEndpoint:            sqsEndpoint,
		SQSRegion:              "us-east-1",
		KeycloakIssuer:         "http://localhost:8081/realms/wagering",
		KeycloakJWKSURL:        "http://localhost:8081/realms/wagering/protocol/openid-connect/certs",
		KeycloakAudience:       "wager-api",
		LogLevel:               "info",
		OutboxEventsQueueURL:   eventsQueueURL("fx"),
		OutboxBatchSize:        10,
		OutboxPollInterval:     200 * time.Millisecond,
		OutboxLease:            30 * time.Second,
		OutboxSendTimeout:      5 * time.Second,
		OutboxBackoffBase:      100 * time.Millisecond,
		OutboxBackoffMax:       10 * time.Second,
		SQSConsumerQueueURL:    inputQueueURL("fx"),
		SQSConsumerDLQURL:      dlqURL("fx"),
		SQSMaxReceiveCount:     5,
		SQSVisibilityTimeout:   5 * time.Second,
		SQSConsumerConcurrency: 2,
		SQSConsumerBackoffBase: time.Second,
		SQSConsumerBackoffMax:  time.Minute,
		MetricsAddr:            bootstrapMetrics,
		ShutdownTimeout:        10 * time.Second,
	}
}

// TestBootstrapValidateApp valida o grafo Fx completo sem iniciar os workers.
func TestBootstrapValidateApp(t *testing.T) {
	os.Setenv("APP_ROLES", "http,sqs-consumer,outbox-publisher")
	if err := fx.ValidateApp(bootstrap.Module(), fx.Replace(bootstrapTestConfig()), fx.NopLogger); err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

// TestBootstrapStartStop com o módulo real, o servidor HTTP e o de métricas
// respondem; no Stop os recursos são liberados (portas devolvidas e um novo
// app no mesmo endereço sobe) e os workers param dentro do prazo.
func TestBootstrapStartStop(t *testing.T) {
	os.Setenv("APP_ROLES", "http,sqs-consumer,outbox-publisher")
	cfg := bootstrapTestConfig()
	newApp := func() *fx.App {
		return fx.New(bootstrap.Module(), fx.Replace(cfg), fx.NopLogger)
	}

	ctx := context.Background()
	app := newApp()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	if code := httpGetCode(t, bootstrapHTTPURL+"/health/live"); code != http.StatusOK {
		t.Fatalf("health/live = %d, want 200", code)
	}
	if code := httpGetCode(t, bootstrapMetricsURL+"/metrics"); code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", code)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	expectPortFree(t, bootstrapHTTPPort)
	expectPortFree(t, bootstrapMetrics)

	// Novo app no mesmo endereço: as portas foram realmente liberadas.
	app2 := newApp()
	if err := app2.Start(ctx); err != nil {
		t.Fatalf("segundo start: %v", err)
	}
	if code := httpGetCode(t, bootstrapHTTPURL+"/health/live"); code != http.StatusOK {
		t.Fatalf("health/live (2º app) = %d, want 200", code)
	}
	if err := app2.Stop(ctx); err != nil {
		t.Fatalf("segundo stop: %v", err)
	}
}

func httpGetCode(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func expectPortFree(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return // recusou: porta livre
	}
	t.Fatalf("porta %s ainda aceita conexões após o shutdown", addr)
}
