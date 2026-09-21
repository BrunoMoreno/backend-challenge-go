// Package config carrega e valida a configuração da aplicação a partir do ambiente.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config é a configuração da aplicação, lida uma única vez na inicialização.
type Config struct {
	// AppRoles é a lista de papéis executados por esta instância
	// (http, sqs-consumer, outbox-publisher, reference-worker).
	AppRoles []string
	// HTTPAddr é o endereço de escuta do servidor HTTP.
	HTTPAddr string
	// DatabaseURL é o DSN PostgreSQL da aplicação.
	DatabaseURL string
	// SQSEndpoint é o endpoint do broker SQS (LocalStack em dev).
	SQSEndpoint string
	// SQSRegion é a região AWS usada pelo cliente SQS.
	SQSRegion string
	// KeycloakIssuer é o issuer OIDC do Keycloak.
	KeycloakIssuer string
	// KeycloakJWKSURL é a URL do JWKS usado para validar a assinatura dos tokens.
	KeycloakJWKSURL string
	// KeycloakAudience é o `aud` esperado nos tokens de negócio (clientId do resource server).
	KeycloakAudience string
	// LogLevel é o nível do logger: debug, info, warn ou error.
	LogLevel string
	// OutboxEventsQueueURL é a fila FIFO de destino dos eventos da outbox.
	OutboxEventsQueueURL string
	// OutboxBatchSize é o limite de eventos por ciclo de publicação.
	OutboxBatchSize int
	// OutboxPollInterval é o intervalo entre ciclos de publicação.
	OutboxPollInterval time.Duration
	// OutboxLease é o tempo de posse de um evento reclamado (SKIP LOCKED);
	// expirado, outra instância assume a publicação.
	OutboxLease time.Duration
	// OutboxSendTimeout é o prazo de cada envio ao SQS.
	OutboxSendTimeout time.Duration
	// OutboxBackoffBase é o atraso inicial do backoff de publicação.
	OutboxBackoffBase time.Duration
	// OutboxBackoffMax é o teto do backoff de publicação.
	OutboxBackoffMax time.Duration
	// SQSConsumerQueueURL é a fila FIFO de entrada de operações.
	SQSConsumerQueueURL string
	// SQSConsumerDLQURL é a fila FIFO de mensagens mortas (permanentes).
	SQSConsumerDLQURL string
	// SQSMaxReceiveCount é o teto de entregas antes do redrive automático
	// para a DLQ (política da fila; o consumidor respeita o mesmo teto).
	SQSMaxReceiveCount int
	// SQSVisibilityTimeout é o prazo em que a mensagem fica invisível enquanto
	// processada; o consumidor estende via heartbeat.
	SQSVisibilityTimeout time.Duration
	// SQSConsumerConcurrency é o número de workers que recebem em paralelo.
	SQSConsumerConcurrency int
	// SQSConsumerBackoffBase é o atraso inicial do backoff de retry.
	SQSConsumerBackoffBase time.Duration
	// SQSConsumerBackoffMax é o teto do backoff de retry.
	SQSConsumerBackoffMax time.Duration
	// ReferenceWorkerBatchSize é o limite de pendências por ciclo do worker
	// de referências.
	ReferenceWorkerBatchSize int
	// ReferenceWorkerPollInterval é o intervalo entre ciclos do worker.
	ReferenceWorkerPollInterval time.Duration
	// ReferenceWorkerMaxAttempts é o limite de tentativas de resolução antes
	// da rejeição final REFERENCE_NOT_FOUND.
	ReferenceWorkerMaxAttempts int
	// ReferenceWorkerTTL é a idade máxima de uma pendência (desde a criação)
	// antes da rejeição final REFERENCE_NOT_FOUND.
	ReferenceWorkerTTL time.Duration
	// ReferenceWorkerBackoffBase é o atraso inicial do backoff de retry.
	ReferenceWorkerBackoffBase time.Duration
	// ReferenceWorkerBackoffMax é o teto do backoff de retry.
	ReferenceWorkerBackoffMax time.Duration
	// MetricsAddr é o endereço de escuta do endpoint /metrics (Prometheus),
	// independente do servidor de negócio para servir qualquer papel.
	MetricsAddr string
	// ShutdownTimeout é o prazo total do desligamento ordenado (parada dos
	// workers, drenagem SQS e fechamento das conexões).
	ShutdownTimeout time.Duration
}

// Load lê as variáveis de ambiente e devolve a configuração.
func Load() (Config, error) {
	cfg := Config{
		AppRoles:                    splitCSV(os.Getenv("APP_ROLES")),
		HTTPAddr:                    envOr("APP_HTTP_ADDR", ":8080"),
		DatabaseURL:                 os.Getenv("APP_DATABASE_URL"),
		SQSEndpoint:                 envOr("APP_SQS_ENDPOINT", "http://localhost:4566"),
		SQSRegion:                   envOr("APP_SQS_REGION", "us-east-1"),
		KeycloakIssuer:              os.Getenv("APP_KEYCLOAK_ISSUER"),
		KeycloakJWKSURL:             os.Getenv("APP_KEYCLOAK_JWKS_URL"),
		KeycloakAudience:            os.Getenv("APP_KEYCLOAK_CLIENT_ID"),
		LogLevel:                    envOr("APP_LOG_LEVEL", "info"),
		OutboxEventsQueueURL:        os.Getenv("APP_SQS_EVENTS_QUEUE_URL"),
		OutboxBatchSize:             envOrInt("APP_OUTBOX_BATCH_SIZE", 10),
		OutboxPollInterval:          envOrDur("APP_OUTBOX_POLL_INTERVAL", time.Second),
		OutboxLease:                 envOrDur("APP_OUTBOX_LEASE", 30*time.Second),
		OutboxSendTimeout:           envOrDur("APP_OUTBOX_SEND_TIMEOUT", 10*time.Second),
		OutboxBackoffBase:           envOrDur("APP_OUTBOX_BACKOFF_BASE", time.Second),
		OutboxBackoffMax:            envOrDur("APP_OUTBOX_BACKOFF_MAX", 15*time.Minute),
		SQSConsumerQueueURL:         os.Getenv("APP_SQS_QUEUE_URL"),
		SQSConsumerDLQURL:           os.Getenv("APP_SQS_DLQ_URL"),
		SQSMaxReceiveCount:          envOrInt("APP_SQS_MAX_RECEIVE_COUNT", 5),
		SQSVisibilityTimeout:        envOrDur("APP_SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		SQSConsumerConcurrency:      envOrInt("APP_SQS_CONSUMER_CONCURRENCY", 4),
		SQSConsumerBackoffBase:      envOrDur("APP_SQS_CONSUMER_BACKOFF_BASE", time.Second),
		SQSConsumerBackoffMax:       envOrDur("APP_SQS_CONSUMER_BACKOFF_MAX", 15*time.Minute),
		ReferenceWorkerBatchSize:    envOrInt("APP_REFERENCE_WORKER_BATCH_SIZE", 10),
		ReferenceWorkerPollInterval: envOrDur("APP_REFERENCE_WORKER_POLL_INTERVAL", time.Second),
		ReferenceWorkerMaxAttempts:  envOrInt("APP_REFERENCE_WORKER_MAX_ATTEMPTS", 30),
		ReferenceWorkerTTL:          envOrDur("APP_REFERENCE_WORKER_TTL", 24*time.Hour),
		ReferenceWorkerBackoffBase:  envOrDur("APP_REFERENCE_WORKER_BACKOFF_BASE", time.Second),
		ReferenceWorkerBackoffMax:   envOrDur("APP_REFERENCE_WORKER_BACKOFF_MAX", 15*time.Minute),
		MetricsAddr:                 envOr("APP_METRICS_ADDR", ":9090"),
		ShutdownTimeout:             envOrDur("APP_SHUTDOWN_TIMEOUT", 30*time.Second),
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if len(c.AppRoles) == 0 {
		return fmt.Errorf("config: APP_ROLES vazio")
	}
	for _, role := range c.AppRoles {
		switch role {
		case "http", "sqs-consumer", "outbox-publisher", "reference-worker":
		default:
			return fmt.Errorf("config: APP_ROLES desconhecido %q", role)
		}
	}
	if c.OutboxBatchSize < 1 {
		return fmt.Errorf("config: APP_OUTBOX_BATCH_SIZE deve ser >= 1")
	}
	if c.OutboxPollInterval <= 0 || c.OutboxLease <= 0 || c.OutboxSendTimeout <= 0 {
		return fmt.Errorf("config: intervalos do outbox devem ser positivos")
	}
	if c.ReferenceWorkerBatchSize < 1 {
		return fmt.Errorf("config: APP_REFERENCE_WORKER_BATCH_SIZE deve ser >= 1")
	}
	if c.ReferenceWorkerPollInterval <= 0 || c.ReferenceWorkerTTL <= 0 ||
		c.ReferenceWorkerBackoffBase <= 0 || c.ReferenceWorkerBackoffMax <= 0 {
		return fmt.Errorf("config: intervalos do worker de referências devem ser positivos")
	}
	if c.ReferenceWorkerMaxAttempts < 1 {
		return fmt.Errorf("config: APP_REFERENCE_WORKER_MAX_ATTEMPTS deve ser >= 1")
	}
	return nil
}

// RolesHTTP informa se a instância expõe o servidor HTTP.
func (c Config) RolesHTTP() bool { return contains(c.AppRoles, "http") }

// RolesOutboxPublisher informa se a instância publica os eventos da outbox.
func (c Config) RolesOutboxPublisher() bool { return contains(c.AppRoles, "outbox-publisher") }

// RolesSQSConsumer informa se a instância consome a fila de entrada de operações.
func (c Config) RolesSQSConsumer() bool { return contains(c.AppRoles, "sqs-consumer") }

// RolesReferenceWorker informa se a instância roda o worker de resolução de
// referências pendentes.
func (c Config) RolesReferenceWorker() bool { return contains(c.AppRoles, "reference-worker") }

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envOrDur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
