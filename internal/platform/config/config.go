// Package config carrega e valida a configuração da aplicação a partir do ambiente.
package config

import (
	"errors"
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
	// KeycloakAudience é o `aud` esperado nos tokens de negócio. É lido da
	// env APP_KEYCLOAK_CLIENT_ID — o clientId do resource server (`wager-api`)
	// que os tokens devem conter em `aud`. A API é um resource server e nunca
	// se autentica no Keycloak; ver docs/AUTHENTICATION.md §2.2.
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

// Load lê as variáveis de ambiente e devolve a configuração. Parse de números
// inválidos é erro (não vira default silencioso) e o necessário por papel é
// exigido — config errônea falha na inicialização, não no meio do tráfego
// (docs/solve/IMPROVEMENTS.md L4).
func Load() (Config, error) {
	var errs []error
	mustInt := func(key string, def int, dst *int) {
		v, err := envOrInt(key, def)
		if err != nil {
			errs = append(errs, err)
			return
		}
		*dst = v
	}
	mustDur := func(key string, def time.Duration, dst *time.Duration) {
		v, err := envOrDur(key, def)
		if err != nil {
			errs = append(errs, err)
			return
		}
		*dst = v
	}

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
		SQSConsumerQueueURL:         os.Getenv("APP_SQS_QUEUE_URL"),
		SQSConsumerDLQURL:           os.Getenv("APP_SQS_DLQ_URL"),
		MetricsAddr:                 envOr("APP_METRICS_ADDR", ":9090"),
		ShutdownTimeout:             30 * time.Second,
		OutboxPollInterval:          time.Second,
		OutboxLease:                 30 * time.Second,
		OutboxSendTimeout:           10 * time.Second,
		OutboxBackoffBase:           time.Second,
		OutboxBackoffMax:            15 * time.Minute,
		SQSVisibilityTimeout:        30 * time.Second,
		SQSConsumerBackoffBase:      time.Second,
		SQSConsumerBackoffMax:       15 * time.Minute,
		ReferenceWorkerPollInterval: time.Second,
		ReferenceWorkerTTL:          24 * time.Hour,
		ReferenceWorkerBackoffBase:  time.Second,
		ReferenceWorkerBackoffMax:   15 * time.Minute,
	}
	mustInt("APP_OUTBOX_BATCH_SIZE", 10, &cfg.OutboxBatchSize)
	mustInt("APP_SQS_MAX_RECEIVE_COUNT", 5, &cfg.SQSMaxReceiveCount)
	mustInt("APP_SQS_CONSUMER_CONCURRENCY", 4, &cfg.SQSConsumerConcurrency)
	mustInt("APP_REFERENCE_WORKER_BATCH_SIZE", 10, &cfg.ReferenceWorkerBatchSize)
	mustInt("APP_REFERENCE_WORKER_MAX_ATTEMPTS", 30, &cfg.ReferenceWorkerMaxAttempts)
	mustDur("APP_SHUTDOWN_TIMEOUT", 30*time.Second, &cfg.ShutdownTimeout)
	mustDur("APP_OUTBOX_POLL_INTERVAL", time.Second, &cfg.OutboxPollInterval)
	mustDur("APP_OUTBOX_LEASE", 30*time.Second, &cfg.OutboxLease)
	mustDur("APP_OUTBOX_SEND_TIMEOUT", 10*time.Second, &cfg.OutboxSendTimeout)
	mustDur("APP_OUTBOX_BACKOFF_BASE", time.Second, &cfg.OutboxBackoffBase)
	mustDur("APP_OUTBOX_BACKOFF_MAX", 15*time.Minute, &cfg.OutboxBackoffMax)
	mustDur("APP_SQS_VISIBILITY_TIMEOUT", 30*time.Second, &cfg.SQSVisibilityTimeout)
	mustDur("APP_SQS_CONSUMER_BACKOFF_BASE", time.Second, &cfg.SQSConsumerBackoffBase)
	mustDur("APP_SQS_CONSUMER_BACKOFF_MAX", 15*time.Minute, &cfg.SQSConsumerBackoffMax)
	mustDur("APP_REFERENCE_WORKER_POLL_INTERVAL", time.Second, &cfg.ReferenceWorkerPollInterval)
	mustDur("APP_REFERENCE_WORKER_TTL", 24*time.Hour, &cfg.ReferenceWorkerTTL)
	mustDur("APP_REFERENCE_WORKER_BACKOFF_BASE", time.Second, &cfg.ReferenceWorkerBackoffBase)
	mustDur("APP_REFERENCE_WORKER_BACKOFF_MAX", 15*time.Minute, &cfg.ReferenceWorkerBackoffMax)
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
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
	if c.DatabaseURL == "" {
		// Todos os papéis leem/escrevem no PostgreSQL (consultas, inbox, outbox).
		return fmt.Errorf("config: APP_DATABASE_URL obrigatória")
	}
	if c.RolesSQSConsumer() {
		if c.SQSConsumerQueueURL == "" {
			return fmt.Errorf("config: papel sqs-consumer exige APP_SQS_QUEUE_URL")
		}
		if c.SQSConsumerDLQURL == "" {
			return fmt.Errorf("config: papel sqs-consumer exige APP_SQS_DLQ_URL")
		}
	}
	if c.RolesOutboxPublisher() && c.OutboxEventsQueueURL == "" {
		return fmt.Errorf("config: papel outbox-publisher exige APP_SQS_EVENTS_QUEUE_URL")
	}
	if c.OutboxBatchSize < 1 {
		return fmt.Errorf("config: APP_OUTBOX_BATCH_SIZE deve ser >= 1")
	}
	if c.OutboxPollInterval <= 0 || c.OutboxLease <= 0 || c.OutboxSendTimeout <= 0 ||
		c.OutboxBackoffBase <= 0 || c.OutboxBackoffMax <= 0 {
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

func envOrInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q inválido", key, v)
	}
	return n, nil
}

func envOrDur(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q inválido", key, v)
	}
	return d, nil
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
