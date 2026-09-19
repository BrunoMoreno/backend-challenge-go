// Package config carrega e valida a configuração da aplicação a partir do ambiente.
package config

import (
	"fmt"
	"os"
	"strings"
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
	// LogLevel é o nível do logger: debug, info, warn ou error.
	LogLevel string
}

// Load lê as variáveis de ambiente e devolve a configuração.
func Load() (Config, error) {
	cfg := Config{
		AppRoles:       splitCSV(os.Getenv("APP_ROLES")),
		HTTPAddr:       envOr("APP_HTTP_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("APP_DATABASE_URL"),
		SQSEndpoint:    envOr("APP_SQS_ENDPOINT", "http://localhost:4566"),
		SQSRegion:      envOr("APP_SQS_REGION", "us-east-1"),
		KeycloakIssuer: os.Getenv("APP_KEYCLOAK_ISSUER"),
		LogLevel:       envOr("APP_LOG_LEVEL", "info"),
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
	return nil
}

// RolesHTTP informa se a instância expõe o servidor HTTP.
func (c Config) RolesHTTP() bool { return contains(c.AppRoles, "http") }

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
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
