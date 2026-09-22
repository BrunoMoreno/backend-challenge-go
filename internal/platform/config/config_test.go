package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("APP_ROLES", "http")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr default = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default = %q, want info", cfg.LogLevel)
	}
	if cfg.SQSRegion != "us-east-1" {
		t.Errorf("SQSRegion default = %q, want us-east-1", cfg.SQSRegion)
	}
	if !cfg.RolesHTTP() {
		t.Error("RolesHTTP() = false, want true")
	}
}

func TestLoadRolesInvalid(t *testing.T) {
	t.Setenv("APP_ROLES", "http,banana")
	if _, err := Load(); err == nil {
		t.Error("Load() com papel inválido = nil, want error")
	}
}

func TestLoadRolesEmpty(t *testing.T) {
	t.Setenv("APP_ROLES", "")
	if _, err := Load(); err == nil {
		t.Error("Load() com APP_ROLES vazio = nil, want error")
	}
}

func TestRolesHTTPFalse(t *testing.T) {
	t.Setenv("APP_ROLES", "sqs-consumer")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	t.Setenv("APP_SQS_QUEUE_URL", "http://queue/in")
	t.Setenv("APP_SQS_DLQ_URL", "http://queue/dlq")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RolesHTTP() {
		t.Error("RolesHTTP() = true, want false sem papel http")
	}
}

func TestRolesReferenceWorker(t *testing.T) {
	t.Setenv("APP_ROLES", "reference-worker")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.RolesReferenceWorker() {
		t.Error("RolesReferenceWorker() = false, want true")
	}
	if cfg.ReferenceWorkerMaxAttempts != 30 || cfg.ReferenceWorkerTTL != 24*3600*1e9 {
		t.Errorf("defaults do worker = %d/%v, want 30/24h",
			cfg.ReferenceWorkerMaxAttempts, cfg.ReferenceWorkerTTL)
	}
}

// TestLoadRejectsBadNumber garante o L4: valor não numérico NÃO vira default
// silencioso — a inicialização falha com o nome da variável.
func TestLoadRejectsBadNumber(t *testing.T) {
	t.Setenv("APP_ROLES", "http")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	t.Setenv("APP_OUTBOX_BATCH_SIZE", "abc")
	if _, err := Load(); err == nil {
		t.Fatal("Load() com APP_OUTBOX_BATCH_SIZE=abc = nil, want error")
	}
}

// TestLoadRejectsBadDuration idem para duração (L4).
func TestLoadRejectsBadDuration(t *testing.T) {
	t.Setenv("APP_ROLES", "http")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	t.Setenv("APP_OUTBOX_SEND_TIMEOUT", "tres-segundos")
	if _, err := Load(); err == nil {
		t.Fatal("Load() com APP_OUTBOX_SEND_TIMEOUT inválido = nil, want error")
	}
}

// TestLoadRejectsMissingDatabase garante o L4: APP_DATABASE_URL é obrigatória.
func TestLoadRejectsMissingDatabase(t *testing.T) {
	t.Setenv("APP_ROLES", "http")
	if _, err := Load(); err == nil {
		t.Fatal("Load() sem APP_DATABASE_URL = nil, want error")
	}
}

// TestLoadRejectsMissingQueueByRole garante o L4: o papel sqs-consumer exige
// as filas, mesmo com todo o resto presente.
func TestLoadRejectsMissingQueueByRole(t *testing.T) {
	t.Setenv("APP_ROLES", "sqs-consumer")
	t.Setenv("APP_DATABASE_URL", "postgres://x")
	if _, err := Load(); err == nil {
		t.Fatal("Load() sqs-consumer sem filas = nil, want error")
	}
}
