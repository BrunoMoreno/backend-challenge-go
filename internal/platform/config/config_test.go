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
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RolesHTTP() {
		t.Error("RolesHTTP() = true, want false sem papel http")
	}
}
