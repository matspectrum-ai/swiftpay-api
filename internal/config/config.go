// Package config carrega configurações via variáveis de ambiente.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config agrupa todas as configurações da aplicação.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	PSP      PSPConfig
	Worker   WorkerConfig
}

// ServerConfig configurações do servidor HTTP.
type ServerConfig struct {
	Port            string        `env:"SERVER_PORT" envDefault:"8080"`
	ReadTimeout     time.Duration `env:"SERVER_READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout    time.Duration `env:"SERVER_WRITE_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout time.Duration `env:"SERVER_SHUTDOWN_TIMEOUT" envDefault:"30s"`
}

// DatabaseConfig configurações do PostgreSQL.
type DatabaseConfig struct {
	Host     string `env:"DB_HOST" envDefault:"localhost"`
	Port     string `env:"DB_PORT" envDefault:"5432"`
	User     string `env:"DB_USER" envDefault:"pix"`
	Password string `env:"DB_PASSWORD" envDefault:"pix"`
	DBName   string `env:"DB_NAME" envDefault:"pix_api"`
	SSLMode  string `env:"DB_SSLMODE" envDefault:"disable"`

	MaxOpenConns    int           `env:"DB_MAX_OPEN_CONNS" envDefault:"25"`
	MaxIdleConns    int           `env:"DB_MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"DB_CONN_MAX_LIFETIME" envDefault:"5m"`
	ConnMaxIdleTime time.Duration `env:"DB_CONN_MAX_IDLE_TIME" envDefault:"1m"`
}

// Validate verifica e ajusta os parâmetros de pool do banco.
func (c *DatabaseConfig) Validate() error {
	if c.MaxOpenConns < 5 {
		c.MaxOpenConns = 5
	}
	if c.MaxOpenConns > 100 {
		c.MaxOpenConns = 100
	}
	if c.MaxIdleConns < 1 {
		c.MaxIdleConns = 1
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}
	if c.ConnMaxLifetime < time.Minute {
		c.ConnMaxLifetime = 5 * time.Minute
	}
	if c.ConnMaxIdleTime < time.Second {
		c.ConnMaxIdleTime = time.Minute
	}
	return nil
}

// DSN retorna a string de conexão PostgreSQL.
func (c DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

// PSPConfig configurações do PSP.
type PSPConfig struct {
	MockEnabled  bool   `env:"PSP_MOCK_ENABLED" envDefault:"true"`
	BaseURL      string `env:"PSP_BASE_URL" envDefault:"http://localhost:9090"`
	ClientID     string `env:"PSP_CLIENT_ID"`
	ClientSecret string `env:"PSP_CLIENT_SECRET"`
}

// WorkerConfig configurações dos workers.
type WorkerConfig struct {
	OutboxPollInterval     time.Duration `env:"WORKER_OUTBOX_POLL_INTERVAL" envDefault:"5s"`
	ReconciliationSchedule string        `env:"WORKER_RECONCILIATION_SCHEDULE" envDefault:"@daily"`
	IdempotencyExpiration  time.Duration `env:"WORKER_IDEMPOTENCY_EXPIRATION" envDefault:"24h"`
}

// Load carrega configurações do ambiente.
func Load() (*Config, error) {
	cfg := &Config{
		Server:   ServerConfig{},
		Database: DatabaseConfig{},
		PSP:      PSPConfig{},
		Worker:   WorkerConfig{},
	}

	opts := env.Options{RequiredIfNoDef: false}
	if err := env.ParseWithOptions(cfg, opts); err != nil {
		return nil, fmt.Errorf("carregando configurações: %w", err)
	}

	return cfg, nil
}

// Validate verifica campos obrigatórios.
func (c *Config) Validate() error {
	if c.Server.Port == "" {
		return fmt.Errorf("SERVER_PORT é obrigatória")
	}
	if c.Database.Host == "" {
		return fmt.Errorf("DB_HOST é obrigatório")
	}
	if !c.PSP.MockEnabled && c.PSP.ClientID == "" {
		return fmt.Errorf("PSP_CLIENT_ID é obrigatório quando PSP_MOCK_ENABLED=false")
	}
	if err := c.Database.Validate(); err != nil {
		return fmt.Errorf("database config: %w", err)
	}
	return nil
}
