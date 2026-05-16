package testhelpers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(ctx context.Context, dsn string) error {
	migrationsPath, err := findMigrationsPath()
	if err != nil {
		return fmt.Errorf("encontrando migrations: %w", err)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		dsn,
	)
	if err != nil {
		return fmt.Errorf("criando migrate: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("executando migrations: %w", err)
	}

	return nil
}

func findMigrationsPath() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(dir, "internal", "store", "postgres", "migrations")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("diretório de migrations não encontrado")
}
