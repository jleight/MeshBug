package store

import (
	"embed"
	"errors"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// RunMigrations applies all pending migrations against databaseURL.
// Accepts the standard `postgres://` or `postgresql://` form and rewrites it
// to `pgx5://` so the pgx/v5 driver picks it up.
func RunMigrations(databaseURL string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	url := databaseURL
	switch {
	case strings.HasPrefix(url, "postgres://"):
		url = "pgx5://" + strings.TrimPrefix(url, "postgres://")
	case strings.HasPrefix(url, "postgresql://"):
		url = "pgx5://" + strings.TrimPrefix(url, "postgresql://")
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
