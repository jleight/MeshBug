// Package store / migration runner.
//
// Migrations are split into scopes so different services can own different
// pieces of the schema:
//
//	common   tables both ingest and project read or write (raw_events, _partition_state)
//	ingest   tables only ingest writes (none today; reserved)
//	project  tables only the projector writes (observers, derived rows, anomalies, ...)
//
// Each scope has its own sequence and its own `schema_migrations_<scope>` table
// (via the migrate driver's `x-migrations-table` option), so they advance
// independently. Order at apply time: common first, then ingest, then project.
//
// In production, the Helm chart runs `meshbug migrate all` as a pre-install /
// pre-upgrade hook. For local dev, each service can apply its own scopes at
// startup when MESHBUG_AUTO_MIGRATE=true.
package store

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed all:migrations
var migrationsFS embed.FS

// Scope identifies a group of migrations that share a schema_migrations table.
type Scope string

const (
	ScopeCommon  Scope = "common"
	ScopeIngest  Scope = "ingest"
	ScopeProject Scope = "project"
)

// AllScopes is the order migrations are applied when running all scopes.
var AllScopes = []Scope{ScopeCommon, ScopeIngest, ScopeProject}

// RunMigrations applies every scope in order. Each scope's progress is tracked
// in its own `schema_migrations_<scope>` table. Pass specific scopes to limit
// what's applied (e.g. just ScopeCommon + ScopeIngest from the ingest service).
func RunMigrations(databaseURL string, scopes ...Scope) error {
	if len(scopes) == 0 {
		scopes = AllScopes
	}
	for _, scope := range scopes {
		if err := runScope(databaseURL, scope); err != nil {
			return fmt.Errorf("migrate scope %s: %w", scope, err)
		}
	}
	return nil
}

func runScope(databaseURL string, scope Scope) error {
	sub, err := fs.Sub(migrationsFS, "migrations/"+string(scope))
	if err != nil {
		return nil // scope dir absent
	}
	hasSQL := false
	_ = fs.WalkDir(sub, ".", func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".sql") {
			hasSQL = true
		}
		return nil
	})
	if !hasSQL {
		return nil // no migrations for this scope yet
	}

	src, err := iofs.New(sub, ".")
	if err != nil {
		return err
	}
	target, err := buildMigrateURL(databaseURL, "schema_migrations_"+string(scope))
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, target)
	if err != nil {
		return err
	}
	defer func() {
		if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
			_ = srcErr
			_ = dbErr
		}
	}()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

// buildMigrateURL converts a postgres:// or postgresql:// DSN to the pgx5://
// form golang-migrate expects, and sets `x-migrations-table` so each scope
// uses its own tracking table.
func buildMigrateURL(databaseURL, table string) (string, error) {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		u.Scheme = "pgx5"
	}
	q := u.Query()
	q.Set("x-migrations-table", table)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
