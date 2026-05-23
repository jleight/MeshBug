// Package store / migration runner.
//
// MeshBug owns two Postgres schemas:
//
//   - `ingest` — the append-only raw_events log, populated by the ingest
//     service. Lives in the database pointed at by MESHBUG_INGEST_DATABASE_URL
//     (falls back to MESHBUG_DATABASE_URL when unset).
//   - `project` (and `web`, reserved for future use) — every read-model
//     table derived from raw_events. Lives in the database pointed at by
//     MESHBUG_DATABASE_URL.
//
// Each schema has its own embedded migration directory and tracks its
// progress in its own `<schema>._schema_migrations` table. In production the
// Helm chart runs `meshbug migrate` as a pre-install / pre-upgrade hook. The
// three long-running services (ingest, project, web) call CheckMigrations at
// startup and refuse to start if the schemas they need aren't at the latest
// embedded version.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
)

//go:embed all:migrations
var migrationsFS embed.FS

// schemaSet describes one of the embedded migration directories. The
// `schema` field is both the Postgres schema the migrations target and the
// subdirectory under internal/store/migrations/ that holds them.
type schemaSet struct {
	schema string
}

var (
	ingestSet  = schemaSet{schema: "ingest"}
	projectSet = schemaSet{schema: "project"}
)

// RunMigrations applies the ingest schema migrations against ingestURL and
// the project schema migrations against projectURL. The two URLs may point
// at the same database (the common case) or different databases (e.g. local
// dev pointing at a prod read-replica for events).
func RunMigrations(
	ctx context.Context,
	ingestURL string,
	projectURL string,
) error {
	for _, target := range []struct {
		url string
		set schemaSet
	}{
		{ingestURL, ingestSet},
		{projectURL, projectSet},
	} {
		if err := runOne(ctx, target.url, target.set); err != nil {
			return fmt.Errorf("migrate %s: %w", target.set.schema, err)
		}
	}

	return nil
}

// CheckMigrations returns nil only when both schemas are at the latest
// embedded version. Used by long-running services at startup.
func CheckMigrations(
	ctx context.Context,
	ingestURL string,
	projectURL string,
) error {
	for _, target := range []struct {
		url string
		set schemaSet
	}{
		{ingestURL, ingestSet},
		{projectURL, projectSet},
	} {
		if err := checkOne(ctx, target.url, target.set); err != nil {
			return fmt.Errorf("check %s: %w", target.set.schema, err)
		}
	}

	return nil
}

func runOne(
	ctx context.Context,
	databaseURL string,
	set schemaSet,
) error {
	err := ensureSchema(ctx, databaseURL, set.schema)
	if err != nil {
		return err
	}

	m, src, err := openMigrate(databaseURL, set)
	if err != nil {
		return err
	}
	defer closeMigrate(m, src)

	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func checkOne(
	ctx context.Context,
	databaseURL string,
	set schemaSet,
) error {
	err := ensureSchema(ctx, databaseURL, set.schema)
	if err != nil {
		return err
	}

	m, src, err := openMigrate(databaseURL, set)
	if err != nil {
		return err
	}
	defer closeMigrate(m, src)

	want, err := latestEmbeddedVersion(set)
	if err != nil {
		return err
	}

	have, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	if dirty {
		return fmt.Errorf("%s._schema_migrations is dirty at version %d — fix the database before starting", set.schema, have)
	}

	if have != want {
		return fmt.Errorf("%s._schema_migrations at version %d, expected %d — run `meshbug migrate`", set.schema, have, want)
	}

	return nil
}

// ensureSchema creates the target schema if it doesn't already exist. We do
// this outside the migration runner because the migrate driver wants to
// create its `_schema_migrations` table inside the schema before the first
// migration runs.
func ensureSchema(
	ctx context.Context,
	databaseURL string,
	schema string,
) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		_ = conn.Close(ctx)
	}(conn, ctx)

	_, err = conn.Exec(
		ctx,
		`CREATE SCHEMA IF NOT EXISTS `+quoteIdentifier(schema),
	)
	if err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}

	return nil
}

func openMigrate(
	databaseURL string,
	set schemaSet,
) (*migrate.Migrate, source.Driver, error) {
	sub, err := fs.Sub(migrationsFS, "migrations/"+set.schema)
	if err != nil {
		return nil, nil, err
	}

	src, err := iofs.New(sub, ".")
	if err != nil {
		return nil, nil, err
	}

	target, err := buildMigrateURL(databaseURL, set)
	if err != nil {
		_ = src.Close()
		return nil, nil, err
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, target)
	if err != nil {
		_ = src.Close()
		return nil, nil, err
	}

	return m, src, nil
}

func closeMigrate(m *migrate.Migrate, src source.Driver) {
	if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
		_ = srcErr
		_ = dbErr
	}

	err := src.Close()
	if err != nil {
		log.Printf("error closing migrations source: %v", err)
	}
}

// latestEmbeddedVersion returns the highest migration version number in
// the embedded migrations directory for the given schema set.
func latestEmbeddedVersion(set schemaSet) (uint, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations/"+set.schema)
	if err != nil {
		return 0, err
	}

	var max uint

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		// Migration filenames are `NNNNNN_name.up.sql`.
		idx := strings.IndexByte(name, '_')
		if idx <= 0 {
			continue
		}

		v, err := strconv.ParseUint(name[:idx], 10, 64)
		if err != nil {
			continue
		}

		if uint(v) > max {
			max = uint(v)
		}
	}

	if max == 0 {
		return 0, fmt.Errorf("no embedded migrations found for %s", set.schema)
	}

	return max, nil
}

// buildMigrateURL converts a postgres:// or postgresql:// DSN to the
// pgx5:// form golang-migrate expects and points the tracking table at
// `<schema>._schema_migrations` (quoted by us — quoted=1 means the driver
// uses the value verbatim instead of treating the whole dotted name as one
// identifier).
//
// Migration SQL uses schema-qualified names everywhere, so we don't bother
// setting search_path on the migration connection — only application pools
// (store.New) want that.
func buildMigrateURL(databaseURL string, set schemaSet) (string, error) {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "postgres", "postgresql":
		u.Scheme = "pgx5"
	}

	q := u.Query()
	q.Set(
		"x-migrations-table",
		quoteIdentifier(set.schema)+"."+quoteIdentifier("_schema_migrations"),
	)
	q.Set("x-migrations-table-quoted", "1")

	u.RawQuery = q.Encode()

	return u.String(), nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
