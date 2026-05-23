package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/store"
)

// runMigrate is a standalone migration runner used by the Helm
// pre-install / pre-upgrade hook (and available locally for `mise run
// migrate-up`). It applies embedded migrations for both the ingest and
// project schemas; idempotent if the databases are already current.
func runMigrate(_ []string) {
	cfg, err := config.LoadMigrate()
	if err != nil {
		fail("config", err)
	}

	handler := slog.NewTextHandler(
		os.Stderr,
		&slog.HandlerOptions{Level: cfg.LogLevel},
	)

	log := slog.New(handler)
	slog.SetDefault(log)

	log.Info(
		"applying migrations",
		"ingest_url_split",
		cfg.IngestDatabaseURL != cfg.DatabaseURL,
	)

	err = store.RunMigrations(
		context.Background(),
		cfg.IngestDatabaseURL,
		cfg.DatabaseURL,
	)
	if err != nil {
		fail("migrate", err)
	}

	log.Info("migrations applied")
}
