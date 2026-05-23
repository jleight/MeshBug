package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/ingest"
	"github.com/jleight/meshbug/internal/mqtt"
	"github.com/jleight/meshbug/internal/store"
)

// runIngest is the MQTT → raw_events capture loop. It does no decoding or
// derivation — everything that turns those bytes into structured rows lives
// in `meshbug project`.
func runIngest(_ []string) {
	cfg, err := config.LoadIngest()
	if err != nil {
		fail("config", err)
	}

	handler := slog.NewTextHandler(
		os.Stderr,
		&slog.HandlerOptions{Level: cfg.LogLevel},
	)

	log := slog.New(handler)
	slog.SetDefault(log)

	if cfg.AutoMigrate {
		log.Info(
			"running migrations",
			"scopes",
			[]store.Scope{store.ScopeCommon, store.ScopeIngest},
		)

		err = store.RunMigrations(
			cfg.DatabaseURL,
			store.ScopeCommon,
			store.ScopeIngest,
		)
		if err != nil {
			fail("migrate", err)
		}
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("store", err)
	}
	defer st.Close()

	// raw_events is partitioned monthly — make sure the current and next
	// month's partition exist (the project service also calls this, but ingest
	// might come up first on a fresh install).
	if err := st.EnsurePartitions(ctx, time.Now().UTC()); err != nil {
		fail("partitions", err)
	}

	mgr := mqtt.NewManager(cfg.Brokers, log)
	if err := mgr.Start(ctx); err != nil {
		fail("mqtt", err)
	}

	ing := ingest.New(st, mgr.Messages(), log)
	go func() {
		if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("ingest stopped", "err", err)
		}
	}()

	log.Info("ingest running", "brokers", len(cfg.Brokers))
	<-ctx.Done()
	log.Info("shutting down")
}
