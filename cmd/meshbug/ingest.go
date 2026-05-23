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

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	err = store.CheckMigrations(
		ctx,
		cfg.IngestDatabaseURL,
		cfg.DatabaseURL,
	)
	if err != nil {
		fail("migrate-check", err)
	}

	st, err := store.NewIngest(
		ctx,
		cfg.IngestDatabaseURL,
	)
	if err != nil {
		fail("store", err)
	}
	defer st.Close()

	err = st.EnsurePartitions(
		ctx,
		time.Now().UTC(),
		"raw_events",
	)
	if err != nil {
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
