package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jleight/meshbug/internal/anomaly"
	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/ingest"
	"github.com/jleight/meshbug/internal/mqtt"
	"github.com/jleight/meshbug/internal/rollup"
	"github.com/jleight/meshbug/internal/store"
)

func runIngest(_ []string) {
	cfg, err := config.LoadIngest()
	if err != nil {
		fail("config", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	if cfg.AutoMigrate {
		log.Info("running migrations")
		if err := store.RunMigrations(cfg.DatabaseURL); err != nil {
			fail("migrate", err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("store", err)
	}
	defer st.Close()

	if err := st.EnsurePartitions(ctx, time.Now().UTC()); err != nil {
		fail("partitions", err)
	}

	mgr := mqtt.NewManager(cfg.Brokers, log)
	if err := mgr.Start(ctx); err != nil {
		fail("mqtt", err)
	}

	ing := ingest.New(cfg.Brokers, st, mgr.Messages(), log)
	go func() {
		if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("ingest stopped", "err", err)
		}
	}()

	go rollup.New(st, log).Run(ctx)
	go anomaly.New(st, log).Run(ctx)

	// daily partition maintainer
	go func() {
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := st.EnsurePartitions(ctx, time.Now().UTC()); err != nil {
					log.Warn("partition maintenance", "err", err)
				}
			}
		}
	}()

	log.Info("ingest running", "brokers", len(cfg.Brokers))
	<-ctx.Done()
	log.Info("shutting down")
}
