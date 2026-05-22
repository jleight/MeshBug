// MeshBug — MeshCore mesh health dashboard.
//
// Copyright (c) 2026 Jonathon Leight.
// Licensed under the Elastic License 2.0; see the LICENSE file in the repo
// root.

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jleight/meshbug/internal/anomaly"
	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/ingest"
	"github.com/jleight/meshbug/internal/mqtt"
	"github.com/jleight/meshbug/internal/rollup"
	"github.com/jleight/meshbug/internal/sse"
	"github.com/jleight/meshbug/internal/store"
	"github.com/jleight/meshbug/internal/web"
	"github.com/jleight/meshbug/internal/web/static"
)

func main() {
	cfg, err := config.Load()
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

	hub := sse.NewHub()
	mgr := mqtt.NewManager(cfg.Brokers, log)
	if err := mgr.Start(ctx); err != nil {
		fail("mqtt", err)
	}

	ing := ingest.New(cfg.Brokers, st, hub, mgr.Messages(), log)
	go func() {
		if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("ingest stopped", "err", err)
		}
	}()

	go rollup.New(st, log).Run(ctx)
	go anomaly.New(st, hub, log).Run(ctx)

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

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           web.NewServer(st, hub, log, static.FS()).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0, // 0 for SSE
		IdleTimeout:       2 * time.Minute,
	}

	go func() {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}

func fail(stage string, err error) {
	slog.Error("startup failed", "stage", stage, "err", err)
	os.Exit(1)
}
