package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jleight/meshbug/internal/anomaly"
	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/project"
	"github.com/jleight/meshbug/internal/rollup"
	"github.com/jleight/meshbug/internal/store"
)

// runProject derives state from raw_events: parses payloads, writes observers /
// observer_status / packet_observations / packets_unique, and runs the rollup
// and anomaly workers. `meshbug project --reset` truncates derived state and
// rebuilds from raw_events on next run.
func runProject(args []string) {
	fs := flag.NewFlagSet("project", flag.ExitOnError)
	reset := fs.Bool("reset", false, "Truncate every derived table and rewind the cursor before running")
	_ = fs.Parse(args)

	// Project uses the same env as ingest minus brokers.
	cfg, err := loadProjectConfig()
	if err != nil {
		fail("config", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	if cfg.AutoMigrate {
		log.Info("running migrations", "scopes", []store.Scope{store.ScopeCommon, store.ScopeProject})
		if err := store.RunMigrations(cfg.DatabaseURL, store.ScopeCommon, store.ScopeProject); err != nil {
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

	proj := project.New(st, log)
	if *reset {
		if err := proj.Reset(ctx); err != nil {
			fail("reset", err)
		}
	}

	go func() {
		if err := proj.Run(ctx, cfg.DatabaseURL); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("project stopped", "err", err)
		}
	}()

	go rollup.New(st, log).Run(ctx)
	go anomaly.New(st, log).Run(ctx)

	// Daily partition maintainer — both raw_events and packet_observations.
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

	log.Info("project running")
	<-ctx.Done()
	log.Info("shutting down")
}

// loadProjectConfig is like LoadIngest minus broker validation — the
// projector doesn't talk to MQTT.
func loadProjectConfig() (*config.Config, error) {
	// Reuse LoadWeb (which doesn't require brokers) and bolt on AutoMigrate.
	c, err := config.LoadWeb()
	if err != nil {
		return nil, err
	}
	c.AutoMigrate = envBool("MESHBUG_AUTO_MIGRATE", true)
	return c, nil
}

func envBool(k string, d bool) bool {
	v := os.Getenv(k)
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return d
	}
}
