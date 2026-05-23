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
	reset := fs.Bool(
		"reset",
		false,
		"Truncate every derived table and rewind the cursor before running",
	)
	_ = fs.Parse(args)

	cfg, err := config.LoadProject()
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
			[]store.Scope{store.ScopeCommon, store.ScopeProject},
		)

		err = store.RunMigrations(
			cfg.DatabaseURL,
			store.ScopeCommon,
			store.ScopeProject,
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
		err := proj.Run(ctx, cfg.DatabaseURL)
		if err != nil && !errors.Is(err, context.Canceled) {
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
