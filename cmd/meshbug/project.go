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
	"github.com/jleight/meshbug/internal/store"
)

// runProject derives state from ingest.raw_events: parses payloads and
// feeds them through the project pipeline, which maintains every derived
// table (observers, observer_status, packet_observations, packets_unique,
// rollups). The anomaly worker runs alongside, reading the rollup tables.
// `meshbug project --reset` truncates derived state and rebuilds from
// raw_events on next run.
//
// The projector talks to two stores: an ingest store (reads raw_events,
// LISTENs for new ones) and a project store (writes derived rows + fires
// pg_notify for the web SSE bridge). In the common single-database
// deployment both stores point at the same Postgres instance and just
// scope themselves to different schemas via search_path.
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

	events, err := store.NewIngest(
		ctx,
		cfg.IngestDatabaseURL,
	)
	if err != nil {
		fail("events-store", err)
	}
	defer events.Close()

	projections, err := store.NewProject(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		fail("projection-store", err)
	}
	defer projections.Close()

	err = projections.EnsurePartitions(
		ctx,
		time.Now().UTC(),
		"packet_observations",
	)
	if err != nil {
		fail("partitions", err)
	}

	proj := project.New(events, projections, log)
	if *reset {
		if err := proj.Reset(ctx); err != nil {
			fail("reset", err)
		}
	}

	go func() {
		err := proj.Run(ctx, cfg.IngestDatabaseURL)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("project stopped", "err", err)
		}
	}()

	go anomaly.New(projections, log).Run(ctx)

	// Daily partition maintainer — packet_observations lives in the
	// project schema; ingest maintains its own raw_events partitions.
	go func() {
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-t.C:
				err := projections.EnsurePartitions(
					ctx,
					time.Now().UTC(),
					"packet_observations",
				)
				if err != nil {
					log.Warn("partition maintenance", "err", err)
				}
			}
		}
	}()

	log.Info("project running")
	<-ctx.Done()
	log.Info("shutting down")
}
