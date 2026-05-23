// Package project drives the read-model. It reads ingest.raw_events,
// decodes each one (MQTT topic + JSON payload + MeshCore wire format),
// and feeds the result into a pipeline of stages that maintain every
// derived table in the `project` schema: observers, observer_status,
// packet_observations, packets_unique, rollup_observer_1m,
// rollup_observer_1h, rollup_neighbor_1m. Each batch's writes and the
// cursor advance commit in a single transaction.
//
// Cursor: a row in projector_state tracks the highest raw_events.id
// we've processed. On startup we resume from there; on `--reset` we
// truncate every derived table, rewind to 0, and replay the full log.
//
// Pipeline shape: events flow through every stage. Adding a new
// derivation = write a Stage, append it to the slice in New. See
// internal/project/pipeline and internal/project/stages.
package project

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jleight/meshbug/internal/ingest"
	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/project/pipeline"
	"github.com/jleight/meshbug/internal/project/stages"
	"github.com/jleight/meshbug/internal/store"
)

const (
	projectorName = "default"
	batchSize     = 500
)

// Projector consumes events from `events` (the ingest schema, possibly
// in a different database) and feeds them through a pipeline that
// writes into `projections` (the project schema). The two stores can
// point at the same database or at different ones; the split lets local
// dev replay against a prod read replica while writing projections to
// a local container.
type Projector struct {
	events      *store.Store
	projections *store.Store
	log         *slog.Logger

	decoder  *decoder
	pipeline *pipeline.Pipeline
}

func New(
	events *store.Store,
	projections *store.Store,
	log *slog.Logger,
) *Projector {
	// Order matters: stages that rehydrate by reading packet_observations
	// must flush before PacketObservations writes the current batch's
	// rows, so their rehydrate SELECT sees only historical state. The
	// in-memory accumulator already has this batch's events.
	//
	// Anomalies runs last: its detectors query the rollup tables, which
	// must be up to date for the current batch before detection runs.
	stageList := []pipeline.Stage{
		stages.NewObservers(),
		stages.NewObserverStatus(),
		stages.NewPacketsUnique(),
		stages.NewRollupObserver1m(),
		stages.NewRollupObserver1h(),
		stages.NewRollupNeighbor1m(),
		stages.NewPacketObservations(),
		stages.NewAnomalies(
			&stages.RSSIDrop{
				RecentWindow:   time.Hour,
				BaselineWindow: time.Hour,
				MinSamples:     10,
				ThresholdDB:    6,
				Severity:       "warn",
			},
			&stages.ObserverSilent{
				RecentWindow:      15 * time.Minute,
				BaselineWindow:    24 * time.Hour,
				BaselineMinPerMin: 1,
				Severity:          "crit",
			},
			&stages.ReceptionGap{
				RecentWindow:      time.Hour,
				BaselineWindow:    24 * time.Hour,
				MinSamples:        30,
				BaselineMinPerMin: 0.5,
				MaxRecentFraction: 0.3,
				MinPeerFraction:   0.5,
				MinPeers:          1,
				Severity:          "warn",
			},
		),
	}

	return &Projector{
		events:      events,
		projections: projections,
		log:         log,
		decoder:     newDecoder(log),
		pipeline:    pipeline.New(projections.Pool, log, stageList...),
	}
}

// Reset truncates every derived table, rewinds the cursor to 0, and
// clears every stage's in-memory state. The next Run call will rebuild
// the entire project schema from raw_events.
func (p *Projector) Reset(ctx context.Context) error {
	p.log.Warn("resetting derived state — will rebuild from raw_events on next run")

	err := p.projections.ResetDerivedState(ctx, projectorName)
	if err != nil {
		return err
	}

	p.pipeline.Clear()
	return nil
}

// Run processes events until ctx is canceled. Catches up from the last
// committed cursor first, then listens for pg_notify('meshbug_raw') on
// the events database and processes new batches as they arrive.
//
// eventsURL is the connection string for the dedicated LISTEN session;
// it must point at the same database `events` was opened against.
func (p *Projector) Run(ctx context.Context, eventsURL string) error {
	cursor, err := p.projections.LoadProjectorCursor(ctx, projectorName)
	if err != nil {
		return err
	}

	p.log.Info("project starting", "cursor", cursor)

	err = p.catchUp(ctx, &cursor)
	if err != nil && !errors.Is(err, context.Canceled) {
		p.log.Error("initial catch-up failed", "err", err)
	}

	errs := make(chan error, 1)

	go func() {
		channels := []string{ingest.NotifyChannel}

		handle := func(_ notify.Notification) {
			err := p.catchUp(ctx, &cursor)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("catch-up failed", "err", err)
			}
		}

		errs <- notify.Listen(ctx, eventsURL, channels, p.log, handle)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errs:
		return err
	}
}

func (p *Projector) catchUp(ctx context.Context, cursor *int64) error {
	target, err := p.events.MaxRawEventID(ctx)
	if err != nil {
		return err
	}

	var (
		startCursor = *cursor
		batches     int
		processed   int
	)

	for {
		raw, err := p.events.FetchRawEventsAfter(ctx, *cursor, batchSize)
		if err != nil {
			return err
		}

		if len(raw) == 0 {
			if processed > 0 {
				p.log.Info(
					"catch-up complete",
					"processed",
					processed,
					"batches",
					batches,
					"start_cursor",
					startCursor,
					"end_cursor",
					*cursor,
				)
			}
			return nil
		}

		events := make([]pipeline.Event, 0, len(raw))
		var highest int64

		for _, r := range raw {
			highest = r.ID

			e, ok := p.decoder.Decode(r)
			if !ok {
				continue
			}

			events = append(events, e)
		}

		err = p.pipeline.ProcessBatch(ctx, events, projectorName, highest)
		if err != nil {
			return err
		}

		*cursor = highest
		batches++
		processed += len(raw)

		remaining := target - *cursor
		if remaining < 0 {
			remaining = 0
		}

		p.log.Info(
			"catch-up batch",
			"batch",
			batches,
			"events",
			len(raw),
			"processed",
			processed,
			"cursor",
			*cursor,
			"remaining",
			remaining,
		)

		if len(raw) < batchSize {
			if processed > 0 {
				p.log.Info(
					"catch-up complete",
					"processed",
					processed,
					"batches",
					batches,
					"start_cursor",
					startCursor,
					"end_cursor",
					*cursor,
				)
			}
			return nil
		}
	}
}
