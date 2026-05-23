package stages

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/project/pipeline"
)

// detectionCadence is the granularity at which anomaly detectors fire.
// One firing per minute boundary that hwm crosses — matches the 1m
// rollup tables the detectors query.
const detectionCadence = time.Minute

// dedupWindow is how far back to look for an existing unresolved
// anomaly of the same (kind, subject). Within this window we suppress
// duplicates; outside it, a fresh anomaly fires. Event-time, not
// wallclock, so live and replay behave identically.
const dedupWindow = 15 * time.Minute

// Detector is one anomaly rule. Run is called once per minute boundary
// (in event-time) with t set to that minute. It returns zero or more
// findings, each of which becomes a candidate row in the anomalies
// table after dedup.
type Detector interface {
	Kind() string
	Run(ctx context.Context, tx pgx.Tx, t time.Time) ([]Finding, error)
}

// Finding is one detector output. SubjectID is whatever the detector
// considers the affected entity (observer id, source prefix, packet
// type bytes, ...); it participates in dedup so the table doesn't fill
// with copies of the same condition.
type Finding struct {
	SubjectID []byte
	Severity  string
	Details   map[string]any
}

// Anomalies is the pipeline stage that owns project.anomalies. It runs
// every registered Detector at each minute boundary crossed by the
// event-time high-water mark, dedupes findings against recent
// unresolved anomalies, and fans out a meshbug_anomaly pg_notify per
// newly inserted row.
//
// State across batches: just the cursor (last minute checked) — kept
// in memory only. Process restart sets the cursor to the first event's
// minute, so detection resumes from now-ish rather than back-filling
// gaps. A `--reset` Clears the cursor; the subsequent replay
// reconstructs the anomalies table from history.
type Anomalies struct {
	detectors []Detector

	cursor    time.Time
	hwm       time.Time
	emitted   []emittedAnomaly
}

type emittedAnomaly struct {
	id       int64
	kind     string
	severity string
}

func NewAnomalies(detectors ...Detector) *Anomalies {
	return &Anomalies{detectors: detectors}
}

func (s *Anomalies) Name() string {
	return "anomalies"
}

func (s *Anomalies) Apply(ctx context.Context, e pipeline.Event) error {
	if e.TS.After(s.hwm) {
		s.hwm = e.TS
	}

	return nil
}

func (s *Anomalies) Flush(ctx context.Context, tx pgx.Tx) error {
	if s.hwm.IsZero() {
		return nil
	}

	if s.cursor.IsZero() {
		// First batch since process start (or since Clear). Anchor
		// to this batch's hwm so we don't burn CPU back-filling. A
		// reset+replay path goes through Clear and then sees its
		// first event with a historical ts, so the cursor anchors
		// to the start of replay automatically.
		s.cursor = s.hwm.UTC().Truncate(detectionCadence)
		return nil
	}

	end := s.hwm.UTC().Truncate(detectionCadence)

	for m := s.cursor.Add(detectionCadence); !m.After(end); m = m.Add(detectionCadence) {
		err := s.runDetectors(ctx, tx, m)
		if err != nil {
			return err
		}
	}

	s.cursor = end
	return nil
}

func (s *Anomalies) runDetectors(
	ctx context.Context,
	tx pgx.Tx,
	t time.Time,
) error {
	for _, d := range s.detectors {
		findings, err := d.Run(ctx, tx, t)
		if err != nil {
			return err
		}

		for _, f := range findings {
			err := s.emit(ctx, tx, t, d.Kind(), f)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Anomalies) emit(
	ctx context.Context,
	tx pgx.Tx,
	t time.Time,
	kind string,
	f Finding,
) error {
	details, err := json.Marshal(f.Details)
	if err != nil {
		return err
	}

	var id *int64

	err = tx.
		QueryRow(
			ctx,
			`
			INSERT INTO anomalies
			    (ts
			    ,kind
			    ,subject_id
			    ,severity
			    ,details)
			SELECT   $1
			        ,$2
			        ,$3
			        ,$4
			        ,$5::jsonb
			WHERE NOT EXISTS (
			    SELECT 1
			    FROM   anomalies
			    WHERE  kind = $2
			       AND subject_id IS NOT DISTINCT FROM $3
			       AND ts >= $1::timestamptz - $6::interval
			       AND ts <  $1::timestamptz
			       AND resolved_at IS NULL
			)
			RETURNING id
			`,
			t,
			kind,
			f.SubjectID,
			f.Severity,
			details,
			dedupWindow,
		).
		Scan(&id)

	switch {
	case err == pgx.ErrNoRows:
		// Deduped; nothing to do.
		return nil

	case err != nil:
		return err
	}

	if id != nil {
		s.emitted = append(s.emitted, emittedAnomaly{
			id:       *id,
			kind:     kind,
			severity: f.Severity,
		})
	}

	return nil
}

func (s *Anomalies) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	for _, a := range s.emitted {
		_ = notify.Publish(
			ctx,
			pool,
			notify.ChannelAnomaly,
			map[string]any{
				"id":       a.id,
				"kind":     a.kind,
				"severity": a.severity,
			},
		)
	}

	s.emitted = s.emitted[:0]
	return nil
}

func (s *Anomalies) Evict(hwm time.Time) {
}

func (s *Anomalies) Clear() {
	s.cursor = time.Time{}
	s.hwm = time.Time{}
	s.emitted = nil
}
