package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// RawEventInput is one MQTT message captured by the ingest service.
type RawEventInput struct {
	Broker     string
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}

// InsertRawEvents appends events to the raw_events log via CopyFrom. The
// sequence-assigned id is filled by us; ReceivedAt is the moment the MQTT
// client callback fired for this message.
func (s *Store) InsertRawEvents(ctx context.Context, msgs []RawEventInput) (int64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	ids := make([]int64, len(msgs))

	err := s.Pool.
		QueryRow(
			ctx,
			`SELECT array_agg(nextval('raw_events_id_seq')) FROM generate_series(1, $1)`,
			len(msgs),
		).
		Scan(&ids)
	if err != nil {
		return 0, err
	}

	src := pgx.CopyFromSlice(
		len(msgs),
		func(i int) ([]any, error) {
			m := msgs[i]
			return []any{ids[i], m.ReceivedAt, m.Broker, m.Topic, m.Payload}, nil
		},
	)

	return s.Pool.CopyFrom(
		ctx,
		pgx.Identifier{"raw_events"},
		[]string{"id", "received_at", "broker", "topic", "payload"},
		src,
	)
}

// RawEvent is one row read back from raw_events.
type RawEvent struct {
	ID         int64
	ReceivedAt time.Time
	Broker     string
	Topic      string
	Payload    []byte
}

// MaxRawEventID returns the highest id currently in raw_events, or 0 when
// the table is empty. Used by the projector to report catch-up progress.
func (s *Store) MaxRawEventID(ctx context.Context) (int64, error) {
	var id *int64

	err := s.Pool.
		QueryRow(ctx, `SELECT MAX(id) FROM raw_events`).
		Scan(&id)
	if err != nil {
		return 0, err
	}

	if id == nil {
		return 0, nil
	}

	return *id, nil
}

// FetchRawEventsAfter returns up to `limit` events with id > since, in id
// order. Used by the projector to stream through history.
func (s *Store) FetchRawEventsAfter(ctx context.Context, since int64, limit int) ([]RawEvent, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   id
		        ,received_at
		        ,broker
		        ,topic
		        ,payload
		FROM    raw_events
		WHERE   id > $1
		ORDER BY id ASC
		LIMIT $2
		`,
		since,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RawEvent, 0, limit)
	for rows.Next() {
		var e RawEvent

		err := rows.Scan(
			&e.ID,
			&e.ReceivedAt,
			&e.Broker,
			&e.Topic,
			&e.Payload,
		)
		if err != nil {
			return nil, err
		}

		out = append(out, e)
	}

	return out, rows.Err()
}

// LoadProjectorCursor returns the highest event id the named projector has
// processed (0 if never).
func (s *Store) LoadProjectorCursor(ctx context.Context, name string) (int64, error) {
	var id int64

	err := s.Pool.
		QueryRow(
			ctx,
			`SELECT last_event_id FROM projector_state WHERE name = $1`,
			name,
		).
		Scan(&id)

	return id, err
}

// SaveProjectorCursor advances the cursor for the named projector.
func (s *Store) SaveProjectorCursor(ctx context.Context, name string, id int64) error {
	_, err := s.Pool.Exec(
		ctx,
		`
		INSERT INTO projector_state
		    (name
		    ,last_event_id
		    ,updated_at)
		VALUES
		    ($1
		    ,$2
		    ,now())
		ON CONFLICT (name) DO UPDATE SET
		     last_event_id = EXCLUDED.last_event_id
		    ,updated_at = now()
		`,
		name,
		id,
	)
	return err
}

// ResetDerivedState empties every table the projector owns and rewinds its
// cursor to 0, so a subsequent run will rebuild from raw_events.
func (s *Store) ResetDerivedState(ctx context.Context, projectorName string) error {
	stmts := []string{
		`TRUNCATE TABLE anomalies RESTART IDENTITY`,
		`TRUNCATE TABLE rollup_neighbor_1m`,
		`TRUNCATE TABLE rollup_observer_1h`,
		`TRUNCATE TABLE rollup_observer_1m`,
		`TRUNCATE TABLE packets_unique`,
		`TRUNCATE TABLE packet_observations`,
		`TRUNCATE TABLE observer_status`,
		`TRUNCATE TABLE nodes`,
		// observers is referenced by observer_status FK; emptied last.
		`DELETE FROM observers`,
		`UPDATE projector_state SET last_event_id = 0, updated_at = now() WHERE name = $1`,
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		_ = tx.Rollback(ctx)
	}(tx, ctx)

	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q, projectorName); err != nil {
			if _, err2 := tx.Exec(ctx, q); err2 != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

