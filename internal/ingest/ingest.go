// Package ingest receives MQTT messages and appends them, byte-for-byte, to
// the `raw_events` table. It does no parsing, no decoding, no derivation —
// that's the projector's job (internal/project). Keeping ingest dumb means
// observer payload quirks (new fields, weird types) never block capture.
package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/jleight/meshbug/internal/mqtt"
	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/store"
)

const NotifyChannel = "meshbug_raw"

type Ingester struct {
	store      *store.Store
	in         <-chan mqtt.Message
	log        *slog.Logger
	batchSize  int
	flushEvery time.Duration
}

func New(
	s *store.Store,
	in <-chan mqtt.Message,
	log *slog.Logger,
) *Ingester {
	return &Ingester{
		store:      s,
		in:         in,
		log:        log,
		batchSize:  500,
		flushEvery: 250 * time.Millisecond,
	}
}

// Run consumes messages from MQTT and writes each one to raw_events. After
// each batch commit it fires a single pg_notify so the projector can wake up
// and process the new events.
func (i *Ingester) Run(ctx context.Context) error {
	batch := make([]mqtt.Message, 0, i.batchSize)

	t := time.NewTimer(i.flushEvery)
	defer t.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		input := make([]store.RawEventInput, len(batch))
		for i, m := range batch {
			input[i] = store.RawEventInput{
				Broker:     m.Broker,
				Topic:      m.Topic,
				Payload:    m.Payload,
				ReceivedAt: m.ReceivedAt,
			}
		}

		n, err := i.store.InsertRawEvents(ctx, input)
		if err != nil {
			i.log.Error(
				"raw_events write failed",
				"err",
				err,
				"n",
				len(batch),
			)
		} else if n > 0 {
			err = notify.Publish(
				ctx,
				i.store.Pool,
				NotifyChannel,
				map[string]any{"count": n},
			)
			if err != nil {
				i.log.Warn(
					"notify publish failed",
					"channel",
					NotifyChannel,
					"err",
					err,
				)
			}
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()

		case <-t.C:
			flush()
			t.Reset(i.flushEvery)

		case msg, ok := <-i.in:
			if !ok {
				flush()
				return nil
			}

			batch = append(batch, msg)

			if len(batch) >= i.batchSize {
				flush()

				if !t.Stop() {
					select {
					case <-t.C:
					default:
					}
				}

				t.Reset(i.flushEvery)
			}
		}
	}
}
