// Package notify is a thin wrapper around Postgres LISTEN/NOTIFY used as the
// cross-process event bus between the meshbug ingest service (which writes)
// and the meshbug web service (which renders SSE updates from the writes).
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channels exchanged between ingest and web.
const (
	ChannelPackets  = "meshbug_packets"
	ChannelStatus   = "meshbug_status"
	ChannelAnomaly  = "meshbug_anomaly"
)

// Publish sends one NOTIFY on the given channel. `payload` is marshalled to
// JSON; the resulting string must fit in Postgres' 8000-byte limit. Safe to
// call from goroutines concurrently — it goes through the pool.
func Publish(ctx context.Context, pool *pgxpool.Pool, channel string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if len(body) > 7900 {
		return fmt.Errorf("payload too large for pg_notify: %d bytes", len(body))
	}
	_, err = pool.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, string(body))
	return err
}

// Notification is one delivered LISTEN event.
type Notification struct {
	Channel string
	Payload []byte
}

// Listen connects to `databaseURL`, subscribes to all the given channels, and
// invokes `handle` for every notification. Reconnects automatically with
// exponential backoff. Returns only when ctx is canceled.
//
// Uses a dedicated connection (not the pool) — Postgres associates LISTEN
// subscriptions with a session, so pooling would lose them.
func Listen(ctx context.Context, databaseURL string, channels []string, log *slog.Logger, handle func(Notification)) error {
	backoff := time.Second
	for ctx.Err() == nil {
		err := listenOnce(ctx, databaseURL, channels, handle)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("notify listener disconnected; retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	return ctx.Err()
}

func listenOnce(ctx context.Context, databaseURL string, channels []string, handle func(Notification)) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	for _, c := range channels {
		// Channel names can't be parameterized, but they're from constants here
		// so this is safe. Quote-identifier defensively anyway.
		if _, err := conn.Exec(ctx, `LISTEN "`+sanitize(c)+`"`); err != nil {
			return fmt.Errorf("listen %s: %w", c, err)
		}
	}

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		handle(Notification{Channel: n.Channel, Payload: []byte(n.Payload)})
	}
}

// sanitize strips any character that isn't valid in a Postgres identifier.
// Channel names here are constants, but defensive against future use.
func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_':
			out = append(out, c)
		}
	}
	return string(out)
}
