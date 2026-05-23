package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/sse"
	"github.com/jleight/meshbug/internal/store"
	"github.com/jleight/meshbug/internal/web"
	"github.com/jleight/meshbug/internal/web/static"
)

func runWeb(_ []string) {
	cfg, err := config.LoadWeb()
	if err != nil {
		fail("config", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("store", err)
	}
	defer st.Close()

	hub := sse.NewHub()

	// Bridge cross-process pg_notify into the in-process SSE hub. Notifications
	// from ingest land here; we re-publish to the relevant in-process topics
	// so connected browsers get a re-render trigger.
	go func() {
		err := notify.Listen(ctx, cfg.DatabaseURL,
			[]string{notify.ChannelPackets, notify.ChannelStatus, notify.ChannelAnomaly},
			log,
			func(n notify.Notification) {
				bridgeNotification(n, hub)
			})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("notify listener exited", "err", err)
		}
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           web.NewServer(st, hub, log, static.FS()).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0,
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

// bridgeNotification maps a Postgres LISTEN event onto the in-process SSE
// hub's topic vocabulary. SSE handlers in internal/web receive an Event and
// then re-query Postgres for the freshest data — so payload shapes here are
// intentionally minimal.
func bridgeNotification(n notify.Notification, hub *sse.Hub) {
	switch n.Channel {
	case notify.ChannelPackets:
		var p struct {
			Count     int      `json:"count"`
			Observers []string `json:"observers"`
		}
		_ = json.Unmarshal(n.Payload, &p)
		hub.Publish(sse.Event{Topic: "live-feed"})
		hub.Publish(sse.Event{Topic: "overview"})
		for _, id := range p.Observers {
			hub.Publish(sse.Event{Topic: "observer:" + id})
		}
	case notify.ChannelStatus:
		var p struct {
			ObserverID string `json:"observer_id"`
		}
		_ = json.Unmarshal(n.Payload, &p)
		hub.Publish(sse.Event{Topic: "overview"})
		if p.ObserverID != "" {
			hub.Publish(sse.Event{Topic: "observer:" + p.ObserverID})
		}
	case notify.ChannelAnomaly:
		hub.Publish(sse.Event{Topic: "anomalies"})
		hub.Publish(sse.Event{Topic: "overview"})
	}
}
