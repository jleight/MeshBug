package web

import (
	"context"
	"encoding/hex"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/jleight/meshbug/internal/sse"
	"github.com/jleight/meshbug/internal/store"
)

type Server struct {
	store *store.Store
	hub   *sse.Hub
	log   *slog.Logger
	mux   http.Handler
}

func NewServer(s *store.Store, hub *sse.Hub, log *slog.Logger, staticFS fs.FS) *Server {
	srv := &Server{store: s, hub: hub, log: log}
	srv.mux = srv.routes(staticFS)
	return srv
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes(staticFS fs.FS) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(5, "text/html", "text/css", "application/javascript", "image/svg+xml"))

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		if err := s.store.Pool.Ping(req.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	r.Get("/", s.overview)
	r.Get("/observers", s.observers)
	r.Get("/observers/{id}", s.observerDetail)
	r.Get("/neighbors", s.neighbors)
	r.Get("/topology", s.topology)
	r.Get("/packets/live", s.live)
	r.Get("/anomalies", s.anomalies)

	r.Get("/sse/overview", s.sseOverview)
	r.Get("/sse/live-feed", s.sseLive)
	r.Get("/sse/anomalies", s.sseAnomalies)
	r.Get("/sse/observer/{id}", s.sseObserver)

	return r
}

func observerIDFromURL(r *http.Request) ([]byte, error) {
	idStr := chi.URLParam(r, "id")
	idStr = strings.TrimSpace(idStr)
	return hex.DecodeString(idStr)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request, topics []string, send func(*DatastarWriter, sse.Event)) {
	dw, err := NewDatastarWriter(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch, unsubscribe := s.hub.Subscribe(64, topics...)
	defer unsubscribe()
	hb := time.NewTicker(15 * time.Second)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-hb.C:
			if err := dw.Heartbeat(); err != nil {
				return
			}
		case ev := <-ch:
			send(dw, ev)
		}
	}
}

// Used by ctx for graceful shutdown helpers.
var _ = context.Background
