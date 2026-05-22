package web

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/jleight/meshbug/internal/sse"
	"github.com/jleight/meshbug/internal/store"
	"github.com/jleight/meshbug/internal/web/templates"
)

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	d, err := queryOverview(r.Context(), s.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.Overview(d).Render(r.Context(), w)
}

func (s *Server) observers(w http.ResponseWriter, r *http.Request) {
	rows, err := queryObservers(r.Context(), s.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.Observers(rows).Render(r.Context(), w)
}

func (s *Server) observerDetail(w http.ResponseWriter, r *http.Request) {
	id, err := observerIDFromURL(r)
	if err != nil {
		http.Error(w, "bad observer id", http.StatusBadRequest)
		return
	}
	d, err := queryObserverDetail(r.Context(), s.store, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.ObserverPage(d).Render(r.Context(), w)
}

func (s *Server) neighbors(w http.ResponseWriter, r *http.Request) {
	rows, err := queryNeighbors(r.Context(), s.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.Neighbors(rows).Render(r.Context(), w)
}

func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	rows, err := queryLiveInitial(r.Context(), s.store, 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.LiveFeed(rows).Render(r.Context(), w)
}

func (s *Server) anomalies(w http.ResponseWriter, r *http.Request) {
	rows, err := queryAnomalies(r.Context(), s.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.Anomalies(rows).Render(r.Context(), w)
}

func (s *Server) topology(w http.ResponseWriter, r *http.Request) {
	d, err := queryTopology(r.Context(), s.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = templates.Topology(d).Render(r.Context(), w)
}

// ---------- SSE endpoints ----------

func (s *Server) sseLive(w http.ResponseWriter, r *http.Request) {
	s.handleSSE(w, r, []string{"live-feed"}, func(dw *DatastarWriter, ev sse.Event) {
		row, ok := ev.Payload.(store.PacketRow)
		if !ok {
			return
		}
		p := liveRowFromPacket(row, s)
		var buf bytes.Buffer
		_ = templates.LivePacketRow(p).Render(r.Context(), &buf)
		_ = dw.Patch("#live-rows", "prepend", buf.String())
	})
}

func liveRowFromPacket(row store.PacketRow, s *Server) templates.LivePacket {
	// best-effort: fetch origin name; if it fails we render anyway
	var origin string
	_ = s.store.Pool.QueryRow(s.bg(), `SELECT origin_name FROM observers WHERE id = $1`, row.ObserverID).Scan(&origin)
	return templates.LivePacket{
		TS: row.TS, ObserverID: row.ObserverID, Origin: origin, Hash: row.PacketHash,
		Type: row.PacketType, Route: row.Route,
		RSSI: row.RSSI, SNR: row.SNR, Score: row.Score, Source: row.SourcePrefix,
	}
}

func (s *Server) bg() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	return ctx
}

func (s *Server) sseOverview(w http.ResponseWriter, r *http.Request) {
	// every event triggers a full overview re-render (cheap; one card replace)
	s.handleSSE(w, r, []string{"overview"}, func(dw *DatastarWriter, _ sse.Event) {
		d, err := queryOverview(r.Context(), s.store)
		if err != nil {
			return
		}
		var buf bytes.Buffer
		_ = templates.Overview(d).Render(r.Context(), &buf)
		// extract the inner body: easier to just replace the entire container.
		_ = dw.Patch("body", "outer", buf.String())
	})
}

func (s *Server) sseAnomalies(w http.ResponseWriter, r *http.Request) {
	s.handleSSE(w, r, []string{"anomalies"}, func(dw *DatastarWriter, ev sse.Event) {
		_ = dw.Patch("#anomaly-toasts", "append",
			"<div class=\"alert alert-danger\">anomaly: "+stringPayload(ev.Payload)+"</div>")
	})
}

func (s *Server) sseObserver(w http.ResponseWriter, r *http.Request) {
	id, err := observerIDFromURL(r)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	topic := "observer:" + templates.HexFull(id)
	s.handleSSE(w, r, []string{topic}, func(dw *DatastarWriter, _ sse.Event) {
		d, err := queryObserverDetail(r.Context(), s.store, id)
		if err != nil {
			return
		}
		var buf bytes.Buffer
		_ = templates.ObserverPage(d).Render(r.Context(), &buf)
		_ = dw.Patch("body", "outer", buf.String())
	})
}

func stringPayload(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		s := ""
		for k, vv := range x {
			s += k + "=" + sprintf("%v", vv) + " "
		}
		return s
	}
	return sprintf("%v", v)
}
