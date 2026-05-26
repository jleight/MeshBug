package web

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jleight/meshbug/internal/sse"
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

func (s *Server) nodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}

	size, _ := strconv.Atoi(q.Get("size"))
	if size <= 0 {
		size = 50
	}
	if size > 500 {
		size = 500
	}

	in := templates.NodesPage{
		Filter: templates.NodeFilter{
			Q:    q.Get("q"),
			Type: q.Get("type"),
		},
		Sort: q.Get("sort"),
		Dir:  q.Get("dir"),
		Page: page,
		Size: size,
	}

	d, err := queryNodes(r.Context(), s.store, in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = templates.Nodes(d).Render(r.Context(), w)
}

func (s *Server) nodeDetail(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimSpace(chi.URLParam(r, "id"))

	pubkey, err := hex.DecodeString(idStr)
	if err != nil || len(pubkey) == 0 {
		http.Error(w, "bad node id", http.StatusBadRequest)
		return
	}

	d, err := queryNodeDetail(r.Context(), s.store, pubkey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	_ = templates.NodeDetailPage(d).Render(r.Context(), w)
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
	// Track the latest ts we've already pushed so each notify only sends the
	// genuinely new rows, not the whole feed every time.
	var lastTS time.Time

	send := func(dw *DatastarWriter, _ sse.Event) {
		rows, err := queryLiveSince(r.Context(), s.store, lastTS, 50)
		if err != nil || len(rows) == 0 {
			return
		}

		// Re-emit oldest-first so prepend leaves the newest at top.
		for i := len(rows) - 1; i >= 0; i-- {
			p := rows[i]
			if p.TS.After(lastTS) {
				lastTS = p.TS
			}

			var buf bytes.Buffer
			_ = templates.LivePacketRow(p).Render(r.Context(), &buf)
			_ = dw.Patch("#live-rows", "prepend", buf.String())
		}
	}

	s.handleSSE(w, r, []string{"live-feed"}, send)
}

func (s *Server) sseOverview(w http.ResponseWriter, r *http.Request) {
	// every event triggers a full overview re-render (cheap; one card replace)
	send := func(dw *DatastarWriter, _ sse.Event) {
		d, err := queryOverview(r.Context(), s.store)
		if err != nil {
			return
		}

		var buf bytes.Buffer
		_ = templates.Overview(d).Render(r.Context(), &buf)

		// extract the inner body: easier to just replace the entire container.
		_ = dw.Patch("body", "outer", buf.String())
	}

	s.handleSSE(w, r, []string{"overview"}, send)
}

func (s *Server) sseAnomalies(w http.ResponseWriter, r *http.Request) {
	// Re-render the full anomalies list on every notification — simpler than
	// diffing, and the table is small.
	send := func(dw *DatastarWriter, _ sse.Event) {
		rows, err := queryAnomalies(r.Context(), s.store)
		if err != nil {
			return
		}

		var buf bytes.Buffer
		_ = templates.Anomalies(rows).Render(r.Context(), &buf)
		_ = dw.Patch("body", "outer", buf.String())
	}

	s.handleSSE(w, r, []string{"anomalies"}, send)
}

func (s *Server) sseObserver(w http.ResponseWriter, r *http.Request) {
	id, err := observerIDFromURL(r)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	topic := "observer:" + templates.HexFull(id)

	send := func(dw *DatastarWriter, _ sse.Event) {
		d, err := queryObserverDetail(r.Context(), s.store, id)
		if err != nil {
			return
		}

		var buf bytes.Buffer
		_ = templates.ObserverPage(d).Render(r.Context(), &buf)
		_ = dw.Patch("body", "outer", buf.String())
	}

	s.handleSSE(w, r, []string{topic}, send)
}
