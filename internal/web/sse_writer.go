package web

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DatastarWriter emits Datastar v1.0 SSE events to the client.
//
// Datastar SSE protocol: every server event uses event type
// `datastar-patch-elements` with one or more `data:` lines, e.g.
//
//	event: datastar-patch-elements
//	data: selector #live-rows
//	data: mode prepend
//	data: elements <tr>...</tr>
//
// Multi-line element fragments are encoded as multiple `data: elements ` lines.
type DatastarWriter struct {
	w  http.ResponseWriter
	fl http.Flusher
}

func NewDatastarWriter(w http.ResponseWriter) (*DatastarWriter, error) {
	fl, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	return &DatastarWriter{w: w, fl: fl}, nil
}

// Patch emits a datastar-patch-elements event. `selector` may be empty when
// the fragment carries its own id; `mode` is one of outer|inner|append|
// prepend|before|after|replace|remove (empty defaults to outer).
func (d *DatastarWriter) Patch(selector, mode, html string) error {
	var b strings.Builder
	b.WriteString("event: datastar-patch-elements\n")
	if selector != "" {
		fmt.Fprintf(&b, "data: selector %s\n", selector)
	}
	if mode != "" {
		fmt.Fprintf(&b, "data: mode %s\n", mode)
	}
	for _, line := range strings.Split(html, "\n") {
		fmt.Fprintf(&b, "data: elements %s\n", line)
	}
	b.WriteString("\n")
	if _, err := io.WriteString(d.w, b.String()); err != nil {
		return err
	}
	d.fl.Flush()
	return nil
}

// Heartbeat writes an SSE comment line to keep proxies from idling out.
func (d *DatastarWriter) Heartbeat() error {
	if _, err := io.WriteString(d.w, ": ping\n\n"); err != nil {
		return err
	}
	d.fl.Flush()
	return nil
}

// CloseOnDisconnect waits for the request context to cancel.
func CloseOnDisconnect(ctx context.Context) <-chan struct{} { return ctx.Done() }
