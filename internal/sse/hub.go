// Package sse is a tiny in-process publish/subscribe hub used to fan out live
// updates from the projector to HTTP SSE handlers.
package sse

import (
	"sync"
	"sync/atomic"
)

// Event is one fan-out message. Handlers know how to render it.
type Event struct {
	Topic   string
	Payload any
}

// Hub fans out events by topic. Subscribers are best-effort: if a subscriber's
// channel is full, the event is dropped for that subscriber rather than
// blocking the writer.
type Hub struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[string]map[uint64]chan Event
}

func NewHub() *Hub {
	return &Hub{
		subs: map[string]map[uint64]chan Event{},
	}
}

// Subscribe returns a buffered channel that will receive events for any of
// the given topics, plus an unsubscribe func.
func (h *Hub) Subscribe(buffer int, topics ...string) (<-chan Event, func()) {
	ch := make(chan Event, buffer)
	id := atomic.AddUint64(&h.nextID, 1)

	h.mu.Lock()
	for _, t := range topics {
		if h.subs[t] == nil {
			h.subs[t] = map[uint64]chan Event{}
		}
		h.subs[t][id] = ch
	}
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		for _, t := range topics {
			if m := h.subs[t]; m != nil {
				delete(m, id)
				if len(m) == 0 {
					delete(h.subs, t)
				}
			}
		}
		h.mu.Unlock()

		close(ch)
	}

	return ch, unsubscribe
}

// Publish sends e to every subscriber of e.Topic. Non-blocking.
func (h *Hub) Publish(e Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, ch := range h.subs[e.Topic] {
		select {
		case ch <- e:
		default:
			// subscriber is slow — drop.
		}
	}
}
