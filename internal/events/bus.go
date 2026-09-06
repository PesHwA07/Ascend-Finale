// Package events provides a thread-safe, in-process event bus for
// broadcasting system events (crashes, reconciliations, node status changes)
// to all subscribers (WebSocket connections, agents, logging).
package events

import (
	"sync"
	"time"
)

// EventType identifies the kind of system event.
type EventType string

const (
	// Node lifecycle
	EventNodeDown EventType = "node_down"
	EventNodeUp   EventType = "node_up"

	// Operations
	EventOperationApplied EventType = "operation_applied"

	// Crash & recovery
	EventCrashInjected         EventType = "crash_injected"
	EventDivergenceDetected    EventType = "divergence_detected"
	EventReconciliationStarted EventType = "reconciliation_started"
	EventReconciliationDone    EventType = "reconciliation_complete"
	EventReplayRejected        EventType = "replay_rejected"

	// Advanced features
	EventProjectionRebuilt  EventType = "projection_rebuilt"
	EventChaosExperimentRun EventType = "chaos_experiment_run"
	EventTemporalQuery      EventType = "temporal_query"

	// Outbox pattern (v3)
	EventOutboxSynced     EventType = "outbox_synced"
	EventOutboxPoisonPill EventType = "outbox_poison_pill"

	// Audit (v3)
	EventAuditLogged EventType = "audit_logged"

	// Metrics
	EventMetricsSnapshot EventType = "metrics_snapshot"
)

// Event is a single system event published through the bus.
type Event struct {
	Type      EventType              `json:"type"`
	NodeID    string                 `json:"node_id,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Message   string                 `json:"message"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// subscriber is a registered listener channel.
type subscriber struct {
	ch   chan Event
	done chan struct{}
}

// Bus is a fan-out event bus. Publishers call Publish(), subscribers
// receive events on the channel returned by Subscribe().
// Thread-safe for concurrent use.
type Bus struct {
	mu          sync.RWMutex
	subscribers []*subscriber
	history     []Event
	maxHistory  int
}

// NewBus creates a new event bus that retains the last maxHistory events.
func NewBus(maxHistory int) *Bus {
	if maxHistory <= 0 {
		maxHistory = 200
	}
	return &Bus{
		maxHistory: maxHistory,
		history:    make([]Event, 0, maxHistory),
	}
}

// Publish broadcasts an event to all subscribers and records it in history.
// Non-blocking: if a subscriber's channel is full, the event is dropped for
// that subscriber (prevents slow consumers from stalling the system).
func (b *Bus) Publish(e Event) {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}

	b.mu.Lock()
	// Append to history, evicting oldest if full
	if len(b.history) >= b.maxHistory {
		b.history = b.history[1:]
	}
	b.history = append(b.history, e)
	// Copy subscriber slice for safe iteration
	subs := make([]*subscriber, len(b.subscribers))
	copy(subs, b.subscribers)
	b.mu.Unlock()

	for _, s := range subs {
		select {
		case s.ch <- e:
		default:
			// Slow consumer, drop event to avoid blocking
		}
	}
}

// Subscribe returns a channel that receives all future events.
// Call Unsubscribe with the returned channel to stop receiving.
// The channel has a buffer of 64 events.
func (b *Bus) Subscribe() chan Event {
	ch := make(chan Event, 64)
	s := &subscriber{ch: ch, done: make(chan struct{})}

	b.mu.Lock()
	b.subscribers = append(b.subscribers, s)
	b.mu.Unlock()

	return ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, s := range b.subscribers {
		if s.ch == ch {
			close(s.ch)
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			return
		}
	}
}

// History returns the last n events (or all if n > stored count).
func (b *Bus) History(n int) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if n <= 0 || n > len(b.history) {
		n = len(b.history)
	}

	// Return the most recent n events
	start := len(b.history) - n
	result := make([]Event, n)
	copy(result, b.history[start:])
	return result
}

// Len returns the number of stored history events.
func (b *Bus) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.history)
}
