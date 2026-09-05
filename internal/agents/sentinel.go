// Package agents provides autonomous goroutines that monitor and heal
// the simulation without manual intervention. The Sentinel detects
// node health changes; the Reconciler repairs divergence.
package agents

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/events"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// Sentinel polls node health at a fixed interval and publishes
// NodeDown/NodeUp events when a node's alive status changes.
// It uses direct in-process checks (no HTTP), matching how a
// production system would use liveness probes.
type Sentinel struct {
	sim      *simulation.Simulation
	bus      *events.Bus
	interval time.Duration

	mu        sync.Mutex
	statusMap map[string]bool // node_id → was_alive (last known state)
	enabled   bool
}

// NewSentinel creates a Sentinel that checks every interval.
// Default interval is 2 seconds if zero is passed.
func NewSentinel(sim *simulation.Simulation, bus *events.Bus, interval time.Duration) *Sentinel {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Sentinel{
		sim:       sim,
		bus:       bus,
		interval:  interval,
		statusMap: make(map[string]bool),
		enabled:   true,
	}
}

// Start begins the polling loop. It blocks until ctx is cancelled.
// Call this in a goroutine: go sentinel.Start(ctx)
func (s *Sentinel) Start(ctx context.Context) {
	// Initialize status map with current state
	s.mu.Lock()
	for _, state := range s.sim.AllStates() {
		s.statusMap[state.NodeID] = state.IsAlive
	}
	s.mu.Unlock()

	log.Printf("SENTINEL: started, polling every %s", s.interval)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("SENTINEL: stopped")
			return
		case <-ticker.C:
			if s.IsEnabled() {
				s.checkAll()
			}
		}
	}
}

// checkAll polls every node and emits events on status transitions.
func (s *Sentinel) checkAll() {
	states := s.sim.AllStates()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, state := range states {
		wasAlive, known := s.statusMap[state.NodeID]

		if !known {
			// First time seeing this node — just record, no event
			s.statusMap[state.NodeID] = state.IsAlive
			continue
		}

		if wasAlive && !state.IsAlive {
			// Transition: alive → dead
			log.Printf("SENTINEL: Node %s detected DOWN (epoch=%d)", state.NodeID, state.Epoch)
			s.bus.Publish(events.Event{
				Type:    events.EventNodeDown,
				NodeID:  state.NodeID,
				Message: fmt.Sprintf("SENTINEL: Node %s detected DOWN", state.NodeID),
				Data: map[string]interface{}{
					"epoch":       state.Epoch,
					"local_value": state.LocalValue,
				},
			})
		} else if !wasAlive && state.IsAlive {
			// Transition: dead → alive
			log.Printf("SENTINEL: Node %s detected UP (epoch=%d)", state.NodeID, state.Epoch)
			s.bus.Publish(events.Event{
				Type:    events.EventNodeUp,
				NodeID:  state.NodeID,
				Message: fmt.Sprintf("SENTINEL: Node %s detected UP", state.NodeID),
				Data: map[string]interface{}{
					"epoch":       state.Epoch,
					"local_value": state.LocalValue,
				},
			})
		}

		s.statusMap[state.NodeID] = state.IsAlive
	}
}

// SetEnabled toggles the sentinel on/off (for manual mode).
func (s *Sentinel) SetEnabled(enabled bool) {
	s.mu.Lock()
	s.enabled = enabled
	s.mu.Unlock()
}

// IsEnabled returns whether the sentinel is actively polling.
func (s *Sentinel) IsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled
}
