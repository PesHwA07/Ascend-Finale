// Package simulation — auto-streaming and auto-chaos goroutines.
//
// AutoStreamer continuously generates orders across all nodes, simulating
// real e-commerce order flow. AutoChaos randomly crashes a node at
// configurable intervals, simulating regional outages.
package simulation

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/events"
)

// StreamConfig controls the auto-streaming and auto-chaos behavior.
type StreamConfig struct {
	StreamInterval time.Duration // Time between orders per node (default 500ms)
	ChaosInterval  time.Duration // Average time between crashes (default 45s)
	ChaosEnabled   bool          // Whether auto-chaos is active
	StreamEnabled  bool          // Whether auto-streaming is active
}

// AutoStreamer generates orders automatically across all nodes.
type AutoStreamer struct {
	sim    *Simulation
	bus    *events.Bus
	mu     sync.Mutex
	config StreamConfig
	cancel context.CancelFunc

	intervalCh chan time.Duration
}

// NewAutoStreamer creates a new auto-streamer.
func NewAutoStreamer(sim *Simulation, bus *events.Bus, cfg StreamConfig) *AutoStreamer {
	if cfg.StreamInterval == 0 {
		cfg.StreamInterval = 500 * time.Millisecond
	}
	if cfg.ChaosInterval == 0 {
		cfg.ChaosInterval = 45 * time.Second
	}
	return &AutoStreamer{
		sim:        sim,
		bus:        bus,
		config:     cfg,
		intervalCh: make(chan time.Duration, 1),
	}
}

// Start begins auto-streaming orders. Call in a goroutine.
func (a *AutoStreamer) Start(ctx context.Context) {
	a.mu.Lock()
	ctx, a.cancel = context.WithCancel(ctx)
	a.mu.Unlock()

	log.Printf("AutoStreamer started: interval=%s, chaos=%v (every %s)",
		a.config.StreamInterval, a.config.ChaosEnabled, a.config.ChaosInterval)

	orderTicker := time.NewTicker(a.config.StreamInterval)
	defer orderTicker.Stop()

	// Chaos timer — random interval around the configured average
	chaosTimer := time.NewTimer(a.nextChaosDelay())
	defer chaosTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("AutoStreamer stopped")
			return

		case newInterval := <-a.intervalCh:
			orderTicker.Reset(newInterval)

		case <-orderTicker.C:
			a.mu.Lock()
			enabled := a.config.StreamEnabled
			a.mu.Unlock()

			if !enabled {
				continue
			}

			// Generate 1 order on a random alive node
			a.generateOrder()

		case <-chaosTimer.C:
			a.mu.Lock()
			chaosOn := a.config.ChaosEnabled
			a.mu.Unlock()

			if chaosOn {
				a.triggerChaos()
			}
			chaosTimer.Reset(a.nextChaosDelay())
		}
	}
}

// generateOrder creates 1 order on a random alive node.
func (a *AutoStreamer) generateOrder() {
	states := a.sim.AllStates()

	// Collect alive nodes
	var aliveNodes []string
	for _, s := range states {
		if s.IsAlive {
			aliveNodes = append(aliveNodes, s.NodeID)
		}
	}
	if len(aliveNodes) == 0 {
		return // All nodes crashed — skip
	}

	// Pick a random alive node
	nodeID := aliveNodes[rand.Intn(len(aliveNodes))]

	// Apply 1 order (amount=1, count=1)
	n := a.sim.GetNode(nodeID)
	if n == nil {
		return
	}
	_, err := n.ApplyN(1, 1)
	if err != nil {
		return // Node might have crashed between check and apply
	}

	// Publish event for WebSocket
	a.bus.Publish(events.Event{
		Type:    events.EventOperationApplied,
		NodeID:  nodeID,
		Message: "order_generated",
		Data: map[string]interface{}{
			"node_id": nodeID,
			"type":    "order",
		},
	})
}

// triggerChaos randomly crashes one alive node.
func (a *AutoStreamer) triggerChaos() {
	states := a.sim.AllStates()

	// Collect alive nodes
	var aliveNodes []string
	for _, s := range states {
		if s.IsAlive {
			aliveNodes = append(aliveNodes, s.NodeID)
		}
	}

	// Need at least 2 alive nodes (keep 1 always alive)
	if len(aliveNodes) < 2 {
		return
	}

	// Pick a random alive node to crash
	nodeID := aliveNodes[rand.Intn(len(aliveNodes))]

	// Crash with 5-12 random ops deleted
	deleteCount := 5 + rand.Intn(8)
	result, err := a.sim.CrashNode(nodeID, deleteCount)
	if err != nil {
		log.Printf("AutoChaos: crash failed for %s: %v", nodeID, err)
		return
	}

	log.Printf("CHAOS: %s crashed! Lost %d orders", nodeID, result.DeletedCount)

	a.bus.Publish(events.Event{
		Type:    events.EventNodeDown,
		NodeID:  nodeID,
		Message: "auto_chaos_crash",
		Data: map[string]interface{}{
			"node_id":       nodeID,
			"deleted_count": result.DeletedCount,
			"trigger":       "auto_chaos",
		},
	})
}

// nextChaosDelay returns a random delay around the configured interval.
// Adds ±30% jitter so crashes feel organic, not periodic.
func (a *AutoStreamer) nextChaosDelay() time.Duration {
	a.mu.Lock()
	base := a.config.ChaosInterval
	a.mu.Unlock()

	jitter := time.Duration(float64(base) * (0.7 + rand.Float64()*0.6))
	return jitter
}

// SetStreamEnabled enables or disables order streaming.
func (a *AutoStreamer) SetStreamEnabled(enabled bool) {
	a.mu.Lock()
	a.config.StreamEnabled = enabled
	a.mu.Unlock()
}

// SetChaosEnabled enables or disables auto-chaos.
func (a *AutoStreamer) SetChaosEnabled(enabled bool) {
	a.mu.Lock()
	a.config.ChaosEnabled = enabled
	a.mu.Unlock()
}

// SetStreamInterval changes the order generation speed.
func (a *AutoStreamer) SetStreamInterval(d time.Duration) {
	a.mu.Lock()
	a.config.StreamInterval = d
	a.mu.Unlock()

	select {
	case a.intervalCh <- d:
	default:
	}
}

// GetConfig returns the current streaming configuration.
func (a *AutoStreamer) GetConfig() StreamConfig {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.config
}
