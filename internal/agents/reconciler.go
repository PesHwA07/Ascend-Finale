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

// Reconciler periodically checks for divergence between the authoritative
// (coordinator) global count and the naive (sum-of-local) count. When
// divergence is detected, it auto-reconciles the affected nodes.
//
// Design: The Reconciler is triggered by divergence detection, not by
// node status events. This is more production-realistic — you detect
// the symptom (wrong count) rather than relying on being told about
// the cause (crash). It runs independently of the Sentinel.
type Reconciler struct {
	sim      *simulation.Simulation
	bus      *events.Bus
	interval time.Duration

	mu      sync.Mutex
	enabled bool

	// Metrics
	totalReconciliations int
	totalDupsIgnored     int64
	lastRepairDuration   time.Duration
}

// NewReconciler creates a Reconciler that checks every interval.
// Default interval is 5 seconds if zero is passed.
func NewReconciler(sim *simulation.Simulation, bus *events.Bus, interval time.Duration) *Reconciler {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Reconciler{
		sim:      sim,
		bus:      bus,
		interval: interval,
		enabled:  true,
	}
}

// Start begins the periodic divergence check loop. Blocks until ctx is cancelled.
// Call this in a goroutine: go reconciler.Start(ctx)
func (r *Reconciler) Start(ctx context.Context) {
	log.Printf("RECONCILER: started, checking every %s", r.interval)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("RECONCILER: stopped")
			return
		case <-ticker.C:
			if r.IsEnabled() {
				r.checkAndRepair()
			}
		}
	}
}

// checkAndRepair compares auth vs naive. If divergence > 0, reconcile all nodes.
func (r *Reconciler) checkAndRepair() {
	naiveGlobal := r.sim.NaiveGlobalValue()
	authGlobal, err := r.sim.AuthoritativeGlobalValue()
	if err != nil {
		log.Printf("RECONCILER: error reading auth value: %v", err)
		return
	}

	divergence := authGlobal - naiveGlobal
	if divergence == 0 {
		return // No divergence, nothing to do
	}

	log.Printf("RECONCILER: divergence detected: auth=%d, naive=%d, diff=%d",
		authGlobal, naiveGlobal, divergence)

	r.bus.Publish(events.Event{
		Type:    events.EventDivergenceDetected,
		Message: fmt.Sprintf("RECONCILER: Divergence detected: auth=%d, naive=%d (diff=%d)", authGlobal, naiveGlobal, divergence),
		Data: map[string]interface{}{
			"authoritative": authGlobal,
			"naive":         naiveGlobal,
			"divergence":    divergence,
		},
	})

	// Reconcile each node that is alive
	startTime := time.Now()
	states := r.sim.AllStates()

	for _, state := range states {
		if !state.IsAlive {
			continue // Skip crashed nodes — can't repair a dead node
		}

		r.bus.Publish(events.Event{
			Type:    events.EventReconciliationStarted,
			NodeID:  state.NodeID,
			Message: fmt.Sprintf("RECONCILER: Starting reconciliation for %s", state.NodeID),
		})

		result, err := r.sim.Reconcile(state.NodeID)
		if err != nil {
			log.Printf("RECONCILER: error reconciling %s: %v", state.NodeID, err)
			r.bus.Publish(events.Event{
				Type:    events.EventReconciliationDone,
				NodeID:  state.NodeID,
				Message: fmt.Sprintf("RECONCILER: ERROR reconciling %s: %v", state.NodeID, err),
				Data: map[string]interface{}{
					"error": err.Error(),
				},
			})
			continue
		}

		if result.ReplayedCount > 0 {
			log.Printf("RECONCILER: repaired %s: %d missing ops replayed, localVal %d → %d",
				state.NodeID, result.ReplayedCount, result.OldLocalValue, result.RecoveredValue)

			r.bus.Publish(events.Event{
				Type:    events.EventReconciliationDone,
				NodeID:  state.NodeID,
				Message: fmt.Sprintf("RECONCILER: Repaired %s — %d ops replayed, localVal %d → %d",
					state.NodeID, result.ReplayedCount, result.OldLocalValue, result.RecoveredValue),
				Data: map[string]interface{}{
					"missing_count":   result.MissingCount,
					"replayed_count":  result.ReplayedCount,
					"old_local_value": result.OldLocalValue,
					"recovered_value": result.RecoveredValue,
				},
			})

			r.mu.Lock()
			r.totalReconciliations++
			r.mu.Unlock()
		}
	}

	// Check if repair was successful
	newNaive := r.sim.NaiveGlobalValue()
	newAuth, _ := r.sim.AuthoritativeGlobalValue()
	repairDuration := time.Since(startTime)

	r.mu.Lock()
	r.lastRepairDuration = repairDuration
	r.mu.Unlock()

	if newNaive == newAuth {
		log.Printf("RECONCILER: ✅ repair complete in %s — naive=%d, auth=%d",
			repairDuration, newNaive, newAuth)
		r.bus.Publish(events.Event{
			Type:    events.EventReconciliationDone,
			Message: fmt.Sprintf("RECONCILER: ✅ Repair complete in %s — naive=%d, auth=%d", repairDuration, newNaive, newAuth),
			Data: map[string]interface{}{
				"naive":            newNaive,
				"authoritative":    newAuth,
				"repair_duration":  repairDuration.Milliseconds(),
				"reconciliations":  r.TotalReconciliations(),
			},
		})
	} else {
		log.Printf("RECONCILER: ⚠ repair incomplete — naive=%d, auth=%d, still diverged by %d",
			newNaive, newAuth, newAuth-newNaive)
	}
}

// SetEnabled toggles the reconciler on/off (for manual mode).
func (r *Reconciler) SetEnabled(enabled bool) {
	r.mu.Lock()
	r.enabled = enabled
	r.mu.Unlock()
}

// IsEnabled returns whether the reconciler is actively checking.
func (r *Reconciler) IsEnabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enabled
}

// TotalReconciliations returns how many successful reconciliations have occurred.
func (r *Reconciler) TotalReconciliations() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalReconciliations
}

// LastRepairDuration returns the duration of the most recent repair.
func (r *Reconciler) LastRepairDuration() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastRepairDuration
}
