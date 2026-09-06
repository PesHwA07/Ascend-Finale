// Package agents contains autonomous background goroutines that monitor
// and maintain system health without manual intervention.
//
// OutboxSyncer is the agent responsible for guaranteeing that every operation
// written to a node's local DB eventually reaches the coordinator DB.
// It implements the consumer side of the Transactional Outbox pattern.
package agents

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/events"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// OutboxSyncer periodically drains unsynced operations from each node's
// outbox table and writes them to the coordinator DB. If an operation
// fails sync 5+ times, it's moved to the dead letter queue (DLQ).
type OutboxSyncer struct {
	sim      *simulation.Simulation
	bus      *events.Bus
	interval time.Duration
}

// NewOutboxSyncer creates a new outbox syncer agent.
// If interval is 0, defaults to 3 seconds.
func NewOutboxSyncer(sim *simulation.Simulation, bus *events.Bus, interval time.Duration) *OutboxSyncer {
	if interval == 0 {
		interval = 3 * time.Second
	}
	return &OutboxSyncer{
		sim:      sim,
		bus:      bus,
		interval: interval,
	}
}

// Start runs the outbox syncer loop until the context is cancelled.
func (o *OutboxSyncer) Start(ctx context.Context) {
	log.Printf("OutboxSyncer started (interval=%s)", o.interval)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("OutboxSyncer stopped")
			return
		case <-ticker.C:
			o.syncAllNodes()
		}
	}
}

// syncAllNodes iterates over all nodes and syncs their outbox entries.
func (o *OutboxSyncer) syncAllNodes() {
	nodes := o.sim.AllNodeDBs()
	coordDB := o.sim.CoordDB

	for nodeID, nodeDB := range nodes {
		// 1. Fetch unsynced operations
		ops, err := db.GetUnsyncedOutboxOps(nodeDB, 50)
		if err != nil {
			log.Printf("OutboxSyncer: error fetching outbox for %s: %v", nodeID, err)
			continue
		}

		if len(ops) == 0 {
			continue
		}

		var synced, failed int
		for _, op := range ops {
			// 2. Try to write to coordinator
			_, err := db.InsertOperation(coordDB, op)
			if err != nil {
				// Mark as failed, increment retry count
				db.MarkOutboxFailed(nodeDB, op.OperationID, err.Error())
				failed++
				continue
			}

			// 3. Mark as synced
			if err := db.MarkOutboxSynced(nodeDB, op.OperationID); err != nil {
				log.Printf("OutboxSyncer: error marking %s synced: %v", op.OperationID, err)
			}
			synced++
		}

		if synced > 0 || failed > 0 {
			msg := fmt.Sprintf("OUTBOX SYNC %s: synced=%d, failed=%d", nodeID, synced, failed)
			log.Println(msg)
			o.bus.Publish(events.Event{
				Type:    events.EventOutboxSynced,
				NodeID:  nodeID,
				Message: msg,
				Data: map[string]interface{}{
					"synced": synced,
					"failed": failed,
				},
			})
		}

		// 4. Handle poison pills (5+ failures → DLQ)
		poisonPills, err := db.GetPoisonPillOps(nodeDB)
		if err != nil {
			continue
		}
		for _, pp := range poisonPills {
			dlqEntry := db.DLQEntry{
				OperationID: pp.OperationID,
				NodeID:      pp.NodeID,
				ErrorReason: "max_sync_retries_exceeded",
				RetryCount:  5,
				FailedAt:    time.Now(),
				Payload: map[string]interface{}{
					"counter_id": pp.CounterID,
					"epoch":      pp.Epoch,
					"sequence":   pp.Sequence,
					"amount":     pp.Amount,
				},
			}
			if err := db.InsertDLQEntry(coordDB, dlqEntry); err != nil {
				log.Printf("OutboxSyncer: error inserting DLQ for %s: %v", pp.OperationID, err)
				continue
			}
			db.DeleteOutboxEntry(nodeDB, pp.OperationID)

			msg := fmt.Sprintf("POISON PILL: op %s moved to DLQ after 5 retries", pp.OperationID)
			log.Println(msg)
			o.bus.Publish(events.Event{
				Type:    events.EventOutboxPoisonPill,
				NodeID:  nodeID,
				Message: msg,
			})
		}
	}
}
