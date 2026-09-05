// reconcile.go — reconciliation engine that detects and repairs partial state loss.
//
// Reconciliation is the core correctness mechanism. It works in two phases:
//
//  1. Sync-up (node → coordinator): ensures the coordinator has every op
//     the node has. This fixes the dual-write gap — if a coordinator write
//     failed during ApplyDelta, this step catches it.
//
//  2. Repair (coordinator → node): finds ops the coordinator has that the
//     node lost (due to crash), replays them into the node DB, and
//     recomputes the node's local value.
//
// After reconciliation, naive global value must equal authoritative global value.
package simulation

import (
	"fmt"
	"log"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/model"
)

// ReconcileResult extends the model with pre/post values for the dashboard.
type ReconcileResult struct {
	model.ReconciliationResult

	MissingCount        int   `json:"missing_count"`
	ReplayedCount       int   `json:"replayed_count"`
	OldLocalValue       int64 `json:"old_local_value"`
	NaiveGlobalBefore   int64 `json:"naive_global_before"`
	NaiveGlobalAfter    int64 `json:"naive_global_after"`
	AuthoritativeGlobal int64 `json:"authoritative_global"`
}

// Reconcile detects and repairs partial state loss on the given node.
//
// The full sequence:
//  1. Sync-up: copy any ops from node DB that are missing in coordinator DB
//     (fixes dual-write failures from ApplyDelta)
//  2. Detect: find ops in coordinator DB that are missing from node DB
//  3. Repair: replay missing ops into node DB (INSERT OR IGNORE)
//  4. Recompute: update node's in-memory local value from durable log
//
// Returns a detailed result showing what was found and fixed.
func (s *Simulation) Reconcile(nodeID string) (*ReconcileResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.getNodeLocked(nodeID)
	if n == nil {
		return nil, fmt.Errorf("node %q not found", nodeID)
	}

	oldLocalVal := n.LocalValue()
	naiveBefore := s.naiveGlobalLocked()

	// ---------------------------------------------------------------
	// Phase 1: Sync-up (node → coordinator)
	// Ensures coordinator has everything the node has.
	// This closes the dual-write gap: if ApplyDelta's coordinator write
	// failed, the op exists in the node DB but not the coordinator.
	// ---------------------------------------------------------------
	nodeOps, err := db.GetOperationsByNode(n.DB, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get node ops: %w", err)
	}

	coordOps, err := db.GetOperationsByNode(s.CoordDB, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get coordinator ops: %w", err)
	}

	// Build set of coordinator operation IDs for O(1) lookup
	coordSet := make(map[string]bool, len(coordOps))
	for _, op := range coordOps {
		coordSet[op.OperationID] = true
	}

	// Sync: node → coordinator (fix dual-write gaps)
	var syncedToCoord int
	for _, op := range nodeOps {
		if !coordSet[op.OperationID] {
			inserted, err := db.InsertOperation(s.CoordDB, op)
			if err != nil {
				log.Printf("WARNING: sync op %s to coordinator failed: %v", op.OperationID, err)
				continue
			}
			if inserted {
				syncedToCoord++
				coordSet[op.OperationID] = true // update the set
			}
		}
	}
	if syncedToCoord > 0 {
		log.Printf("RECONCILE %s: synced %d ops from node → coordinator (dual-write repair)",
			nodeID, syncedToCoord)
		// Re-fetch coordinator ops since we just added some
		coordOps, err = db.GetOperationsByNode(s.CoordDB, nodeID)
		if err != nil {
			return nil, fmt.Errorf("re-fetch coordinator ops after sync: %w", err)
		}
	}

	// ---------------------------------------------------------------
	// Phase 2: Detect missing (coordinator → node)
	// Find ops the coordinator has that the node lost due to crash.
	// ---------------------------------------------------------------
	nodeSet := make(map[string]bool, len(nodeOps))
	for _, op := range nodeOps {
		nodeSet[op.OperationID] = true
	}

	var missingOps []model.Operation
	var missingIDs []string
	for _, op := range coordOps {
		if !nodeSet[op.OperationID] {
			missingOps = append(missingOps, op)
			missingIDs = append(missingIDs, op.OperationID)
		}
	}

	// ---------------------------------------------------------------
	// Phase 3: Repair (replay missing ops into node DB)
	// INSERT OR IGNORE handles any edge case where the op somehow
	// already exists (shouldn't happen, but defense in depth).
	// ---------------------------------------------------------------
	var replayed, dupsIgnored int
	for _, op := range missingOps {
		inserted, err := db.InsertOperation(n.DB, op)
		if err != nil {
			log.Printf("WARNING: replay op %s to node %s failed: %v", op.OperationID, nodeID, err)
			continue
		}
		if inserted {
			replayed++
		} else {
			dupsIgnored++
		}
	}

	log.Printf("RECONCILE %s: found %d missing ops, replayed %d, dups_ignored %d",
		nodeID, len(missingOps), replayed, dupsIgnored)

	// ---------------------------------------------------------------
	// Phase 4: Recompute local value from durable log
	// ---------------------------------------------------------------
	if err := n.ReloadLocalVal(); err != nil {
		return nil, fmt.Errorf("reload local value: %w", err)
	}

	recoveredVal := n.LocalValue()
	naiveAfter := s.naiveGlobalLocked()
	authGlobal, _ := db.SumAmounts(s.CoordDB)

	log.Printf("RECONCILE %s: localVal %d → %d, naive %d → %d, auth=%d",
		nodeID, oldLocalVal, recoveredVal, naiveBefore, naiveAfter, authGlobal)

	return &ReconcileResult{
		ReconciliationResult: model.ReconciliationResult{
			NodeID:              nodeID,
			MissingOperationIDs: missingIDs,
			RecoveredValue:      recoveredVal,
			DuplicatesIgnored:   dupsIgnored,
		},
		MissingCount:        len(missingOps),
		ReplayedCount:       replayed,
		OldLocalValue:       oldLocalVal,
		NaiveGlobalBefore:   naiveBefore,
		NaiveGlobalAfter:    naiveAfter,
		AuthoritativeGlobal: authGlobal,
	}, nil
}
