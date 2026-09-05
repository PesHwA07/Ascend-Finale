package simulation

import (
	"fmt"
	"log"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
)

// RebuildResult holds the outcome of a projection rebuild.
type RebuildResult struct {
	NodeID          string `json:"node_id"`
	OpsDeleted      int64  `json:"ops_deleted"`       // how many ops were wiped from node DB
	OpsReplayed     int    `json:"ops_replayed"`       // how many coordinator ops were re-inserted
	DupsIgnored     int    `json:"dups_ignored"`       // should be 0 for a clean rebuild
	OldLocalValue   int64  `json:"old_local_value"`
	NewLocalValue   int64  `json:"new_local_value"`
	NaiveGlobal     int64  `json:"naive_global"`
	AuthGlobal      int64  `json:"authoritative_global"`
	RebuildDuration int64  `json:"rebuild_duration_ms"` // milliseconds
}

// RebuildFromLog performs a complete projection rebuild for a node.
// This is the nuclear option: wipe ALL local state and replay from the
// authoritative coordinator log. Proves the event log is the true source
// of truth.
//
// Steps:
//  1. Read all coordinator ops for this node
//  2. Delete ALL ops from node DB (full wipe)
//  3. Re-insert every coordinator op (INSERT OR IGNORE)
//  4. Recompute local value from durable log
//
// The result should be: node's local value == coordinator's total for that node.
func (s *Simulation) RebuildFromLog(nodeID string) (*RebuildResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.getNodeLocked(nodeID)
	if n == nil {
		return nil, fmt.Errorf("node %q not found", nodeID)
	}

	if !n.IsAlive() {
		return nil, fmt.Errorf("node %q is not alive", nodeID)
	}

	startTime := time.Now()
	oldLocalVal := n.LocalValue()

	// 1. Get all coordinator ops for this node
	coordOps, err := db.GetOperationsByNode(s.CoordDB, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get coordinator ops for %s: %w", nodeID, err)
	}

	log.Printf("REBUILD %s: coordinator has %d ops for this node", nodeID, len(coordOps))

	// 2. Wipe node DB completely
	deleted, err := db.DeleteAllOperations(n.DB)
	if err != nil {
		return nil, fmt.Errorf("wipe node DB %s: %w", nodeID, err)
	}

	log.Printf("REBUILD %s: wiped %d ops from node DB", nodeID, deleted)

	// 3. Replay all coordinator ops into node DB
	var replayed, dupsIgnored int
	for _, op := range coordOps {
		inserted, err := db.InsertOperation(n.DB, op)
		if err != nil {
			log.Printf("WARNING: rebuild replay op %s to %s failed: %v", op.OperationID, nodeID, err)
			continue
		}
		if inserted {
			replayed++
		} else {
			dupsIgnored++
		}
	}

	// 4. Recompute local value
	if err := n.ReloadLocalVal(); err != nil {
		return nil, fmt.Errorf("reload local value after rebuild: %w", err)
	}

	newLocalVal := n.LocalValue()
	naiveGlobal := s.naiveGlobalLocked()
	authGlobal, _ := db.SumAmounts(s.CoordDB)
	duration := time.Since(startTime)

	log.Printf("REBUILD %s: complete in %s — localVal %d → %d, replayed %d ops, naive=%d, auth=%d",
		nodeID, duration, oldLocalVal, newLocalVal, replayed, naiveGlobal, authGlobal)

	return &RebuildResult{
		NodeID:          nodeID,
		OpsDeleted:      deleted,
		OpsReplayed:     replayed,
		DupsIgnored:     dupsIgnored,
		OldLocalValue:   oldLocalVal,
		NewLocalValue:   newLocalVal,
		NaiveGlobal:     naiveGlobal,
		AuthGlobal:      authGlobal,
		RebuildDuration: duration.Milliseconds(),
	}, nil
}
