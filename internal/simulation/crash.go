// crash.go — crash injection, manifest generation, and delayed replay simulation.
// Separated from simulation.go to keep lifecycle/aggregation logic distinct from
// failure simulation logic.
package simulation

import (
	"crypto/sha256"
	"fmt"
	"log"
	"sort"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/model"
	"github.com/PesHwA07/Ascend-Finale/internal/node"
)

// CrashResult captures what happened during a simulated crash.
// This is returned to the dashboard for display.
type CrashResult struct {
	NodeID            string   `json:"node_id"`
	DeletedOpIDs      []string `json:"deleted_op_ids"`
	DeletedCount      int      `json:"deleted_count"`
	DeletedAmount     int64    `json:"deleted_amount"`     // total delta lost
	OldLocalValue     int64    `json:"old_local_value"`
	NewLocalValue     int64    `json:"new_local_value"`
	OldEpoch          int64    `json:"old_epoch"`
	NewEpoch          int64    `json:"new_epoch"`
	NaiveGlobal       int64    `json:"naive_global"`       // wrong number after crash
	AuthoritativeGlobal int64  `json:"authoritative_global"` // correct number (unchanged)
}

// CrashNode simulates a partial state loss on the given node.
//
// The crash sequence:
//  1. Get all operations from the node's local DB
//  2. Use the seeded RNG to select which ops to delete (deterministic!)
//  3. Delete selected ops from node DB ONLY (coordinator keeps them — that's the point)
//  4. Mark the node as dead, then restart it with a new epoch
//  5. Recompute local value from surviving operations
//
// deleteCount specifies how many operations to delete.
// If deleteCount <= 0 or > total ops, it defaults to a reasonable subset.
func (s *Simulation) CrashNode(nodeID string, deleteCount int) (*CrashResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the node
	n := s.getNodeLocked(nodeID)
	if n == nil {
		return nil, fmt.Errorf("node %q not found", nodeID)
	}

	if !n.IsAlive() {
		return nil, fmt.Errorf("node %q is already crashed", nodeID)
	}

	oldLocalVal := n.LocalValue()
	oldEpoch := n.Epoch()

	// 1. Get all operations for this node from its local DB
	ops, err := db.GetOperationsByNode(n.DB, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get operations for %s: %w", nodeID, err)
	}

	if len(ops) == 0 {
		return nil, fmt.Errorf("node %q has no operations to crash", nodeID)
	}

	// 2. Determine how many to delete
	if deleteCount <= 0 || deleteCount > len(ops) {
		// Default: delete ~40% of operations (reasonable partial loss)
		deleteCount = len(ops) * 2 / 5
		if deleteCount < 1 {
			deleteCount = 1
		}
	}

	// 3. Select operations to delete using seeded RNG (deterministic!)
	//    Strategy: pick a random contiguous block for clean gap visualization.
	//    The start index is chosen randomly; the block wraps if needed.
	selected := s.selectContiguousBlock(ops, deleteCount)

	// Collect IDs and total amount being deleted
	deletedIDs := make([]string, len(selected))
	var deletedAmount int64
	for i, op := range selected {
		deletedIDs[i] = op.OperationID
		deletedAmount += op.Amount
	}

	// 4. Store deleted ops for Scenario B (delayed replay simulation)
	//    We save the full Operation structs so they can be re-delivered later.
	StoreCrashedOps(nodeID, selected)

	// 5. Delete from node DB ONLY (coordinator keeps everything — this is the key!)
	deleted, err := db.DeleteOperations(n.DB, deletedIDs)
	if err != nil {
		return nil, fmt.Errorf("delete operations from %s: %w", nodeID, err)
	}
	log.Printf("CRASH %s: deleted %d/%d operations (amount=%d) from node DB",
		nodeID, deleted, len(ops), deletedAmount)

	// 6. Simulate crash + restart
	n.SetAlive(false)

	// Increment epoch (new incarnation)
	if err := n.IncrementEpoch(); err != nil {
		return nil, fmt.Errorf("increment epoch for %s: %w", nodeID, err)
	}

	// Recompute local value from surviving operations
	if err := n.ReloadLocalVal(); err != nil {
		return nil, fmt.Errorf("reload local val for %s: %w", nodeID, err)
	}

	n.SetAlive(true)

	newLocalVal := n.LocalValue()
	newEpoch := n.Epoch()

	// Compute global values for the response
	naiveGlobal := s.naiveGlobalLocked()
	authGlobal, _ := db.SumAmounts(s.CoordDB)

	log.Printf("CRASH %s: localVal %d → %d, epoch %d → %d, naive_global=%d, auth_global=%d",
		nodeID, oldLocalVal, newLocalVal, oldEpoch, newEpoch, naiveGlobal, authGlobal)

	return &CrashResult{
		NodeID:              nodeID,
		DeletedOpIDs:        deletedIDs,
		DeletedCount:        int(deleted),
		DeletedAmount:       deletedAmount,
		OldLocalValue:       oldLocalVal,
		NewLocalValue:       newLocalVal,
		OldEpoch:            oldEpoch,
		NewEpoch:            newEpoch,
		NaiveGlobal:         naiveGlobal,
		AuthoritativeGlobal: authGlobal,
	}, nil
}

// selectContiguousBlock picks a contiguous block of operations to delete.
// Uses the simulation's seeded RNG for deterministic, reproducible selection.
// This produces a single clean gap in ProcessedRanges — easy to visualize.
func (s *Simulation) selectContiguousBlock(ops []model.Operation, count int) []model.Operation {
	if count >= len(ops) {
		return ops
	}
	// Pick a random start index within the valid range
	maxStart := len(ops) - count
	start := s.Rng.Intn(maxStart + 1)
	return ops[start : start+count]
}

// naiveGlobalLocked computes naive global without acquiring the RWMutex
// (caller must already hold it).
func (s *Simulation) naiveGlobalLocked() int64 {
	var sum int64
	for _, n := range s.Nodes {
		sum += n.LocalValue()
	}
	return sum
}

// getNodeLocked returns a node by ID without acquiring the RWMutex
// (caller must already hold it).
func (s *Simulation) getNodeLocked(id string) *node.Node {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Manifest Generation (derived from operation log, NOT stored)
// ---------------------------------------------------------------------------

// BuildManifest computes a manifest for the given node by scanning its
// durable operation log. The manifest's ProcessedRanges reveal gaps where
// operations were lost — this is the evidence that distinguishes "lost data"
// from "legitimately never had it."
//
// This is a read-only computation — it does not modify any state.
func (s *Simulation) BuildManifest(nodeID string) (*model.Manifest, error) {
	n := s.GetNode(nodeID)
	if n == nil {
		return nil, fmt.Errorf("node %q not found", nodeID)
	}

	// Get all operations for this node from its local DB
	ops, err := db.GetOperationsByNode(n.DB, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get operations for manifest: %w", err)
	}

	// Compute processed ranges from sequence numbers
	ranges := computeProcessedRanges(ops)

	// Compute highest contiguous sequence
	var highestContiguous int64
	if len(ranges) > 0 && ranges[0][0] == 1 {
		highestContiguous = ranges[0][1]
	}

	// Compute hash over sorted operation IDs (integrity check)
	opHash := computeOpHash(ops)

	return &model.Manifest{
		NodeID:               nodeID,
		Epoch:                n.Epoch(),
		HighestContiguousSeq: highestContiguous,
		ProcessedRanges:      ranges,
		LocalValue:           n.LocalValue(),
		ProcessedOpHash:      opHash,
	}, nil
}

// computeProcessedRanges builds contiguous sequence ranges from operations.
// Example: ops with sequences [1,2,3,5,6,8] → [[1,3],[5,6],[8,8]]
// A gap (like missing 4 and 7) is evidence of partial state loss.
func computeProcessedRanges(ops []model.Operation) [][2]int64 {
	if len(ops) == 0 {
		return nil
	}

	// Collect and sort unique sequence numbers across all epochs
	seqs := make([]int64, 0, len(ops))
	for _, op := range ops {
		seqs = append(seqs, op.Sequence)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	// Build contiguous ranges
	var ranges [][2]int64
	start := seqs[0]
	end := seqs[0]

	for i := 1; i < len(seqs); i++ {
		if seqs[i] == end+1 {
			// Extend current range
			end = seqs[i]
		} else {
			// Gap detected — close current range, start new one
			ranges = append(ranges, [2]int64{start, end})
			start = seqs[i]
			end = seqs[i]
		}
	}
	// Close final range
	ranges = append(ranges, [2]int64{start, end})

	return ranges
}

// computeOpHash computes a SHA-256 hash over sorted operation IDs.
// This is a cheap integrity check — if two parties agree on the hash,
// they agree on the exact set of operations processed.
func computeOpHash(ops []model.Operation) string {
	ids := make([]string, len(ops))
	for i, op := range ops {
		ids[i] = op.OperationID
	}
	sort.Strings(ids)

	h := sha256.New()
	for _, id := range ids {
		h.Write([]byte(id))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ---------------------------------------------------------------------------
// Delayed Replay (Scenario B)
// ---------------------------------------------------------------------------

// LastCrashedOps stores the operation IDs from the most recent crash.
// Used by Scenario B to simulate delayed re-delivery of pre-crash deltas.
// This is a simulation convenience — in a real system, these would arrive
// from a network queue.
var lastCrashedOps = make(map[string][]model.Operation)

// StoreCrashedOps saves operations that were "in flight" at crash time.
// Called internally by CrashNode. Scenario B retrieves them later.
func StoreCrashedOps(nodeID string, ops []model.Operation) {
	lastCrashedOps[nodeID] = ops
}

// GetCrashedOps retrieves the stored pre-crash operations for replay.
func GetCrashedOps(nodeID string) []model.Operation {
	return lastCrashedOps[nodeID]
}
