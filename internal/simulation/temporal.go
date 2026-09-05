package simulation

import (
	"fmt"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
)

// TemporalQueryResult holds the result of a "time-travel" query.
type TemporalQueryResult struct {
	AsOf         time.Time        `json:"as_of"`
	GlobalValue  int64            `json:"global_value"`  // coordinator sum up to asOf
	CurrentValue int64            `json:"current_value"` // current coordinator sum
	NodeValues   map[string]int64 `json:"node_values"`   // per-node sums up to asOf
	TotalOps     int64            `json:"total_ops"`     // total ops in coordinator
}

// TemporalQuery returns the authoritative global counter value at a specific
// point in time. This is the "time-travel" feature: since every operation
// has a created_at timestamp, we can reconstruct the counter state at any
// historical moment by summing only operations with created_at <= asOf.
//
// This is a read-only operation on the coordinator DB.
func (s *Simulation) TemporalQuery(asOf time.Time) (*TemporalQueryResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Global sum up to asOf
	globalAsOf, err := db.SumAmountsAsOf(s.CoordDB, asOf)
	if err != nil {
		return nil, fmt.Errorf("temporal global sum: %w", err)
	}

	// Current global for comparison
	currentGlobal, err := db.SumAmounts(s.CoordDB)
	if err != nil {
		return nil, fmt.Errorf("current global sum: %w", err)
	}

	// Per-node breakdown at asOf
	nodeValues := make(map[string]int64)
	for _, n := range s.Nodes {
		nid := n.ID
		val, err := db.SumAmountsAsOfByNode(s.CoordDB, nid, asOf)
		if err != nil {
			return nil, fmt.Errorf("temporal node %s sum: %w", nid, err)
		}
		nodeValues[nid] = val
	}

	// Total ops
	totalOps, _ := db.CountOperations(s.CoordDB)

	return &TemporalQueryResult{
		AsOf:         asOf,
		GlobalValue:  globalAsOf,
		CurrentValue: currentGlobal,
		NodeValues:   nodeValues,
		TotalOps:     totalOps,
	}, nil
}
