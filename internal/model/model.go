// Package model defines the core data types for CounterGhost.
// These are pure value types with no business logic — they're imported
// by every other package (db, node, reconciler) so keeping them
// dependency-free prevents import cycles.
package model

import "time"

// Operation represents a single counter delta applied to a node.
// The OperationID is the durability key — it's generated once at creation
// and never regenerated on retry, which is what makes duplicate-replay
// rejection possible at the storage layer (SQLite UNIQUE constraint).
type Operation struct {
	OperationID string    `json:"operation_id"` // UUID, globally unique, PRIMARY KEY in SQLite
	CounterID   string    `json:"counter_id"`   // e.g. "inventory:sku-42" — single counter for MVP
	NodeID      string    `json:"node_id"`      // which node generated this operation
	Epoch       int64     `json:"epoch"`         // node incarnation at creation time
	Sequence    int64     `json:"sequence"`      // monotonic within a (node, epoch) pair
	Amount      int64     `json:"amount"`        // +/- delta to the counter
	CreatedAt   time.Time `json:"created_at"`
}

// Manifest is what a node publishes after restart to declare its current state.
// The key field is ProcessedRanges — gaps in these ranges are the evidence
// that distinguishes "lost data" from "legitimately never had it."
// Without manifests, a naive system can't tell a crash-induced undercount
// from a normal reset.
type Manifest struct {
	NodeID               string     `json:"node_id"`
	Epoch                int64      `json:"epoch"`
	HighestContiguousSeq int64      `json:"highest_contiguous_seq"`
	ProcessedRanges      [][2]int64 `json:"processed_ranges"`      // e.g. [[1,800],[802,842]] — gap at 801
	LocalValue           int64      `json:"local_value"`
	ProcessedOpHash      string     `json:"processed_op_hash"`     // SHA-256 over sorted operation IDs
}

// ReconciliationResult is returned by the reconciliation engine after
// comparing a node's manifest against the authoritative operation log.
type ReconciliationResult struct {
	NodeID              string   `json:"node_id"`
	MissingOperationIDs []string `json:"missing_operation_ids"`
	RecoveredValue      int64    `json:"recovered_value"`     // corrected local counter value
	DuplicatesIgnored   int      `json:"duplicates_ignored"`  // replayed ops rejected by UNIQUE constraint
}

// NodeState tracks the runtime state of a simulated node.
// This is the in-memory representation — the durable state lives in SQLite.
type NodeState struct {
	NodeID     string `json:"node_id"`
	Epoch      int64  `json:"epoch"`
	LocalValue int64  `json:"local_value"` // current counter value (sum of applied deltas)
	NextSeq    int64  `json:"next_seq"`    // next sequence number to assign
	IsAlive    bool   `json:"is_alive"`    // false after crash, true after restart
}
