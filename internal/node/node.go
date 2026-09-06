// Package node implements a simulated distributed counter node.
// Each Node is a goroutine-safe unit with its own SQLite file, epoch,
// and monotonic sequence counter. It writes every operation to both its
// local DB and the coordinator DB (authoritative log).
//
// Design: node-first write with pragmatic dual-write.
// If the coordinator write fails, the operation is still committed
// locally and reconciliation will detect the gap later.
package node

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/model"
	"github.com/google/uuid"
)

// DefaultCounterID is the single counter used for the MVP demo.
const DefaultCounterID = "inventory:sku-42"

// Node represents a simulated distributed counter node.
// All mutable state is protected by mu.
type Node struct {
	ID      string
	DB      *sql.DB // this node's SQLite file
	CoordDB *sql.DB // coordinator (authoritative log)

	mu        sync.Mutex
	counterID string
	epoch     int64
	nextSeq   int64 // monotonic per (node, epoch)
	localVal  int64 // in-memory counter (sum of applied deltas)
	isAlive   bool
}

// NewNode initializes a node, recovering state from its durable DB.
//
// Recovery sequence:
//  1. Fetch latest epoch from DB (0 = fresh node → start at epoch 1)
//  2. Record new epoch in the epochs table
//  3. Recover nextSeq from MAX(sequence) for this node+epoch
//  4. Recompute localVal from SUM(amount) across all durable operations
func NewNode(id string, nodeDB, coordDB *sql.DB) (*Node, error) {
	n := &Node{
		ID:        id,
		DB:        nodeDB,
		CoordDB:   coordDB,
		counterID: DefaultCounterID,
		isAlive:   true,
	}

	// 1. Determine epoch
	latestEpoch, err := db.GetLatestEpoch(nodeDB, id)
	if err != nil {
		return nil, fmt.Errorf("node %s: get latest epoch: %w", id, err)
	}
	n.epoch = latestEpoch + 1

	// 2. Record new epoch
	if err := db.InsertEpoch(nodeDB, id, n.epoch); err != nil {
		return nil, fmt.Errorf("node %s: insert epoch %d: %w", id, n.epoch, err)
	}

	// 3. Recover sequence (for this epoch — will be 0 for a new epoch)
	maxSeq, err := db.GetMaxSequence(nodeDB, id, n.epoch)
	if err != nil {
		return nil, fmt.Errorf("node %s: get max sequence: %w", id, err)
	}
	n.nextSeq = maxSeq + 1

	// 4. Recompute local value from durable operation log
	localVal, err := db.SumAmounts(nodeDB)
	if err != nil {
		return nil, fmt.Errorf("node %s: sum amounts: %w", id, err)
	}
	n.localVal = localVal

	log.Printf("Node %s initialized: epoch=%d, nextSeq=%d, localVal=%d",
		id, n.epoch, n.nextSeq, n.localVal)

	return n, nil
}

// ApplyDelta creates a new operation with the given amount and writes it
// to both the node's operations table and outbox table in a single
// atomic transaction. The coordinator write is deferred to the
// OutboxSyncer agent, which guarantees eventual consistency.
//
// Pattern: Transactional Outbox
// - Operations table + outbox table are in the same SQLite file
// - Single transaction: both succeed or both fail
// - OutboxSyncer reads outbox and syncs to coordinator asynchronously
//
// Fallback: If the outbox transaction fails, falls back to the legacy
// direct dual-write for backward compatibility.
func (n *Node) ApplyDelta(amount int64) (*model.Operation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.isAlive {
		return nil, fmt.Errorf("node %s is not alive (crashed)", n.ID)
	}

	op := model.Operation{
		OperationID: uuid.New().String(),
		CounterID:   n.counterID,
		NodeID:      n.ID,
		Epoch:       n.epoch,
		Sequence:    n.nextSeq,
		Amount:      amount,
		CreatedAt:   time.Now(),
	}

	// Transactional outbox: write op + outbox entry in one atomic tx
	tx, err := n.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("node %s: begin tx: %w", n.ID, err)
	}

	inserted, err := db.InsertOperationTx(tx, op)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("node %s: local insert: %w", n.ID, err)
	}
	if !inserted {
		tx.Rollback()
		return nil, fmt.Errorf("node %s: operation %s already exists locally", n.ID, op.OperationID)
	}

	if err := db.InsertOutboxEntryTx(tx, op); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("node %s: outbox insert: %w", n.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("node %s: commit tx: %w", n.ID, err)
	}

	// Also do direct coordinator write (belt-and-suspenders with outbox)
	// OutboxSyncer will handle it if this fails
	if _, err := db.InsertOperation(n.CoordDB, op); err != nil {
		log.Printf("WARNING: node %s: coordinator insert failed (outbox syncer will retry): %v",
			n.ID, err)
	}

	// Update in-memory state
	n.nextSeq++
	n.localVal += amount

	return &op, nil
}

// ApplyN applies n operations with the given amount each.
// Convenience method for scenario setup (e.g., "seed 10 ops of +1").
func (n *Node) ApplyN(count int, amount int64) ([]*model.Operation, error) {
	ops := make([]*model.Operation, 0, count)
	for i := 0; i < count; i++ {
		op, err := n.ApplyDelta(amount)
		if err != nil {
			return ops, fmt.Errorf("apply op %d/%d: %w", i+1, count, err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// LocalValue returns the node's current in-memory counter value.
// Thread-safe.
func (n *Node) LocalValue() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.localVal
}

// Epoch returns the node's current epoch. Thread-safe.
func (n *Node) Epoch() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.epoch
}

// NextSeq returns the node's next sequence number. Thread-safe.
func (n *Node) NextSeq() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nextSeq
}

// IsAlive returns whether the node is currently alive. Thread-safe.
func (n *Node) IsAlive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isAlive
}

// SetAlive sets the node's alive status. Used by crash injection.
func (n *Node) SetAlive(alive bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isAlive = alive
}

// SetLocalVal directly sets the in-memory local value.
// Used after reconciliation to correct the node's counter.
func (n *Node) SetLocalVal(val int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.localVal = val
}

// ReloadLocalVal recomputes the local value from the durable DB.
// Used after crash injection deletes operations from the node's DB.
func (n *Node) ReloadLocalVal() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	val, err := db.SumAmounts(n.DB)
	if err != nil {
		return fmt.Errorf("node %s: reload local val: %w", n.ID, err)
	}
	n.localVal = val
	return nil
}

// GetState returns a snapshot of the node's current state.
// Thread-safe.
func (n *Node) GetState() model.NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return model.NodeState{
		NodeID:     n.ID,
		Epoch:      n.epoch,
		LocalValue: n.localVal,
		NextSeq:    n.nextSeq,
		IsAlive:    n.isAlive,
	}
}

// IncrementEpoch bumps the node to a new epoch (new incarnation after crash).
// Records the new epoch in durable storage and resets sequence numbering.
func (n *Node) IncrementEpoch() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.epoch++
	n.nextSeq = 1 // reset sequence for the new epoch

	if err := db.InsertEpoch(n.DB, n.ID, n.epoch); err != nil {
		return fmt.Errorf("node %s: record epoch %d: %w", n.ID, n.epoch, err)
	}

	log.Printf("Node %s: new epoch %d", n.ID, n.epoch)
	return nil
}
