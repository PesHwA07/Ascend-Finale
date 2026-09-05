// Package simulation orchestrates all simulated nodes and provides
// global-level views of the counter system. It owns node lifecycle,
// exposes both naive and authoritative aggregation, and is the
// primary API surface for the HTTP server and test harness.
package simulation

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/model"
	"github.com/PesHwA07/Ascend-Finale/internal/node"
)

// Simulation manages N simulated nodes and their shared coordinator DB.
type Simulation struct {
	Nodes   []*node.Node
	CoordDB *sql.DB
	Seed    int64
	Rng     *rand.Rand

	mu      sync.RWMutex
	nodeDBs []*sql.DB // tracked for cleanup
	dbDir   string
}

// NewSimulation creates a simulation with the specified number of nodes.
// It opens per-node SQLite databases and initializes each node,
// recovering any existing state from durable storage.
func NewSimulation(cfg config.Config, coordDB *sql.DB) (*Simulation, error) {
	s := &Simulation{
		CoordDB: coordDB,
		Seed:    cfg.Seed,
		Rng:     rand.New(rand.NewSource(cfg.Seed)),
		dbDir:   cfg.DBDir,
	}

	for i := 0; i < cfg.Nodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i)

		nodeDB, err := db.OpenNodeDB(cfg.DBDir, nodeID)
		if err != nil {
			s.Close() // clean up already-opened DBs
			return nil, fmt.Errorf("open node DB %s: %w", nodeID, err)
		}
		s.nodeDBs = append(s.nodeDBs, nodeDB)

		n, err := node.NewNode(nodeID, nodeDB, coordDB)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("init node %s: %w", nodeID, err)
		}
		s.Nodes = append(s.Nodes, n)
	}

	log.Printf("Simulation initialized: %d nodes, seed=%d", len(s.Nodes), s.Seed)
	return s, nil
}

// NaiveGlobalValue returns the sum of all nodes' in-memory local values.
// This is what a naive system would report — it's WRONG after a crash
// because the crashed node's local value reflects the post-loss state.
func (s *Simulation) NaiveGlobalValue() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var sum int64
	for _, n := range s.Nodes {
		sum += n.LocalValue()
	}
	return sum
}

// AuthoritativeGlobalValue computes the global counter from the coordinator
// DB's durable operation log. This is the ground truth — it includes every
// operation that was ever successfully written, regardless of node crashes.
func (s *Simulation) AuthoritativeGlobalValue() (int64, error) {
	return db.SumAmounts(s.CoordDB)
}

// GetNode returns the node with the given ID, or nil if not found.
func (s *Simulation) GetNode(id string) *node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, n := range s.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// AllStates returns a snapshot of every node's current state.
func (s *Simulation) AllStates() []model.NodeState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	states := make([]model.NodeState, len(s.Nodes))
	for i, n := range s.Nodes {
		states[i] = n.GetState()
	}
	return states
}

// Close shuts down all node databases. The coordinator DB is NOT closed
// here — it's owned by the caller (main.go).
func (s *Simulation) Close() {
	for _, d := range s.nodeDBs {
		if d != nil {
			d.Close()
		}
	}
	log.Println("Simulation shut down: all node DBs closed")
}

// GetCoordinatorOperations returns all operations from the coordinator DB.
// Used by the audit trail API to show the complete authoritative log.
func (s *Simulation) GetCoordinatorOperations() ([]model.Operation, error) {
	return db.GetAllOperations(s.CoordDB)
}
