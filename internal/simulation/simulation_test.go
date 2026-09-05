package simulation_test

import (
	"testing"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// setupTestSimulation creates a temporary simulation with the given node count.
func setupTestSimulation(t *testing.T, nodeCount int) (*simulation.Simulation, func()) {
	t.Helper()
	dir := t.TempDir()

	if err := db.EnsureDir(dir, true); err != nil {
		t.Fatalf("ensure dir: %v", err)
	}

	coordDB, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		t.Fatalf("open coord DB: %v", err)
	}

	cfg := config.Config{
		Nodes: nodeCount,
		Seed:  42,
		DBDir: dir,
	}

	sim, err := simulation.NewSimulation(cfg, coordDB)
	if err != nil {
		coordDB.Close()
		t.Fatalf("new simulation: %v", err)
	}

	cleanup := func() {
		sim.Close()
		coordDB.Close()
	}
	return sim, cleanup
}

func TestGlobalAggregation(t *testing.T) {
	sim, cleanup := setupTestSimulation(t, 3)
	defer cleanup()

	// node-0: 10 ops × 1 = 10
	n0 := sim.GetNode("node-0")
	if n0 == nil {
		t.Fatal("node-0 not found")
	}
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply to node-0: %v", err)
	}

	// node-1: 5 ops × 2 = 10
	n1 := sim.GetNode("node-1")
	if n1 == nil {
		t.Fatal("node-1 not found")
	}
	if _, err := n1.ApplyN(5, 2); err != nil {
		t.Fatalf("apply to node-1: %v", err)
	}

	// node-2: 4 ops × 5 = 20
	n2 := sim.GetNode("node-2")
	if n2 == nil {
		t.Fatal("node-2 not found")
	}
	if _, err := n2.ApplyN(4, 5); err != nil {
		t.Fatalf("apply to node-2: %v", err)
	}

	// Expected naive global: 10 + 10 + 20 = 40
	naive := sim.NaiveGlobalValue()
	if naive != 40 {
		t.Errorf("NaiveGlobalValue() = %d, want 40", naive)
	}
}

func TestNaiveVsAuthoritative(t *testing.T) {
	sim, cleanup := setupTestSimulation(t, 3)
	defer cleanup()

	// Apply varied ops to each node
	n0 := sim.GetNode("node-0")
	n1 := sim.GetNode("node-1")
	n2 := sim.GetNode("node-2")

	if _, err := n0.ApplyN(10, 3); err != nil {
		t.Fatalf("apply to node-0: %v", err)
	}
	if _, err := n1.ApplyN(7, 5); err != nil {
		t.Fatalf("apply to node-1: %v", err)
	}
	if _, err := n2.ApplyN(4, 8); err != nil {
		t.Fatalf("apply to node-2: %v", err)
	}

	// Before any crash, both values must agree
	naive := sim.NaiveGlobalValue()
	auth, err := sim.AuthoritativeGlobalValue()
	if err != nil {
		t.Fatalf("AuthoritativeGlobalValue: %v", err)
	}

	// Expected: (10×3) + (7×5) + (4×8) = 30 + 35 + 32 = 97
	if naive != 97 {
		t.Errorf("NaiveGlobalValue() = %d, want 97", naive)
	}
	if auth != 97 {
		t.Errorf("AuthoritativeGlobalValue() = %d, want 97", auth)
	}
	if naive != auth {
		t.Errorf("naive (%d) != authoritative (%d) — they should agree before any crash", naive, auth)
	}
}

func TestNodeLookup(t *testing.T) {
	sim, cleanup := setupTestSimulation(t, 3)
	defer cleanup()

	for _, id := range []string{"node-0", "node-1", "node-2"} {
		n := sim.GetNode(id)
		if n == nil {
			t.Errorf("GetNode(%q) returned nil", id)
			continue
		}
		if n.ID != id {
			t.Errorf("GetNode(%q).ID = %q", id, n.ID)
		}
	}

	// Non-existent node
	if n := sim.GetNode("node-99"); n != nil {
		t.Errorf("GetNode(\"node-99\") should be nil, got %v", n)
	}
}
