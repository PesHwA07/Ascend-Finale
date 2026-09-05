package simulation_test

import (
	"testing"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// setupReconcileTestSimulation creates a 3-node simulation for reconciliation tests.
func setupReconcileTestSimulation(t *testing.T) (*simulation.Simulation, func()) {
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
		Nodes: 3,
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

// TestReconcile_FullScenarioA is the PRD's Scenario A end-to-end:
// Seed ops → crash → verify naive≠auth → reconcile → verify naive==auth
func TestReconcile_FullScenarioA(t *testing.T) {
	sim, cleanup := setupReconcileTestSimulation(t)
	defer cleanup()

	// Seed: 3 nodes, 20 ops each at +1 = 60 total
	for _, id := range []string{"node-0", "node-1", "node-2"} {
		n := sim.GetNode(id)
		if _, err := n.ApplyN(20, 1); err != nil {
			t.Fatalf("apply to %s: %v", id, err)
		}
	}

	// Pre-crash: both values agree
	naive := sim.NaiveGlobalValue()
	auth, _ := sim.AuthoritativeGlobalValue()
	if naive != 60 {
		t.Fatalf("pre-crash naive = %d, want 60", naive)
	}
	if naive != auth {
		t.Fatalf("pre-crash: naive (%d) != auth (%d)", naive, auth)
	}

	// Crash node-0: delete 8 operations
	crashResult, err := sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}
	t.Logf("Crash: deleted %d ops (amount=%d), localVal %d → %d",
		crashResult.DeletedCount, crashResult.DeletedAmount,
		crashResult.OldLocalValue, crashResult.NewLocalValue)

	// Post-crash: naive is WRONG, auth is correct
	naiveAfterCrash := sim.NaiveGlobalValue()
	authAfterCrash, _ := sim.AuthoritativeGlobalValue()
	if naiveAfterCrash == authAfterCrash {
		t.Fatal("after crash: naive should NOT equal authoritative")
	}
	if authAfterCrash != 60 {
		t.Errorf("auth after crash = %d, want 60 (unchanged)", authAfterCrash)
	}
	t.Logf("Post-crash: naive=%d, auth=%d (gap=%d)",
		naiveAfterCrash, authAfterCrash, authAfterCrash-naiveAfterCrash)

	// Reconcile node-0
	result, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Verify reconciliation found and fixed the missing ops
	if result.MissingCount != 8 {
		t.Errorf("missing count = %d, want 8", result.MissingCount)
	}
	if result.ReplayedCount != 8 {
		t.Errorf("replayed count = %d, want 8", result.ReplayedCount)
	}
	if result.RecoveredValue != 20 {
		t.Errorf("recovered value = %d, want 20", result.RecoveredValue)
	}

	// THE KEY INVARIANT: after reconciliation, naive == authoritative
	naiveAfterReconcile := sim.NaiveGlobalValue()
	authAfterReconcile, _ := sim.AuthoritativeGlobalValue()
	if naiveAfterReconcile != authAfterReconcile {
		t.Errorf("INVARIANT VIOLATED: after reconcile, naive (%d) != auth (%d)",
			naiveAfterReconcile, authAfterReconcile)
	}
	if naiveAfterReconcile != 60 {
		t.Errorf("after reconcile: global = %d, want 60", naiveAfterReconcile)
	}
	t.Logf("Post-reconcile: naive=%d, auth=%d ✓", naiveAfterReconcile, authAfterReconcile)
}

// TestReconcile_Idempotent verifies reconciling twice changes nothing.
func TestReconcile_Idempotent(t *testing.T) {
	sim, cleanup := setupReconcileTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 3); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Crash and reconcile
	if _, err := sim.CrashNode("node-0", 4); err != nil {
		t.Fatalf("crash: %v", err)
	}
	r1, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if r1.MissingCount != 4 {
		t.Errorf("first reconcile: missing = %d, want 4", r1.MissingCount)
	}

	// Second reconcile — should find nothing missing
	r2, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if r2.MissingCount != 0 {
		t.Errorf("second reconcile: missing = %d, want 0 (idempotent)", r2.MissingCount)
	}
	if r2.ReplayedCount != 0 {
		t.Errorf("second reconcile: replayed = %d, want 0", r2.ReplayedCount)
	}
}

// TestReconcile_NoCrash verifies reconcile on a healthy node is a no-op.
func TestReconcile_NoCrash(t *testing.T) {
	sim, cleanup := setupReconcileTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Reconcile without crashing — should find nothing
	result, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.MissingCount != 0 {
		t.Errorf("missing = %d, want 0 (no crash happened)", result.MissingCount)
	}
	if result.RecoveredValue != 10 {
		t.Errorf("recovered value = %d, want 10", result.RecoveredValue)
	}
}

// TestReconcile_ManifestGapCleared verifies that after reconciliation,
// the manifest shows a single contiguous range (no gaps).
func TestReconcile_ManifestGapCleared(t *testing.T) {
	sim, cleanup := setupReconcileTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Crash: creates a gap in ProcessedRanges
	if _, err := sim.CrashNode("node-0", 4); err != nil {
		t.Fatalf("crash: %v", err)
	}

	manifestBefore, _ := sim.BuildManifest("node-0")
	if len(manifestBefore.ProcessedRanges) < 2 {
		t.Fatalf("expected gap in manifest after crash, got ranges=%v", manifestBefore.ProcessedRanges)
	}
	t.Logf("Before reconcile: ranges=%v", manifestBefore.ProcessedRanges)

	// Reconcile: should fill the gap
	if _, err := sim.Reconcile("node-0"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	manifestAfter, _ := sim.BuildManifest("node-0")
	if len(manifestAfter.ProcessedRanges) != 1 {
		t.Errorf("after reconcile: expected 1 contiguous range, got %v", manifestAfter.ProcessedRanges)
	}
	if manifestAfter.ProcessedRanges[0] != [2]int64{1, 10} {
		t.Errorf("after reconcile: range = %v, want [1,10]", manifestAfter.ProcessedRanges[0])
	}
	t.Logf("After reconcile: ranges=%v ✓", manifestAfter.ProcessedRanges)
}

// TestReconcile_MultipleNodes verifies reconciling one node doesn't affect others.
func TestReconcile_MultipleNodes(t *testing.T) {
	sim, cleanup := setupReconcileTestSimulation(t)
	defer cleanup()

	// Seed all nodes
	for _, id := range []string{"node-0", "node-1", "node-2"} {
		n := sim.GetNode(id)
		if _, err := n.ApplyN(10, 1); err != nil {
			t.Fatalf("apply to %s: %v", id, err)
		}
	}

	// Crash only node-0
	if _, err := sim.CrashNode("node-0", 4); err != nil {
		t.Fatalf("crash: %v", err)
	}

	// node-1 and node-2 should still have localVal=10
	n1Val := sim.GetNode("node-1").LocalValue()
	n2Val := sim.GetNode("node-2").LocalValue()
	if n1Val != 10 || n2Val != 10 {
		t.Errorf("other nodes affected: node-1=%d, node-2=%d, want both 10", n1Val, n2Val)
	}

	// Reconcile node-0
	if _, err := sim.Reconcile("node-0"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// All nodes should now be at 10
	for _, id := range []string{"node-0", "node-1", "node-2"} {
		val := sim.GetNode(id).LocalValue()
		if val != 10 {
			t.Errorf("%s localVal = %d, want 10", id, val)
		}
	}
}
