package simulation_test

import (
	"testing"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// setupCrashTestSimulation creates a simulation with 3 nodes, seeds operations,
// and returns the simulation + cleanup function.
// Mirrors Scenario A setup: 3 nodes with ops summing to 100, then 20 more to node-0.
func setupCrashTestSimulation(t *testing.T) (*simulation.Simulation, func()) {
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

func TestCrashNode_PartialLoss(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	// Seed operations: each node gets some ops
	n0 := sim.GetNode("node-0")
	n1 := sim.GetNode("node-1")
	n2 := sim.GetNode("node-2")

	if _, err := n0.ApplyN(20, 1); err != nil {
		t.Fatalf("apply to node-0: %v", err)
	}
	if _, err := n1.ApplyN(10, 1); err != nil {
		t.Fatalf("apply to node-1: %v", err)
	}
	if _, err := n2.ApplyN(10, 1); err != nil {
		t.Fatalf("apply to node-2: %v", err)
	}

	// Before crash: naive == authoritative == 40
	naive := sim.NaiveGlobalValue()
	auth, _ := sim.AuthoritativeGlobalValue()
	if naive != 40 || auth != 40 {
		t.Fatalf("pre-crash: naive=%d, auth=%d, want both 40", naive, auth)
	}

	// Crash node-0, delete exactly 8 operations
	result, err := sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}

	// Verify crash result
	if result.DeletedCount != 8 {
		t.Errorf("deleted count = %d, want 8", result.DeletedCount)
	}
	if result.DeletedAmount != 8 {
		t.Errorf("deleted amount = %d, want 8 (8 ops × 1)", result.DeletedAmount)
	}
	if result.NewLocalValue != 12 {
		t.Errorf("new local value = %d, want 12 (20 - 8)", result.NewLocalValue)
	}
	if result.NewEpoch != result.OldEpoch+1 {
		t.Errorf("epoch not incremented: old=%d, new=%d", result.OldEpoch, result.NewEpoch)
	}

	// After crash: naive is WRONG, authoritative is still correct
	naiveAfter := sim.NaiveGlobalValue()
	authAfter, _ := sim.AuthoritativeGlobalValue()
	if naiveAfter != 32 {
		t.Errorf("post-crash naive = %d, want 32 (40 - 8)", naiveAfter)
	}
	if authAfter != 40 {
		t.Errorf("post-crash authoritative = %d, want 40 (unchanged)", authAfter)
	}

	// The key assertion: naive ≠ authoritative after crash
	if naiveAfter == authAfter {
		t.Error("naive should NOT equal authoritative after crash — this is the bug we're demonstrating")
	}
}

func TestCrashNode_EpochIncrement(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	epochBefore := n0.Epoch()

	_, err := sim.CrashNode("node-0", 3)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}

	if n0.Epoch() != epochBefore+1 {
		t.Errorf("epoch = %d, want %d (incremented)", n0.Epoch(), epochBefore+1)
	}

	// Node should be alive again after crash
	if !n0.IsAlive() {
		t.Error("node should be alive after crash recovery")
	}
}

func TestCrashNode_StoredOpsForReplay(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 5); err != nil {
		t.Fatalf("apply: %v", err)
	}

	_, err := sim.CrashNode("node-0", 4)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}

	// Verify crashed ops are stored for Scenario B
	stored := simulation.GetCrashedOps("node-0")
	if len(stored) != 4 {
		t.Errorf("stored crashed ops = %d, want 4", len(stored))
	}
}

func TestBuildManifest_ShowsGap(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Manifest before crash — should be one contiguous range
	manifestBefore, err := sim.BuildManifest("node-0")
	if err != nil {
		t.Fatalf("BuildManifest (before): %v", err)
	}
	if len(manifestBefore.ProcessedRanges) != 1 {
		t.Errorf("ranges before crash = %d ranges, want 1 contiguous", len(manifestBefore.ProcessedRanges))
	}
	if manifestBefore.LocalValue != 10 {
		t.Errorf("manifest local value = %d, want 10", manifestBefore.LocalValue)
	}

	// Crash: delete 4 operations (contiguous block)
	_, err = sim.CrashNode("node-0", 4)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}

	// Manifest after crash — should show a gap
	manifestAfter, err := sim.BuildManifest("node-0")
	if err != nil {
		t.Fatalf("BuildManifest (after): %v", err)
	}

	// Should have more than 1 range (gap exists)
	if len(manifestAfter.ProcessedRanges) < 2 {
		t.Errorf("ranges after crash = %v, expected at least 2 ranges (gap)", manifestAfter.ProcessedRanges)
	}
	if manifestAfter.LocalValue != 6 {
		t.Errorf("manifest local value after crash = %d, want 6 (10 - 4)", manifestAfter.LocalValue)
	}

	t.Logf("Manifest before: ranges=%v, localVal=%d", manifestBefore.ProcessedRanges, manifestBefore.LocalValue)
	t.Logf("Manifest after:  ranges=%v, localVal=%d", manifestAfter.ProcessedRanges, manifestAfter.LocalValue)
}

func TestBuildManifest_HashChanges(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(10, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	m1, _ := sim.BuildManifest("node-0")

	// Crash deletes ops — hash must change
	_, err := sim.CrashNode("node-0", 3)
	if err != nil {
		t.Fatalf("CrashNode: %v", err)
	}

	m2, _ := sim.BuildManifest("node-0")

	if m1.ProcessedOpHash == m2.ProcessedOpHash {
		t.Error("hash should change after ops are deleted")
	}
}

func TestCrashNode_AlreadyCrashed(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(5, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// First crash should succeed
	_, err := sim.CrashNode("node-0", 2)
	if err != nil {
		t.Fatalf("first crash: %v", err)
	}

	// Second crash should also work (node was restarted after first crash)
	if _, err := n0.ApplyN(3, 1); err != nil {
		t.Fatalf("apply after first crash: %v", err)
	}
	_, err = sim.CrashNode("node-0", 1)
	if err != nil {
		t.Fatalf("second crash should succeed: %v", err)
	}
}

func TestCrashNode_NoOps(t *testing.T) {
	sim, cleanup := setupCrashTestSimulation(t)
	defer cleanup()

	// Try to crash a node with no operations — should fail gracefully
	_, err := sim.CrashNode("node-0", 0)
	if err == nil {
		t.Error("crashing a node with no operations should return an error")
	}
}
