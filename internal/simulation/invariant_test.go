// invariant_test.go — Machine-checked correctness invariants (PRD §7).
//
// These are the core graded deliverable. Each test corresponds to one of the
// 6 invariants specified in the PRD, plus a full end-to-end Scenario A+B test
// that produces the required assertion block output.
package simulation_test

import (
	"testing"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// setupInvariantSimulation creates a standard 3-node simulation for invariant tests.
func setupInvariantSimulation(t *testing.T, seed int64) (*simulation.Simulation, func()) {
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
		Seed:  seed,
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

// ---------------------------------------------------------------------------
// I1: No duplicate contribution
// "count(operation_id applied) ≤ 1 for every operation"
// ---------------------------------------------------------------------------
func TestInvariant_I1_NoDuplicateContribution(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	n0 := sim.GetNode("node-0")

	// Apply 20 operations
	ops, err := n0.ApplyN(20, 1)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Verify all operation IDs are unique
	seen := make(map[string]bool, len(ops))
	for _, op := range ops {
		if seen[op.OperationID] {
			t.Fatalf("I1 VIOLATED: duplicate operation_id %s", op.OperationID)
		}
		seen[op.OperationID] = true
	}

	// Verify: trying to re-insert the same ops is rejected
	for _, op := range ops {
		inserted, err := db.InsertOperation(n0.DB, *op)
		if err != nil {
			t.Fatalf("re-insert op %s: %v", op.OperationID, err)
		}
		if inserted {
			t.Fatalf("I1 VIOLATED: duplicate op %s was inserted again", op.OperationID)
		}
	}

	// Verify count matches (no doubles)
	count, err := db.CountOperations(n0.DB)
	if err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if count != 20 {
		t.Fatalf("I1 VIOLATED: expected 20 ops, got %d (duplicates exist)", count)
	}

	t.Log("I1 PASS: No duplicate contribution — all 20 operation IDs unique, re-inserts rejected")
}

// ---------------------------------------------------------------------------
// I2: No acknowledged operation disappears
// "Every operation in a trusted global count remains discoverable in durable storage"
// ---------------------------------------------------------------------------
func TestInvariant_I2_NoAcknowledgedOpDisappears(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	// Seed all 3 nodes
	for _, id := range []string{"node-0", "node-1", "node-2"} {
		n := sim.GetNode(id)
		if _, err := n.ApplyN(20, 1); err != nil {
			t.Fatalf("apply to %s: %v", id, err)
		}
	}

	// Get all operations from coordinator (authoritative log)
	coordOps, err := db.GetAllOperations(sim.CoordDB)
	if err != nil {
		t.Fatalf("get coord ops: %v", err)
	}
	if len(coordOps) != 60 {
		t.Fatalf("expected 60 ops in coordinator, got %d", len(coordOps))
	}

	// Crash node-0 — deletes ops from node DB
	_, err = sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("crash: %v", err)
	}

	// Verify: ALL 60 ops still exist in coordinator (crash doesn't touch it)
	coordOpsAfter, err := db.GetAllOperations(sim.CoordDB)
	if err != nil {
		t.Fatalf("get coord ops after crash: %v", err)
	}
	if len(coordOpsAfter) != 60 {
		t.Fatalf("I2 VIOLATED: coordinator lost ops after crash — had 60, now %d", len(coordOpsAfter))
	}

	// Every original op must still be discoverable
	afterSet := make(map[string]bool, len(coordOpsAfter))
	for _, op := range coordOpsAfter {
		afterSet[op.OperationID] = true
	}
	for _, op := range coordOps {
		if !afterSet[op.OperationID] {
			t.Fatalf("I2 VIOLATED: operation %s disappeared from coordinator", op.OperationID)
		}
	}

	t.Log("I2 PASS: No acknowledged operation disappears — all 60 ops remain in coordinator after crash")
}

// ---------------------------------------------------------------------------
// I3: Recovery is monotonic
// "Replaying the same missing operation twice cannot increase the count twice"
// ---------------------------------------------------------------------------
func TestInvariant_I3_RecoveryIsMonotonic(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(20, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Crash, then reconcile
	_, err := sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("crash: %v", err)
	}

	r1, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	valueAfterFirst := r1.RecoveredValue

	// Reconcile again — value must NOT increase
	r2, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	valueAfterSecond := r2.RecoveredValue

	if valueAfterSecond != valueAfterFirst {
		t.Fatalf("I3 VIOLATED: value changed on second reconcile: %d → %d",
			valueAfterFirst, valueAfterSecond)
	}
	if r2.MissingCount != 0 {
		t.Fatalf("I3 VIOLATED: second reconcile found %d missing (should be 0)", r2.MissingCount)
	}

	t.Logf("I3 PASS: Recovery is monotonic — value stayed at %d after double reconcile", valueAfterFirst)
}

// ---------------------------------------------------------------------------
// I4: Epochs are unique per node
// "A restarted node cannot silently overwrite its previous incarnation's identity"
// ---------------------------------------------------------------------------
func TestInvariant_I4_EpochsAreUnique(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	if _, err := n0.ApplyN(5, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	epoch1 := n0.Epoch()

	// First crash → epoch increments
	_, err := sim.CrashNode("node-0", 2)
	if err != nil {
		t.Fatalf("first crash: %v", err)
	}
	epoch2 := n0.Epoch()

	if epoch2 <= epoch1 {
		t.Fatalf("I4 VIOLATED: epoch did not increment after crash: %d → %d", epoch1, epoch2)
	}

	// Apply more, crash again → epoch increments again
	if _, err := n0.ApplyN(3, 1); err != nil {
		t.Fatalf("apply after first crash: %v", err)
	}
	_, err = sim.CrashNode("node-0", 1)
	if err != nil {
		t.Fatalf("second crash: %v", err)
	}
	epoch3 := n0.Epoch()

	if epoch3 <= epoch2 {
		t.Fatalf("I4 VIOLATED: epoch did not increment on second crash: %d → %d", epoch2, epoch3)
	}

	// Verify all epochs are strictly increasing
	if !(epoch1 < epoch2 && epoch2 < epoch3) {
		t.Fatalf("I4 VIOLATED: epochs not strictly increasing: %d, %d, %d", epoch1, epoch2, epoch3)
	}

	t.Logf("I4 PASS: Epochs are unique — %d < %d < %d", epoch1, epoch2, epoch3)
}

// ---------------------------------------------------------------------------
// I5: Global count is reproducible
// "Recomputing from the durable operation set independently matches the live aggregate"
// ---------------------------------------------------------------------------
func TestInvariant_I5_GlobalCountReproducible(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	// Seed all nodes with varying amounts
	n0 := sim.GetNode("node-0")
	n1 := sim.GetNode("node-1")
	n2 := sim.GetNode("node-2")

	if _, err := n0.ApplyN(15, 3); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := n1.ApplyN(10, 5); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := n2.ApplyN(8, 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Expected: (15×3) + (10×5) + (8×7) = 45 + 50 + 56 = 151

	// Method 1: naive aggregation (sum of in-memory locals)
	naive := sim.NaiveGlobalValue()

	// Method 2: authoritative (SUM from coordinator DB)
	auth, err := sim.AuthoritativeGlobalValue()
	if err != nil {
		t.Fatalf("auth global: %v", err)
	}

	// Method 3: independent recomputation from durable storage
	allOps, err := db.GetAllOperations(sim.CoordDB)
	if err != nil {
		t.Fatalf("get all ops: %v", err)
	}
	var recomputed int64
	for _, op := range allOps {
		recomputed += op.Amount
	}

	// All three must agree
	if naive != auth {
		t.Fatalf("I5 VIOLATED: naive (%d) != auth (%d)", naive, auth)
	}
	if auth != recomputed {
		t.Fatalf("I5 VIOLATED: auth (%d) != recomputed (%d)", auth, recomputed)
	}
	if recomputed != 151 {
		t.Fatalf("I5 VIOLATED: expected 151, got %d", recomputed)
	}

	t.Logf("I5 PASS: Global count reproducible — naive=%d, auth=%d, recomputed=%d", naive, auth, recomputed)
}

// ---------------------------------------------------------------------------
// I6: Reconciliation is convergent/idempotent
// "Reconcile(Reconcile(state)) == Reconcile(state)"
// ---------------------------------------------------------------------------
func TestInvariant_I6_ReconciliationIdempotent(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	n0 := sim.GetNode("node-0")
	n1 := sim.GetNode("node-1")

	if _, err := n0.ApplyN(20, 2); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := n1.ApplyN(10, 3); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Crash node-0
	_, err := sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("crash: %v", err)
	}

	// First reconcile
	r1, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	state1 := r1.RecoveredValue
	naive1 := sim.NaiveGlobalValue()

	// Second reconcile — must produce identical result
	r2, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	state2 := r2.RecoveredValue
	naive2 := sim.NaiveGlobalValue()

	// Third reconcile — triple-check convergence
	r3, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	state3 := r3.RecoveredValue
	naive3 := sim.NaiveGlobalValue()

	if state1 != state2 || state2 != state3 {
		t.Fatalf("I6 VIOLATED: reconcile not idempotent: %d, %d, %d", state1, state2, state3)
	}
	if naive1 != naive2 || naive2 != naive3 {
		t.Fatalf("I6 VIOLATED: global not stable: %d, %d, %d", naive1, naive2, naive3)
	}
	if r2.MissingCount != 0 || r3.MissingCount != 0 {
		t.Fatalf("I6 VIOLATED: subsequent reconciles found missing ops: r2=%d, r3=%d",
			r2.MissingCount, r3.MissingCount)
	}

	t.Logf("I6 PASS: Reconciliation idempotent — value=%d stable across 3 reconciles", state1)
}

// ===========================================================================
// Full End-to-End: Scenario A + Scenario B with assertion block output
// ===========================================================================

func TestScenarioAB_FullEndToEnd(t *testing.T) {
	sim, cleanup := setupInvariantSimulation(t, 42)
	defer cleanup()

	// ---- SCENARIO A SETUP ----
	// Seed 3 nodes: 100 total ops at +1 each = 100
	// Then 20 more to node-0 = 120 total
	n0 := sim.GetNode("node-0")
	n1 := sim.GetNode("node-1")
	n2 := sim.GetNode("node-2")

	if _, err := n0.ApplyN(40, 1); err != nil {
		t.Fatalf("seed node-0: %v", err)
	}
	if _, err := n1.ApplyN(30, 1); err != nil {
		t.Fatalf("seed node-1: %v", err)
	}
	if _, err := n2.ApplyN(30, 1); err != nil {
		t.Fatalf("seed node-2: %v", err)
	}
	// Add 20 more to node-0: total now 100 + 20 = 120
	if _, err := n0.ApplyN(20, 1); err != nil {
		t.Fatalf("extra ops node-0: %v", err)
	}

	expectedGlobal := int64(120)

	// Verify pre-crash state
	naive := sim.NaiveGlobalValue()
	auth, _ := sim.AuthoritativeGlobalValue()
	if naive != expectedGlobal || auth != expectedGlobal {
		t.Fatalf("pre-crash: naive=%d, auth=%d, want %d", naive, auth, expectedGlobal)
	}
	t.Logf("=== SCENARIO A: Pre-crash global = %d ===", expectedGlobal)

	// ---- SCENARIO A: CRASH ----
	crashResult, err := sim.CrashNode("node-0", 8)
	if err != nil {
		t.Fatalf("crash: %v", err)
	}

	naiveAfterCrash := sim.NaiveGlobalValue()
	authAfterCrash, _ := sim.AuthoritativeGlobalValue()

	t.Logf("CRASH: deleted %d ops (amount=%d), node-0 localVal %d → %d",
		crashResult.DeletedCount, crashResult.DeletedAmount,
		crashResult.OldLocalValue, crashResult.NewLocalValue)
	t.Logf("POST-CRASH: naive=%d (WRONG), auth=%d (correct)", naiveAfterCrash, authAfterCrash)

	// naive must be wrong, auth must be unchanged
	if naiveAfterCrash == expectedGlobal {
		t.Fatal("naive should be WRONG after crash")
	}
	if authAfterCrash != expectedGlobal {
		t.Fatalf("auth should be unchanged: got %d, want %d", authAfterCrash, expectedGlobal)
	}

	// ---- SCENARIO A: RECONCILE ----
	reconResult, err := sim.Reconcile("node-0")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	naiveAfterRecon := sim.NaiveGlobalValue()
	authAfterRecon, _ := sim.AuthoritativeGlobalValue()

	t.Logf("RECONCILE: found %d missing, replayed %d, localVal %d → %d",
		reconResult.MissingCount, reconResult.ReplayedCount,
		reconResult.OldLocalValue, reconResult.RecoveredValue)

	if naiveAfterRecon != expectedGlobal {
		t.Fatalf("INVARIANT FAIL: post-reconcile naive=%d, want %d", naiveAfterRecon, expectedGlobal)
	}
	if authAfterRecon != expectedGlobal {
		t.Fatalf("INVARIANT FAIL: post-reconcile auth=%d, want %d", authAfterRecon, expectedGlobal)
	}

	// ---- SCENARIO B: DELAYED REPLAY ----
	replayResult, err := sim.ReplayDelayedOps("node-0")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	naiveAfterReplay := sim.NaiveGlobalValue()

	t.Logf("REPLAY: attempted %d, dups_ignored=%d, new_inserts=%d",
		replayResult.AttemptedCount, replayResult.DuplicatesIgnored, replayResult.NewInserts)

	if replayResult.NewInserts != 0 {
		t.Fatalf("INVARIANT FAIL: replay should insert 0 new ops, got %d", replayResult.NewInserts)
	}
	if naiveAfterReplay != expectedGlobal {
		t.Fatalf("INVARIANT FAIL: post-replay naive=%d, want %d", naiveAfterReplay, expectedGlobal)
	}

	// ---- ASSERTION BLOCK (PRD §7 required output format) ----
	t.Log("")
	t.Log("========================================")
	t.Log("  COUNTERGHOST ASSERTION BLOCK")
	t.Log("========================================")
	t.Logf("  expected_global_count        = %d", expectedGlobal)
	t.Logf("  recovered_global_count       = %d", naiveAfterRecon)
	t.Logf("  duplicate_operations_ignored = %d", replayResult.DuplicatesIgnored)

	if naiveAfterRecon == expectedGlobal {
		t.Log("  ASSERT expected == recovered   -> PASS")
	} else {
		t.Log("  ASSERT expected == recovered   -> FAIL")
	}

	if naiveAfterReplay == expectedGlobal {
		t.Log("  ASSERT post_replay == expected -> PASS")
	} else {
		t.Log("  ASSERT post_replay == expected -> FAIL")
	}

	if replayResult.DuplicatesIgnored == crashResult.DeletedCount {
		t.Log("  ASSERT dups_ignored == deleted -> PASS")
	} else {
		t.Logf("  ASSERT dups_ignored == deleted -> FAIL (got %d, want %d)",
			replayResult.DuplicatesIgnored, crashResult.DeletedCount)
	}
	t.Log("========================================")
}
