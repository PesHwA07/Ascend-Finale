package node_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/node"
)

// setupTestNode creates a temporary directory with fresh node + coordinator DBs,
// initializes a Node, and returns it along with a cleanup function.
func setupTestNode(t *testing.T, nodeID string) (*node.Node, func()) {
	t.Helper()
	dir := t.TempDir()

	nodeDB, err := db.OpenNodeDB(dir, nodeID)
	if err != nil {
		t.Fatalf("open node DB: %v", err)
	}

	// Initialize outbox schema (required since v3 transactional outbox)
	if err := db.InitOutboxSchema(nodeDB); err != nil {
		nodeDB.Close()
		t.Fatalf("init outbox schema: %v", err)
	}

	coordDB, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		nodeDB.Close()
		t.Fatalf("open coord DB: %v", err)
	}

	n, err := node.NewNode(nodeID, nodeDB, coordDB)
	if err != nil {
		nodeDB.Close()
		coordDB.Close()
		t.Fatalf("new node: %v", err)
	}

	cleanup := func() {
		nodeDB.Close()
		coordDB.Close()
	}
	return n, cleanup
}

func TestApplyDelta(t *testing.T) {
	n, cleanup := setupTestNode(t, "test-node")
	defer cleanup()

	op, err := n.ApplyDelta(5)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}

	if op == nil {
		t.Fatal("ApplyDelta returned nil operation")
	}
	if op.Amount != 5 {
		t.Errorf("op.Amount = %d, want 5", op.Amount)
	}
	if op.NodeID != "test-node" {
		t.Errorf("op.NodeID = %q, want %q", op.NodeID, "test-node")
	}
	if n.LocalValue() != 5 {
		t.Errorf("LocalValue() = %d, want 5", n.LocalValue())
	}
}

func TestApplyMultiple(t *testing.T) {
	n, cleanup := setupTestNode(t, "test-node")
	defer cleanup()

	ops, err := n.ApplyN(20, 3)
	if err != nil {
		t.Fatalf("ApplyN: %v", err)
	}

	if len(ops) != 20 {
		t.Errorf("len(ops) = %d, want 20", len(ops))
	}

	// 20 ops × 3 = 60
	if n.LocalValue() != 60 {
		t.Errorf("LocalValue() = %d, want 60", n.LocalValue())
	}
}

func TestDualWrite(t *testing.T) {
	dir := t.TempDir()

	nodeDB, err := db.OpenNodeDB(dir, "dual-test")
	if err != nil {
		t.Fatalf("open node DB: %v", err)
	}
	defer nodeDB.Close()
	if err := db.InitOutboxSchema(nodeDB); err != nil {
		t.Fatalf("init outbox schema: %v", err)
	}

	coordDB, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		t.Fatalf("open coord DB: %v", err)
	}
	defer coordDB.Close()

	n, err := node.NewNode("dual-test", nodeDB, coordDB)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	_, err = n.ApplyN(5, 10)
	if err != nil {
		t.Fatalf("ApplyN: %v", err)
	}

	// Check node DB
	nodeSum, err := db.SumAmounts(nodeDB)
	if err != nil {
		t.Fatalf("node SumAmounts: %v", err)
	}
	if nodeSum != 50 {
		t.Errorf("node DB sum = %d, want 50", nodeSum)
	}

	// Check coordinator DB
	coordSum, err := db.SumAmounts(coordDB)
	if err != nil {
		t.Fatalf("coord SumAmounts: %v", err)
	}
	if coordSum != 50 {
		t.Errorf("coordinator DB sum = %d, want 50", coordSum)
	}
}

func TestSequenceMonotonicity(t *testing.T) {
	n, cleanup := setupTestNode(t, "seq-test")
	defer cleanup()

	var prevSeq int64 = -1
	for i := 0; i < 10; i++ {
		op, err := n.ApplyDelta(1)
		if err != nil {
			t.Fatalf("ApplyDelta %d: %v", i, err)
		}
		if op.Sequence <= prevSeq {
			t.Errorf("op %d: sequence %d <= previous %d", i, op.Sequence, prevSeq)
		}
		prevSeq = op.Sequence
	}
}

func TestUUIDUniqueness(t *testing.T) {
	n, cleanup := setupTestNode(t, "uuid-test")
	defer cleanup()

	ops, err := n.ApplyN(100, 1)
	if err != nil {
		t.Fatalf("ApplyN: %v", err)
	}

	seen := make(map[string]bool, len(ops))
	for _, op := range ops {
		if seen[op.OperationID] {
			t.Errorf("duplicate OperationID: %s", op.OperationID)
		}
		seen[op.OperationID] = true
	}
}

func TestInsertDuplicateIgnored(t *testing.T) {
	dir := t.TempDir()

	nodeDB, err := db.OpenNodeDB(dir, "dup-test")
	if err != nil {
		t.Fatalf("open node DB: %v", err)
	}
	defer nodeDB.Close()
	if err := db.InitOutboxSchema(nodeDB); err != nil {
		t.Fatalf("init outbox schema: %v", err)
	}

	coordDB, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		t.Fatalf("open coord DB: %v", err)
	}
	defer coordDB.Close()

	n, err := node.NewNode("dup-test", nodeDB, coordDB)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	// Apply one operation
	op, err := n.ApplyDelta(42)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}

	// Try to insert the same operation directly into node DB
	inserted, err := db.InsertOperation(nodeDB, *op)
	if err != nil {
		t.Fatalf("InsertOperation duplicate: %v", err)
	}
	if inserted {
		t.Error("duplicate insert should return inserted=false, got true")
	}

	// Verify count is still 1
	count, err := db.CountOperations(nodeDB)
	if err != nil {
		t.Fatalf("CountOperations: %v", err)
	}
	if count != 1 {
		t.Errorf("operation count = %d, want 1", count)
	}
}

func TestSequenceRecovery(t *testing.T) {
	dir := t.TempDir()

	nodeDB, err := db.OpenNodeDB(dir, "recovery-test")
	if err != nil {
		t.Fatalf("open node DB: %v", err)
	}
	if err := db.InitOutboxSchema(nodeDB); err != nil {
		t.Fatalf("init outbox schema: %v", err)
	}

	coordDB, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		nodeDB.Close()
		t.Fatalf("open coord DB: %v", err)
	}

	// Create first incarnation and apply some ops
	n1, err := node.NewNode("recovery-test", nodeDB, coordDB)
	if err != nil {
		nodeDB.Close()
		coordDB.Close()
		t.Fatalf("new node (1st): %v", err)
	}

	_, err = n1.ApplyN(5, 10)
	if err != nil {
		nodeDB.Close()
		coordDB.Close()
		t.Fatalf("ApplyN: %v", err)
	}

	firstEpoch := n1.Epoch()
	firstLocalVal := n1.LocalValue()

	// Close and reopen DBs (simulate restart)
	nodeDB.Close()
	coordDB.Close()

	nodeDB2, err := db.OpenNodeDB(dir, "recovery-test")
	if err != nil {
		t.Fatalf("reopen node DB: %v", err)
	}
	defer nodeDB2.Close()
	if err := db.InitOutboxSchema(nodeDB2); err != nil {
		t.Fatalf("init outbox schema (reopen): %v", err)
	}

	coordDB2, err := db.OpenCoordinatorDB(dir)
	if err != nil {
		t.Fatalf("reopen coord DB: %v", err)
	}
	defer coordDB2.Close()

	// Create second incarnation — should recover state
	n2, err := node.NewNode("recovery-test", nodeDB2, coordDB2)
	if err != nil {
		t.Fatalf("new node (2nd): %v", err)
	}

	// Epoch should have incremented
	if n2.Epoch() != firstEpoch+1 {
		t.Errorf("epoch = %d, want %d (incremented)", n2.Epoch(), firstEpoch+1)
	}

	// Local value should be recovered from DB
	if n2.LocalValue() != firstLocalVal {
		t.Errorf("localVal = %d, want %d (recovered)", n2.LocalValue(), firstLocalVal)
	}

	// Sequence should start at 1 for the new epoch (no ops in new epoch yet)
	if n2.NextSeq() != 1 {
		t.Errorf("nextSeq = %d, want 1 (new epoch)", n2.NextSeq())
	}

	// Apply more ops in new epoch — should work without conflict
	op, err := n2.ApplyDelta(7)
	if err != nil {
		t.Fatalf("ApplyDelta in new epoch: %v", err)
	}
	if op.Epoch != firstEpoch+1 {
		t.Errorf("op.Epoch = %d, want %d", op.Epoch, firstEpoch+1)
	}
	if op.Sequence != 1 {
		t.Errorf("op.Sequence = %d, want 1", op.Sequence)
	}

	// Clean up temp directory manually since t.TempDir() might not work across
	// the DB close/reopen cycle on Windows
	_ = os.RemoveAll(filepath.Join(dir))
}
