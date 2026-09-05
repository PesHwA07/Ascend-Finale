// Package db handles SQLite database initialization and schema setup.
// Each simulated node gets its own SQLite file (avoids write contention
// per PRD §12 risk mitigation), plus one coordinator DB that serves as
// the authoritative operation log for reconciliation.
//
// Design decision: Manifests are derived, not stored.
// A manifest (processed ranges, local value, highest contiguous sequence)
// is a projection computed on-demand by scanning the operation log.
// We deliberately do NOT store manifests in a separate table because:
//   - It avoids drift between a stored manifest and the actual operations
//   - The operation log is the single source of truth (event-sourcing pattern)
//   - Recomputing is cheap at demo scale (hundreds of ops, not millions)
package db


import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/model"
	_ "modernc.org/sqlite" // Pure-Go SQLite driver — no CGo, no C compiler needed
)

// operationsSchema creates the operations table.
// The UNIQUE constraint on operation_id is critical — this is what makes
// duplicate-replay rejection a storage-level guarantee (PRD §5.1).
const operationsSchema = `
CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY,
    counter_id   TEXT    NOT NULL,
    node_id      TEXT    NOT NULL,
    epoch        INTEGER NOT NULL,
    sequence     INTEGER NOT NULL,
    amount       INTEGER NOT NULL,
    created_at   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operations_node_epoch_seq
    ON operations (node_id, epoch, sequence);
`

// epochsSchema tracks node incarnations.
// Every restart increments the epoch — this is how the system knows
// a "fresh" node from a "lost state" node (PRD G4).
const epochsSchema = `
CREATE TABLE IF NOT EXISTS epochs (
    node_id       TEXT    NOT NULL,
    epoch         INTEGER NOT NULL,
    started_at    TEXT    NOT NULL,
    PRIMARY KEY (node_id, epoch)
);
`

// EnsureDir creates the data directory if it doesn't exist.
// If reset is true, wipes the directory first for a clean demo run.
func EnsureDir(dbDir string, reset bool) error {
	if reset {
		if err := os.RemoveAll(dbDir); err != nil {
			return fmt.Errorf("failed to reset db dir: %w", err)
		}
	}
	return os.MkdirAll(dbDir, 0755)
}

// OpenNodeDB opens (or creates) a per-node SQLite database and applies the schema.
// Each node gets its own file to avoid SQLite write contention across nodes.
// Connection pool is tuned for single-writer SQLite: MaxOpenConns=1.
func OpenNodeDB(dbDir, nodeID string) (*sql.DB, error) {
	path := filepath.Join(dbDir, fmt.Sprintf("node_%s.db", nodeID))
	d, err := openAndMigrate(path)
	if err != nil {
		return nil, err
	}
	// SQLite serializes writes per file; one connection avoids contention.
	d.SetMaxOpenConns(1)
	d.SetMaxIdleConns(1)
	d.SetConnMaxLifetime(0) // No expiry for embedded SQLite
	return d, nil
}

// OpenCoordinatorDB opens (or creates) the coordinator database.
// This is the authoritative log — every operation from every node gets
// copied here. Reconciliation compares node manifests against this.
// Pool allows a few concurrent reads from multiple goroutines.
func OpenCoordinatorDB(dbDir string) (*sql.DB, error) {
	path := filepath.Join(dbDir, "coordinator.db")
	d, err := openAndMigrate(path)
	if err != nil {
		return nil, err
	}
	// Allow concurrent reads; writes still serialized by SQLite.
	d.SetMaxOpenConns(4)
	d.SetMaxIdleConns(2)
	d.SetConnMaxLifetime(5 * time.Minute)
	return d, nil
}

// openAndMigrate opens a SQLite DB at the given path, applies pragmas
// for performance, and runs schema migrations.
func openAndMigrate(path string) (*sql.DB, error) {
	d, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open db %s: %w", path, err)
	}

	// WAL mode gives better concurrent read performance.
	// Journal mode pragma must be set before any other operations.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := d.Exec(p); err != nil {
			d.Close()
			return nil, fmt.Errorf("failed to set pragma %q: %w", p, err)
		}
	}

	// Apply schema
	if _, err := d.Exec(operationsSchema); err != nil {
		d.Close()
		return nil, fmt.Errorf("failed to apply operations schema: %w", err)
	}
	if _, err := d.Exec(epochsSchema); err != nil {
		d.Close()
		return nil, fmt.Errorf("failed to apply epochs schema: %w", err)
	}

	return d, nil
}

// ---------------------------------------------------------------------------
// Operation CRUD
// ---------------------------------------------------------------------------

// InsertOperation inserts an operation using INSERT OR IGNORE.
// Returns (true, nil) if the row was inserted, (false, nil) if it was a
// duplicate (silently ignored), or (false, err) on failure.
//
// This is append-only: operations are immutable events. We never use
// INSERT OR REPLACE or ON CONFLICT DO UPDATE for the event log.
func InsertOperation(d *sql.DB, op model.Operation) (inserted bool, err error) {
	result, err := d.Exec(`
		INSERT OR IGNORE INTO operations (
			operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, op.OperationID, op.CounterID, op.NodeID, op.Epoch, op.Sequence,
		op.Amount, op.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return false, fmt.Errorf("insert operation: %w", err)
	}
	changes, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return changes > 0, nil
}

// GetOperationsByNodeEpoch returns all operations for a given node and epoch,
// ordered by sequence. Used for manifest generation and reconciliation.
func GetOperationsByNodeEpoch(d *sql.DB, nodeID string, epoch int64) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM operations
		WHERE node_id = ? AND epoch = ?
		ORDER BY sequence
	`, nodeID, epoch)
	if err != nil {
		return nil, fmt.Errorf("query ops by node+epoch: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// GetOperationsByNode returns all operations for a given node across all epochs.
func GetOperationsByNode(d *sql.DB, nodeID string) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM operations
		WHERE node_id = ?
		ORDER BY epoch, sequence
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("query ops by node: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// GetAllOperations returns every operation in the database, ordered by creation time.
// Primarily used on the coordinator DB to get the authoritative log.
func GetAllOperations(d *sql.DB) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM operations
		ORDER BY created_at, sequence
	`)
	if err != nil {
		return nil, fmt.Errorf("query all ops: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// DeleteOperations removes specific operations by ID.
// Returns the number of rows deleted. Used by crash injection (Phase 3)
// to simulate partial state loss.
func DeleteOperations(d *sql.DB, opIDs []string) (int64, error) {
	if len(opIDs) == 0 {
		return 0, nil
	}
	// Build placeholders: (?, ?, ?)
	placeholders := make([]string, len(opIDs))
	args := make([]interface{}, len(opIDs))
	for i, id := range opIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(
		"DELETE FROM operations WHERE operation_id IN (%s)",
		strings.Join(placeholders, ","),
	)
	result, err := d.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete operations: %w", err)
	}
	return result.RowsAffected()
}

// DeleteAllOperations removes every operation from the given database.
// Used by projection rebuild to wipe a node DB before replaying from coordinator.
func DeleteAllOperations(d *sql.DB) (int64, error) {
	result, err := d.Exec("DELETE FROM operations")
	if err != nil {
		return 0, fmt.Errorf("delete all operations: %w", err)
	}
	return result.RowsAffected()
}

// SumAmounts returns the total sum of all operation amounts in the database.
// For a node DB, this gives the node's local counter value.
// For the coordinator DB, this gives the authoritative global value.
func SumAmounts(d *sql.DB) (int64, error) {
	var sum int64
	err := d.QueryRow(`SELECT COALESCE(SUM(amount), 0) FROM operations`).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum amounts: %w", err)
	}
	return sum, nil
}

// SumAmountsAsOf returns the total sum of amounts where created_at <= asOf.
// This enables temporal queries: "what was the counter at time T?"
func SumAmountsAsOf(d *sql.DB, asOf time.Time) (int64, error) {
	var sum int64
	err := d.QueryRow(
		`SELECT COALESCE(SUM(amount), 0) FROM operations WHERE created_at <= ?`,
		asOf,
	).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum amounts as-of: %w", err)
	}
	return sum, nil
}

// SumAmountsAsOfByNode returns the per-node sum where created_at <= asOf.
func SumAmountsAsOfByNode(d *sql.DB, nodeID string, asOf time.Time) (int64, error) {
	var sum int64
	err := d.QueryRow(
		`SELECT COALESCE(SUM(amount), 0) FROM operations WHERE node_id = ? AND created_at <= ?`,
		nodeID, asOf,
	).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum amounts as-of by node: %w", err)
	}
	return sum, nil
}

// SumAmountsByNode returns the sum of amounts for a specific node.
func SumAmountsByNode(d *sql.DB, nodeID string) (int64, error) {
	var sum int64
	err := d.QueryRow(
		`SELECT COALESCE(SUM(amount), 0) FROM operations WHERE node_id = ?`,
		nodeID,
	).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("sum amounts by node: %w", err)
	}
	return sum, nil
}

// CountOperations returns the total number of operations in the database.
func CountOperations(d *sql.DB) (int64, error) {
	var count int64
	err := d.QueryRow(`SELECT COUNT(*) FROM operations`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count operations: %w", err)
	}
	return count, nil
}

// GetMaxSequence returns the highest sequence number for a given node and epoch.
// Returns 0 if no operations exist for that node+epoch pair.
// Used on restart to recover the next sequence number.
func GetMaxSequence(d *sql.DB, nodeID string, epoch int64) (int64, error) {
	var maxSeq sql.NullInt64
	err := d.QueryRow(`
		SELECT MAX(sequence) FROM operations
		WHERE node_id = ? AND epoch = ?
	`, nodeID, epoch).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("max sequence: %w", err)
	}
	if !maxSeq.Valid {
		return 0, nil
	}
	return maxSeq.Int64, nil
}

// ---------------------------------------------------------------------------
// Epoch management
// ---------------------------------------------------------------------------

// InsertEpoch records a new epoch (incarnation) for a node.
func InsertEpoch(d *sql.DB, nodeID string, epoch int64) error {
	_, err := d.Exec(`
		INSERT INTO epochs (node_id, epoch, started_at)
		VALUES (?, ?, ?)
	`, nodeID, epoch, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert epoch: %w", err)
	}
	return nil
}

// GetLatestEpoch returns the highest epoch number for a node.
// Returns 0 if no epochs exist yet (fresh node).
func GetLatestEpoch(d *sql.DB, nodeID string) (int64, error) {
	var epoch sql.NullInt64
	err := d.QueryRow(`
		SELECT MAX(epoch) FROM epochs WHERE node_id = ?
	`, nodeID).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("get latest epoch: %w", err)
	}
	if !epoch.Valid {
		return 0, nil
	}
	return epoch.Int64, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// scanOperations reads rows from a query result into a slice of Operations.
func scanOperations(rows *sql.Rows) ([]model.Operation, error) {
	var ops []model.Operation
	for rows.Next() {
		var op model.Operation
		var createdAt string
		if err := rows.Scan(
			&op.OperationID, &op.CounterID, &op.NodeID,
			&op.Epoch, &op.Sequence, &op.Amount, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan operation: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			// Fall back to RFC3339 if nano parse fails
			t, err = time.Parse(time.RFC3339, createdAt)
			if err != nil {
				return nil, fmt.Errorf("parse created_at %q: %w", createdAt, err)
			}
		}
		op.CreatedAt = t
		ops = append(ops, op)
	}
	return ops, rows.Err()
}
