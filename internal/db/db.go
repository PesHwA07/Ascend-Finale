// Package db handles SQLite database initialization and schema setup.
// Each simulated node gets its own SQLite file (avoids write contention
// per PRD §12 risk mitigation), plus one coordinator DB that serves as
// the authoritative operation log for reconciliation.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

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
func OpenNodeDB(dbDir, nodeID string) (*sql.DB, error) {
	path := filepath.Join(dbDir, fmt.Sprintf("node_%s.db", nodeID))
	return openAndMigrate(path)
}

// OpenCoordinatorDB opens (or creates) the coordinator database.
// This is the authoritative log — every operation from every node gets
// copied here. Reconciliation compares node manifests against this.
func OpenCoordinatorDB(dbDir string) (*sql.DB, error) {
	path := filepath.Join(dbDir, "coordinator.db")
	return openAndMigrate(path)
}

// openAndMigrate opens a SQLite DB at the given path, applies pragmas
// for performance, and runs schema migrations.
func openAndMigrate(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
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
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set pragma %q: %w", p, err)
		}
	}

	// Apply schema
	if _, err := db.Exec(operationsSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to apply operations schema: %w", err)
	}
	if _, err := db.Exec(epochsSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to apply epochs schema: %w", err)
	}

	return db, nil
}
