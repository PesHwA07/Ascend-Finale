// Package db — Postgres coordinator support.
//
// When the -postgres flag is provided, CounterGhost uses a real Postgres
// database for the coordinator (source of truth) instead of SQLite.
// Node-local storage remains SQLite — this is architecturally correct
// because nodes keep cheap local storage while the central authority
// uses a production-grade database.
package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"time"

	_ "github.com/lib/pq" // Postgres driver
)

// postgresCoordSchema is the schema applied to Postgres coordinator.
// Uses ON CONFLICT DO NOTHING (Postgres equivalent of INSERT OR IGNORE).
const postgresCoordSchema = `
CREATE TABLE IF NOT EXISTS counter_operations (
    operation_id    TEXT        PRIMARY KEY,
    counter_id      TEXT        NOT NULL,
    node_id         TEXT        NOT NULL,
    epoch           INTEGER     NOT NULL,
    sequence        INTEGER     NOT NULL,
    amount          INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (node_id, epoch, sequence)
);

CREATE INDEX IF NOT EXISTS idx_ops_node_epoch_seq
    ON counter_operations(node_id, epoch, sequence);
CREATE INDEX IF NOT EXISTS idx_ops_counter
    ON counter_operations(counter_id);
CREATE INDEX IF NOT EXISTS idx_ops_created_at
    ON counter_operations(created_at);

CREATE TABLE IF NOT EXISTS node_epochs (
    node_id     TEXT        NOT NULL,
    epoch       INTEGER     NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, epoch)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id              SERIAL      PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action          TEXT        NOT NULL,
    resource        TEXT        NOT NULL,
    resource_id     TEXT        NOT NULL,
    details         JSONB       NOT NULL DEFAULT '{}',
    success         BOOLEAN     NOT NULL DEFAULT TRUE,
    error_msg       TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              SERIAL      PRIMARY KEY,
    operation_id    TEXT        NOT NULL UNIQUE,
    node_id         TEXT        NOT NULL,
    error_reason    TEXT        NOT NULL,
    retry_count     INTEGER     NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload         JSONB       NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_dlq_node ON dead_letter_queue(node_id);
CREATE INDEX IF NOT EXISTS idx_dlq_failed ON dead_letter_queue(failed_at);
`

// InitPostgresSchemas applies the migrations/001_initial.sql script to Postgres.
// Used when the -reset flag is passed to ensure tables exist and are clean.
func InitPostgresSchemas(db *sql.DB, projectRoot string) error {
	schemaPath := filepath.Join(projectRoot, "migrations", "001_initial.sql")
	bytes, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("could not read migrations: %w", err)
	}
	
	// Execute the entire schema script
	_, err = db.Exec(string(bytes))
	return err
}

// OpenPostgresCoordinator connects to Postgres and applies the schema.
// connStr example: "postgres://counterghost:counterghost_dev@localhost:5432/counterghost?sslmode=disable"
func OpenPostgresCoordinator(connStr string) (*sql.DB, error) {
	d, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres: %w", err)
	}

	// Connection pool tuning for coordinator workload
	d.SetMaxOpenConns(10)
	d.SetMaxIdleConns(5)
	d.SetConnMaxLifetime(5 * time.Minute)

	// Verify connectivity
	if err := d.Ping(); err != nil {
		d.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	// Apply schema
	if _, err := d.Exec(postgresCoordSchema); err != nil {
		d.Close()
		return nil, fmt.Errorf("failed to apply postgres schema: %w", err)
	}

	log.Println("Coordinator DB ready (Postgres)")
	return d, nil
}

// IsPostgres returns true if the database connection is to Postgres.
// Used to select the correct SQL dialect at runtime.
func IsPostgres(d *sql.DB) bool {
	// lib/pq driver name is "postgres"
	return d.Driver() != nil && fmt.Sprintf("%T", d.Driver()) == "*pq.Driver"
}
