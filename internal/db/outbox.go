package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/model"
)

// outboxSchema creates the operation_outbox table in each node DB.
// The outbox guarantees that every operation written to the node DB
// will eventually be synced to the coordinator — even if the coordinator
// was temporarily unreachable during the original write.
//
// Pattern: Transactional Outbox (https://microservices.io/patterns/data/transactional-outbox.html)
const outboxSchema = `
CREATE TABLE IF NOT EXISTS operation_outbox (
    operation_id   TEXT    PRIMARY KEY,
    counter_id     TEXT    NOT NULL,
    node_id        TEXT    NOT NULL,
    epoch          INTEGER NOT NULL,
    sequence       INTEGER NOT NULL,
    amount         INTEGER NOT NULL,
    created_at     TEXT    NOT NULL,
    synced         INTEGER NOT NULL DEFAULT 0,
    sync_attempts  INTEGER NOT NULL DEFAULT 0,
    last_attempt   TEXT    NOT NULL DEFAULT '',
    error_message  TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_outbox_unsynced
    ON operation_outbox (synced) WHERE synced = 0;
`

// InitOutboxSchema applies the outbox table schema to a node DB.
func InitOutboxSchema(d *sql.DB) error {
	_, err := d.Exec(outboxSchema)
	if err != nil {
		return fmt.Errorf("apply outbox schema: %w", err)
	}
	return nil
}

// InsertOutboxEntry inserts an operation into the outbox using the provided
// transaction. This MUST be called in the same transaction as InsertOperation
// to guarantee atomicity — either both succeed or both fail.
func InsertOutboxEntryTx(tx *sql.Tx, op model.Operation) error {
	_, err := tx.Exec(`
		INSERT OR IGNORE INTO operation_outbox (
			operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, op.OperationID, op.CounterID, op.NodeID, op.Epoch, op.Sequence,
		op.Amount, op.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert outbox entry: %w", err)
	}
	return nil
}

// InsertOperationTx inserts an operation within a transaction (for outbox pattern).
func InsertOperationTx(tx *sql.Tx, op model.Operation) (inserted bool, err error) {
	result, err := tx.Exec(`
		INSERT OR IGNORE INTO operations (
			operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, op.OperationID, op.CounterID, op.NodeID, op.Epoch, op.Sequence,
		op.Amount, op.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return false, fmt.Errorf("insert operation tx: %w", err)
	}
	changes, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return changes > 0, nil
}

// GetUnsyncedOutboxOps fetches a batch of operations that haven't been synced
// to the coordinator yet. Used by the OutboxSyncer agent.
func GetUnsyncedOutboxOps(d *sql.DB, limit int) ([]model.Operation, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM operation_outbox
		WHERE synced = 0 AND sync_attempts < 5
		ORDER BY created_at
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query unsynced outbox: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// MarkOutboxSynced marks an outbox entry as successfully synced.
func MarkOutboxSynced(d *sql.DB, operationID string) error {
	_, err := d.Exec(`
		UPDATE operation_outbox SET synced = 1, last_attempt = ?
		WHERE operation_id = ?
	`, time.Now().Format(time.RFC3339Nano), operationID)
	return err
}

// MarkOutboxFailed increments the sync_attempts counter and records the error.
func MarkOutboxFailed(d *sql.DB, operationID, errMsg string) error {
	_, err := d.Exec(`
		UPDATE operation_outbox
		SET sync_attempts = sync_attempts + 1,
		    last_attempt = ?,
		    error_message = ?
		WHERE operation_id = ?
	`, time.Now().Format(time.RFC3339Nano), errMsg, operationID)
	return err
}

// GetPoisonPillOps returns outbox entries that have failed 5+ times.
// These are candidates for the dead letter queue.
func GetPoisonPillOps(d *sql.DB) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM operation_outbox
		WHERE synced = 0 AND sync_attempts >= 5
		ORDER BY created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("query poison pills: %w", err)
	}
	defer rows.Close()
	return scanOperations(rows)
}

// DeleteOutboxEntry removes a synced or DLQ'd entry from the outbox.
func DeleteOutboxEntry(d *sql.DB, operationID string) error {
	_, err := d.Exec(`DELETE FROM operation_outbox WHERE operation_id = ?`, operationID)
	return err
}

// OutboxStats returns counts of pending, synced, and failed outbox entries.
type OutboxStats struct {
	Pending int64 `json:"pending"`
	Synced  int64 `json:"synced"`
	Failed  int64 `json:"failed"`
}

// GetOutboxStats returns aggregate outbox statistics for a node DB.
func GetOutboxStats(d *sql.DB) (*OutboxStats, error) {
	var s OutboxStats
	err := d.QueryRow(`SELECT COUNT(*) FROM operation_outbox WHERE synced = 0 AND sync_attempts < 5`).Scan(&s.Pending)
	if err != nil {
		return nil, err
	}
	err = d.QueryRow(`SELECT COUNT(*) FROM operation_outbox WHERE synced = 1`).Scan(&s.Synced)
	if err != nil {
		return nil, err
	}
	err = d.QueryRow(`SELECT COUNT(*) FROM operation_outbox WHERE synced = 0 AND sync_attempts >= 5`).Scan(&s.Failed)
	if err != nil {
		return nil, err
	}
	return &s, nil
}
