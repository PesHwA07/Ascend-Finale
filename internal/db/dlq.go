package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// dlqSchema creates the dead_letter_queue table in the coordinator DB.
// Operations that repeatedly fail outbox sync (5+ attempts) are moved here
// for manual inspection rather than being retried forever.
const dlqSchema = `
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id   TEXT    NOT NULL UNIQUE,
    node_id        TEXT    NOT NULL,
    error_reason   TEXT    NOT NULL,
    retry_count    INTEGER NOT NULL,
    failed_at      TEXT    NOT NULL,
    payload        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dlq_node    ON dead_letter_queue (node_id);
CREATE INDEX IF NOT EXISTS idx_dlq_failed  ON dead_letter_queue (failed_at);
`

// DLQEntry represents a dead letter queue record.
type DLQEntry struct {
	ID          int64                  `json:"id"`
	OperationID string                 `json:"operation_id"`
	NodeID      string                 `json:"node_id"`
	ErrorReason string                 `json:"error_reason"`
	RetryCount  int                    `json:"retry_count"`
	FailedAt    time.Time              `json:"failed_at"`
	Payload     map[string]interface{} `json:"payload"`
}

// InitDLQSchema applies the dead_letter_queue table schema to the coordinator DB.
func InitDLQSchema(d *sql.DB) error {
	_, err := d.Exec(dlqSchema)
	if err != nil {
		return fmt.Errorf("apply DLQ schema: %w", err)
	}
	return nil
}

// InsertDLQEntry moves a failed operation into the dead letter queue.
func InsertDLQEntry(d *sql.DB, entry DLQEntry) error {
	payloadJSON, err := json.Marshal(entry.Payload)
	if err != nil {
		return fmt.Errorf("marshal DLQ payload: %w", err)
	}

	_, err = d.Exec(`
		INSERT OR IGNORE INTO dead_letter_queue
			(operation_id, node_id, error_reason, retry_count, failed_at, payload)
		VALUES (?, ?, ?, ?, ?, ?)
	`, entry.OperationID, entry.NodeID, entry.ErrorReason,
		entry.RetryCount, entry.FailedAt.Format(time.RFC3339Nano),
		string(payloadJSON))
	if err != nil {
		return fmt.Errorf("insert DLQ entry: %w", err)
	}
	return nil
}

// GetDLQEntries retrieves all dead letter queue entries, newest first.
func GetDLQEntries(d *sql.DB, limit int) ([]DLQEntry, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	rows, err := d.Query(`
		SELECT id, operation_id, node_id, error_reason, retry_count, failed_at, payload
		FROM dead_letter_queue
		ORDER BY id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query DLQ: %w", err)
	}
	defer rows.Close()

	var entries []DLQEntry
	for rows.Next() {
		var e DLQEntry
		var failedAtStr, payloadStr string
		err := rows.Scan(&e.ID, &e.OperationID, &e.NodeID, &e.ErrorReason,
			&e.RetryCount, &failedAtStr, &payloadStr)
		if err != nil {
			return nil, fmt.Errorf("scan DLQ row: %w", err)
		}
		e.FailedAt, _ = time.Parse(time.RFC3339Nano, failedAtStr)
		if err := json.Unmarshal([]byte(payloadStr), &e.Payload); err != nil {
			e.Payload = map[string]interface{}{"raw": payloadStr}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// GetDLQCount returns the number of entries in the dead letter queue.
func GetDLQCount(d *sql.DB) (int64, error) {
	var count int64
	err := d.QueryRow(`SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count)
	return count, err
}
