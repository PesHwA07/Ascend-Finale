// Package db — Postgres-compatible operation CRUD.
//
// These functions use Postgres SQL dialect (ON CONFLICT DO NOTHING,
// $1-style parameters, COALESCE) instead of SQLite dialect.
// They mirror the SQLite functions in db.go but target the
// counter_operations table (Postgres naming convention).
package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/model"
)

// InsertOperationPG inserts an operation into Postgres using ON CONFLICT DO NOTHING.
// Returns (true, nil) if inserted, (false, nil) if duplicate, (false, err) on failure.
func InsertOperationPG(d *sql.DB, op model.Operation) (inserted bool, err error) {
	result, err := d.Exec(`
		INSERT INTO counter_operations (
			operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (operation_id) DO NOTHING
	`, op.OperationID, op.CounterID, op.NodeID, op.Epoch, op.Sequence,
		op.Amount, op.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("pg insert operation: %w", err)
	}
	changes, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("pg rows affected: %w", err)
	}
	return changes > 0, nil
}

// SumAmountsPG returns the authoritative global count from Postgres.
func SumAmountsPG(d *sql.DB) (int64, error) {
	var total int64
	err := d.QueryRow(`SELECT COALESCE(SUM(amount), 0) FROM counter_operations`).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("pg sum amounts: %w", err)
	}
	return total, nil
}

// GetOperationsForNodePG returns all operations for a specific node from Postgres.
func GetOperationsForNodePG(d *sql.DB, nodeID string) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM counter_operations
		WHERE node_id = $1
		ORDER BY epoch, sequence
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("pg query ops for node: %w", err)
	}
	defer rows.Close()
	return scanOperationsPG(rows)
}

// GetOperationsBeforePG returns operations created before a given timestamp (temporal query).
func GetOperationsBeforePG(d *sql.DB, before time.Time) ([]model.Operation, error) {
	rows, err := d.Query(`
		SELECT operation_id, counter_id, node_id, epoch, sequence, amount, created_at
		FROM counter_operations
		WHERE created_at <= $1
		ORDER BY created_at
	`, before)
	if err != nil {
		return nil, fmt.Errorf("pg query ops before: %w", err)
	}
	defer rows.Close()
	return scanOperationsPG(rows)
}

// SumAmountsBeforePG returns the global value at a specific timestamp (time-travel).
func SumAmountsBeforePG(d *sql.DB, before time.Time) (int64, error) {
	var total int64
	err := d.QueryRow(`
		SELECT COALESCE(SUM(amount), 0)
		FROM counter_operations
		WHERE created_at <= $1
	`, before).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("pg sum before: %w", err)
	}
	return total, nil
}

// CountOperationsForNodePG returns the operation count for a node.
func CountOperationsForNodePG(d *sql.DB, nodeID string) (int64, error) {
	var count int64
	err := d.QueryRow(`
		SELECT COUNT(*) FROM counter_operations WHERE node_id = $1
	`, nodeID).Scan(&count)
	return count, err
}

// DeleteAllOperationsForNodePG removes all operations for a node (used by rebuild).
func DeleteAllOperationsForNodePG(d *sql.DB, nodeID string) (int64, error) {
	result, err := d.Exec(`DELETE FROM counter_operations WHERE node_id = $1`, nodeID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// scanOperationsPG scans rows into Operation structs from Postgres.
// Postgres returns time.Time directly (no string parsing needed).
func scanOperationsPG(rows *sql.Rows) ([]model.Operation, error) {
	var ops []model.Operation
	for rows.Next() {
		var op model.Operation
		if err := rows.Scan(
			&op.OperationID, &op.CounterID, &op.NodeID,
			&op.Epoch, &op.Sequence, &op.Amount, &op.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("pg scan operation: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// --- Audit + DLQ for Postgres ---

// InitAuditSchemaPG is a no-op because OpenPostgresCoordinator already creates the tables.
func InitAuditSchemaPG(d *sql.DB) error {
	return nil // Schema already applied in OpenPostgresCoordinator
}

// InitDLQSchemaPG is a no-op because OpenPostgresCoordinator already creates the tables.
func InitDLQSchemaPG(d *sql.DB) error {
	return nil // Schema already applied in OpenPostgresCoordinator
}

// InsertAuditLogPG inserts an audit entry into Postgres.
func InsertAuditLogPG(d *sql.DB, entry AuditEntry) error {
	detailsJSON, err := json.Marshal(entry.Details)
	if err != nil {
		detailsJSON = []byte("{}")
	}
	_, err = d.Exec(`
		INSERT INTO audit_log (timestamp, action, resource, resource_id, details, success, error_msg)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
	`, entry.Timestamp, entry.Action, entry.Resource, entry.ResourceID,
		string(detailsJSON), entry.Success, entry.ErrorMsg)
	return err
}

// GetAuditLogsPG retrieves recent audit entries from Postgres.
func GetAuditLogsPG(d *sql.DB, action, resourceID string, limit int) ([]AuditEntry, error) {
	query := `SELECT id, timestamp, action, resource, resource_id, details, success, error_msg FROM audit_log`
	var conditions []string
	var args []interface{}
	argN := 1

	if action != "" {
		conditions = append(conditions, fmt.Sprintf("action = $%d", argN))
		args = append(args, action)
		argN++
	}
	if resourceID != "" {
		conditions = append(conditions, fmt.Sprintf("resource_id = $%d", argN))
		args = append(args, resourceID)
		argN++
	}

	if len(conditions) > 0 {
		query += " WHERE " + conditions[0]
		for _, c := range conditions[1:] {
			query += " AND " + c
		}
	}
	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.Action, &e.Resource, &e.ResourceID, &e.Details, &e.Success, &e.ErrorMsg); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// InsertDLQEntryPG inserts a dead letter queue entry into Postgres.
func InsertDLQEntryPG(d *sql.DB, entry DLQEntry) error {
	payload := "{}"
	if entry.Payload != nil {
		// Serialize payload as JSON string for Postgres JSONB
		b, err := json.Marshal(entry.Payload)
		if err == nil {
			payload = string(b)
		}
	}
	_, err := d.Exec(`
		INSERT INTO dead_letter_queue (operation_id, node_id, error_reason, retry_count, failed_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (operation_id) DO NOTHING
	`, entry.OperationID, entry.NodeID, entry.ErrorReason, entry.RetryCount, entry.FailedAt, payload)
	return err
}

// GetDLQEntriesPG returns recent DLQ entries from Postgres.
func GetDLQEntriesPG(d *sql.DB, limit int) ([]DLQEntry, error) {
	rows, err := d.Query(`
		SELECT id, operation_id, node_id, error_reason, retry_count, failed_at
		FROM dead_letter_queue
		ORDER BY failed_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []DLQEntry
	for rows.Next() {
		var e DLQEntry
		if err := rows.Scan(&e.ID, &e.OperationID, &e.NodeID, &e.ErrorReason, &e.RetryCount, &e.FailedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetDLQCountPG returns the total number of DLQ entries.
func GetDLQCountPG(d *sql.DB) (int64, error) {
	var count int64
	err := d.QueryRow(`SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count)
	return count, err
}
