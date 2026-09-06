package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// auditSchema creates the audit_log table in the coordinator DB.
// Every state-mutating API call is logged here for compliance and debugging.
const auditSchema = `
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT    NOT NULL,
    action      TEXT    NOT NULL,
    resource    TEXT    NOT NULL,
    resource_id TEXT    NOT NULL,
    details     TEXT    NOT NULL,
    success     INTEGER NOT NULL DEFAULT 1,
    error_msg   TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log (timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_action    ON audit_log (action);
`

// AuditEntry represents a single audit log record.
type AuditEntry struct {
	ID         int64                  `json:"id"`
	Timestamp  time.Time              `json:"timestamp"`
	Action     string                 `json:"action"`
	Resource   string                 `json:"resource"`
	ResourceID string                 `json:"resource_id"`
	Details    map[string]interface{} `json:"details"`
	Success    bool                   `json:"success"`
	ErrorMsg   string                 `json:"error_msg,omitempty"`
}

// InitAuditSchema applies the audit_log table schema to the given DB.
func InitAuditSchema(d *sql.DB) error {
	_, err := d.Exec(auditSchema)
	if err != nil {
		return fmt.Errorf("apply audit schema: %w", err)
	}
	return nil
}

// InsertAuditLog writes a single audit entry to the coordinator DB.
func InsertAuditLog(d *sql.DB, entry AuditEntry) error {
	detailsJSON, err := json.Marshal(entry.Details)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}

	successInt := 0
	if entry.Success {
		successInt = 1
	}

	_, err = d.Exec(`
		INSERT INTO audit_log (timestamp, action, resource, resource_id, details, success, error_msg)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, entry.Timestamp.Format(time.RFC3339Nano),
		entry.Action, entry.Resource, entry.ResourceID,
		string(detailsJSON), successInt, entry.ErrorMsg)
	if err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	}
	return nil
}

// GetAuditLogs retrieves audit log entries with optional filters.
// Supports filtering by action and resource_id. Returns newest first, limited to `limit` rows.
func GetAuditLogs(d *sql.DB, action, resourceID string, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := `SELECT id, timestamp, action, resource, resource_id, details, success, error_msg
	          FROM audit_log WHERE 1=1`
	args := []interface{}{}

	if action != "" {
		query += " AND action = ?"
		args = append(args, action)
	}
	if resourceID != "" {
		query += " AND resource_id = ?"
		args = append(args, resourceID)
	}

	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit logs: %w", err)
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var ts, detailsStr string
		var successInt int
		err := rows.Scan(&e.ID, &ts, &e.Action, &e.Resource, &e.ResourceID,
			&detailsStr, &successInt, &e.ErrorMsg)
		if err != nil {
			return nil, fmt.Errorf("scan audit row: %w", err)
		}
		e.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		e.Success = successInt == 1
		if err := json.Unmarshal([]byte(detailsStr), &e.Details); err != nil {
			e.Details = map[string]interface{}{"raw": detailsStr}
		}
		entries = append(entries, e)
	}
	return entries, nil
}
