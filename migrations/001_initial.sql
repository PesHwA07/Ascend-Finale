-- CounterGhost Phase 2: Postgres Schema
-- Replaces per-node SQLite files with a single Postgres instance.
--
-- Key differences from SQLite schema:
--   1. TIMESTAMPTZ instead of TEXT for timestamps
--   2. JSONB for manifests (indexed, queryable)
--   3. Proper SERIAL primary keys for audit/DLQ tables
--   4. Partial indexes for outbox sync queries
--   5. GIN indexes for JSONB fields
--
-- Applied automatically by docker-entrypoint-initdb.d

-- ════════════════════════════════════════════════════════════════
-- Counter Operations (replaces per-node SQLite operations table)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE counter_operations (
    operation_id    TEXT        PRIMARY KEY,
    counter_id      TEXT        NOT NULL,
    node_id         TEXT        NOT NULL,
    epoch           INTEGER     NOT NULL,
    sequence        INTEGER     NOT NULL,
    amount          INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (node_id, epoch, sequence)
);

-- Fast reconciliation: find ops for a node in a specific epoch
CREATE INDEX idx_ops_node_epoch_seq ON counter_operations(node_id, epoch, sequence);

-- Fast aggregation: sum amounts for a specific counter
CREATE INDEX idx_ops_counter ON counter_operations(counter_id);

-- Temporal queries: find ops before a timestamp
CREATE INDEX idx_ops_created_at ON counter_operations(created_at);


-- ════════════════════════════════════════════════════════════════
-- Node Epochs (tracks crash incarnations)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE node_epochs (
    node_id     TEXT        NOT NULL,
    epoch       INTEGER     NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_id, epoch)
);


-- ════════════════════════════════════════════════════════════════
-- Transactional Outbox (per-node, for Kafka sync)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE operation_outbox (
    operation_id    TEXT        PRIMARY KEY,
    counter_id      TEXT        NOT NULL,
    node_id         TEXT        NOT NULL,
    epoch           INTEGER     NOT NULL,
    sequence        INTEGER     NOT NULL,
    amount          INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced          BOOLEAN     NOT NULL DEFAULT FALSE,
    sync_attempts   INTEGER     NOT NULL DEFAULT 0,
    last_attempt    TIMESTAMPTZ,
    error_message   TEXT        NOT NULL DEFAULT ''
);

-- Partial index: only unsynced entries (most queries target these)
CREATE INDEX idx_outbox_unsynced ON operation_outbox(synced)
    WHERE synced = FALSE;

-- Retry logic: find stale unsynced entries
CREATE INDEX idx_outbox_retry ON operation_outbox(sync_attempts, last_attempt)
    WHERE synced = FALSE;


-- ════════════════════════════════════════════════════════════════
-- Audit Log (compliance trail for all state-mutating actions)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE audit_log (
    id              SERIAL      PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action          TEXT        NOT NULL,
    resource        TEXT        NOT NULL,
    resource_id     TEXT        NOT NULL,
    details         JSONB       NOT NULL DEFAULT '{}',
    success         BOOLEAN     NOT NULL DEFAULT TRUE,
    error_msg       TEXT        NOT NULL DEFAULT ''
);

-- Fast time-range queries for audit review
CREATE INDEX idx_audit_timestamp ON audit_log(timestamp);

-- Filter by action type (e.g., "show all crashes")
CREATE INDEX idx_audit_action ON audit_log(action);

-- JSONB index for querying audit details
CREATE INDEX idx_audit_details ON audit_log USING GIN (details);


-- ════════════════════════════════════════════════════════════════
-- Dead Letter Queue (operations that failed outbox sync 5+ times)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE dead_letter_queue (
    id              SERIAL      PRIMARY KEY,
    operation_id    TEXT        NOT NULL UNIQUE,
    node_id         TEXT        NOT NULL,
    error_reason    TEXT        NOT NULL,
    retry_count     INTEGER     NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload         JSONB       NOT NULL DEFAULT '{}'
);

-- Find DLQ entries by node (for per-node debugging)
CREATE INDEX idx_dlq_node ON dead_letter_queue(node_id);

-- Time-range queries for DLQ review
CREATE INDEX idx_dlq_failed ON dead_letter_queue(failed_at);

-- JSONB index for querying DLQ payloads
CREATE INDEX idx_dlq_payload ON dead_letter_queue USING GIN (payload);


-- ════════════════════════════════════════════════════════════════
-- Node Manifests (materialized view of node health)
-- ════════════════════════════════════════════════════════════════
CREATE TABLE node_manifests (
    node_id                     TEXT        PRIMARY KEY,
    current_epoch               INTEGER     NOT NULL,
    highest_contiguous_seq      INTEGER     NOT NULL DEFAULT 0,
    processed_ranges            JSONB       NOT NULL DEFAULT '[]',
    local_value                 BIGINT      NOT NULL DEFAULT 0,
    last_reconciled_at          TIMESTAMPTZ,
    last_health_check           TIMESTAMPTZ
);

-- Fast manifest lookups by epoch (for reconciliation)
CREATE INDEX idx_manifests_epoch ON node_manifests(current_epoch);
