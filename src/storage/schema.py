"""SQLite Schema definitions and indexes for RelayForge."""

CREATE_TABLES_SQL = """
-- Core webhook deliveries table
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id TEXT PRIMARY KEY,
    destination_url TEXT NOT NULL,
    http_method TEXT NOT NULL DEFAULT 'POST',
    payload_raw TEXT NOT NULL,
    headers_raw TEXT NOT NULL DEFAULT '{}',
    secret_token TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    idempotency_key TEXT UNIQUE,
    next_retry_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Index for polling scheduler: efficiently fetch due deliveries
CREATE INDEX IF NOT EXISTS idx_deliveries_status_retry
ON webhook_deliveries (status, next_retry_at);

-- Index for timeline and pagination
CREATE INDEX IF NOT EXISTS idx_deliveries_created_at
ON webhook_deliveries (created_at DESC);

-- Execution telemetry attempts
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    status_code INTEGER,
    response_body TEXT,
    error_message TEXT,
    execution_duration_ms REAL NOT NULL,
    attempted_at TEXT NOT NULL,
    FOREIGN KEY (delivery_id) REFERENCES webhook_deliveries (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_attempts_delivery_id
ON delivery_attempts (delivery_id, attempt_number ASC);

-- Dead Letter Queue storage
CREATE TABLE IF NOT EXISTS dead_letter_records (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    failure_reason TEXT NOT NULL,
    moved_to_dlq_at TEXT NOT NULL,
    replay_count INTEGER NOT NULL DEFAULT 0,
    last_replayed_at TEXT,
    FOREIGN KEY (delivery_id) REFERENCES webhook_deliveries (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dlq_moved_at
ON dead_letter_records (moved_to_dlq_at DESC);
"""
