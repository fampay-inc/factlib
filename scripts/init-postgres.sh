#!/bin/bash
set -e

# Configure PostgreSQL for logical replication
cat >> /var/lib/postgresql/data/postgresql.conf << EOF
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
EOF

# Create outbox_example database if it doesn't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" << EOF
-- Create outbox_events table for non-logical replication approach (optional)
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata JSONB,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate_type ON outbox_events (aggregate_type);
CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate_id ON outbox_events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_outbox_events_created_at ON outbox_events (created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_events_processed ON outbox_events (processed);

-- Create a publication for the logical replication
CREATE PUBLICATION factlib_publication FOR ALL TABLES;

-- Create a replication slot (with error handling for if it already exists)
DO $$
BEGIN
  PERFORM pg_create_logical_replication_slot('factlib_replication_slot', 'pgoutput');
EXCEPTION WHEN duplicate_object THEN
  RAISE NOTICE 'Replication slot factlib_replication_slot already exists.';
END;
$$;

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE outbox_events TO postgres;
EOF

# Make the script executable
chmod +x /var/lib/postgresql/data/postgresql.conf
