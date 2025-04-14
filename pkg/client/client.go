package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	outboxChannel = "outbox_events"
)

// PostgresConfig represents the configuration for the Postgres client
type PostgresConfig struct {
	ConnectionString string
}

// PostgresClient implements the common.OutboxClient interface
type PostgresClient struct {
	pool   *pgxpool.Pool
	logger *logger.Logger
}

// NewPostgresClient creates a new Postgres client
func NewPostgresClient(ctx context.Context, cfg PostgresConfig, log *logger.Logger) (*PostgresClient, error) {
	pool, err := pgxpool.New(ctx, cfg.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create connection pool")
	}

	// Validate connection and PostgreSQL version
	if err := ensurePgNotifyExtension(ctx, pool); err != nil {
		return nil, err
	}

	return &PostgresClient{
		pool:   pool,
		logger: log,
	}, nil
}

// Publish publishes an event to the outbox via logical decoding message
func (c *PostgresClient) Publish(ctx context.Context, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error) {
	if aggregateType == "" {
		return "", errors.New("aggregate type is required")
	}
	if aggregateID == "" {
		return "", errors.New("aggregate ID is required")
	}
	if eventType == "" {
		return "", errors.New("event type is required")
	}

	eventID := uuid.New().String()
	createdAt := time.Now().UTC()

	event := common.OutboxEvent{
		ID:            eventID,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		CreatedAt:     createdAt,
		Metadata:      metadata,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal event")
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return "", errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// Emit logical replication message directly to WAL
	// Parameters:
	// 1. true = transactional (message is part of current transaction)
	// 2. outboxChannel = prefix for identifying the message
	// 3. eventJSON = content of the message
	_, err = tx.Exec(ctx, fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", outboxChannel), eventJSON)
	if err != nil {
		return "", errors.Wrap(err, "failed to emit logical message")
	}

	if err := tx.Commit(ctx); err != nil {
		return "", errors.Wrap(err, "failed to commit transaction")
	}

	c.logger.Debug("published event",
		"id", eventID,
		"aggregate_type", aggregateType,
		"event_type", eventType)

	return eventID, nil
}

// Close closes the client
func (c *PostgresClient) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

// ensureConnection ensures the PostgreSQL connection is valid
func ensurePgNotifyExtension(ctx context.Context, pool *pgxpool.Pool) error {
	// Ping the database to ensure connection is valid
	err := pool.Ping(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to ping database")
	}

	// Check if pg_logical_emit_message function is available (PostgreSQL 10+)
	var version string
	err = pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return errors.Wrap(err, "failed to get PostgreSQL version")
	}

	return nil
}
