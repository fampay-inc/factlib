package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	// Default prefix for logical decoding messages
	defaultOutboxPrefix = "outbox"
)

// Connection interface that abstracts different types of database connections
type Connection interface {
	ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error
}

// PgxConnAdapter is an adapter for pgx.Conn
type PgxConnAdapter struct {
	conn   *pgx.Conn
	prefix string
	logger *logger.Logger
}

// PgxPoolAdapter is an adapter for pgxpool.Pool
type PgxPoolAdapter struct {
	pool   *pgxpool.Pool
	prefix string
	logger *logger.Logger
}

// PgxTxAdapter is an adapter for pgx.Tx
type PgxTxAdapter struct {
	tx     pgx.Tx
	prefix string
	logger *logger.Logger
}

// SqlDBAdapter is an adapter for database/sql.DB
type SqlDBAdapter struct {
	db     *sql.DB
	prefix string
	logger *logger.Logger
}

// SqlTxAdapter is an adapter for database/sql.Tx
type SqlTxAdapter struct {
	tx     *sql.Tx
	prefix string
	logger *logger.Logger
}

// NewPgxConnAdapter creates a new adapter for pgx.Conn
func NewPgxConnAdapter(conn *pgx.Conn, log *logger.Logger) *PgxConnAdapter {
	return &PgxConnAdapter{
		conn:   conn,
		prefix: defaultOutboxPrefix,
		logger: log,
	}
}

// NewPgxPoolAdapter creates a new adapter for pgxpool.Pool
func NewPgxPoolAdapter(pool *pgxpool.Pool, log *logger.Logger) *PgxPoolAdapter {
	return &PgxPoolAdapter{
		pool:   pool,
		prefix: defaultOutboxPrefix,
		logger: log,
	}
}

// NewPgxTxAdapter creates a new adapter for pgx.Tx
func NewPgxTxAdapter(tx pgx.Tx, log *logger.Logger) *PgxTxAdapter {
	return &PgxTxAdapter{
		tx:     tx,
		prefix: defaultOutboxPrefix,
		logger: log,
	}
}

// NewSqlDBAdapter creates a new adapter for database/sql.DB
func NewSqlDBAdapter(db *sql.DB, log *logger.Logger) *SqlDBAdapter {
	return &SqlDBAdapter{
		db:     db,
		prefix: defaultOutboxPrefix,
		logger: log,
	}
}

// NewSqlTxAdapter creates a new adapter for database/sql.Tx
func NewSqlTxAdapter(tx *sql.Tx, log *logger.Logger) *SqlTxAdapter {
	return &SqlTxAdapter{
		tx:     tx,
		prefix: defaultOutboxPrefix,
		logger: log,
	}
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PgxConnAdapter) WithPrefix(prefix string) *PgxConnAdapter {
	a.prefix = prefix
	return a
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PgxPoolAdapter) WithPrefix(prefix string) *PgxPoolAdapter {
	a.prefix = prefix
	return a
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PgxTxAdapter) WithPrefix(prefix string) *PgxTxAdapter {
	a.prefix = prefix
	return a
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *SqlDBAdapter) WithPrefix(prefix string) *SqlDBAdapter {
	a.prefix = prefix
	return a
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *SqlTxAdapter) WithPrefix(prefix string) *SqlTxAdapter {
	a.prefix = prefix
	return a
}

// ExecOutboxEvent emits a logical decoding message using pgx.Conn
func (a *PgxConnAdapter) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	
	sql := fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", a.prefix)
	_, err = a.conn.Exec(ctx, sql, string(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to emit logical message")
	}
	
	a.logger.Debug("emitted logical message", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"event_type", event.EventType)
	
	return nil
}

// ExecOutboxEvent emits a logical decoding message using pgxpool.Pool
func (a *PgxPoolAdapter) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	
	sql := fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", a.prefix)
	_, err = a.pool.Exec(ctx, sql, string(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to emit logical message")
	}
	
	a.logger.Debug("emitted logical message", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"event_type", event.EventType)
	
	return nil
}

// ExecOutboxEvent emits a logical decoding message using pgx.Tx
func (a *PgxTxAdapter) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	
	sql := fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", a.prefix)
	_, err = a.tx.Exec(ctx, sql, string(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to emit logical message")
	}
	
	a.logger.Debug("emitted logical message", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"event_type", event.EventType)
	
	return nil
}

// ExecOutboxEvent emits a logical decoding message using database/sql.DB
func (a *SqlDBAdapter) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	
	sql := fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", a.prefix)
	_, err = a.db.ExecContext(ctx, sql, string(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to emit logical message")
	}
	
	a.logger.Debug("emitted logical message", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"event_type", event.EventType)
	
	return nil
}

// ExecOutboxEvent emits a logical decoding message using database/sql.Tx
func (a *SqlTxAdapter) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	
	sql := fmt.Sprintf("SELECT pg_logical_emit_message(true, '%s', $1)", a.prefix)
	_, err = a.tx.ExecContext(ctx, sql, string(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to emit logical message")
	}
	
	a.logger.Debug("emitted logical message", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"event_type", event.EventType)
	
	return nil
}

// EmitEvent is a helper function that constructs an OutboxEvent and emits it
func EmitEvent(ctx context.Context, conn Connection, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error) {
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
		ID:           eventID,
		AggregateType: aggregateType,
		AggregateID:  aggregateID,
		EventType:    eventType,
		Payload:      payload,
		CreatedAt:    createdAt,
		Metadata:     metadata,
	}

	if err := conn.ExecOutboxEvent(ctx, event); err != nil {
		return "", err
	}

	return eventID, nil
}
