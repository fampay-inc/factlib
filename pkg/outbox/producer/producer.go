package producer

import (
	"context"
	"database/sql"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	// Default prefix for logical decoding messages
	defaultOutboxPrefix = "proto_outbox"
)

// OutboxProducer defines the interface for emitting outbox events
type OutboxProducer interface {
	// EmitEvent emits a Protobuf outbox event
	EmitEvent(ctx context.Context, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error)

	// WithPrefix sets a custom prefix for logical decoding messages
	WithPrefix(prefix string) OutboxProducer
}

// SQLExecutor defines an interface for executing SQL queries
type SQLExecutor interface {
	Exec(ctx context.Context, query string, args ...any) error
}

// PostgresAdapter is the implementation of OutboxProducer that works with different PostgreSQL connection types
type PostgresAdapter struct {
	executor SQLExecutor
	prefix   string
	logger   *logger.Logger
}

// NewPostgresAdapter creates a new PostgresAdapter with the provided SQLExecutor
func NewPostgresAdapter(executor SQLExecutor, log *logger.Logger) (*PostgresAdapter, error) {
	if executor == nil {
		return nil, errors.New("executor cannot be nil")
	}

	return &PostgresAdapter{
		executor: executor,
		prefix:   defaultOutboxPrefix,
		logger:   log,
	}, nil
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PostgresAdapter) WithPrefix(prefix string) OutboxProducer {
	a.prefix = prefix
	return a
}

// EmitEvent emits a Protobuf outbox event
func (a *PostgresAdapter) EmitEvent(ctx context.Context, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error) {
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
	createdAt := time.Now().UTC().UnixNano()

	event := &pb.OutboxEvent{
		Id:            eventID,
		AggregateType: aggregateType,
		AggregateId:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		CreatedAt:     createdAt,
		Metadata:      metadata,
	}

	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal proto event")
	}
	a.logger.Debug("proto bytes", "proto", protoBytes)
	sqlQuery := "SELECT pg_logical_emit_message(true, $1, $2::bytea)"
	err = a.executor.Exec(ctx, sqlQuery, a.prefix, protoBytes)

	if err != nil {
		return "", errors.Wrap(err, "failed to emit logical message")
	}

	a.logger.Debug("emitted proto logical message",
		"id", eventID,
		"aggregate_type", aggregateType,
		"event_type", eventType)

	return eventID, nil
}

// ValidateConnection validates the PostgreSQL connection and ensures the required extensions are available
func ValidateConnection(ctx context.Context, conn any) error {
	// Create appropriate executor based on connection type
	var executor SQLExecutor
	var err error

	switch conn.(type) {
	case *pgx.Conn, *pgxpool.Pool, pgx.Tx:
		executor, err = NewPgxExecutor(conn)
		if err != nil {
			return err
		}
	case *sql.DB, *sql.Tx:
		executor, err = NewSQLDBExecutor(conn)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported connection type")
	}

	// Query PostgreSQL version using the executor
	err = executor.Exec(ctx, "SELECT version()")
	if err != nil {
		return errors.Wrap(err, "failed to get PostgreSQL version")
	}

	// PostgreSQL 10+ is required for pg_logical_emit_message
	return nil
}
