package producer

import (
	"context"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// OutboxProducer defines the interface for emitting outbox events
type OutboxProducer interface {
	// EmitEvent emits a Protobuf outbox event
	EmitEvent(ctx context.Context, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error)

	// WithPrefix creates a new producer that sets a custom prefix for logical decoding messages
	WithPrefix(prefix string) OutboxProducer
	// WithTx creates a new producer that uses the provided transaction or connection
	WithTx(tx SqlExecutor) OutboxProducer
}

// PostgresAdapter is the implementation of OutboxProducer that works with different PostgreSQL connection types
type PostgresAdapter struct {
	conn   SqlExecutor
	prefix string
	logger *logger.Logger
}

type SqlExecutor interface {
	Exec(ctx context.Context, query string, args ...any) (string, error)
}

// NewPostgresAdapter creates a new PostgresAdapter with the provided SQLExecutor
func NewPostgresAdapter(prefix string, log *logger.Logger) (*PostgresAdapter, error) {
	return &PostgresAdapter{
		prefix: prefix,
		logger: log,
	}, nil
}

// WithTx creates a producer instance that uses the provided transaction or connection
func (a *PostgresAdapter) WithTx(conn SqlExecutor) OutboxProducer {
	return &PostgresAdapter{
		conn:   conn,
		prefix: a.prefix,
		logger: a.logger,
	}
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PostgresAdapter) WithPrefix(prefix string) OutboxProducer {
	return &PostgresAdapter{
		conn:   a.conn,
		prefix: prefix,
		logger: a.logger,
	}
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

	eventID, _ := uuid.NewV7()
	createdAt := time.Now().UTC().UnixNano()

	event := &pb.OutboxEvent{
		Id:            eventID.String(),
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
	sqlQuery := "SELECT pg_logical_emit_message(true, $1, $2::bytea)"
	_, err = a.conn.Exec(ctx, sqlQuery, a.prefix, protoBytes)

	if err != nil {
		return "", errors.Wrap(err, "failed to emit logical message")
	}
	a.logger.Debug("emitted message",
		"id", eventID,
		"aggregate_id", aggregateID,
		"aggregate_type", aggregateType,
		"event_type", eventType)

	return event.Id, nil
}

// // execSQL executes SQL on the appropriate connection type
// func (a *PostgresAdapter) execSQL(ctx context.Context, query string, args ...any) error {
// 	switch conn := a.conn.(type) {
// 	case *pgx.Conn:
// 		_, err := conn.Exec(ctx, query, args...)
// 		return err
// 	case pgx.Tx:
// 		_, err := conn.Exec(ctx, query, args...)
// 		return err
// 	case *pgxpool.Pool:
// 		_, err := conn.Exec(ctx, query, args...)
// 		return err
// 	case *sql.DB:
// 		_, err := conn.ExecContext(ctx, query, args...)
// 		return err
// 	case *sql.Tx:
// 		_, err := conn.ExecContext(ctx, query, args...)
// 		return err
// 	default:
// 		return errors.New("unsupported connection type")
// 	}
// }
