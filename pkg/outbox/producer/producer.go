package producer

import (
	"context"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type PostgresAdapter struct {
	conn     postgres.SQLExecutor
	logger *logger.Logger
	prefix   string
}

func NewPostgresAdapter(prefix string, log *logger.Logger) (*PostgresAdapter, error) {
	return &PostgresAdapter{
		prefix: prefix,
		logger: log,
	}, nil
}

func (a *PostgresAdapter) WithTxn(txn postgres.SQLExecutor) (postgres.OutboxProducer, error) {
	return &PostgresAdapter{
		conn:     txn,
		logger:   a.logger,
		prefix:   a.prefix,
	}, nil
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PostgresAdapter) WithPrefix(prefix string) postgres.OutboxProducer {
	return &PostgresAdapter{
		logger:   a.logger,
		conn:     a.conn,
		prefix:   prefix,
	}
}

// EmitEvent emits a Protobuf outbox event
func (a *PostgresAdapter) Emit(ctx context.Context, fact *pb.OutboxEvent) (string, error) {
	protoBytes, err := proto.Marshal(fact)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal proto event")
	}
	sqlQuery := "SELECT pg_logical_emit_message(true, $1, $2::bytea)"
	err = a.conn.Exec(ctx, sqlQuery, a.prefix, protoBytes)

	if err != nil {
		return "", errors.Wrap(err, "failed to emit logical message")
	}
	a.logger.Debug("emitted message",
		"id", fact.Id,
		"aggregate_id", fact.GetAggregateId(),
		"aggregate_type", fact.GetAggregateType(),
		"event_type", fact.GetEventType(),
	)
	return fact.Id, nil
}
