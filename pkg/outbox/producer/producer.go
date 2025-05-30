package producer

import (
	"context"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type PostgresAdapter struct {
	conn   postgres.SQLExecutor
	logger logger.Logger
	prefix string
}

func NewPostgresAdapter(prefix string, log logger.Logger) (*PostgresAdapter, error) {
	return &PostgresAdapter{
		prefix: prefix,
		logger: log,
	}, nil
}

func (a *PostgresAdapter) WithTxn(txn postgres.SQLExecutor) (postgres.OutboxProducer, error) {
	return &PostgresAdapter{
		conn:   txn,
		logger: a.logger,
		prefix: a.prefix,
	}, nil
}

// WithPrefix sets a custom prefix for logical decoding messages
func (a *PostgresAdapter) WithPrefix(prefix string) postgres.OutboxProducer {
	return &PostgresAdapter{
		logger: a.logger,
		conn:   a.conn,
		prefix: prefix,
	}
}

// EmitEvent emits a Protobuf outbox event
func (a *PostgresAdapter) Emit(ctx context.Context, fact *common.Fact) (string, error) {
	err := fact.Validate()
	if err != nil {
		return "", errors.Wrap(err, "failed to validate fact")
	}
	eventId, err := uuid.NewV7()
	if err != nil {
		return "", errors.Wrap(err, "failed to generate event ID")
	}
	outboxEvent := &pb.OutboxEvent{
		Id:            eventId.String(),
		AggregateType: fact.AggregateType,
		AggregateId:   fact.AggregateID,
		EventType:     fact.EventType,
		Payload:       fact.Payload,
		Metadata:      fact.Metadata,
		TraceInfo:     fact.TraceInfo,
		CreatedAt:     time.Now().UTC().UnixNano(),
	}
	protoBytes, err := proto.Marshal(outboxEvent)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal proto event")
	}
	sqlQuery := "SELECT pg_logical_emit_message(true, $1, $2::bytea)"
	err = a.conn.Exec(ctx, sqlQuery, a.prefix, protoBytes)

	if err != nil {
		return "", errors.Wrap(err, "failed to emit logical message")
	}
	a.logger.Debug("emitted message",
		"id", outboxEvent.Id,
		"aggregate_id", outboxEvent.AggregateId,
		"aggregate_type", outboxEvent.AggregateType,
		"event_type", outboxEvent.EventType,
	)
	return outboxEvent.Id, nil
}
