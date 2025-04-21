package postgres

import (
	"context"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
)

type SQLExecutor interface {
	Exec(ctx context.Context, query string, args ...any) error
}

type OutboxProducer interface {
	Emit(ctx context.Context, fact *pb.OutboxEvent) (string, error)
	WithPrefix(prefix string) OutboxProducer
}