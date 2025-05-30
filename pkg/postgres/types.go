package postgres

import (
	"context"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
)

type SQLExecutor interface {
	Exec(ctx context.Context, query string, args ...any) error
}

type OutboxProducer interface {
	Emit(ctx context.Context, fact *common.Fact) (string, error)
	WithPrefix(prefix string) OutboxProducer
}
