package client

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// PgxExecutor implements SQLExecutor for pgx connections
type PgxExecutor struct {
	conn any
}

// NewPgxExecutor creates a new executor for pgx connections
func NewPgxExecutor(conn any) (*PgxExecutor, error) {
	switch conn.(type) {
	case *pgx.Conn, *pgxpool.Pool, pgx.Tx:
		return &PgxExecutor{conn: conn}, nil
	default:
		return nil, errors.New("unsupported pgx connection type")
	}
}

// ExecSQL executes a SQL query using a pgx connection
func (e *PgxExecutor) ExecSQL(ctx context.Context, query string, args ...any) error {
	switch conn := e.conn.(type) {
	case *pgx.Conn:
		_, err := conn.Exec(ctx, query, args...)
		return err
	case *pgxpool.Pool:
		_, err := conn.Exec(ctx, query, args...)
		return err
	case pgx.Tx:
		_, err := conn.Exec(ctx, query, args...)
		return err
	default:
		return errors.New("unsupported pgx connection type")
	}
}

// SQLDBExecutor implements SQLExecutor for database/sql connections
type SQLDBExecutor struct {
	conn any
}

// NewSQLDBExecutor creates a new executor for database/sql connections
func NewSQLDBExecutor(conn any) (*SQLDBExecutor, error) {
	switch conn.(type) {
	case *sql.DB, *sql.Tx:
		return &SQLDBExecutor{conn: conn}, nil
	default:
		return nil, errors.New("unsupported sql connection type")
	}
}

// ExecSQL executes a SQL query using a database/sql connection
func (e *SQLDBExecutor) ExecSQL(ctx context.Context, query string, args ...any) error {
	switch conn := e.conn.(type) {
	case *sql.DB:
		_, err := conn.ExecContext(ctx, query, args...)
		return err
	case *sql.Tx:
		_, err := conn.ExecContext(ctx, query, args...)
		return err
	default:
		return errors.New("unsupported sql connection type")
	}
}

// MockExecutor implements SQLExecutor for testing
type MockExecutor struct {
	ExecSQLFunc func(ctx context.Context, query string, args ...any) error
}

// ExecSQL calls the mock function
func (m *MockExecutor) ExecSQL(ctx context.Context, query string, args ...any) error {
	if m.ExecSQLFunc != nil {
		return m.ExecSQLFunc(ctx, query, args...)
	}
	return nil
}

// NewExecutorFromConnection creates an appropriate executor based on the connection type
func NewExecutorFromConnection(conn any) (SQLExecutor, error) {
	switch conn.(type) {
	case *pgx.Conn, *pgxpool.Pool, pgx.Tx:
		return NewPgxExecutor(conn)
	case *sql.DB, *sql.Tx:
		return NewSQLDBExecutor(conn)
	default:
		return nil, errors.New("unsupported connection type")
	}
}
