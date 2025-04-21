package postgres

import (
	"context"
	"database/sql"
)

type SqlDb struct {
	conn *sql.DB
}

func (s *SqlDb) Exec(ctx context.Context, query string, args ...any) error {
	_, err := s.conn.ExecContext(ctx, query, args...)
	return err
}

func NewSqlConn(conn *sql.DB) SQLExecutor {
	return &SqlDb{
		conn: conn,
	}
}

type SqlTxn struct {
	conn *sql.Tx
}

func (s *SqlTxn) Exec(ctx context.Context, query string, args ...any) error {
	_, err := s.conn.ExecContext(ctx, query, args...)
	return err
}

func GetSqlTxn(conn *sql.Tx) SQLExecutor {
	return &SqlTxn{
		conn: conn,
	}
}
