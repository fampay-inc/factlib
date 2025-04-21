package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgxConn struct {
	conn *pgx.Conn
}

func (p *PgxConn) Exec(ctx context.Context, query string, args ...any) error {
	_, err := p.conn.Exec(ctx, query, args...)
	return err
}

func GetPgxConn(conn *pgx.Conn) SQLExecutor {
	return &PgxConn{
		conn: conn,
	}
}

type PgxTxn struct {
	conn pgx.Tx
}

func (p *PgxTxn) Exec(ctx context.Context, query string, args ...any) error {
	_, err := p.conn.Exec(ctx, query, args...)
	return err
}

func GetPgxTxn(conn pgx.Tx) SQLExecutor {
	return &PgxTxn{
		conn: conn,
	}
}

type PgxPool struct {
	conn *pgxpool.Pool
}

func (p *PgxPool) Exec(ctx context.Context, query string, args ...any) error {
	_, err := p.conn.Exec(ctx, query, args...)
	return err
}

func GetPgxPool(conn *pgxpool.Pool) SQLExecutor {
	return &PgxPool{
		conn: conn,
	}
}
