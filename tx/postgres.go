package tx

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xsqrty/op/driver"
)

type PostgresProvider interface {
	Manager
	driver.PostgresQueryExec
}

type postgresProvider struct {
	pool *pgxpool.Pool
}

func NewPostgresProvider(pool *pgxpool.Pool) PostgresProvider {
	return &postgresProvider{
		pool: pool,
	}
}

func (t *postgresProvider) Do(ctx context.Context, handler Handler) (err error) {
	tx, err := t.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})

	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			rollBackErr := tx.Rollback(ctx)
			if rollBackErr != nil {
				err = errors.Join(err, rollBackErr)
			}
		} else {
			commitErr := tx.Commit(ctx)
			if commitErr != nil {
				err = commitErr
			}
		}
	}()

	err = handler(context.WithValue(ctx, txKey, tx))
	return
}

func (t *postgresProvider) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	txVal := ctx.Value(txKey)
	if txVal != nil {
		return txVal.(pgx.Tx).Exec(ctx, sql, arguments...)
	}

	return t.pool.Exec(ctx, sql, arguments...)
}

func (t *postgresProvider) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	txVal := ctx.Value(txKey)
	if txVal != nil {
		return txVal.(pgx.Tx).Query(ctx, sql, args...)
	}

	return t.pool.Query(ctx, sql, args...)
}

func (t *postgresProvider) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	txVal := ctx.Value(txKey)
	if txVal != nil {
		return txVal.(pgx.Tx).QueryRow(ctx, sql, args...)
	}

	return t.pool.QueryRow(ctx, sql, args...)
}
