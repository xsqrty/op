package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xsqrty/op/driver"
	"iter"
	"time"
)

var ErrPgxUnsupported = errors.New("unsupported")

type pgxQueryExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type pgxAdapter struct {
	pool    *pgxpool.Pool
	options *driver.SqlOptions
}

type pgxRows struct {
	rows pgx.Rows
	err  error
}

type pgxExecResult struct {
	commonTags *pgconn.CommandTag
}

var pgxTxOptions = pgx.TxOptions{
	IsoLevel: pgx.ReadCommitted,
}

var _ ConnPool = (*pgxAdapter)(nil)

func (pa *pgxAdapter) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	tags, err := pa.get(ctx).Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxExecResult{commonTags: &tags}, nil
}

func (pa *pgxAdapter) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := pa.get(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxRows{
		rows: rows,
	}, nil
}

func (pa *pgxAdapter) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return pa.get(ctx).QueryRow(ctx, sql, args...)
}

func (pa *pgxAdapter) Transact(ctx context.Context, handler TransactHandler) (err error) {
	tx, err := pa.pool.BeginTx(ctx, pgxTxOptions)
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

func (pa *pgxAdapter) Close() error {
	pa.pool.Close()
	return nil
}

func (pa *pgxAdapter) SetConnMaxLifetime(d time.Duration) {
	pa.SetConnMaxLifetime(d)
}

func (pa *pgxAdapter) SetMaxOpenConns(n int) {
	pa.SetMaxOpenConns(n)
}

func (pa *pgxAdapter) SetMaxIdleConns(n int) {
	pa.SetMaxIdleConns(n)
}

func (pa *pgxAdapter) SetConnMaxIdleTime(d time.Duration) {
	pa.SetConnMaxIdleTime(d)
}

func (pa *pgxAdapter) Sql(b driver.Sqler) (string, []any, error) {
	return driver.Sql(b, pa.options)
}

func (pa *pgxAdapter) get(ctx context.Context) pgxQueryExec {
	tx := ctx.Value(txKey)
	if tx != nil {
		return tx.(pgxQueryExec)
	}

	return pa.pool
}

func (pr *pgxRows) Rows() iter.Seq2[int, Scanner] {
	return func(yield func(int, Scanner) bool) {
		index := 0
		for pr.rows.Next() {
			if !yield(index, pr.rows) {
				break
			}

			index++
		}

		pr.err = pr.rows.Err()
	}
}

func (pr *pgxRows) Close() {
	pr.rows.Close()
}

func (pr *pgxRows) Err() error {
	return pr.err
}

func (er *pgxExecResult) RowsAffected() (int64, error) {
	return er.commonTags.RowsAffected(), nil
}

func (er *pgxExecResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("pgx last insert id is not supported: %w", ErrPgxUnsupported)
}
