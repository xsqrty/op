package db

import (
	"context"
	"database/sql"
	"errors"
	"github.com/xsqrty/op/driver"
	"io"
	"iter"
)

type ConnPool interface {
	io.Closer
	QueryExec
	Transacter
}

type Transacter interface {
	Transact(ctx context.Context, handler func(ctx context.Context) error) error
}

type stdDb interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type connPool struct {
	stdDb   *sql.DB
	options *driver.SqlOptions
}

type rowsResult struct {
	rows *sql.Rows
	err  error
}

type txProp string

var txKey = txProp("tx")

var txOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

func (cp *connPool) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	return cp.get(ctx).ExecContext(ctx, sql, args...)
}

func (cp *connPool) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := cp.get(ctx).QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &rowsResult{
		rows: rows,
	}, nil
}

func (cp *connPool) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return cp.get(ctx).QueryRowContext(ctx, sql, args...)
}

func (cp *connPool) Transact(ctx context.Context, handler func(ctx context.Context) error) (err error) {
	tx, err := cp.stdDb.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			rollBackErr := tx.Rollback()
			if rollBackErr != nil {
				err = errors.Join(err, rollBackErr)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				err = commitErr
			}
		}
	}()

	err = handler(context.WithValue(ctx, txKey, tx))
	return
}

func (cp *connPool) Close() error {
	return cp.stdDb.Close()
}

func (cp *connPool) Sql(b driver.Sqler) (string, []any, error) {
	return driver.Sql(b, cp.options)
}

func (cp *connPool) get(ctx context.Context) stdDb {
	tx := ctx.Value(txKey)
	if tx != nil {
		return tx.(stdDb)
	}

	return cp.stdDb
}

func (rr *rowsResult) Rows() iter.Seq2[int, Scanner] {
	return func(yield func(int, Scanner) bool) {
		index := 0
		for rr.rows.Next() {
			if !yield(index, rr.rows) {
				break
			}

			index++
		}

		rr.err = rr.rows.Err()
	}
}

func (rr *rowsResult) Close() {
	rr.rows.Close()
}

func (rr *rowsResult) Err() error {
	return rr.err
}
