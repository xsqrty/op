package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

var ErrPgxUnsupported = errors.New("unsupported")

type pgxQueryExec interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (commandTag pgconn.CommandTag, err error)
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

type pgxTxProp string

var pgxTxOptions = pgx.TxOptions{
	IsoLevel: pgx.ReadCommitted,
}

var pgxTxKey = pgxTxProp("tx")

var _ db.ConnPool = (*pgxAdapter)(nil)

func (pa *pgxAdapter) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	tags, err := pa.get(ctx).Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxExecResult{commonTags: &tags}, nil
}

func (pa *pgxAdapter) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	rows, err := pa.get(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxRows{
		rows: rows,
	}, nil
}

func (pa *pgxAdapter) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	return pa.get(ctx).QueryRow(ctx, sql, args...)
}

func (pa *pgxAdapter) Transact(
	ctx context.Context,
	handler func(ctx context.Context) error,
) (err error) {
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

	err = handler(context.WithValue(ctx, pgxTxKey, tx))
	return
}

func (pa *pgxAdapter) Close() error {
	pa.pool.Close()
	return nil
}

func (pa *pgxAdapter) Sql(b driver.Sqler) (string, []any, error) {
	return driver.Sql(b, pa.options)
}

func (pa *pgxAdapter) SqlOptions() *driver.SqlOptions {
	return pa.options
}

func (pa *pgxAdapter) get(ctx context.Context) pgxQueryExec {
	tx := ctx.Value(pgxTxKey)
	if tx != nil {
		return tx.(pgxQueryExec)
	}

	return pa.pool
}

func (pr *pgxRows) Rows() iter.Seq2[int, db.Scanner] {
	return func(yield func(int, db.Scanner) bool) {
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

func (pr *pgxRows) Columns() ([]string, error) {
	fields := pr.rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, f := range fields {
		columns[i] = f.Name
	}

	return columns, nil
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
