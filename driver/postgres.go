package driver

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"iter"
)

type PostgresQueryExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type postgresDriver struct {
	pool    PostgresQueryExec
	options *SqlOptions
}

type postgresDriverRows struct {
	rows pgx.Rows
	err  error
}

type postgresExecResult struct {
	commonTags *pgconn.CommandTag
}

func NewPostgresDriver(pool PostgresQueryExec) QueryExec {
	return &postgresDriver{
		pool:    pool,
		options: NewPostgresSqlOptions(),
	}
}

func NewPostgresSqlOptions() *SqlOptions {
	return NewSqlOptions(
		WithSafeColumns(),
		WithColumnsDelim('.'),
		WithFieldsDelim(','),
		WithWrapColumn('"', '"'),
		WithWrapAlias('"', '"'),
		WithCastFormat(func(val string, typ string) string {
			return fmt.Sprintf("%s::%s", val, typ)
		}),
		WithPlaceholderFormat(func(n int) string {
			return fmt.Sprintf("$%d", n)
		}),
	)
}

func (d *postgresDriver) Sql(b Sqler) (string, []any, error) {
	return Sql(b, d.options)
}

func (d *postgresDriver) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	tags, err := d.pool.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &postgresExecResult{commonTags: &tags}, nil
}

func (d *postgresDriver) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return d.pool.QueryRow(ctx, sql, args...)
}

func (d *postgresDriver) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := d.pool.Query(ctx, sql, args...)
	return &postgresDriverRows{
		rows: rows,
	}, err
}

func (d *postgresDriver) SqlOptions() *SqlOptions {
	return d.options
}

func (d *postgresDriverRows) Rows() iter.Seq2[int, Scanner] {
	return func(yield func(int, Scanner) bool) {
		index := 0
		for d.rows.Next() {
			if !yield(index, d.rows) {
				break
			}

			index++
		}

		d.err = d.rows.Err()
	}
}

func (d *postgresDriverRows) Close() {
	d.rows.Close()
}

func (d *postgresDriverRows) Err() error {
	return d.err
}

func (d *postgresExecResult) RowsAffected() int64 {
	return d.commonTags.RowsAffected()
}
