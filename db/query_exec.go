package db

import (
	"context"
	"github.com/xsqrty/op/driver"
	"iter"
)

type QueryExec interface {
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Sql(sqler driver.Sqler) (string, []any, error)
}

type ExecResult interface {
	RowsAffected() (int64, error)
	LastInsertId() (int64, error)
}

type Scanner interface {
	Scan(dest ...any) error
}

type Row interface {
	Scan(dest ...any) error
}

type Rows interface {
	Close()
	Rows() iter.Seq2[int, Scanner]
	Err() error
}
