package db

import (
	"context"
	"iter"

	"github.com/xsqrty/op/driver"
)

// QueryExec defines an interface for executing SQL commands and queries.
type QueryExec interface {
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	SqlOptions() *driver.SqlOptions
}

// ExecResult represents the result of a SQL execution.
type ExecResult interface {
	RowsAffected() (int64, error)
	LastInsertId() (int64, error)
}

// Scanner is an interface used to read database rows into destination variables.
// Scan assigns the column values of the current row to the provided destination variables.
type Scanner interface {
	Scan(dest ...any) error
}

// Row provides an abstraction for a single database query result row.
// Scan maps the row result into the provided destination variables.
type Row interface {
	Scan(dest ...any) error
}

// Rows represents a result set obtained from a database query.
type Rows interface {
	Close()
	Rows() iter.Seq2[int, Scanner]
	Columns() ([]string, error)
	Err() error
}
