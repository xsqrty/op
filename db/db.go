package db

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"iter"

	"github.com/xsqrty/op/driver"
)

// ConnPool represents a connection pool interface.
type ConnPool interface {
	io.Closer
	QueryExec
	Transacter
}

// Transacter defines an interface for handling transactional operations.
type Transacter interface {
	Transact(ctx context.Context, handler func(ctx context.Context) error) error
}

// stdDb describes an interface of the standard library.
type stdDb interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// connPool implementation of ConnPool for standard sql.DB
type connPool struct {
	stdDb   *sql.DB
	options *driver.SqlOptions
}

// rowsResult implementation of Rows for sql.Rows
type rowsResult struct {
	rows *sql.Rows
	err  error
}

// txType represents a custom string type used to define specific transaction types or keys within the application.
type txType string

// txKey is a predefined key of type txType used to store and retrieve transaction objects within a context.
var txKey = txType("tx")

// txOptions specifies transaction options with Read Committed isolation level for database transactions.
var txOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// NewConnPool create ConnPool based on sql.DB
func NewConnPool(db *sql.DB, options *driver.SqlOptions) ConnPool {
	return &connPool{stdDb: db, options: options}
}

// Exec executes a SQL query that doesn't return rows, such as an INSERT, UPDATE, or DELETE statement.
// It uses the provided context for request cancellation and timeout management.
// The SQL statement can include placeholders for arguments, which are provided via the variadic args parameter.
// Returns an ExecResult that contains methods for retrieving the number of affected rows and last inserted ID.
func (cp *connPool) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	return cp.get(ctx).ExecContext(ctx, sql, args...)
}

// Query executes a SQL query that returns rows, such as a SELECT statement, using the provided context and arguments.
func (cp *connPool) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := cp.get(ctx).QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &rowsResult{
		rows: rows,
	}, nil
}

// QueryRow executes a SQL query that is expected to return at most one row using the provided context and arguments.
func (cp *connPool) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return cp.get(ctx).QueryRowContext(ctx, sql, args...)
}

// Transact executes a function within a database transaction, handling commit or rollback based on an execution outcome.
// Accepts a context and a handler function as parameters. Returns an error if the transaction fails or the handler returns an error.
func (cp *connPool) Transact(
	ctx context.Context,
	handler func(ctx context.Context) error,
) (err error) {
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

// Close closes the underlying database connection pool, releasing any open connections and associated resources.
func (cp *connPool) Close() error {
	return cp.stdDb.Close()
}

// Sql generates an SQL query string and arguments using the provided Sqler and connection pool options.
func (cp *connPool) Sql(b driver.Sqler) (string, []any, error) {
	return driver.Sql(b, cp.options)
}

// SqlOptions returns the SQL configuration options used for customizing SQL generation behavior in the connection pool.
func (cp *connPool) SqlOptions() *driver.SqlOptions {
	return cp.options
}

// get retrieves the database from the context if a transaction exists, otherwise returns the default standard database.
func (cp *connPool) get(ctx context.Context) stdDb {
	tx := ctx.Value(txKey)
	if tx != nil {
		return tx.(stdDb)
	}

	return cp.stdDb
}

// Rows return a sequence of indexed rows from the sql.Rows object, allowing iteration with a yield function.
// Each row is accessed as an index and a Scanner interface.
// The method also updates the error state of the rowsResult instance after iteration.
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

// Columns returns the names of the columns from the underlying sql.Rows object or an error if retrieval fails.
func (rr *rowsResult) Columns() ([]string, error) {
	return rr.rows.Columns()
}

// Close releases the resources held by the underlying sql.Rows object. It should be called after finishing row processing.
func (rr *rowsResult) Close() {
	rr.rows.Close() // nolint: errcheck,gosec
}

// Err returns the error encountered during iteration over rows or nil if no error occurred.
func (rr *rowsResult) Err() error {
	return rr.err
}
