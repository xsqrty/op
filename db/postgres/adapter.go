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

// ErrPgxUnsupported is an error indicating that the operation is not supported by the pgx library.
var ErrPgxUnsupported = errors.New("unsupported")

// pgxQueryExec defines an interface for executing SQL queries and commands within a PostgreSQL context.
// It provides methods for executing non-query commands, querying multiple rows, and querying a single row.
type pgxQueryExec interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// pgxAdapter represents an adapter for database interactions using pgx and a connection pool.
type pgxAdapter struct {
	pool    *pgxpool.Pool
	options *driver.SqlOptions
}

// pgxRows wraps a database query result set and its associated error.
// It implements the db.Rows interface for compatibility with database handling logic.
// pgxRows is used to manage and iterate over query results retrieved via pgx.
type pgxRows struct {
	rows pgx.Rows
	err  error
}

// pgxExecResult wraps pgconn.CommandTag to implement the db.ExecResult interface, providing query execution results.
type pgxExecResult struct {
	commonTags *pgconn.CommandTag
}

// pgxTxType represents a custom type for identifying or categorizing PostgreSQL transaction contexts as a string value.
type pgxTxType string

// pgxTxOptions defines transaction options with an isolation level set to ReadCommitted for PostgreSQL operations.
var pgxTxOptions = pgx.TxOptions{
	IsoLevel: pgx.ReadCommitted,
}

// pgxTxKey is a context key used to store and retrieve a transaction from a context object for pgx operations.
var pgxTxKey = pgxTxType("tx")

// ensures that *pgxAdapter implements the db.ConnPool interface at compile-time.
var _ db.ConnPool = (*pgxAdapter)(nil)

// Exec executes a SQL query with the provided arguments and returns the execution result or an error.
func (pa *pgxAdapter) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	tags, err := pa.get(ctx).Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxExecResult{commonTags: &tags}, nil
}

// Query executes a database query with the given SQL string and arguments, returning the resulting rows or an error.
func (pa *pgxAdapter) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	rows, err := pa.get(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgxRows{
		rows: rows,
	}, nil
}

// QueryRow executes a database query that expects a single row result and returns it as a db.Row for processing.
func (pa *pgxAdapter) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	return pa.get(ctx).QueryRow(ctx, sql, args...)
}

// Transact executes a function within a database transaction.
// Commits the transaction if the function succeeds, rolls back on error.
// Ctx provides context; handler is the function to execute.
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

// Close gracefully closes the adapter's connection pool and releases all associated resources.
func (pa *pgxAdapter) Close() error {
	pa.pool.Close()
	return nil
}

// Sql generates an SQL query string and arguments using the provided Sqler and adapter-specific options.
func (pa *pgxAdapter) Sql(b driver.Sqler) (string, []any, error) {
	return driver.Sql(b, pa.options)
}

// SqlOptions returns the SQL options configuration associated with the pgxAdapter instance.
func (pa *pgxAdapter) SqlOptions() *driver.SqlOptions {
	return pa.options
}

// get retrieves the current execution context, returning a transaction if present or falling back to the connection pool.
func (pa *pgxAdapter) get(ctx context.Context) pgxQueryExec {
	tx := ctx.Value(pgxTxKey)
	if tx != nil {
		return tx.(pgxQueryExec)
	}

	return pa.pool
}

// Rows return a sequence of key-value pairs where the key is the row index, and the value is a db.Scanner for row contents.
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

// Columns retrieves the column names of the current result set and returns them as a slice of strings.
func (pr *pgxRows) Columns() ([]string, error) {
	fields := pr.rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, f := range fields {
		columns[i] = f.Name
	}

	return columns, nil
}

// Close terminates the underlying query result set and releases associated resources.
func (pr *pgxRows) Close() {
	pr.rows.Close()
}

// Err returns the error encountered, if any, during iteration over the rows or result processing.
func (pr *pgxRows) Err() error {
	return pr.err
}

// RowsAffected returns the number of rows affected by the execution of the query.
func (er *pgxExecResult) RowsAffected() (int64, error) {
	return er.commonTags.RowsAffected(), nil
}

// LastInsertId returns an error indicating that retrieving the last insert ID is not supported by the pgx driver.
func (er *pgxExecResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("pgx last insert id is not supported: %w", ErrPgxUnsupported)
}
