package orm

import (
	"context"

	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// ExecBuilder defines methods for configuring and executing a SQL statement.
type ExecBuilder interface {
	Log(handler LoggerHandler) ExecBuilder
	With(ctx context.Context, db Executable) (db.ExecResult, error)
}

// Executable defines the interface for executing SQL commands and retrieving SQL configuration options.
type Executable interface {
	Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error)
	SqlOptions() *driver.SqlOptions
}

// execBuilder is a concrete implementation of the ExecBuilder interface for building and executing SQL statements.
// It uses a PreparedSqler to generate SQL queries and supports logging via a LoggerHandler.
// The exp field holds the PreparedSqler instance responsible for generating prepared statements.
// The logger field is an optional LoggerHandler used for recording SQL execution details.
type execBuilder struct {
	exp    driver.PreparedSqler
	logger LoggerHandler
}

// Exec initializes an ExecBuilder instance using a PreparedSqler implementation.
func Exec(sqler driver.PreparedSqler) ExecBuilder {
	return &execBuilder{exp: sqler}
}

// With prepares and executes a SQL statement using the provided context and Executable instance, returning the execution result.
func (eb *execBuilder) With(ctx context.Context, db Executable) (db.ExecResult, error) {
	sql, args, err := eb.exp.PreparedSql(db.SqlOptions())
	if eb.logger != nil {
		eb.logger(sql, args, err)
	}
	if err != nil {
		return nil, err
	}

	return db.Exec(ctx, sql, args...)
}

// Log sets the logger handler used for logging SQL, arguments, and errors during execution and returns the ExecBuilder instance.
func (eb *execBuilder) Log(lh LoggerHandler) ExecBuilder {
	eb.logger = lh
	return eb
}
