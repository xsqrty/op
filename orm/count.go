package orm

import (
	"context"

	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// CountBuilder defines an interface for building and executing count operations in ORM.
type CountBuilder interface {
	// By sets a column to count by without using DISTINCT
	By(key string) CountBuilder
	// ByDistinct sets a column to count by using DISTINCT
	ByDistinct(key string) CountBuilder
	// Log sets a logger handler for the count operation
	Log(handler LoggerHandler) CountBuilder
	// With executes the count operation using the provided context and database connection
	With(ctx context.Context, db db.QueryExec) (int64, error)
}

// countResultModel represents the structure for holding a count result, typically returned from a query operation.
// The Count field holds the aggregated count value based on the query and configuration.
type countResultModel struct {
	Count int64 `op:"total_count,aggregated"`
}

// count represents a structure supporting count operations, with optional logging and column/distinct configurations.
type count struct {
	ret        op.Returnable
	logger     LoggerHandler
	byColumn   op.Column
	byDistinct bool
}

const totalCountColumn = "total_count"

// Count creates a new CountBuilder instance for the given returnable query
func Count(ret op.Returnable) CountBuilder {
	return &count{
		ret: ret,
	}
}

// With executes the count operation using the provided context and database connection.
// Returns the count result as int64 and any error encountered.
func (co *count) With(ctx context.Context, db db.QueryExec) (int64, error) {
	if co.ret.CounterType() == op.CounterQuery {
		return co.getQueryResult(ctx, db)
	}
	return co.getExecResult(ctx, db)
}

// By configures the count operation to count by a specific column without using DISTINCT
func (co *count) By(key string) CountBuilder {
	co.byDistinct = false
	co.byColumn = op.Column(key)
	return co
}

// ByDistinct configures the count operation to count by a specific column using DISTINCT
func (co *count) ByDistinct(key string) CountBuilder {
	co.byDistinct = true
	co.byColumn = op.Column(key)
	return co
}

// Log sets a logger handler for the count operation
func (co *count) Log(lh LoggerHandler) CountBuilder {
	co.logger = lh
	return co
}

// getExecResult executes the count operation for non-query operations (like INSERT, UPDATE, DELETE)
// and returns the number of affected rows
func (co *count) getExecResult(ctx context.Context, db Executable) (int64, error) {
	result, err := Exec(co.ret).Log(co.logger).With(ctx, db)
	if err != nil {
		return 0, err
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return count, nil
}

// getQueryResult executes the count operation for SELECT queries
// and returns the count result using COUNT or COUNT DISTINCT based on configuration
func (co *count) getQueryResult(ctx context.Context, db Queryable) (int64, error) {
	switch {
	case !co.byColumn.IsZero() && co.byDistinct:
		co.ret.SetReturning([]op.Alias{op.As(totalCountColumn, op.CountDistinct(co.byColumn))})
	case !co.byColumn.IsZero():
		co.ret.SetReturning([]op.Alias{op.As(totalCountColumn, op.Count(co.byColumn))})
	default:
		co.ret.SetReturning([]op.Alias{op.As(totalCountColumn, op.Count(driver.Pure("*")))})
	}

	result, err := Query[countResultModel](co.ret).Log(co.logger).GetOne(ctx, db)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}
