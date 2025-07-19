package orm

import (
	"context"

	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// QueryBuilder is a generic interface for building and executing database queries of type T.
type QueryBuilder[T any] interface {
	// GetOne retrieves a single result of type T from the database using the provided context and Queryable.
	GetOne(ctx context.Context, db Queryable) (*T, error)
	// GetMany retrieves multiple results of type T from the database as a slice using the provided context and Queryable.
	GetMany(ctx context.Context, db Queryable) ([]*T, error)
	// Log sets a LoggerHandler for logging SQL queries, arguments, and errors for debugging purposes.
	Log(LoggerHandler) QueryBuilder[T]
	// Wrap allows nesting or wrapping the query with a named SQL SelectBuilder, enabling composable query operations.
	Wrap(name string, wrap op.SelectBuilder) QueryBuilder[T]
}

// Queryable is an interface that abstracts querying capabilities for a database connection or layer.
type Queryable interface {
	Query(ctx context.Context, sql string, args ...any) (db.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) db.Row
	SqlOptions() *driver.SqlOptions
}

// query defines a generic type for constructing and executing SQL queries, encapsulating query logic and metadata.
type query[T any] struct {
	with        string
	ret         op.Returnable
	logger      LoggerHandler
	wrapper     *wrapper
	usingTables []string
}

// wrapper provides a container for a named SQL query built using the op.SelectBuilder interface.
type wrapper struct {
	name string
	sb   op.SelectBuilder
}

// Query creates a QueryBuilder for executing database operations using the specified Returnable object.
func Query[T any](ret op.Returnable) QueryBuilder[T] {
	return &query[T]{
		usingTables: ret.UsingTables(),
		with:        ret.With(),
		ret:         ret,
	}
}

// GetOne fetches a single record from the database based on the query and maps it to the specified generic type.
func (q *query[T]) GetOne(ctx context.Context, db Queryable) (*T, error) {
	result := new(T)
	md, keys, err := setQueryReturning(q, result)
	if err != nil {
		return nil, err
	}

	pointers, err := getKeysPointers(result, md.setters, keys)
	if err != nil {
		return nil, err
	}

	q.ret.LimitReturningOne()
	sql, args, err := q.sql(db)
	q.log(sql, args, err)
	if err != nil {
		return nil, err
	}

	err = db.QueryRow(ctx, sql, args...).Scan(pointers...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetMany retrieves multiple records from the database, mapping rows to instances of type T in the provided query context.
func (q *query[T]) GetMany(ctx context.Context, db Queryable) ([]*T, error) {
	result := make([]*T, 0)

	md, keys, err := setQueryReturning(q, new(T))
	if err != nil {
		return nil, err
	}

	sql, args, err := q.sql(db)
	q.log(sql, args, err)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for _, row := range rows.Rows() {
		item := new(T)
		pointers, err := getKeysPointers(item, md.setters, keys)
		if err != nil {
			return nil, err
		}

		err = row.Scan(pointers...)
		if err != nil {
			return nil, err
		}

		result = append(result, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Wrap sets a wrapper with a name and a SelectBuilder instance to modify the current query and returns the QueryBuilder.
func (q *query[T]) Wrap(name string, wrap op.SelectBuilder) QueryBuilder[T] {
	q.wrapper = &wrapper{name: name, sb: wrap}
	return q
}

// Log sets the LoggerHandler to log SQL queries, their arguments, and any errors encountered during query execution.
func (q *query[T]) Log(lh LoggerHandler) QueryBuilder[T] {
	q.logger = lh
	return q
}

// sql generates the SQL string, arguments, and error for the query, optionally wrapping it with a defined wrapper.
func (q *query[T]) sql(db Queryable) (string, []any, error) {
	if q.wrapper != nil {
		return q.wrapper.sb.From(op.As(q.wrapper.name, q.ret)).PreparedSql(db.SqlOptions())
	}

	return q.ret.PreparedSql(db.SqlOptions())
}

// log logs the SQL query, its arguments, and any associated error using the logger if it's defined.
func (q *query[T]) log(sql string, args []any, err error) {
	if q.logger != nil {
		q.logger(sql, args, err)
	}
}
