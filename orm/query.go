package orm

import (
	"context"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

type QueryBuilder[T any] interface {
	GetOne(ctx context.Context, db Queryable) (*T, error)
	GetMany(ctx context.Context, db Queryable) ([]*T, error)
	Log(LoggerHandler) QueryBuilder[T]
	Wrap(name string, wrap op.SelectBuilder) QueryBuilder[T]
}

type Returnable interface {
	UsingTables() []string
	With() string
	GetReturning() []op.Alias
	SetReturning([]any) error
	SetReturningAliases([]op.Alias)
	Sql(options *driver.SqlOptions) (string, []interface{}, error)
	LimitReturningOne()
}

type Queryable interface {
	Query(ctx context.Context, sql string, args ...any) (db.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) db.Row
	Sql(sqler driver.Sqler) (string, []any, error)
}

type query[T any] struct {
	with        string
	ret         Returnable
	logger      LoggerHandler
	wrap        *wrapper
	aliasMapper func(op.Alias)
	usingTables []string
}

type wrapper struct {
	name string
	sb   op.SelectBuilder
}

func Query[T any](ret Returnable) QueryBuilder[T] {
	return &query[T]{
		usingTables: ret.UsingTables(),
		with:        ret.With(),
		ret:         ret,
	}
}

func (q *query[T]) GetOne(ctx context.Context, db Queryable) (*T, error) {
	result := new(T)
	md, keys, err := prepareModelQuery(q, result)
	if err != nil {
		return nil, err
	}

	pointers, err := getPointersByModelSetters(result, md.setters, keys)
	if err != nil {
		return nil, err
	}

	q.ret.LimitReturningOne()
	sql, args, err := q.getQuery(db)
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

func (q *query[T]) GetMany(ctx context.Context, db Queryable) ([]*T, error) {
	result := make([]*T, 0)

	md, keys, err := prepareModelQuery(q, new(T))
	if err != nil {
		return nil, err
	}

	sql, args, err := q.getQuery(db)
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
		md, err = getModelDetails(q.with, item)
		if err != nil {
			return nil, err
		}

		pointers, err := getPointersByModelSetters(item, md.setters, keys)
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

func (q *query[T]) Wrap(name string, wrap op.SelectBuilder) QueryBuilder[T] {
	q.wrap = &wrapper{name: name, sb: wrap}
	return q
}

func (q *query[T]) Log(lh LoggerHandler) QueryBuilder[T] {
	q.logger = lh
	return q
}

func (q *query[T]) getQuery(db Queryable) (string, []any, error) {
	if q.wrap != nil {
		return db.Sql(q.wrap.sb.From(op.As(q.wrap.name, q.ret)))
	}

	return db.Sql(q.ret)
}

func (q *query[T]) log(sql string, args []any, err error) {
	if q.logger != nil {
		q.logger(sql, args, err)
	}
}
