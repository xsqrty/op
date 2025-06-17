package op

import (
	"context"
	"github.com/xsqrty/op/driver"
)

type QueryBuilder[T any] interface {
	GetOne(ctx context.Context, db Queryable) (*T, error)
	GetMany(ctx context.Context, db Queryable) ([]*T, error)
	Wrap(name string, sb SelectBuilder) QueryBuilder[T]
	MapAliases(aliasMapper func(Alias)) QueryBuilder[T]
}

type Returnable interface {
	UsingTables() []string
	With() string
	GetReturning() []Alias
	SetReturning([]any) error
	SetReturningAliases([]Alias)
	Sql(options *driver.SqlOptions) (string, []interface{}, error)
	LimitReturningOne()
}

type Queryable interface {
	Query(ctx context.Context, sql string, args ...any) (driver.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) driver.Row
	Sql(sqler driver.Sqler) (string, []any, error)
}

type query[T any] struct {
	with        string
	ret         Returnable
	wrap        *wrapper
	aliasMapper func(Alias)
	usingTables []string
}

type wrapper struct {
	name string
	sb   SelectBuilder
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
	sql, args, err := db.Sql(q.getQuery())
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
	var result []*T

	md, keys, err := prepareModelQuery(q, new(T))
	if err != nil {
		return nil, err
	}

	sql, args, err := db.Sql(q.getQuery())
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

func (q *query[T]) Wrap(name string, sb SelectBuilder) QueryBuilder[T] {
	q.wrap = &wrapper{
		name: name,
		sb:   sb,
	}

	return q
}

func (q *query[T]) MapAliases(aliasMapper func(Alias)) QueryBuilder[T] {
	q.aliasMapper = aliasMapper
	return q
}

func (q *query[T]) getQuery() driver.Sqler {
	var query driver.Sqler = q.ret
	if q.wrap != nil {
		query = q.wrap.sb.From(As(q.wrap.name, q.ret))
	}

	if q.aliasMapper != nil {
		aliases := q.ret.GetReturning()
		for i := 0; i < len(aliases); i++ {
			q.aliasMapper(aliases[i])
		}
	}

	return query
}
