package orm

import (
	"context"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

type CountBuilder interface {
	By(key string) CountBuilder
	ByDistinct(key string) CountBuilder
	Log(handler LoggerHandler) CountBuilder
	With(ctx context.Context, db db.QueryExec) (uint64, error)
}

type countResultModel struct {
	Count uint64 `op:"total_count,aggregated"`
}

type count struct {
	ret        op.Returnable
	logger     LoggerHandler
	byColumn   op.Column
	byDistinct bool
}

const totalCountColumn = "total_count"

func Count(ret op.Returnable) CountBuilder {
	return &count{
		ret: ret,
	}
}

func (co *count) With(ctx context.Context, db db.QueryExec) (uint64, error) {
	if co.ret.CounterType() == op.CounterQuery {
		return co.getQueryResult(ctx, db)
	}

	return co.getExecResult(ctx, db)
}

func (co *count) By(key string) CountBuilder {
	co.byDistinct = false
	co.byColumn = op.Column(key)

	return co
}

func (co *count) ByDistinct(key string) CountBuilder {
	co.byDistinct = true
	co.byColumn = op.Column(key)

	return co
}

func (co *count) Log(lh LoggerHandler) CountBuilder {
	co.logger = lh
	return co
}

func (co *count) getExecResult(ctx context.Context, db Executable) (uint64, error) {
	result, err := Exec(co.ret).Log(co.logger).With(ctx, db)
	if err != nil {
		return 0, err
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return uint64(count), nil
}

func (co *count) getQueryResult(ctx context.Context, db Queryable) (uint64, error) {
	if !co.byColumn.IsZero() && co.byDistinct {
		co.ret.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.CountDistinct(co.byColumn))})
	} else if !co.byColumn.IsZero() {
		co.ret.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.Count(co.byColumn))})
	} else {
		co.ret.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.Count(driver.Pure("*")))})
	}

	result, err := Query[countResultModel](co.ret).Log(co.logger).GetOne(ctx, db)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}
