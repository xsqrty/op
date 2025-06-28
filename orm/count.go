package orm

import (
	"context"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
)

type CountBuilder interface {
	By(key string) CountBuilder
	ByDistinct(key string) CountBuilder
	Where(exp driver.Sqler) CountBuilder
	Join(table any, on driver.Sqler) CountBuilder
	LeftJoin(table any, on driver.Sqler) CountBuilder
	RightJoin(table any, on driver.Sqler) CountBuilder
	InnerJoin(table any, on driver.Sqler) CountBuilder
	CrossJoin(table any, on driver.Sqler) CountBuilder
	GroupBy(groups ...any) CountBuilder
	Log(handler LoggerHandler) CountBuilder
	With(ctx context.Context, db Queryable) (uint64, error)
}

type countResultModel struct {
	Count uint64 `op:"total_count,aggregated"`
}

type count struct {
	sb         op.SelectBuilder
	logger     LoggerHandler
	byColumn   op.Column
	byDistinct bool
}

const totalCountColumn = "total_count"

func Count(table string) CountBuilder {
	return &count{
		sb: op.Select().From(table),
	}
}

func (co *count) With(ctx context.Context, db Queryable) (uint64, error) {
	if !co.byColumn.IsZero() && co.byDistinct {
		co.sb.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.CountDistinct(co.byColumn))})
	} else if !co.byColumn.IsZero() {
		co.sb.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.Count(co.byColumn))})
	} else {
		co.sb.SetReturningAliases([]op.Alias{op.As(totalCountColumn, op.Count(driver.Pure("*")))})
	}

	result, err := Query[countResultModel](co.sb).Log(co.logger).GetOne(ctx, db)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
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

func (co *count) Where(exp driver.Sqler) CountBuilder {
	co.sb.Where(exp)
	return co
}

func (co *count) Join(table any, on driver.Sqler) CountBuilder {
	co.sb.Join(table, on)
	return co
}

func (co *count) LeftJoin(table any, on driver.Sqler) CountBuilder {
	co.sb.LeftJoin(table, on)
	return co
}

func (co *count) RightJoin(table any, on driver.Sqler) CountBuilder {
	co.sb.RightJoin(table, on)
	return co
}

func (co *count) InnerJoin(table any, on driver.Sqler) CountBuilder {
	co.sb.InnerJoin(table, on)
	return co
}

func (co *count) CrossJoin(table any, on driver.Sqler) CountBuilder {
	co.sb.CrossJoin(table, on)
	return co
}

func (co *count) GroupBy(groups ...any) CountBuilder {
	co.sb.GroupBy(groups...)
	return co
}

func (co *count) Log(lh LoggerHandler) CountBuilder {
	co.logger = lh
	return co
}
