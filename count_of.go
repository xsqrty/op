package op

import (
	"context"
	"github.com/xsqrty/op/driver"
)

type CountOfBuilder interface {
	By(key string) CountOfBuilder
	ByDistinct(key string) CountOfBuilder
	Where(exp driver.Sqler) CountOfBuilder
	With(ctx context.Context, db Queryable) (uint64, error)
}

type countOfResult struct {
	Count uint64 `op:"total_count,aggregated"`
}

type countOf struct {
	sb         SelectBuilder
	byColumn   Column
	byDistinct bool
}

const totalCountColumn = "total_count"

func CountOf(table string) CountOfBuilder {
	return &countOf{
		sb: Select().From(table),
	}
}

func (co *countOf) By(key string) CountOfBuilder {
	co.byDistinct = false
	co.byColumn = Column(key)

	return co
}

func (co *countOf) ByDistinct(key string) CountOfBuilder {
	co.byDistinct = true
	co.byColumn = Column(key)

	return co
}

func (co *countOf) Where(exp driver.Sqler) CountOfBuilder {
	co.sb.Where(exp)
	return co
}

func (co *countOf) With(ctx context.Context, db Queryable) (uint64, error) {
	if !co.byColumn.IsZero() && co.byDistinct {
		co.sb.SetReturningAliases([]Alias{As(totalCountColumn, CountDistinct(co.byColumn))})
	} else if !co.byColumn.IsZero() {
		co.sb.SetReturningAliases([]Alias{As(totalCountColumn, Count(co.byColumn))})
	} else {
		co.sb.SetReturningAliases([]Alias{As(totalCountColumn, Count(Pure("*")))})
	}

	result, err := Query[countOfResult](co.sb).GetOne(ctx, db)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}
